#!/usr/bin/env python3
"""ptybridge: a PTY-backed bridge for an AI client.

This prototype owns a child process attached to a PTY and exposes a small
JSON-over-socket protocol for reading output and injecting input.

The bridge is intentionally local and POSIX-oriented. It is a better fit than
screen when you want the bridge itself to own the terminal session.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import pty
import select
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple


DEFAULT_STATE_DIR = Path(os.environ.get("PTYBRIDGE_STATE_DIR", str(Path.home() / ".ptybridge")))
DEFAULT_SOCKET_DIR = Path(os.environ.get("PTYBRIDGE_SOCKET_DIR", str(Path.home() / "ptybridge")))
DEFAULT_HOST = os.environ.get("PTYBRIDGE_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.environ.get("PTYBRIDGE_PORT", "0"))
DEFAULT_SHELL = os.environ.get("SHELL", "/bin/sh")
POLL_INTERVAL_S = 0.05
MAX_SNAPSHOT_BYTES = 1024 * 1024


def eprint(*args: object) -> None:
    print(*args, file=sys.stderr)


def sanitize_name(name: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name.strip())
    return safe or "session"


def runtime_paths(name: str) -> Dict[str, Path]:
    safe = sanitize_name(name)
    base = DEFAULT_STATE_DIR / safe
    socket_dir = DEFAULT_SOCKET_DIR
    socket_dir.mkdir(parents=True, exist_ok=True)
    return {
        "base": base,
        "socket": socket_dir / f"ptybridge-{safe}.sock",
        "transport": base / "transport.json",
        "pid": base / "bridge.pid",
        "snapshot": base / "snapshot.bin",
        "log": base / "pty.log",
    }


def load_transport_metadata(name: str) -> Optional[Dict[str, object]]:
    paths = runtime_paths(name)
    if not paths["transport"].exists():
        return None
    try:
        return json.loads(paths["transport"].read_text(encoding="utf-8"))
    except OSError:
        return None


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def encode_event(event_type: str, payload: bytes) -> bytes:
    msg = {
        "type": event_type,
        "data_b64": base64.b64encode(payload).decode("ascii"),
        "length": len(payload),
    }
    return (json.dumps(msg, ensure_ascii=True) + "\n").encode("utf-8")


def decode_event_payload(message: Dict[str, object]) -> str:
    data_b64 = message.get("data_b64")
    if not isinstance(data_b64, str):
        return ""
    return base64.b64decode(data_b64).decode("utf-8", "replace")


def safe_send(conn: socket.socket, payload: bytes) -> bool:
    try:
        conn.sendall(payload)
        return True
    except OSError:
        return False


class BroadcastHub:
    def __init__(self) -> None:
        self._clients: List[socket.socket] = []
        self._lock = threading.Lock()

    def add(self, conn: socket.socket) -> None:
        with self._lock:
            self._clients.append(conn)

    def remove(self, conn: socket.socket) -> None:
        with self._lock:
            if conn in self._clients:
                self._clients.remove(conn)

    def broadcast(self, payload: bytes) -> None:
        dead: List[socket.socket] = []
        with self._lock:
            for conn in self._clients:
                try:
                    conn.sendall(payload)
                except OSError:
                    dead.append(conn)
            for conn in dead:
                if conn in self._clients:
                    self._clients.remove(conn)


class SnapshotBuffer:
    def __init__(self, limit: int = MAX_SNAPSHOT_BYTES) -> None:
        self._limit = limit
        self._chunks: Deque[bytes] = deque()
        self._size = 0
        self._lock = threading.Lock()

    def append(self, data: bytes) -> None:
        if not data:
            return
        with self._lock:
            self._chunks.append(data)
            self._size += len(data)
            while self._size > self._limit and self._chunks:
                chunk = self._chunks.popleft()
                self._size -= len(chunk)

    def read(self) -> bytes:
        with self._lock:
            return b"".join(self._chunks)


class PTYBridge:
    def __init__(self, name: str, command: List[str], cwd: Optional[Path]) -> None:
        self.name = name
        self.command = command
        self.cwd = cwd
        self.paths = runtime_paths(name)
        self.stop = threading.Event()
        self.hub = BroadcastHub()
        self.snapshot = SnapshotBuffer()
        self.master_fd = -1
        self.child_pid = -1
        self.server: Optional[socket.socket] = None
        self.transport_metadata: Dict[str, object] = {}
        self._pty_write_lock = threading.Lock()
        self._child_exit: Optional[int] = None

    def start_child(self) -> None:
        pid, master_fd = pty.fork()
        if pid == 0:
            if self.cwd is not None:
                os.chdir(self.cwd)
            argv = self.command if self.command else [DEFAULT_SHELL]
            os.execvp(argv[0], argv)
            raise SystemExit(1)
        self.child_pid = pid
        self.master_fd = master_fd
        os.set_blocking(self.master_fd, False)

    def stop_child(self) -> None:
        if self.child_pid > 0:
            try:
                os.kill(self.child_pid, signal.SIGTERM)
            except OSError:
                pass

    def write_input(self, text: str, newline: bool = True) -> None:
        if self.master_fd < 0 or not text:
            return
        payload = text.replace("\n", "\r")
        if newline and not payload.endswith("\r"):
            payload += "\r"
        data = payload.encode("utf-8")
        with self._pty_write_lock:
            os.write(self.master_fd, data)

    def broadcast_output(self, data: bytes) -> None:
        self.snapshot.append(data)
        self.hub.broadcast(encode_event("output", data))

    def read_loop(self) -> None:
        while not self.stop.is_set():
            if self.master_fd < 0:
                break
            rlist, _, _ = select.select([self.master_fd], [], [], POLL_INTERVAL_S)
            if self.master_fd not in rlist:
                self._check_child()
                continue
            try:
                data = os.read(self.master_fd, 4096)
            except BlockingIOError:
                self._check_child()
                continue
            except OSError as exc:
                self.hub.broadcast(encode_event("error", f"pty read failed: {exc}".encode("utf-8")))
                break
            if not data:
                self._check_child()
                break
            self.broadcast_output(data)
            self._check_child()
        self.stop.set()

    def _check_child(self) -> None:
        if self.child_pid <= 0 or self._child_exit is not None:
            return
        try:
            pid, status = os.waitpid(self.child_pid, os.WNOHANG)
        except ChildProcessError:
            self._child_exit = 0
            self.stop.set()
            return
        if pid == self.child_pid:
            self._child_exit = status
            self.stop.set()

    def client_handler(self, conn: socket.socket) -> None:
        try:
            if not safe_send(
                conn,
                (
                    json.dumps(
                        {
                            "type": "hello",
                            "name": self.name,
                            "pid": os.getpid(),
                            "child_pid": self.child_pid,
                        },
                        ensure_ascii=True,
                    )
                    + "\n"
                ).encode("utf-8"),
            ):
                return

            if not safe_send(conn, encode_event("snapshot", self.snapshot.read())):
                return

            self.hub.add(conn)
            buffer = b""
            while not self.stop.is_set():
                data = conn.recv(4096)
                if not data:
                    break
                buffer += data
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        request = json.loads(line.decode("utf-8"))
                    except json.JSONDecodeError as exc:
                        safe_send(conn, (json.dumps({"type": "error", "message": f"invalid json: {exc}"}) + "\n").encode("utf-8"))
                        continue

                    op = request.get("op")
                    if op == "send":
                        text = str(request.get("text", ""))
                        newline = bool(request.get("newline", True))
                        self.write_input(text, newline=newline)
                        safe_send(conn, (json.dumps({"type": "ack", "op": op}) + "\n").encode("utf-8"))
                    elif op == "snapshot":
                        if not safe_send(conn, encode_event("snapshot", self.snapshot.read())):
                            return
                    elif op == "status":
                        payload = {
                            "type": "status",
                            "name": self.name,
                            "child_pid": self.child_pid,
                            "alive": self._child_exit is None,
                        }
                        safe_send(conn, (json.dumps(payload) + "\n").encode("utf-8"))
                    elif op == "ping":
                        safe_send(conn, (json.dumps({"type": "pong"}) + "\n").encode("utf-8"))
                    elif op == "shutdown":
                        safe_send(conn, (json.dumps({"type": "ack", "op": op}) + "\n").encode("utf-8"))
                        self.stop.set()
                    else:
                        safe_send(conn, (json.dumps({"type": "error", "message": f"unknown op: {op!r}"}) + "\n").encode("utf-8"))
        finally:
            self.hub.remove(conn)
            try:
                conn.close()
            except OSError:
                pass

    def serve(self, transport: str, host: str, port: int, socket_path: Optional[Path]) -> int:
        ensure_dir(self.paths["base"])
        self.paths["pid"].write_text(f"{os.getpid()}\n", encoding="utf-8")
        self.start_child()

        if transport == "unix":
            if socket_path is None:
                raise SystemExit("unix transport requires a socket path")
            if socket_path.exists():
                socket_path.unlink()
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(str(socket_path))
            os.chmod(socket_path, 0o600)
            self.transport_metadata = {
                "name": self.name,
                "transport": "unix",
                "socket": str(socket_path),
            }
        else:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((host, port))
            bound_host, bound_port = server.getsockname()
            self.transport_metadata = {
                "name": self.name,
                "transport": "tcp",
                "host": bound_host,
                "port": bound_port,
            }

        server.listen(5)
        server.settimeout(0.5)
        self.server = server
        write_json(self.paths["transport"], self.transport_metadata)
        eprint(f"name:     {self.name}")
        if self.transport_metadata["transport"] == "unix":
            eprint(f"socket:   {self.transport_metadata['socket']}")
        else:
            eprint(f"tcp:      {self.transport_metadata['host']}:{self.transport_metadata['port']}")
        eprint(f"child:    {self.child_pid}")
        eprint("ctrl-c to stop the bridge")

        reader = threading.Thread(target=self.read_loop, daemon=True)
        reader.start()
        try:
            while not self.stop.is_set():
                try:
                    conn, _ = server.accept()
                except socket.timeout:
                    self._check_child()
                    continue
                thread = threading.Thread(target=self.client_handler, args=(conn,), daemon=True)
                thread.start()
        finally:
            self.stop.set()
            self.stop_child()
            try:
                server.close()
            except OSError:
                pass
            try:
                if self.paths["pid"].exists():
                    self.paths["pid"].unlink()
                if self.paths["transport"].exists():
                    self.paths["transport"].unlink()
                if self.paths["socket"].exists():
                    self.paths["socket"].unlink()
            except OSError:
                pass
        return 0


def connect_socket(path: Path) -> socket.socket:
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.connect(str(path))
    return conn


def connect_tcp(host: str, port: int) -> socket.socket:
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect((host, port))
    return conn


def recv_json_message(conn: socket.socket, buffer: bytes, timeout: float) -> Tuple[Dict[str, object], bytes]:
    deadline = time.time() + timeout
    while True:
        if b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            if not line.strip():
                continue
            return json.loads(line.decode("utf-8")), buffer
        remaining = deadline - time.time()
        if remaining <= 0:
            raise TimeoutError("timed out waiting for bridge response")
        conn.settimeout(remaining)
        chunk = conn.recv(65536)
        if not chunk:
            raise ConnectionError("bridge connection closed")
        buffer += chunk


def request_bridge(name: str, transport: str, host: str, port: int, socket_path: Optional[Path], request: Optional[Dict[str, object]] = None, timeout: float = 2.0) -> List[Dict[str, object]]:
    if transport == "unix":
        if socket_path is None:
            raise SystemExit("unix transport requires a socket path")
        conn = connect_socket(socket_path)
    else:
        conn = connect_tcp(host, port)

    messages: List[Dict[str, object]] = []
    buffer = b""
    try:
        message, buffer = recv_json_message(conn, buffer, timeout)
        messages.append(message)
        message, buffer = recv_json_message(conn, buffer, timeout)
        messages.append(message)
        if request is None:
            return messages

        conn.sendall((json.dumps(request) + "\n").encode("utf-8"))
        while True:
            try:
                message, buffer = recv_json_message(conn, buffer, timeout)
            except TimeoutError:
                break
            messages.append(message)
            op = request.get("op")
            if op == "send" and message.get("type") == "ack":
                break
            if op in {"snapshot", "ping", "status", "shutdown"} and message.get("type") in {"snapshot", "pong", "status", "ack"}:
                break
    finally:
        conn.close()
    return messages


def connect_bridge(args: argparse.Namespace) -> Tuple[socket.socket, bytes]:
    metadata = load_transport_metadata(args.name)
    socket_path = Path(args.socket).expanduser() if args.socket else None
    host = args.host
    port = args.port
    transport = args.transport

    if metadata is not None:
        transport = str(metadata.get("transport", transport))
        if transport == "unix" and socket_path is None and isinstance(metadata.get("socket"), str):
            socket_path = Path(str(metadata["socket"])).expanduser()
        elif transport == "tcp":
            if host == DEFAULT_HOST and isinstance(metadata.get("host"), str):
                host = str(metadata["host"])
            if port == DEFAULT_PORT and isinstance(metadata.get("port"), int):
                port = int(metadata["port"])

    if transport == "unix":
        if socket_path is None:
            raise SystemExit("unix transport requires a socket path")
        conn = connect_socket(socket_path)
    else:
        conn = connect_tcp(host, port)

    buffer = b""
    hello, buffer = recv_json_message(conn, buffer, args.timeout)
    snapshot, buffer = recv_json_message(conn, buffer, args.timeout)
    print(json.dumps(hello))
    if snapshot.get("type") == "snapshot":
        payload = decode_event_payload(snapshot)
        if payload:
            sys.stdout.write(payload)
            if not payload.endswith("\n"):
                sys.stdout.write("\n")
    return conn, buffer


def cmd_client(args: argparse.Namespace) -> int:
    conn, buffer = connect_bridge(args)
    stop = threading.Event()

    def reader() -> None:
        nonlocal buffer
        try:
            while not stop.is_set():
                conn.settimeout(0.25)
                try:
                    chunk = conn.recv(65536)
                except socket.timeout:
                    continue
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    message = json.loads(line.decode("utf-8"))

                    msg_type = message.get("type")
                    if msg_type in {"snapshot", "output"}:
                        payload = decode_event_payload(message)
                        sys.stdout.write(payload)
                        if not payload.endswith("\n"):
                            sys.stdout.write("\n")
                        sys.stdout.flush()
                    else:
                        print(json.dumps(message))
        except OSError as exc:
            if not stop.is_set():
                eprint(f"client recv failed: {exc}")
        finally:
            stop.set()

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    try:
        for line in sys.stdin:
            if stop.is_set():
                break
            payload = line.rstrip("\n")
            if not payload:
                continue
            try:
                conn.sendall((json.dumps({"op": "send", "text": payload, "newline": True}) + "\n").encode("utf-8"))
            except OSError as exc:
                eprint(f"client send failed: {exc}")
                break
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        try:
            conn.close()
        except OSError:
            pass
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    command = list(args.command or [])
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        command = [args.shell]
    bridge = PTYBridge(args.name, command, Path(args.cwd).expanduser() if args.cwd else None)
    return bridge.serve(args.transport, args.host, args.port, Path(args.socket) if args.socket else None)


def cmd_status(args: argparse.Namespace) -> int:
    paths = runtime_paths(args.name)
    payload: Dict[str, object] = {
        "name": args.name,
        "alive": paths["pid"].exists(),
    }
    if paths["transport"].exists():
        try:
            payload.update(json.loads(paths["transport"].read_text(encoding="utf-8")))
        except OSError:
            pass
    print(json.dumps(payload, indent=2))
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    paths = runtime_paths(args.name)
    if paths["pid"].exists():
        try:
            pid = int(paths["pid"].read_text(encoding="utf-8").strip())
            os.kill(pid, signal.SIGTERM)
            print(json.dumps({"type": "ack", "op": "shutdown", "pid": pid}))
            return 0
        except (OSError, ValueError):
            pass
    raise SystemExit(f"bridge '{args.name}' is not running")


def cmd_probe(args: argparse.Namespace) -> int:
    metadata = load_transport_metadata(args.name)
    socket_path = Path(args.socket).expanduser() if args.socket else None
    host = args.host
    port = args.port
    transport = args.transport

    if metadata is not None:
        transport = str(metadata.get("transport", transport))
        if transport == "unix" and socket_path is None and isinstance(metadata.get("socket"), str):
            socket_path = Path(str(metadata["socket"])).expanduser()
        elif transport == "tcp":
            if host == DEFAULT_HOST and isinstance(metadata.get("host"), str):
                host = str(metadata["host"])
            if port == DEFAULT_PORT and isinstance(metadata.get("port"), int):
                port = int(metadata["port"])

    messages: List[Dict[str, object]] = []
    if args.send is not None:
        messages.extend(
            request_bridge(
                args.name,
                transport,
                host,
                port,
                socket_path,
                request={"op": "send", "text": args.send, "newline": not args.no_enter},
                timeout=args.timeout,
            )
        )
        if args.snapshot:
            snapshot_messages = request_bridge(
                args.name,
                transport,
                host,
                port,
                socket_path,
                request={"op": "snapshot"},
                timeout=args.timeout,
            )
            if len(snapshot_messages) >= 2:
                messages.append(snapshot_messages[-1])
    else:
        request = None
        if args.snapshot:
            request = {"op": "snapshot"}
        elif args.ping:
            request = {"op": "ping"}
        elif args.status:
            request = {"op": "status"}
        messages = request_bridge(args.name, transport, host, port, socket_path, request=request, timeout=args.timeout)
    for message in messages:
        if message.get("type") in {"snapshot", "output"}:
            payload = decode_event_payload(message)
            sys.stdout.write(payload)
            if not payload.endswith("\n"):
                sys.stdout.write("\n")
        else:
            print(json.dumps(message))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ptybridge", description="PTy-backed bridge for terminal sessions")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("run", help="start a PTY-backed session and expose it over a socket")
    p.add_argument("name", help="bridge name")
    p.add_argument("command", nargs=argparse.REMAINDER, help="command to run; defaults to the user's shell")
    p.add_argument("--shell", default=DEFAULT_SHELL, help="shell to run when no command is provided")
    p.add_argument("--cwd", help="working directory for the child command")
    p.add_argument("--transport", choices=("tcp", "unix"), default="tcp", help="listener transport")
    p.add_argument("--host", default=DEFAULT_HOST, help="TCP listen host")
    p.add_argument("--port", type=int, default=DEFAULT_PORT, help="TCP listen port; 0 picks an ephemeral port")
    p.add_argument("--socket", help="UNIX socket path when --transport unix is used")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("status", help="show bridge metadata")
    p.add_argument("name", help="bridge name")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("stop", help="stop a running bridge")
    p.add_argument("name", help="bridge name")
    p.set_defaults(func=cmd_stop)

    p = sub.add_parser("probe", help="connect to a running bridge and print responses")
    p.add_argument("name", help="bridge name")
    p.add_argument("--transport", choices=("tcp", "unix"), default="tcp")
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--socket")
    p.add_argument("--send")
    p.add_argument("--no-enter", action="store_true")
    p.add_argument("--snapshot", action="store_true")
    p.add_argument("--ping", action="store_true")
    p.add_argument("--status", action="store_true")
    p.add_argument("--timeout", type=float, default=2.0)
    p.set_defaults(func=cmd_probe)

    p = sub.add_parser("client", help="persistent client mode for interactive control")
    p.add_argument("name", help="bridge name")
    p.add_argument("--transport", choices=("tcp", "unix"), default="tcp")
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--socket")
    p.add_argument("--timeout", type=float, default=2.0)
    p.set_defaults(func=cmd_client)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
