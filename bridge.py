#!/usr/bin/env python3
"""bridge: attach to an existing screen session and expose it to a local AI client.

The intended use is:

  1. Start `screen` yourself.
  2. Run your SSH session or shell work inside that screen session.
  3. Start this bridge against the existing session.
  4. Let an AI client connect over a UNIX socket or localhost TCP and send input.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional


POLL_INTERVAL_S = 0.2
DEFAULT_TCP_OUT_PORT = 8765


def eprint(*args: object) -> None:
    print(*args, file=sys.stderr)


def runtime_home() -> Path:
    return Path(os.environ.get("BRIDGE_HOME", str(Path.home()))).expanduser()


def sanitize_session_name(name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name.strip())
    return safe or "session"


def session_exists(session: str) -> bool:
    try:
        result = subprocess.run(
            ["screen", "-ls"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError:
        raise SystemExit("screen is not installed or not on PATH")

    pattern = re.compile(rf"^\s*\d+\.{re.escape(session)}\s+\(")
    for line in result.stdout.splitlines():
        if pattern.search(line):
            return True
    return False


def run_screen(args: List[str], cwd: Optional[Path] = None) -> None:
    subprocess.run(["screen", *args], check=True, cwd=str(cwd) if cwd is not None else None)


def make_state_paths(session: str) -> Dict[str, Path]:
    safe = sanitize_session_name(session)
    state_root = Path(os.environ.get("BRIDGE_STATE_DIR", runtime_home() / ".bridge")).expanduser()
    base = state_root / safe
    default_socket_dir = runtime_home() / "bridge" / "sockets"
    socket_dir = Path(os.environ.get("BRIDGE_SOCKET_DIR", default_socket_dir))
    socket_dir.mkdir(parents=True, exist_ok=True)
    return {
        "base": base,
        "log": base / "screenlog.0",
        "socket": socket_dir / f"bridge-{safe}.sock",
        "snapshot": base / "snapshot.txt",
        "pid": base / "bridge.pid",
        "transport": base / "transport.json",
    }


def detect_wsl_host() -> str:
    resolv_conf = Path("/etc/resolv.conf")
    try:
        for line in resolv_conf.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2 and parts[0] == "nameserver" and parts[1]:
                return parts[1]
    except OSError:
        pass
    return os.environ.get("BRIDGE_WSL_HOST", "host.docker.internal")


def dedupe_hosts(hosts: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for host in hosts:
        host = host.strip()
        if not host or host in seen:
            continue
        seen.add(host)
        ordered.append(host)
    return ordered


def ensure_state_dir(paths: Dict[str, Path]) -> None:
    paths["base"].mkdir(parents=True, exist_ok=True)


def configure_screen_logging(session: str, log_path: Path) -> None:
    run_screen(["-S", session, "-X", "logfile", str(log_path)])
    run_screen(["-S", session, "-X", "log", "on"])


def write_transport_metadata(path: Path, metadata: Dict[str, object]) -> None:
    path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")


def remove_runtime_artifacts(paths: Dict[str, Path]) -> None:
    for key in ("socket", "transport", "pid"):
        try:
            if paths[key].exists():
                paths[key].unlink()
        except OSError:
            pass


def resolve_runtime_metadata(
    session: str, paths: Dict[str, Path], expected_transport: Optional[str] = None
) -> Dict[str, object]:
    if paths["transport"].exists():
        try:
            metadata = json.loads(paths["transport"].read_text(encoding="utf-8"))
        except OSError:
            metadata = None
        else:
            transport = str(metadata.get("transport", ""))
            if expected_transport is not None and transport != expected_transport:
                raise SystemExit(
                    f"bridge session '{session}' is using transport '{transport}', not '{expected_transport}'"
                )
            return metadata

    if paths["socket"].exists():
        metadata = {"transport": "unix", "socket": str(paths["socket"])}
        if expected_transport is not None and expected_transport != "unix":
            raise SystemExit(
                f"bridge session '{session}' is using transport 'unix', not '{expected_transport}'"
            )
        return metadata

    raise SystemExit(f"bridge runtime metadata not found for session '{session}'")


def hardcopy_snapshot(session: str, snapshot_path: Path) -> bytes:
    if snapshot_path.exists():
        snapshot_path.unlink()
    run_screen(["-S", session, "-X", "hardcopy", "-h", str(snapshot_path)])
    return snapshot_path.read_bytes() if snapshot_path.exists() else b""


def stuff_text(session: str, text: str) -> None:
    if not text:
        return
    run_screen(["-S", session, "-X", "stuff", text])


def send_line(session: str, text: str, newline: bool = True) -> None:
    payload = text.replace("\n", "\r")
    if newline and not payload.endswith("\r"):
        payload += "\r"
    stuff_text(session, payload)


def encode_event(event_type: str, payload: bytes) -> bytes:
    msg = {
        "type": event_type,
        "data_b64": base64.b64encode(payload).decode("ascii"),
        "length": len(payload),
    }
    return (json.dumps(msg, ensure_ascii=True) + "\n").encode("utf-8")


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


def tail_log(session: str, log_path: Path, hub: BroadcastHub, stop: threading.Event) -> None:
    offset = 0
    while not stop.is_set():
        try:
            if not log_path.exists():
                time.sleep(POLL_INTERVAL_S)
                continue
            size = log_path.stat().st_size
            if size < offset:
                offset = 0
            if size > offset:
                with log_path.open("rb") as fh:
                    fh.seek(offset)
                    chunk = fh.read(size - offset)
                    offset = fh.tell()
                if chunk:
                    hub.broadcast(encode_event("output", chunk))
        except OSError as exc:
            hub.broadcast(
                encode_event("error", f"log read failed for {session}: {exc}".encode("utf-8"))
            )
        time.sleep(POLL_INTERVAL_S)


def json_error(conn: socket.socket, message: str) -> None:
    safe_send(
        conn,
        (json.dumps({"type": "error", "message": message}, ensure_ascii=True) + "\n").encode(
            "utf-8"
        ),
    )


def connect_socket(path: Path) -> socket.socket:
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.connect(str(path))
    return conn


def connect_tcp(host: str, port: int) -> socket.socket:
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect((host, port))
    return conn


def connect_tcp_outbound(host: str, port: int) -> socket.socket:
    return connect_tcp(host, port)


def resolve_connect_targets(args: argparse.Namespace) -> tuple[List[str], int]:
    host = str(getattr(args, "connect_host", "")).strip()
    if host:
        hosts = [host]
    elif args.transport == "wsl":
        hosts = [detect_wsl_host()]
        env_host = os.environ.get("BRIDGE_WSL_HOST", "").strip()
        if env_host:
            hosts.append(env_host)
        hosts.append("host.docker.internal")
    else:
        hosts = ["127.0.0.1"]
    port = int(getattr(args, "connect_port", 0))
    if port <= 0:
        raise SystemExit("connect port must be greater than zero for tcp-out and wsl transports")
    return dedupe_hosts(hosts), port


def connect_transport(session: str, paths: Dict[str, Path]) -> socket.socket:
    metadata = resolve_runtime_metadata(session, paths)
    transport = metadata["transport"]
    if transport == "unix":
        return connect_socket(Path(str(metadata["socket"])))
    if transport in {"tcp", "tcp-out", "wsl"}:
        return connect_tcp(str(metadata["host"]), int(metadata["port"]))
    raise SystemExit(f"unsupported bridge transport for session '{session}': {transport!r}")


def recv_json_message(conn: socket.socket, buffer: bytes, timeout: float) -> tuple[dict, bytes]:
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


def decode_event_payload(message: Dict[str, object]) -> str:
    data_b64 = message.get("data_b64")
    if not isinstance(data_b64, str):
        return ""
    return base64.b64decode(data_b64).decode("utf-8", "replace")


def request_bridge(
    session: str,
    request: Optional[Dict[str, object]] = None,
    timeout: float = 2.0,
    expect_output: bool = False,
) -> List[Dict[str, object]]:
    paths = make_state_paths(session)
    conn = connect_transport(session, paths)
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
        send_ack_seen = False
        while True:
            try:
                message, buffer = recv_json_message(conn, buffer, timeout)
            except TimeoutError:
                if request["op"] == "send" and expect_output and send_ack_seen:
                    conn.sendall((json.dumps({"op": "snapshot"}) + "\n").encode("utf-8"))
                    message, buffer = recv_json_message(conn, buffer, timeout)
                    messages.append(message)
                break
            messages.append(message)
            if request["op"] == "send":
                if message.get("type") == "ack" and message.get("op") == "send":
                    send_ack_seen = True
                    if not expect_output:
                        break
                elif expect_output and message.get("type") == "output":
                    break
            elif request["op"] == "ping" and message.get("type") == "pong":
                break
            elif request["op"] == "snapshot" and message.get("type") == "snapshot":
                break
            elif request["op"] == "status" and message.get("type") == "status":
                break
            elif request["op"] == "shutdown" and message.get("type") == "ack":
                break
    finally:
        conn.close()
    return messages


def handle_client(
    conn: socket.socket,
    session: str,
    snapshot_path: Path,
    hub: BroadcastHub,
    stop: threading.Event,
) -> None:
    try:
        if not safe_send(
            conn,
            (
                json.dumps(
                    {
                        "type": "hello",
                        "session": session,
                        "pid": os.getpid(),
                    },
                    ensure_ascii=True,
                )
                + "\n"
            ).encode("utf-8")
        ):
            return

        snapshot = hardcopy_snapshot(session, snapshot_path)
        if not safe_send(conn, encode_event("snapshot", snapshot)):
            return
        hub.add(conn)

        buffer = b""
        while True:
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
                    json_error(conn, f"invalid json: {exc}")
                    continue

                op = request.get("op")
                if op == "send":
                    text = str(request.get("text", ""))
                    newline = bool(request.get("newline", True))
                    send_line(session, text, newline=newline)
                    if not safe_send(
                        conn, (json.dumps({"type": "ack", "op": op}) + "\n").encode("utf-8")
                    ):
                        return
                elif op == "snapshot":
                    snapshot = hardcopy_snapshot(session, snapshot_path)
                    if not safe_send(conn, encode_event("snapshot", snapshot)):
                        return
                elif op == "status":
                    if not safe_send(
                        conn,
                        (
                            json.dumps(
                                {
                                    "type": "status",
                                    "session": session,
                                    "screen_exists": session_exists(session),
                                },
                                ensure_ascii=True,
                            )
                            + "\n"
                        ).encode("utf-8")
                    ):
                        return
                elif op == "ping":
                    if not safe_send(conn, (json.dumps({"type": "pong"}) + "\n").encode("utf-8")):
                        return
                elif op == "shutdown":
                    safe_send(conn, (json.dumps({"type": "ack", "op": op}) + "\n").encode("utf-8"))
                    stop.set()
                else:
                    json_error(conn, f"unknown op: {op!r}")
    finally:
        hub.remove(conn)
        try:
            conn.close()
        except OSError:
            pass


def attach(args: argparse.Namespace) -> int:
    paths = make_state_paths(args.session)
    ensure_state_dir(paths)
    remove_runtime_artifacts(paths)

    if not session_exists(args.session):
        raise SystemExit(f"screen session '{args.session}' does not exist")

    paths["pid"].write_text(f"{os.getpid()}\n", encoding="utf-8")

    configure_screen_logging(args.session, paths["log"])

    hub = BroadcastHub()
    stop = threading.Event()

    def shutdown(*_signals: object) -> None:
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, shutdown)

    tail_thread = threading.Thread(
        target=tail_log,
        args=(args.session, paths["log"], hub, stop),
        daemon=True,
    )
    tail_thread.start()

    transport_metadata: Dict[str, object]
    server: Optional[socket.socket] = None
    outbound_targets: Optional[List[str]] = None
    outbound_port: Optional[int] = None

    if args.transport == "unix":
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(paths["socket"]))
        os.chmod(paths["socket"], 0o600)
        transport_metadata = {
            "session": args.session,
            "transport": "unix",
            "socket": str(paths["socket"]),
        }
    elif args.transport == "tcp":
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.host, args.port))
        bound_host, bound_port = server.getsockname()
        transport_metadata = {
            "session": args.session,
            "transport": "tcp",
            "host": bound_host,
            "port": bound_port,
        }
    else:
        hosts, port = resolve_connect_targets(args)
        outbound_targets = hosts
        outbound_port = port
        transport_metadata = {
            "session": args.session,
            "transport": args.transport,
            "host": hosts[0],
            "port": port,
            "host_candidates": hosts,
        }

    write_transport_metadata(paths["transport"], transport_metadata)

    eprint(f"session: {args.session}")
    if transport_metadata["transport"] == "unix":
        eprint(f"socket:   {transport_metadata['socket']}")
    elif transport_metadata["transport"] == "tcp":
        eprint(f"tcp:      {transport_metadata['host']}:{transport_metadata['port']}")
    else:
        eprint(
            f"{transport_metadata['transport']}:  {transport_metadata['host']}:{transport_metadata['port']}"
        )
        candidates = transport_metadata.get("host_candidates")
        if isinstance(candidates, list) and len(candidates) > 1:
            eprint(f"candidates: {', '.join(str(candidate) for candidate in candidates)}")
    eprint(f"log:      {paths['log']}")
    eprint("ctrl-c to stop the bridge; the screen session is left running")

    try:
        if server is not None:
            server.listen(5)
            server.settimeout(0.5)
            while not stop.is_set():
                try:
                    conn, _ = server.accept()
                except socket.timeout:
                    continue
                thread = threading.Thread(
                    target=handle_client,
                    args=(conn, args.session, paths["snapshot"], hub, stop),
                    daemon=True,
                )
                thread.start()
        else:
            while not stop.is_set():
                assert outbound_targets is not None
                assert outbound_port is not None
                conn = None
                last_exc: Optional[OSError] = None
                for host in outbound_targets:
                    try:
                        conn = connect_tcp_outbound(host, outbound_port)
                        break
                    except OSError as exc:
                        last_exc = exc
                        continue
                if conn is None:
                    if last_exc is not None:
                        eprint(
                            f"waiting for outbound peer at {','.join(outbound_targets)}:{outbound_port}: {last_exc}"
                        )
                    if stop.wait(1.0):
                        break
                    continue

                try:
                    handle_client(conn, args.session, paths["snapshot"], hub, stop)
                finally:
                    try:
                        conn.close()
                    except OSError:
                        pass

                if not stop.is_set():
                    eprint(
                        f"outbound peer disconnected; reconnecting to {','.join(outbound_targets)}:{outbound_port}"
                    )
                    if stop.wait(1.0):
                        break
    finally:
        stop.set()
        if server is not None:
            try:
                server.close()
            except OSError:
                pass
        remove_runtime_artifacts(paths)
    return 0


def stop_bridge(args: argparse.Namespace) -> int:
    paths = make_state_paths(args.session)
    if paths["pid"].exists():
        try:
            pid = int(paths["pid"].read_text(encoding="utf-8").strip())
            os.kill(pid, signal.SIGTERM)
            print(json.dumps({"type": "ack", "op": "shutdown", "method": "signal", "pid": pid}))
            return 0
        except (OSError, ValueError):
            pass

    messages = request_bridge(args.session, {"op": "shutdown"}, timeout=2.0)
    for message in messages:
        if message.get("type") == "ack" and message.get("op") == "shutdown":
            print(json.dumps(message))
            break
    return 0


def cmd_send(args: argparse.Namespace) -> int:
    send_line(args.session, args.text, newline=args.enter)
    return 0


def cmd_snapshot(args: argparse.Namespace) -> int:
    paths = make_state_paths(args.session)
    snapshot = hardcopy_snapshot(args.session, paths["snapshot"])
    sys.stdout.buffer.write(snapshot)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    paths = make_state_paths(args.session)
    runtime = None
    try:
        runtime = resolve_runtime_metadata(args.session, paths)
    except SystemExit:
        runtime = None

    payload: Dict[str, object] = {
        "session": args.session,
        "screen_exists": session_exists(args.session),
        "log": str(paths["log"]),
    }
    if paths["pid"].exists():
        try:
            payload["pid"] = int(paths["pid"].read_text(encoding="utf-8").strip())
        except ValueError:
            pass
    if runtime is not None:
        payload["transport"] = runtime["transport"]
        if runtime["transport"] == "unix":
            payload["socket"] = runtime["socket"]
        elif runtime["transport"] in {"tcp", "tcp-out", "wsl"}:
            payload["host"] = runtime["host"]
            payload["port"] = runtime["port"]
            if "host_candidates" in runtime:
                payload["host_candidates"] = runtime["host_candidates"]
    else:
        payload["transport"] = "unix"
        payload["socket"] = str(paths["socket"])

    print(
        json.dumps(payload, indent=2)
    )
    return 0


def cmd_probe(args: argparse.Namespace) -> int:
    request = None
    expect_output = False
    if args.send is not None:
        request = {"op": "send", "text": args.send, "newline": not args.no_enter}
        expect_output = args.wait_output
    elif args.snapshot:
        request = {"op": "snapshot"}
    elif args.ping:
        request = {"op": "ping"}
    elif args.bridge_status:
        request = {"op": "status"}

    messages = request_bridge(args.session, request, timeout=args.timeout, expect_output=expect_output)
    for message in messages:
        msg_type = message.get("type")
        if msg_type in {"snapshot", "output"}:
            payload = decode_event_payload(message)
            sys.stdout.write(payload)
            if not payload.endswith("\n"):
                sys.stdout.write("\n")
        else:
            print(json.dumps(message))
    return 0


def listener_recv_loop(conn: socket.socket, stop: threading.Event) -> None:
    buffer = b""
    try:
        while not stop.is_set():
            data = conn.recv(65536)
            if not data:
                break
            buffer += data
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError:
                    print(line.decode("utf-8", "replace"))
                    continue
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
            eprint(f"listener recv failed: {exc}")
    finally:
        stop.set()


def listener_stdin_loop(conn: socket.socket, stop: threading.Event) -> None:
    try:
        for line in sys.stdin:
            if stop.is_set():
                break
            payload = line.rstrip("\n")
            if not payload:
                continue
            try:
                conn.sendall((payload + "\n").encode("utf-8"))
            except OSError as exc:
                if not stop.is_set():
                    eprint(f"listener send failed: {exc}")
                break
    finally:
        stop.set()


def cmd_listen(args: argparse.Namespace) -> int:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((args.host, args.port))
    server.listen(1)
    bound_host, bound_port = server.getsockname()
    eprint(f"listening on {bound_host}:{bound_port}")
    conn: Optional[socket.socket] = None
    stop = threading.Event()
    try:
        conn, addr = server.accept()
        eprint(f"connection from {addr[0]}:{addr[1]}")
        recv_thread = threading.Thread(target=listener_recv_loop, args=(conn, stop), daemon=True)
        recv_thread.start()
        if not args.no_stdin:
            stdin_thread = threading.Thread(target=listener_stdin_loop, args=(conn, stop), daemon=True)
            stdin_thread.start()
        while not stop.is_set():
            time.sleep(0.1)
    except KeyboardInterrupt:
        stop.set()
    finally:
        stop.set()
        if conn is not None:
            try:
                conn.close()
            except OSError:
                pass
        try:
            server.close()
        except OSError:
            pass
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bridge", description="attach to an existing screen session")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("attach", help="attach the bridge to an existing screen session")
    p.add_argument("session", help="screen session name")
    p.add_argument(
        "--transport",
        choices=("unix", "tcp", "tcp-out", "wsl"),
        default=os.environ.get("BRIDGE_TRANSPORT", "unix"),
        help="client transport to expose (default: %(default)s)",
    )
    p.add_argument(
        "--host",
        default=os.environ.get("BRIDGE_HOST", "127.0.0.1"),
        help="TCP listen host when --transport tcp is used",
    )
    p.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("BRIDGE_PORT", "0")),
        help="TCP listen port when --transport tcp is used; 0 picks an ephemeral port",
    )
    p.add_argument(
        "--connect-host",
        default=os.environ.get("BRIDGE_CONNECT_HOST", ""),
        help="outbound host when --transport tcp-out or wsl is used",
    )
    p.add_argument(
        "--connect-port",
        type=int,
        default=int(os.environ.get("BRIDGE_CONNECT_PORT", str(DEFAULT_TCP_OUT_PORT))),
        help="outbound port when --transport tcp-out or wsl is used",
    )
    p.set_defaults(func=attach)

    p = sub.add_parser("send", help="send text to the screen session")
    p.add_argument("session", help="screen session name")
    p.add_argument("text", help="text to inject")
    p.add_argument("--enter", action="store_true", help="append an Enter key")
    p.set_defaults(func=cmd_send)

    p = sub.add_parser("stop", help="ask a running bridge to shut down")
    p.add_argument("session", help="screen session name")
    p.set_defaults(func=stop_bridge)

    p = sub.add_parser("snapshot", help="dump the current screen to stdout")
    p.add_argument("session", help="screen session name")
    p.set_defaults(func=cmd_snapshot)

    p = sub.add_parser("status", help="print session metadata")
    p.add_argument("session", help="screen session name")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("probe", help="connect to a running bridge and print live responses")
    p.add_argument("session", help="screen session name")
    p.add_argument("--send", help="send a command through the bridge")
    p.add_argument("--no-enter", action="store_true", help="do not append Enter when using --send")
    p.add_argument("--wait-output", action="store_true", help="wait for at least one output event after --send")
    p.add_argument("--snapshot", action="store_true", help="request a fresh snapshot after connect")
    p.add_argument("--ping", action="store_true", help="send a ping request")
    p.add_argument("--bridge-status", action="store_true", help="request bridge status over the live transport")
    p.add_argument("--timeout", type=float, default=2.0, help="seconds to wait for bridge responses")
    p.set_defaults(func=cmd_probe)

    p = sub.add_parser("listen", help="accept a reverse bridge connection and print events")
    p.add_argument(
        "--host",
        default=os.environ.get("BRIDGE_LISTEN_HOST", "0.0.0.0"),
        help="TCP listen host for reverse-connect testing",
    )
    p.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("BRIDGE_LISTEN_PORT", str(DEFAULT_TCP_OUT_PORT))),
        help="TCP listen port for reverse-connect testing",
    )
    p.add_argument("--no-stdin", action="store_true", help="do not forward stdin lines to the bridge")
    p.set_defaults(func=cmd_listen)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
