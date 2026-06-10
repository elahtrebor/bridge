#!/usr/bin/env python3
"""bridge: attach to an existing screen session and expose it to a local AI client.

The intended use is:

  1. Start `screen` yourself.
  2. Run your SSH session or shell work inside that screen session.
  3. Start this bridge against the existing session.
  4. Let an AI client connect to the local UNIX socket and send input.
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


STATE_DIR = Path.home() / ".bridge"
POLL_INTERVAL_S = 0.2


def eprint(*args: object) -> None:
    print(*args, file=sys.stderr)


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
    base = STATE_DIR / safe
    return {
        "base": base,
        "log": base / "screenlog.0",
        "socket": Path("/private/tmp") / f"bridge-{safe}.sock",
        "snapshot": base / "snapshot.txt",
    }


def ensure_state_dir(paths: Dict[str, Path]) -> None:
    paths["base"].mkdir(parents=True, exist_ok=True)


def configure_screen_logging(session: str, log_path: Path) -> None:
    run_screen(["-S", session, "-X", "logfile", str(log_path)])
    run_screen(["-S", session, "-X", "log", "on"])


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
    conn.sendall(
        (json.dumps({"type": "error", "message": message}, ensure_ascii=True) + "\n").encode(
            "utf-8"
        )
    )


def connect_socket(path: Path) -> socket.socket:
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.connect(str(path))
    return conn


def handle_client(
    conn: socket.socket,
    session: str,
    snapshot_path: Path,
    hub: BroadcastHub,
    stop: threading.Event,
) -> None:
    try:
        conn.sendall(
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
        )

        snapshot = hardcopy_snapshot(session, snapshot_path)
        conn.sendall(encode_event("snapshot", snapshot))
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
                    conn.sendall((json.dumps({"type": "ack", "op": op}) + "\n").encode("utf-8"))
                elif op == "snapshot":
                    snapshot = hardcopy_snapshot(session, snapshot_path)
                    conn.sendall(encode_event("snapshot", snapshot))
                elif op == "status":
                    conn.sendall(
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
                    )
                elif op == "ping":
                    conn.sendall((json.dumps({"type": "pong"}) + "\n").encode("utf-8"))
                elif op == "shutdown":
                    conn.sendall((json.dumps({"type": "ack", "op": op}) + "\n").encode("utf-8"))
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
    if paths["socket"].exists():
        paths["socket"].unlink()

    if not session_exists(args.session):
        raise SystemExit(f"screen session '{args.session}' does not exist")

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

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(paths["socket"]))
    os.chmod(paths["socket"], 0o600)
    server.listen(5)
    server.settimeout(0.5)

    eprint(f"session: {args.session}")
    eprint(f"socket:   {paths['socket']}")
    eprint(f"log:      {paths['log']}")
    eprint("ctrl-c to stop the bridge; the screen session is left running")

    try:
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
    finally:
        stop.set()
        try:
            server.close()
        except OSError:
            pass
        try:
            if paths["socket"].exists():
                paths["socket"].unlink()
        except OSError:
            pass
    return 0


def stop_bridge(args: argparse.Namespace) -> int:
    paths = make_state_paths(args.session)
    if not paths["socket"].exists():
        raise SystemExit(f"bridge socket not found for session '{args.session}'")

    conn = connect_socket(paths["socket"])
    try:
        conn.sendall((json.dumps({"op": "shutdown"}) + "\n").encode("utf-8"))
        conn.settimeout(2.0)
        buf = b""
        while b"\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buf += chunk
        if buf:
            print(buf.decode("utf-8", "replace").strip())
    finally:
        conn.close()
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
    print(
        json.dumps(
            {
                "session": args.session,
                "screen_exists": session_exists(args.session),
                "log": str(paths["log"]),
                "socket": str(paths["socket"]),
            },
            indent=2,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bridge", description="attach to an existing screen session")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("attach", help="attach the bridge to an existing screen session")
    p.add_argument("session", help="screen session name")
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

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
