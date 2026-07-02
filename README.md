# bridge

`bridge` is a small bridge for an existing `screen` session on Linux or WSL.

It does two things:

1. Attaches to an existing `screen` session.
2. Exposes either a local UNIX socket or a localhost TCP port that returns snapshots and accepts injected input.

## Why this shape

The bridge lives next to the terminal you already own and lets an AI client
share the same input path that you use manually.

## Usage

Start `screen` yourself:

```bash
screen -S work
```

Run whatever you want inside that session, for example:

```bash
ssh user@host
```

Then start the bridge on the existing session:

```bash
python3 bridge.py attach work
```

That uses a UNIX socket by default. On WSL, or anywhere you prefer a simple
TCP endpoint, start it in TCP mode instead:

```bash
python3 bridge.py attach work --transport tcp
```

That binds to `127.0.0.1` on an ephemeral port by default. You can override the
listener with `--host` and `--port`, or the matching `BRIDGE_TRANSPORT`,
`BRIDGE_HOST`, and `BRIDGE_PORT` environment variables.

For WSL users, TCP mode is the simplest path because both the bridge and the AI
client can run inside WSL without involving Windows sockets.

WSL quick start:

```bash
# terminal 1
python3 bridge.py attach work --transport tcp

# terminal 2
python3 bridge.py probe work --send "ls" --wait-output
```

Clients can connect to the advertised endpoint and send newline-delimited JSON:

```json
{"op":"snapshot"}
{"op":"send","text":"ls -la","newline":true}
{"op":"ping"}
```

Supported events:

- `hello`
- `snapshot`
- `output`
- `status`
- `ack`
- `pong`

## Convenience commands

```bash
python3 bridge.py status work
python3 bridge.py probe work
python3 bridge.py probe work --send "pwd" --wait-output
python3 bridge.py snapshot work > /tmp/work.txt
python3 bridge.py send work "echo hello" --enter
python3 bridge.py stop work
```

## Smoke Testing

Basic live probe against a running bridge:

```bash
python3 bridge.py probe work
```

Send a command through the bridge and wait for the first output event:

```bash
python3 bridge.py probe work --send "pwd" --wait-output
```

PTY-backed prototype:

```bash
python3 ptybridge.py run demo
python3 ptybridge.py probe demo --send "ls" --snapshot
```

Persistent PTY client:

```bash
python3 ptybridge.py client demo
```

You can also run an explicit command under the PTY bridge:

```bash
python3 ptybridge.py run demo -- bash
```

Portability check for TCP mode:

```bash
python3 bridge.py attach work --transport tcp
python3 bridge.py status work
python3 bridge.py probe work --send "echo tcp ok" --wait-output
```

## Notes

- The `attach` command expects `screen` to be installed on the machine where the session lives.
- `bridge` enables logging on the session it attaches to, so it can stream output and take snapshots.
- `bridge.py status <session>` prints the active transport details, including the TCP host/port when applicable.
- `bridge.py probe <session>` is a simple client-side smoke test that works with either transport.
- `ptybridge.py` is the PTY-first prototype and is the better base if you want the bridge itself to own the terminal session.
- `stop` asks the running bridge to exit cleanly; it does not kill the `screen` session itself.
- On Linux, install `screen` first if it is missing, for example `sudo apt install screen`.
