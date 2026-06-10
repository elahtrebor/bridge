# bridge

`bridge` is a small bridge for an existing `screen` session.

It does two things:

1. Attaches to an existing `screen` session.
2. Exposes a local UNIX socket that returns snapshots and accepts injected input.

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

The bridge prints the socket path it is listening on, typically under
`/private/tmp/bridge-work.sock`.

Clients can connect to that socket and send newline-delimited JSON:

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
python3 bridge.py snapshot work > /tmp/work.txt
python3 bridge.py send work "echo hello" --enter
python3 bridge.py stop work
```

## Notes

- The `attach` command expects `screen` to be installed on the machine where the session lives.
- `bridge` enables logging on the session it attaches to, so it can stream output and take snapshots.
- `stop` asks the running bridge to exit cleanly; it does not kill the `screen` session itself.
