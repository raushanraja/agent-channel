# chan

Durable, typed message channels for orchestrating agents — direct messages **and**
pub/sub rooms, over a file-log source of truth, with a live `opencode` attach and a
direct MCP surface.

- **Durable-first.** Every `send` appends to the file log before any live delivery, so
  a rate-limited or dead child never loses a message (store-and-forward).
- **Two address forms.** `direct:<participant>` (point-to-point), `room:<id>` (fan-out to
  subscribers), and `broadcast` (the implicit `all` room).

## Workspace

| Crate | Role |
|---|---|
| `chan-core` | protocol + `FileBackend` (sync, dependency-light, source of truth) |
| `chan-opencode` | live attach to `opencode serve` (async; degrades to `chan-core`) |
| `chan-mcp` | MCP server so agents attach with native tools |
| `chan-cli` | the `chan` binary |

`chan-core` depends on none of the others.

## CLI

```bash
chan init
chan send    --from orch --to direct:w1 --kind task --payload '{"phase":1}'
chan publish --room build --from w1 --kind progress --payload '"halfway"'
chan recv    --as w1                 # read inbox, advance cursor
chan await   --as w1 --kind task --timeout 30s   # block; exit 3 on timeout
chan room    --as orch --room build  # read a room
chan subscribe --as orch --room build
chan ack     --as w1 --id <message-id>
chan inbox   --as w1                 # cursor + unread
chan rooms | chan list
```

`--dir` / `CHAN_DIR` select the channel directory (default `.chan`). Output is JSON.
Exit codes: `0` ok, `3` timeout/no-message, `2` usage, `1` error.

## MCP

Point a session's MCP config at the binary and it gets channel tools natively:

```json
{
  "mcp": {
    "chan": {
      "command": "chan-mcp",
      "args": ["--dir", ".chan", "--as", "worker-1"]
    }
  }
}
```

Tools: `chan_send`, `chan_publish`, `chan_recv`, `chan_await`, `chan_room`,
`chan_subscribe`, `chan_ack`, `chan_inbox`. Identity defaults to `--as` /
`CHAN_PARTICIPANT`.

## Server (multi-host)

Run the daemon once and let participants on any host talk to it over HTTP + SSE:

```bash
chan token                     # mint a bearer token (or use --token / CHAN_TOKEN)
chan serve --bind 0.0.0.0:27400 --dir .chan --token <token>
```

Clients point at it with `--server` (and `--token`); without `--server` the CLI uses the
local file log as before:

```bash
chan --server http://host:27400 --token <token> send --from orch --to direct:w1 --kind task --payload '{"x":1}'
chan --server http://host:27400 --token <token> await --as w1 --kind task --timeout 30s
```

`RemoteBackend` (crate `chan-client`) implements the same `Channel` trait over the
network, so the CLI and tools are backend-agnostic. The daemon owns the file log —
it stays the single source of truth.

### HTTP API (v1)

| Method | Path | Notes |
|---|---|---|
| GET | `/v1/health` | open; `{status, participants, rooms}` |
| POST | `/v1/send` | `{message}` → `{id}` |
| GET | `/v1/inbox/{participant}` | `?cursor=&limit=` → `{messages, cursor}` |
| GET | `/v1/room/{room}` | `?participant=&cursor=&limit=` |
| POST | `/v1/subscribe` | `{participant, room}` |
| POST | `/v1/ack` | `{participant, id}` |
| GET | `/v1/rooms` | `{rooms:[...]}` |
| GET | `/v1/inbox/{participant}/stream` | SSE of `message` events |

Auth: `Authorization: Bearer <token>` (or `?token=`). Health is always open. Tokens are a
single shared secret for now; TLS/ACLs/rotation are future work.

## Layout on disk

```
.chan/
  participants/<id>.jsonl   # direct inbox (append-only)
  rooms/<id>.jsonl          # room log (append-only)
  state/cursors.json        # per-subscriber read positions
  state/acks.json           # receipts
```

Logs are append-only with a per-file `flock` and `fsync`; a partial trailing line
(a crash mid-write) is ignored. Cursors and acks are written atomically (temp + rename).

## Build & test

```bash
cargo test --workspace
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```
