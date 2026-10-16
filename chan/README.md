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
| `chan-live` | the **live-session port** (product-agnostic `Backend` trait; engine-replaceable) |
| `chan-opencode` | an **adapter**: implements `chan-live` over the opencode v2 HTTP API (async; degrades) |
| `chan-mcp` | MCP server so agents attach with native tools |
| `chan-cli` | the `chan` binary |

`chan-core` depends on none of the others.

### Live control (send more data / interrupt a running child)

The live layer is a **port and adapter**: `chan-live` defines a stable `Backend` trait
(`inject` / `interrupt` / `active`) over opaque sessions; `chan-opencode` implements it
against opencode v2. Call sites hold a `dyn Backend`, so the engine can change (opencode
v3, or another engine) without touching them. `LiveError::Unavailable` is the degrade
signal — the message stays durable in `.chan/` and is delivered on the child's next read.

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
chan attach  --participant w1 --session ses_…   # bridge w1's inbox into a live session
chan interrupt --session ses_…                  # stop a running session
```

`chan attach` is the **live bridge**: durable-first, it reads a participant's inbox and
injects each message into a running opencode session (`--once` drains and exits; default
follows). `chan interrupt` aborts a session's active execution. Both degrade: if the server
is down, `attach` keeps the durable log intact; `interrupt` fails loudly.

**Targeting the server (zero-config).** The opencode server always guards `/api` with HTTP
Basic (`opencode:<password>`). You usually don't have to supply anything: when `--opencode`
is omitted, `chan` **auto-discovers** a running background service from
`~/.local/state/opencode/service.json` (`{url, password}`, wildcard binds rewritten to
loopback). To target a specific server, pass `--opencode <root-url>` (the API is appended
as `/api`) with `--opencode-password` / `OPENCODE_PASSWORD`. For a private server, pin a
known password with `OPENCODE_SERVER_PASSWORD=<value>` when starting `opencode serve`.

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
`chan_subscribe`, `chan_ack`, `chan_inbox`. Live control of running sessions:
`chan_inject` (send more data into a live session), `chan_interrupt` (abort it), and
`chan_active` (list live sessions). Identity defaults to `--as` / `CHAN_PARTICIPANT`; the
live tools use `--opencode` / `CHAN_OPENCODE`.

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

## Per-sender audit (query "all messages by X")

```bash
chan history --from orch                 # everything orch authored, any destination
chan history --from orch --kind result   # filter by kind
```

Over the network: `GET /v1/messages?from=orch` → `{messages}` (auth). In MCP:
`chan_history {from, kind?, to?, limit?}`. Messages are scanned across every inbox and
room log and returned in time order. Add to the HTTP API table above:

| Method | Path | Notes |
|---|---|---|
| GET | `/v1/messages` | `?from=<id>`; auth; `{messages}` |

## Attaching to Claude Code

Claude Code reads `.mcp.json` at the project root; this repo ships one that launches
`chan-mcp --as orchestrator`, plus `.claude-plugin/{plugin.json,marketplace.json}` so it
can be installed as a Claude plugin. The crate `chan-claude` is the thin adapter
(degrades via `NoopBackend`; exposes `mcp_config(participant)` and `project_mcp_path`).

## MCP tools (full list)

Send: `chan_send`, `chan_publish`, `chan_recv`, `chan_await`, `chan_room`,
`chan_subscribe`, `chan_ack`, `chan_inbox`, `chan_history`.
Live control (running sessions): `chan_inject`, `chan_interrupt`, `chan_active`,
`chan_status`, `chan_wait`, `chan_env`.

## Build & test

```bash
cargo test --workspace
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```
