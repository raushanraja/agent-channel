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
