# chan — channel message-passing system (design)

**Date:** 2026-10-16
**Status:** validated design, ready for implementation planning

## Problem

Orchestrating `opencode run` children today uses an ad-hoc convention: a brief goes
down, and the child's files/report/log come back. There is no first-class, durable,
typed way to exchange messages — no live steering, no reliable async transport, no
multi-agent bus.

## Insight

opencode v2 already ships the channel primitives over its local HTTP API:
`session.prompt` (inject), `session.interrupt`, the `inbox` (steer/cancel/update),
`event.subscribe` (SSE down-stream), `session.form` + `form.reply` (structured
request/response), `session.message.list` (durable history), and
`session.active`/`create` (multi-agent).

So `chan` is **not new transport** — it is a coherent channel **protocol** wrapping
those primitives, with a durable file substrate underneath so nothing is lost when a
child dies or rate-limits.

## Core principle

The **file log is the durable source of truth**; the **opencode attach is a live
accelerant** layered on top that **degrades gracefully**. Durable-first, live-second
(store-and-forward).

## Architecture — Cargo workspace (`chan/`)

Hard rule: `chan-core` depends on none of the others.

| Crate | Role | Runtime | Key deps |
|---|---|---|---|
| `chan-core` | protocol + `FileBackend` (durable substrate) | sync | `serde`, `serde_json`, `thiserror`, `uuid`, `fs2`(flock) |
| `chan-opencode` | live attach to `opencode serve` | async | `tokio`, `reqwest`, `serde`, `thiserror` |
| `chan-mcp` | MCP server so agents attach directly | async | `rmcp`, `tokio`, `serde` |
| `chan-cli` | the `chan` binary | sync entry | `clap`, `anyhow`, `tracing` |

## Message & address model (in `chan-core`, serde-serializable)

**Identity (newtypes):** `ParticipantId` (`orchestrator`, `worker-1`, `oc:<sessionID>`),
`RoomId` (`build`, `review`, `all`), `MessageId` (**UUIDv7**, time-sortable).

**Address** — direct *and* pub/sub:
```rust
enum Address {
    Direct(ParticipantId),   // point-to-point
    Room(RoomId),            // publish → fan-out to subscribers
    Broadcast,               // → implicit `all` room
}
```

**Message envelope** (immutable once written):
```rust
struct Message {
    id: MessageId,
    from: ParticipantId,
    to: Address,
    ts: DateTime<Utc>,
    kind: MessageKind,                 // Task|Progress|Question|Answer|Result|Ack|Control|Event
    correlation_id: Option<MessageId>, // threads a reply to its request
    reply_to: Option<MessageId>,
    payload: serde_json::Value,        // typed per-kind via constructors
    ttl: Option<Duration>,
}
```

Delivery state (`acked_at`, cursor position) is tracked **separately** so the log stays
immutable/append-only. A `Router` resolves `Address` against the `Channel`.

## Backends & delivery

**`Channel` trait** (core seam):
```rust
trait Channel {
    fn send(&self, msg: &Message) -> Result<MessageId>;
    fn recv(&self, who: &ParticipantId, cursor: Cursor, limit: usize) -> Result<Vec<Message>>;
    fn subscribe(&self, who: &ParticipantId, room: &RoomId) -> Result<Cursor>;
    fn ack(&self, who: &ParticipantId, id: &MessageId) -> Result<()>;
    fn list(&self) -> Result<ChannelStats>;
}
```

**`FileBackend`** (sync, always works):
```
.chan/
  participants/<id>.jsonl     # direct inbox, append-only
  rooms/<id>.jsonl            # room log, append-only
  state/cursors.json          # per-subscriber read position
  state/acks.json             # explicit receipts
```

**`OpencodeBackend`** (async, live): participant ⇄ session id; down-stream via
`GET /api/event` (SSE) + `session.message.list` catch-up; up-stream via
`session.prompt`/`session.synthetic`, inbox `steer`, `session.interrupt`.

**Delivery = durable-first, live-second.** Every `send` writes the file log first, then
attempts live delivery → survives a dead/rate-limited child. At-least-once, deduped by
`MessageId`. Request/response via `correlation_id`. **Resume** = a message carrying the
original `Task`'s `correlation_id`; the child drains unacked inbox on startup. TTL drops
stale control messages.

**Sync/async bridge:** core stays sync; `chan-opencode` exposes an async `Pump` that tails
the file log *and* pushes/pulls live. With the server down, the Pump is a pure file tail.

## CLI surface (`chan`, clap derive)

Data → stdout (JSON default, `--format text` optional); errors/progress → stderr.
Exit codes: `0` ok, `3` timeout/no-message, `2` usage, `1` error.

```
chan init    [--dir .chan]
chan send    --to <addr> --kind <k> --payload <json|@file> [--reply-to <id>] [--corr <id>] [--ttl <dur>]
chan publish --room <id> --kind <k> --payload <json>
chan recv    --as <participant> [--cursor <c>] [--limit N]
chan await   --as <participant> [--kind <k>] [--timeout <dur>]
chan subscribe --as <participant> --room <id>
chan ack     --as <participant> --id <id>
chan inbox   --as <participant> | chan rooms | chan list
chan attach  --participant <id> --session <ses_…>
chan pump    --as <participant> [--follow]
```

## MCP surface (`chan-mcp`)

Identity from `--as <participant>` or `CHAN_PARTICIPANT`. Transport: **stdio first**,
SSE later.

- **Tools:** `chan_send`, `chan_recv`, `chan_await`, `chan_publish`, `chan_subscribe`,
  `chan_ack`, `chan_inbox`, `chan_attach`, `chan_interrupt`.
- **Resources:** `chan://rooms`, `chan://rooms/{id}/log`, `chan://participants/{id}/inbox`.
- **Prompt:** `coordinate` (ask → wait → ack).

Payoff: point an opencode session's MCP config at `chan-mcp --as worker-1` → the child
gets `chan_send`/`chan_await` as native tools, no CLI shelling.

## Errors

`thiserror` in libs, `anyhow` in the bin; mapped centrally to exit/MCP codes.
- `chan-core::ChanError`: `Io`, `Serde`, `Lock`, `NotFound`, `Cursor`, `TtlExpired`, `Protocol`.
- `chan-opencode::AttachError`: `Http(#[from] reqwest::Error)`, `ServerUnavailable`
  *(triggers degrade)*, `SessionNotFound`, `Sse`.

## Durability, concurrency, security

- append-only JSONL + `fsync`; **partial trailing line tolerated** (truncated on read).
- atomic cursor/ack updates (write-temp + rename); per-file `flock` for writers,
  lock-free readers via cursors; idempotent `send` by `MessageId`.
- `.chan/` is the source of truth; opencode reconciliation is best-effort.
- local-only by default; `0600` perms; no network listener in core; MCP over stdio
  unless SSE is explicitly enabled.

## Testing (TDD)

- `chan-core` unit tests in temp dirs (no server): envelope serde round-trip, cursor
  fan-out (exactly-once per subscriber), ack, TTL expiry, partial-line tolerance,
  concurrent writers.
- a `Channel` **conformance suite** run against `FileBackend` (and later a mock backend).
- `chan-opencode` against a fake HTTP/SSE server (`wiremock`).
- `chan-mcp`: tool-schema snapshots + a stdio end-to-end.
- **E2E gate:** orchestrator ⇄ two `chan` children exchanging `Question`/`Answer` via
  `chan await`.
- Gate: `cargo test --workspace`, `clippy --all-targets -D warnings`, `fmt --check`.

## Decisions locked

1. `MessageId` = **UUIDv7** (`uuid` crate).
2. Single **tokio** runtime for `chan-opencode`/`chan-mcp`; core sync.
3. MCP transport: **stdio first**, SSE later.
4. `.chan/` = **repo-local**, gitignored; overridable via `--dir`/`CHAN_DIR`.
5. **No daemon** (YAGNI) — `Pump` covers streaming, MCP covers attach.

## Out of scope (for now)

Daemon/service mode, remote/multi-host channels, auth on the file backend, SSE MCP
transport, persistence beyond the local filesystem.
