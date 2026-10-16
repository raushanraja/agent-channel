# chan — provenance, liveness, and echo cancellation

**Date:** 2026-10-16
**Status:** spec (ready for implementation planning)
**Depends on:** `2026-10-16-chan-live-layer-design.md` (the port/adapter split),
`2026-10-16-chan-phase8-plan.md` (the network block)

## Problem

Three gaps bite when agents actually collaborate over `chan`:

1. **No liveness.** The orchestrator cannot tell whether a child session finished
   (successfully, with an error, or by interruption) vs. is still running vs.
   never started. `chan_active` returns a flat list of running session IDs;
   absence means "not running right now" — it does not distinguish "succeeded
   5 seconds ago" from "crashed" from "never existed". `chan await` hangs to its
   full timeout when the worker died mid-flight, wasting minutes.

2. **No provenance.** When multiple sessions share one `opencode serve`, the
   chan MCP server is injected into the *server* config with a single hardcoded
   `--as chan` because opencode does not load a session's own `opencode.json`
   for API-created sessions. Every message lands with `from: "chan"`. Workers
   must manually embed `"name": "w1"` in payloads. LLMs forget this routinely,
   so the orchestrator cannot reliably tell *which* agent sent what.

3. **No echo cancellation.** `FileBackend::recv_room` ignores the `who`
   parameter entirely — a worker reading a room receives its own publications.
   In an attach loop (`chan attach --follow`) or a live bridge, an agent's own
   output is fed back as a new prompt, creating infinite echo loops.

All three have been confirmed against live infrastructure (opencode v2.0.29,
probed `GET /openapi.json`).

## Goals

- An orchestrator can detect whether each child is running, idle-with-outcome,
  or gone, and can block-until-idle without polling.
- Every message carries the session it originated from. Each session's MCP /
  CLI automatically uses a distinct identity without manual payload annotation.
- Room reads and attach loops never echo a participant's own messages.

## Non-goals

- Watchdog / auto-restart (out of scope — the orchestrator decides).
- ACL / per-participant auth (future work, single-token for now).
- Distributed consensus or causal ordering across hosts.

---

## 1. Session liveness

### 1.1 New types in `chan-live::port`

```rust
/// What the engine says about a session right now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SessionStatus {
    /// The session's agent loop is actively executing.
    Running,
    /// The agent loop is idle; `outcome` is the engine's verdict.
    Idle {
        /// `"succeeded"`, `"failed"`, `"interrupted"`, or absent if the
        /// session is idle but has never completed a turn.
        outcome: Option<String>,
    },
    /// The engine has no record of this session.
    NotFound,
}
```

### 1.2 New methods on `Backend`

```rust
pub trait Backend: Send + Sync {
    // — existing —
    fn inject<'a>(…) -> BoxFuture<'a, Result<(), LiveError>>;
    fn interrupt<'a>(…) -> BoxFuture<'a, Result<bool, LiveError>>;
    fn active(…) -> BoxFuture<'_, Result<Vec<SessionRef>, LiveError>>;

    // — new —

    /// Snapshot the status of a single session.
    fn status<'a>(
        &'a self,
        session: &'a SessionRef,
    ) -> BoxFuture<'a, Result<SessionStatus, LiveError>>;

    /// Block until the session's agent loop becomes idle, or until `timeout`
    /// elapses. Returns the status at the moment it unblocked.
    fn wait_idle<'a>(
        &'a self,
        session: &'a SessionRef,
        timeout: std::time::Duration,
    ) -> BoxFuture<'a, Result<SessionStatus, LiveError>>;
}
```

`NoopBackend` returns `Err(LiveError::Unavailable)` for both (same degrade
contract as the existing methods).

### 1.3 Adapter implementation (`chan-opencode`)

| method | engine calls | behavior |
|---|---|---|
| `status(ses)` | `GET /api/session/active` → if `ses` in map → `Running`; else `GET /api/session/{ses}` → inspect `data.outcome` → `Idle { outcome }`. 404 → `NotFound`. |
| `wait_idle(ses, timeout)` | `POST /api/experimental/session/{ses}/wait` (with `tokio::time::timeout`). On `204` → `GET /api/session/{ses}` → extract outcome → `Idle { outcome }`. On `404` → `NotFound`. On timeout → return last-known or `Running`. |

The `/wait` endpoint is confirmed in the live OpenAPI spec:
```
POST /api/experimental/session/{sessionID}/wait
200/204 — "Wait for a session agent loop to become idle."
```

### 1.4 Surfaces

**CLI:**
```
chan status  --session <ses>                          # JSON: SessionStatus
chan wait    --session <ses> [--timeout 60s]          # blocks; exit 0 idle, 3 timeout
```

**MCP tools:**
```
chan_status  { session }                              # → SessionStatus JSON
chan_wait    { session, timeout_ms? }                 # → SessionStatus JSON or { timeout: true }
```

### 1.5 Orchestrator pattern (new)

Replace the current blind `chan await --timeout 400s` with:

```bash
# wait for the worker's session to finish (not the message — the session)
chan wait --session "$SID_W1" --timeout 120s
# if idle with outcome, read the result it left on the channel
chan await --as orch --kind result --timeout 5s
```

Or the MCP equivalent: `chan_wait { session: sid_w1, timeout_ms: 120000 }`,
then `chan_await { kind: "result", timeout_ms: 5000 }`.

The effect: if the worker crashes or rate-limits, the orchestrator detects it
in seconds instead of blocking for the full await timeout.

---

## 2. Provenance

### 2.1 `session_id` field on `Message`

Add to `chan-core::Message`:

```rust
pub struct Message {
    // — existing fields —
    pub id: MessageId,
    pub from: ParticipantId,
    pub to: Address,
    pub ts: DateTime<Utc>,
    pub kind: MessageKind,
    pub correlation_id: Option<MessageId>,
    pub reply_to: Option<MessageId>,
    pub payload: serde_json::Value,
    pub ttl_secs: Option<u64>,

    // — new —
    /// The engine session that produced this message, if known. Populated
    /// automatically by the MCP server / CLI from `OPENCODE_SESSION_ID`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}
```

This is **metadata on the envelope**, not a change to addressing. It lets the
orchestrator say "this result came from session `ses_abc`" without parsing the
payload. Old messages (without the field) deserialize with `None` —
backward-compatible.

### 2.2 Automatic identity resolution

Both `chan-mcp` and `chan-cli` resolve the participant identity in this order:

| priority | source | example value |
|---|---|---|
| 1 | explicit `--as <name>` / `--from <name>` | `w1` |
| 2 | `CHAN_PARTICIPANT` env var | `w1` |
| 3 | `OPENCODE_SESSION_ID` env var | `ses_abc123` → participant `oc:ses_abc123` |
| 4 | hardcoded default | `orchestrator` |

**Why `OPENCODE_SESSION_ID` works:** opencode injects it into every tool
execution and shell subprocess automatically (confirmed in binary inspection:
`ae.env.OPENCODE_SESSION_ID = z.sessionID`). When `chan-mcp` is launched as an
MCP server inside a session, it inherits the env var. No per-session
`opencode.json` needed.

Implementation — shared helper in a new `chan-core::identity` module:

```rust
/// Resolve participant identity from arguments and the environment.
pub fn resolve_participant(
    explicit: Option<&str>,
    default: &str,
) -> ParticipantId {
    if let Some(name) = explicit {
        return ParticipantId::new(name);
    }
    if let Ok(name) = std::env::var("CHAN_PARTICIPANT") {
        return ParticipantId::new(name);
    }
    if let Ok(ses) = std::env::var("OPENCODE_SESSION_ID") {
        return ParticipantId::new(format!("oc:{ses}"));
    }
    ParticipantId::new(default)
}
```

### 2.3 Session environment setup for live sessions

When the orchestrator creates a live session, it calls the existing opencode
API to set its environment *before* the first prompt:

```
PUT /api/session/{sessionID}/environment
{ "variables": { "CHAN_PARTICIPANT": "w1" } }
```

This is confirmed in the OpenAPI spec. After this call, every tool the session
executes (including `chan-mcp`) sees `CHAN_PARTICIPANT=w1` and identifies as
`w1`. No shared `--as chan` hack needed.

Add a convenience method to `OpencodeClient`:

```rust
/// Set environment variables on a session.
pub async fn set_environment(
    &self,
    session: &str,
    vars: &HashMap<String, String>,
) -> Result<(), LiveError>
```

And a CLI command + MCP tool:

```
chan env --session <ses> --set CHAN_PARTICIPANT=w1
chan_env { session, variables: { "CHAN_PARTICIPANT": "w1" } }
```

### 2.4 `session_id` auto-population

When `chan-mcp` or `chan-cli` constructs a message, it sets `msg.session_id`
from `OPENCODE_SESSION_ID` (if present). The orchestrator never has to pass it
explicitly — it appears on the wire automatically.

---

## 3. Echo cancellation

### 3.1 Room reads filter out self

`FileBackend::recv_room` currently ignores `who`:

```rust
// BEFORE (current)
fn recv_room(&self, _who: &ParticipantId, room: &RoomId, …)
```

Change: after reading, filter out messages where `msg.from == who`:

```rust
fn recv_room(
    &self,
    who: &ParticipantId,
    room: &RoomId,
    cursor: Cursor,
    limit: usize,
) -> Result<(Vec<Message>, Cursor)> {
    let (all_msgs, next) = self.read_messages(
        &self.root.join("rooms").join(format!("{room}.jsonl")),
        cursor,
        limit,
    )?;
    // cursor advances across ALL messages (including own),
    // but only non-self messages are returned to the reader.
    let filtered = all_msgs
        .into_iter()
        .filter(|m| m.from != *who)
        .collect();
    Ok((filtered, next))
}
```

The cursor still advances to `next` (not `next - filtered_out_count`), so
self-messages are consumed and never re-evaluated. This is the key invariant:
**the cursor covers what was scanned, the vec covers what was delivered.**

### 3.2 Same filter in `chan-server` and `RemoteBackend`

`chan-server::room` currently constructs a dummy `ParticipantId::new("_")`
when no `participant` query param is given. Change: **require** the
`participant` parameter on the `/v1/room/{room}` endpoint (it is already
accepted, just defaulting to `_`). Document that the server applies the same
self-filter.

`RemoteBackend::recv_room` already passes `who` as the query parameter — no
change needed on the client side, only behavior changes on the server.

### 3.3 Opt-in echo for special cases

Add an `include_self` flag to the CLI and MCP for the rare case an agent
wants to read its own room publications (e.g. debugging, audit):

```
chan room --as w1 --room team --include-self
chan_room { participant: "w1", room: "team", include_self: true }
```

Default is `false` (self-messages are filtered).

### 3.4 Echo suppression in `AttachBackend`

`AttachBackend::send` persists a message durably, then attempts to inject it
into a live session. If the message was sent *by* the session it is about to
be injected into, the injection must be skipped:

```rust
pub async fn send(
    &self,
    session: &SessionRef,
    msg: &Message,
) -> Result<MessageId, ChanError> {
    let id = self.file.send(msg)?;

    // echo guard: never inject a message back into the session that sent it
    let is_echo = msg.session_id.as_deref() == Some(session.as_str())
        || msg.from.as_str() == session.as_str();
    if is_echo {
        tracing::debug!(%id, "skipping live inject (echo from self)");
        return Ok(id);
    }

    let text = serde_json::to_string(msg).unwrap_or_default();
    match self.live.inject(session, &text).await {
        Ok(()) => tracing::debug!(session = %session, %id, "live delivery ok"),
        Err(LiveError::Unavailable) => tracing::warn!(
            session = %session,
            "live backend unavailable; message is durable in the file log"
        ),
        Err(e) => {
            tracing::warn!(session = %session, error = %e, "live delivery failed; message is durable");
        }
    }
    Ok(id)
}
```

In `chan attach --follow` (the CLI follow loop), the same check: skip
injecting any message whose `from` matches the attached participant or whose
`session_id` matches the target session.

---

## 4. Conformance & testing

### 4.1 Extend the conformance suite

Add to `chan-core::conformance::run`:

```rust
fn room_echo_suppressed<C: Channel>(new: &impl Fn() -> (C, TempDir)) {
    let (c, _d) = new();
    let room = RoomId::new("team");
    let w1 = ParticipantId::new("w1");
    let w2 = ParticipantId::new("w2");
    c.subscribe(&w1, &room).unwrap();
    c.subscribe(&w2, &room).unwrap();

    // w1 publishes to the room
    c.send(&Message::task(
        w1.clone(),
        Address::Room(room.clone()),
        serde_json::json!("hello from w1"),
    )).unwrap();

    // w1 reading the room must NOT see its own message
    let (msgs_w1, _) = c.recv_room(&w1, &room, 0, 10).unwrap();
    assert!(msgs_w1.is_empty(), "w1 must not see its own room publication");

    // w2 reading the room MUST see w1's message
    let (msgs_w2, _) = c.recv_room(&w2, &room, 0, 10).unwrap();
    assert_eq!(msgs_w2.len(), 1, "w2 must see w1's publication");
    assert_eq!(msgs_w2[0].from, w1);
}
```

### 4.2 New unit tests

| crate | test | asserts |
|---|---|---|
| `chan-live` | `session_status_variants_serde` | `SessionStatus` roundtrips through JSON |
| `chan-opencode` | `status_running_when_in_active` | `GET /session/active` hit → `Running` |
| `chan-opencode` | `status_idle_when_not_active` | `GET /session/{id}` fallback → `Idle` |
| `chan-opencode` | `status_not_found_on_404` | 404 → `NotFound` |
| `chan-opencode` | `wait_idle_blocks_until_204` | wiremock delayed response → `Idle` |
| `chan-opencode` | `set_environment_puts_vars` | `PUT /session/{id}/environment` |
| `chan-core` | `message_session_id_roundtrips` | serde with/without `session_id` |
| `chan-core` | `message_session_id_absent_is_none` | old JSON (no field) → `None` |
| `chan-core` | (conformance) `room_echo_suppressed` | see §4.1 |
| `chan-mcp` | `chan_status_tool_schema` | tool schema snapshot |
| `chan-mcp` | `chan_wait_tool_schema` | tool schema snapshot |

### 4.3 E2E test update

Update `demos/chan-collaboration/live-control.sh`:

1. After creating sessions, call `chan env` to set `CHAN_PARTICIPANT` per session.
2. Replace the shared `--as chan` MCP identity with per-session identity.
3. After injecting the main brief, call `chan wait --session $SID_W1 --timeout 120s`
   before `chan await --as orch --kind result`.
4. Assert that room messages have distinct `from` values (`w1`, `w2`), not `chan`.

---

## 5. Impact on existing code

### 5.1 Wire compatibility

The `session_id` field on `Message` uses `#[serde(default, skip_serializing_if)]`.
Old messages (without the field) deserialize cleanly. Old clients that don't
know about the field ignore it. No breaking change.

### 5.2 `recv_room` behavioral change

Filtering self in `recv_room` **is a behavioral change**: agents that currently
read their own publications from a room will stop seeing them. This is the
correct default (and the fix for echo loops), but callers that depend on the
echo must opt in with `include_self`.

Audit of current callers:
- `chan-cli::Cmd::Room` — uses `--as` for identity; will filter self. Correct.
- `chan-mcp::chan_room` — uses participant identity; will filter self. Correct.
- `chan-server::room` — uses query `participant`; will filter self. Correct.
- `chan-core::conformance::room_fanout_exactly_once` — sender `o`, reader `a`;
  they differ, so the test still passes.

### 5.3 `Backend` trait extension

Adding `status` and `wait_idle` to the `Backend` trait is a breaking change for
any out-of-tree implementations. Since the only implementations are
`OpencodeClient` and `NoopBackend` (both in-tree), this is safe. Provide
default impls returning `Unavailable` if future extensibility matters.

---

## 6. Implementation phases

| phase | crate(s) | what | depends on |
|---|---|---|---|
| **P1** | `chan-core` | Add `session_id` to `Message`; add `identity` module with `resolve_participant`; serde tests | — |
| **P2** | `chan-core` | Echo filter in `FileBackend::recv_room` + `include_self` parameter; update conformance suite | P1 |
| **P3** | `chan-live` | Add `SessionStatus`, `status`, `wait_idle` to `Backend`; update `NoopBackend` | — |
| **P4** | `chan-opencode` | Implement `status`, `wait_idle`, `set_environment` on `OpencodeClient`; adapter for new `Backend` methods; wiremock tests | P3 |
| **P5** | `chan-mcp` | Add `chan_status`, `chan_wait`, `chan_env` tools; auto-populate `session_id` from env; use `resolve_participant` for identity | P1, P3, P4 |
| **P6** | `chan-cli` | Add `status`, `wait`, `env` subcommands; use `resolve_participant`; `--include-self` flag on `room`; echo guard in `attach` loop | P1, P2, P3, P4 |
| **P7** | `chan-server`, `chan-client` | Enforce `participant` on `/v1/room/{room}` (self-filter server-side); pass `include_self` query param | P2 |
| **P8** | demos | Update `live-control.sh` to use per-session env, `chan wait`, and assert distinct `from` | P5, P6 |

Gate every phase: `cargo test --workspace && cargo clippy --all-targets -- -D warnings && cargo fmt --check`.

---

## Decisions to lock

1. `session_id` is an **optional string**, not a newtype — it's engine-native
   and opaque to `chan-core`.
2. Self-filter is the **default** in room reads; opt-in echo with
   `include_self`.
3. `resolve_participant` lives in `chan-core` (sync, no network) so both the
   MCP server and CLI share it.
4. `status` / `wait_idle` return `SessionStatus` (not `bool`) — the caller
   always knows the outcome.
5. `POST /api/experimental/session/{id}/wait` is used for `wait_idle` despite
   the `experimental` prefix — it's the only non-polling option and is served
   by the running v2.0.29 server.

## Out of scope

- Heartbeat / keep-alive between agents (the orchestrator polls or waits).
- Auto-restart on `failed` or `interrupted` outcome (orchestrator's decision).
- Per-participant channel ACLs.
- Distributed clock / causal ordering.
