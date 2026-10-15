# chan live layer — port/adapter design

**Date:** 2026-10-16
**Status:** accepted, implementing
**Supersedes (in part):** Phase 5 of `2026-10-16-chan-implementation.md`

## Problem

The shipped `chan-opencode::OpencodeClient` targets a **fictional HTTP API**:

| `client.rs` calls | opencode v2.0.29 reality |
|---|---|
| `POST /api/session/{id}/prompt` | `POST /session/{sessionID}/prompt_async` (`{parts:[…]}` → `204`) |
| `POST /api/session/{id}/prompt` `{delivery:"steer"}` | *no steer/delivery concept; steer == prompt_async while busy* |
| `POST /api/session/{id}/synthetic` | *no such route* (`TextPartInput.synthetic` is a **field**, not a route) |
| `POST /api/session/{id}/interrupt?resume=` | `POST /session/{sessionID}/abort` → `200 bool` |
| `GET /api/session/active` | `GET /session/status` → `{sessionID: idle\|retry\|busy}` |
| `GET /api/event` | `GET /event` (SSE of `Event` JSON) |
| `GET /api/session/{id}/message` | `GET /session/{sessionID}/message` (path ok, `/api` wrong) |

The `wiremock` tests only assert the paths the code itself invented → circular, no
protection. The local checkout (`side/generic/opencode`) is **v1.1.29**, not the
runtime (**v2.0.29**). Authoritative contract = the generated SDK at
`~/.opencode/node_modules/@opencode-ai/sdk/dist/v2`.

## Requirement

Wire live control (inject more data into a **running** child, interrupt it) and keep
the durable channel as-is. The concrete product **must be swappable** — the API may
change (opencode v2 → v3) or be replaced (another engine) **without touching the call
sites** in `chan-cli` / `chan-mcp`.

## Decision — port & adapter (hexagonal)

- **New crate `chan-live`** = the *port*. Product-agnostic: a `Backend` trait over
  opaque `SessionRef`s, speaking `chan-core::Message`. No HTTP, no tokio, no product
  vocabulary. This is the stable thing call sites depend on.
- **`chan-opencode`** = one *adapter*. Implements `chan_live::Backend` for opencode v2
  over HTTP. Absorbs all v2 route/body shapes; a v3 adapter is an isolated change here.
- **Call sites** (`chan-cli`, `chan-mcp`) hold a `Box<dyn Backend>`, so the engine is a
  construction-time choice, never a compile-time coupling.

Why a crate (not the trait in `chan-core`): `chan-core` is deliberately **sync and
network-free**; the port is inherently async. A sibling `chan-live` keeps core's rule
intact and names the seam honestly (`chan-core` = durable channel, `chan-live` = live
session control).

### Port (draft)

```rust
pub struct SessionRef(String);            // opaque product handle
pub enum LiveError { Unavailable, NotFound(String), Status(u16, String), Transport(String) }
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait Backend: Send + Sync {
    /// Down direction: deliver a channel message into a live session (queued/steering).
    fn inject<'a>(&'a self, session: &'a SessionRef, msg: &'a Message) -> BoxFuture<'a, Result<(), LiveError>>;
    /// Control: stop the session's active execution. Returns whether it was running.
    fn interrupt<'a>(&'a self, session: &'a SessionRef) -> BoxFuture<'a, Result<bool, LiveError>>;
    /// Enumerate live sessions.
    fn active<'a>(&'a self) -> BoxFuture<'a, Result<Vec<SessionRef>, LiveError>>;
}
```

`LiveError::Unavailable` is the **degrade trigger**: the attach layer swallows it and
falls back to the durable file log (store-and-forward), exactly as the design requires.

## Phased plan (TDD, gate per phase)

- **L1 — `chan-live`**: port types + trait + a `NoopBackend` (always `Unavailable`, for
  degrade tests). Unit tests only.
- **L2 — `chan-opencode` adapter**: rewrite `OpencodeClient` to the real v2 routes;
  `impl chan_live::Backend`. Real-shape `wiremock` tests.
- **L3 — `AttachBackend`**: persist-then-inject via `dyn Backend`; degrade on
  `Unavailable`; `interrupt` pass-through.
- **L4 — surfaces**: CLI `attach` (replace the stub) drives a participant↔session bind +
  inject loop; MCP `chan_attach` / `chan_interrupt`.
- **L5 (later) — `Pump`**: SSE `/event` + file-tail → unified `Message` stream.

Gate: `cargo test --workspace && cargo clippy --all-targets -- -D warnings && cargo fmt --check`.
