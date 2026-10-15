# chan live layer — port/adapter design

**Date:** 2026-10-16
**Status:** implemented + probe-verified against opencode v2.0.29
**Supersedes (in part):** Phase 5 of `2026-10-16-chan-implementation.md`

## Problem

The original `chan-opencode::OpencodeClient` was coded against a **fictional HTTP API**
(`/api/session/{id}/prompt`, `/interrupt?resume=`, `/synthetic`, `/session/active`,
`/api/event`). Two traps made this hard to see:

1. The `wiremock` tests only asserted the paths the code itself invented → circular.
2. The generated SDK shipped under `~/.opencode/node_modules/@opencode-ai/sdk/v2` is
   **stale** (it advertises `prompt_async`/`abort`/`/session/status`, which the running
   server does not serve). The `opencode` CLI on PATH is also a version behind the server.

The authoritative source is **the running server itself**: `GET /openapi.json`.

## Verified v2 contract (probed against a real server)

Base path **`/api`** (the SPA is a catch-all for non-`/api` paths). Auth is **HTTP Basic**
with the fixed username `opencode` and the password printed by `opencode serve`
(`WWW-Authenticate: Basic realm="Secure Area"`). Env: `OPENCODE_PASSWORD`.

| operation | route (under `/api`) | body → response |
|---|---|---|
| inject | `POST /session/{id}/prompt` | `{text, delivery?, resume?}` → `{data: Session.Inbox.User}` |
| interrupt | `POST /session/{id}/interrupt` | — → `{interrupted: bool}` |
| active | `GET /session/active` | — → `{data: {<ses…>: {type:"running"}}}` |
| events | `GET /event` | SSE of event JSON |
| history | `GET /session/{id}/message` | `?limit=` → messages |

`delivery` ∈ `steer|queue`. The design's predicted primitives (`session.prompt`,
`session.interrupt`, `session.inbox`, `session.form`) **do** exist on the real server —
the stale SDK is what misled the original implementation.

## Decision — port & adapter (hexagonal)

- **`chan-live`** = the *port*: a product-agnostic `Backend` trait
  (`inject`/`interrupt`/`active`) over opaque `SessionRef`s, plus `LiveError`
  (`Unavailable` = degrade, `Unauthorized`, `NotFound`, `Status`, `Transport`).
  No HTTP, no engine vocabulary.
- **`chan-opencode`** = one *adapter* implementing the port over the real `/api` routes
  with Basic auth. A v3 or non-opencode engine is a new adapter crate.
- **Call sites** (`chan-cli`, `chan-mcp`) hold a `dyn Backend`; base URL + password are
  configuration, not compile-time couplings.

Why a crate (not the trait in `chan-core`): `chan-core` is deliberately **sync and
network-free**; the live port is async. `chan-core` = durable channel, `chan-live` = live
control.

## Phases (all landed)

- **L1** `chan-live`: port + `NoopBackend` degrade path.
- **L2** `chan-opencode`: `OpencodeClient` on the real routes + Basic auth; adapter.
- **L3** `AttachBackend`: persist-then-inject via `dyn Backend`; serializes `Message`→text.
- **L4** surfaces: CLI `chan attach` / `chan interrupt`; MCP `chan_inject` / `chan_interrupt` / `chan_active`.
- **L5 (open)** `Pump`: `GET /event` SSE → unified `Message` stream (observation direction).

## Probe evidence (live server, 2.0.29)

```
session=ses_ebee…  durable msg 01a14119…  (chan send --to direct:probe)
chan attach --participant probe --session ses_ebee… --once   -> injected 01a14119… -> ses_ebee…
GET /api/session/active      -> {"data":{"ses_ebee…":{"type":"running"}}}
chan interrupt --session ses_ebee…          -> {"interrupted":true}
GET /api/session/ses_ebee…/message -> INJECTED_ENVELOPE_FOUND
```

i.e. durable message → live inject → observed running → interrupt → verified in history.

Gate: `cargo test --workspace && cargo clippy --all-targets -- -D warnings && cargo fmt --check`.
