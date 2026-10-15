# chan Phase 8 — network block (daemon + multi-host + auth)

**Goal:** turn the local `.chan/` file log into a shared, network-reachable message bus by
adding a **daemon** (`chan serve`) that owns the log and speaks HTTP + SSE, a **client**
(`RemoteBackend`) that implements the existing `Channel` trait over the network, and
**participant tokens**. `chan` becomes reachable across hosts; the file log stays the
source of truth.

**Principle:** `chan-core` stays sync and network-free. Networking lives in two new
crates; the `Channel` trait is the seam, so the CLI is backend-agnostic (local files by
default, remote when `--server` is given).

**Tech:** axum 0.8 (server, async), `reqwest` **blocking** (client → keeps `Channel`
sync), `tower-http` (traces), bearer-token auth.

## New crates

| Crate | Role | Runtime |
|---|---|---|
| `chan-server` | `chan-serve` daemon: HTTP+SSE over a `FileBackend`, auth middleware | async (axum/tokio) |
| `chan-client` | `RemoteBackend: Channel` over HTTP; SSE stream helper | sync (reqwest blocking) |

## Protocol v1

All JSON. Auth: `Authorization: Bearer <token>` (or `?token=`) unless the daemon was
started without a token. `GET /v1/health` is always open.

| Method | Path | Body / Query | Returns |
|---|---|---|---|
| GET  | `/v1/health` | — | `{status, participants, rooms}` |
| POST | `/v1/send` | `{message}` | `{id}` |
| GET  | `/v1/inbox/{participant}` | `?cursor=&limit=` | `{messages, cursor}` |
| GET  | `/v1/room/{room}` | `?participant=&cursor=&limit=` | `{messages, cursor}` |
| POST | `/v1/subscribe` | `{participant, room}` | `{cursor}` |
| POST | `/v1/ack` | `{participant, id}` | `{acked}` |
| GET  | `/v1/rooms` | — | `{rooms:[...]}` |
| GET  | `/v1/inbox/{participant}/stream` | `?cursor=` | SSE stream of `message` events |

## Tasks

- **8.0 Scaffold** `chan-server` (bin `chan-serve`) + `chan-client`; add `axum`,
  `tower-http`, `reqwest` blocking to the workspace. Both compile empty; gate green.
- **8.1 Server core** — `serve(FileBackend, addr)`; routes: health, send, inbox. Tests:
  spawn on port 0, hit with `reqwest`, assert roundtrip.
- **8.2 Server rooms** — room, subscribe, ack, rooms. Fan-out exactly-once via cursors.
- **8.3 Auth** — bearer middleware; health open, everything else 401 without a valid
  token. Tests: no token → 401, wrong token → 401, right token → 200.
- **8.4 SSE stream** — `/v1/inbox/{p}/stream` emits new messages as they land (poll the
  file tail server-side, emit SSE `message` events). Test: connect, `send`, read event.
- **8.5 Client** — `RemoteBackend` implementing `Channel` with reqwest blocking; run the
  `chan-core` **conformance suite** against a live server. Degrade/clear errors.
- **8.6 CLI** — global `--server <url>` + `--token <t>`: route through `RemoteBackend`
  when set, else local `FileBackend`. Add `chan serve` (start daemon) and `chan token`
  (mint a random token).
- **8.7 E2E over the network** — orchestrator + two children talking to one `chan serve`,
  with auth on; assert the exchange and that an unauthenticated client is refused.
- **8.8 Docs + gate** — README server section; full workspace gate.

## Out of scope

TLS/HTTP2 hardening, token rotation/expiry, per-participant ACLs (single shared token
for now), reconnection/offline queueing beyond cursor replay, a supervisor/systemd unit.
