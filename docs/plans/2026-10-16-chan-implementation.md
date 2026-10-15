# chan Implementation Plan

> **For the executing agent:** implement this plan task-by-task, TDD style. Do NOT skip
> the "run it and watch it fail" steps. Commit after every task. See the design at
> `docs/plans/2026-10-16-chan-design.md`.

**Goal:** Build `chan`, a durable, typed channel message-passing system for orchestrating
`opencode run` children — direct + pub/sub messages over a file-log source of truth, with
a live opencode attach and a direct MCP surface.

**Architecture:** Cargo workspace, four crates with a strict dependency rule
(`chan-core` depends on none of the others): `chan-core` (sync protocol + `FileBackend`),
`chan-opencode` (async live attach), `chan-mcp` (MCP server via `rmcp`), `chan-cli`
(the `chan` binary). Durable-first, live-second: every send writes the file log before
attempting live delivery.

**Tech stack:** Rust edition 2024 (toolchain 1.96), `serde`/`serde_json`, `thiserror`,
`uuid` (v7), `chrono`, `fs4` (file locks), `clap` (derive), `anyhow`, `tracing`, `tokio`,
`reqwest`, `rmcp` 3.x; dev: `tempfile`, `assert_cmd`, `wiremock`.

**Conventions:** data → stdout, errors/progress → stderr. Exit codes: `0` ok,
`3` timeout/no-message, `2` usage, `1` error. Gate every phase with
`cargo test --workspace && cargo clippy --all-targets -- -D warnings && cargo fmt --check`.

---

## Phase 0 — Scaffold the workspace

### Task 0.1: Workspace + crate skeletons

**Files:**
- Create: `chan/Cargo.toml` (workspace)
- Create: `chan/rust-toolchain.toml`
- Create: `chan/.gitignore`
- Create: `chan/crates/chan-core/{Cargo.toml,src/lib.rs}`
- Create: `chan/crates/chan-opencode/{Cargo.toml,src/lib.rs}`
- Create: `chan/crates/chan-mcp/{Cargo.toml,src/main.rs}`
- Create: `chan/crates/chan-cli/{Cargo.toml,src/main.rs}`

**Step 1: Write the workspace manifest**

`chan/Cargo.toml`:
```toml
[workspace]
resolver = "2"
members = ["crates/*"]

[workspace.package]
edition = "2024"
rust-version = "1.85"
license = "MIT"

[workspace.dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
uuid = { version = "1", features = ["v7", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
fs4 = { version = "1", features = ["sync"] }
clap = { version = "4", features = ["derive"] }
anyhow = "1"
tracing = "0.1"
tracing-subscriber = "0.3"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time", "signal", "fs", "io-util"] }
reqwest = { version = "0.13", features = ["json", "stream"] }
rmcp = "3"
tempfile = "3"
assert_cmd = "2"
wiremock = "0.6"
futures = "0.3"
eventsource-stream = "0.2"

[workspace.lints.rust]
unsafe_code = "warn"

[workspace.lints.clippy]
all = "warn"
pedantic = "warn"
```

**Step 2: Toolchain + gitignore**

`chan/rust-toolchain.toml`:
```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```
`chan/.gitignore`:
```
/target
.chan/
```
Also add `**/.chan/` to the repo root `.gitignore` (create if absent).

**Step 3: Create the four crates**

`chan-core` = lib; `chan-opencode` = lib; `chan-mcp` = bin; `chan-cli` = bin. Each
`Cargo.toml` sets `edition.workspace = true`, `rust-version.workspace = true`,
`license.workspace = true`, `[lints] workspace = true`, and `[dependencies]` using
`{ workspace = true }`. Minimal `src` files:
```rust
// chan-core/src/lib.rs and chan-opencode/src/lib.rs
#![forbid(unsafe_code)]
```
```rust
// chan-mcp/src/main.rs and chan-cli/src/main.rs
fn main() { println!("chan"); }
```

**Step 4: Verify the workspace builds**

Run: `cd chan && cargo build --workspace`
Expected: compiles (downloads deps). If offline, vendor first (`cargo vendor`) — note the failure and stop.

**Step 5: Verify the gate is green on an empty tree**

Run: `cargo test --workspace && cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: all pass (no tests yet). Run `cargo fmt --all` first to normalize.

**Step 6: Commit**
```bash
git add chan && git commit -m "chan: scaffold workspace (core/opencode/mcp/cli)"
```

---

## Phase 1 — `chan-core`: identity, envelope, errors

### Task 1.1: Error type

**Files:** Create `chan/crates/chan-core/src/error.rs`; Modify `src/lib.rs`.

**Step 1: Write the failing test** (`src/error.rs`, inline `#[cfg(test)]`)
```rust
#[test]
fn error_display_mentions_context() {
    let e = ChanError::NotFound("room:build".into());
    assert!(e.to_string().contains("room:build"));
}
```
**Step 2: Run it fails** — `cargo test -p chan-core error` → fails (no `ChanError`).
**Step 3: Implement**
```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChanError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("lock: {0}")]
    Lock(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("cursor out of range: {0}")]
    Cursor(String),
    #[error("expired: {0}")]
    TtlExpired(String),
    #[error("protocol: {0}")]
    Protocol(String),
}
pub type Result<T> = std::result::Result<T, ChanError>;
```
Add `pub mod error; pub use error::{ChanError, Result};` to `lib.rs`.
**Step 4: Pass** — `cargo test -p chan-core error` → PASS.
**Step 5: Commit** — `git commit -m "chan-core: error type"`.

### Task 1.2: Identity newtypes + Address

**Files:** Create `src/address.rs`; Modify `src/lib.rs`.

**Step 1: Failing test**
```rust
#[test]
fn address_serializes_and_roundtrips() {
    let a = Address::Room(RoomId::new("build"));
    let s = serde_json::to_string(&a).unwrap();
    let back: Address = serde_json::from_str(&s).unwrap();
    assert_eq!(a, back);
    assert!(matches!(back, Address::Room(r) if r.as_str() == "build"));
}
```
**Step 2: Run fails** — `cargo test -p chan-core address` → fails.
**Step 3: Implement**
```rust
use serde::{Deserialize, Serialize};

macro_rules! id_newtype {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);
        impl $name {
            pub fn new(s: impl Into<String>) -> Self { Self(s.into()) }
            pub fn as_str(&self) -> &str { &self.0 }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.0) }
        }
    };
}
id_newtype!(ParticipantId);
id_newtype!(RoomId);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum Address {
    Direct(ParticipantId),
    Room(RoomId),
    Broadcast,
}
impl Address {
    pub fn all() -> Self { Address::Room(RoomId::new("all")) }
}
```
Add `pub mod address; pub use address::{Address, ParticipantId, RoomId};`.
**Step 4: Pass** → `cargo test -p chan-core address`.
**Step 5: Commit** — `chan-core: identity newtypes and Address`.

### Task 1.3: MessageKind + Message envelope

**Files:** Create `src/message.rs`; Modify `src/lib.rs`.

**Step 1: Failing tests**
```rust
#[test]
fn message_id_is_time_sortable() {
    let a = MessageId::now();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let b = MessageId::now();
    assert!(a.as_uuid() < b.as_uuid());
}

#[test]
fn task_envelope_carries_reply_threading() {
    let m = Message::task(ParticipantId::new("orch"), Address::Room(RoomId::new("build")), serde_json::json!({"phase":1}));
    assert_eq!(m.kind, MessageKind::Task);
    assert_eq!(m.from.as_str(), "orch");
}

#[test]
fn envelope_roundtrips_through_json() {
    let m = Message::question(ParticipantId::new("w1"), Address::Direct(ParticipantId::new("orch")), serde_json::json!("can I delete store.rs?"));
    let s = serde_json::to_string(&m).unwrap();
    let back: Message = serde_json::from_str(&s).unwrap();
    assert_eq!(m, back);
}
```
**Step 2: Run fails**.
**Step 3: Implement**
```rust
use crate::address::{Address, ParticipantId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MessageId(Uuid);
impl MessageId {
    pub fn now() -> Self { Self(Uuid::now_v7()) }
    pub fn as_uuid(&self) -> Uuid { self.0 }
    pub fn parse(s: &str) -> crate::Result<Self> { Ok(Self(Uuid::parse_str(s).map_err(|e| crate::ChanError::Protocol(e.to_string()))?)) }
}
impl std::fmt::Display for MessageId { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) } }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind { Task, Progress, Question, Answer, Result, Ack, Control, Event }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub id: MessageId,
    pub from: ParticipantId,
    pub to: Address,
    pub ts: DateTime<Utc>,
    pub kind: MessageKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<MessageId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<MessageId>,
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
}

impl Message {
    fn base(from: ParticipantId, to: Address, kind: MessageKind, payload: serde_json::Value) -> Self {
        Self { id: MessageId::now(), from, to, ts: Utc::now(), kind, correlation_id: None, reply_to: None, payload, ttl_secs: None }
    }
    pub fn task(from: ParticipantId, to: Address, payload: serde_json::Value) -> Self { Self::base(from, to, MessageKind::Task, payload) }
    pub fn question(from: ParticipantId, to: Address, payload: serde_json::Value) -> Self { Self::base(from, to, MessageKind::Question, payload) }
    pub fn answer(from: ParticipantId, to: Address, payload: serde_json::Value) -> Self { Self::base(from, to, MessageKind::Answer, payload) }
    /// Reply to `req`, threading the correlation id.
    pub fn reply(&self, from: ParticipantId, payload: serde_json::Value) -> Self {
        let mut m = Self::base(from, self.from.clone().into_direct_or_broadcast(), MessageKind::Ack, payload);
        m.correlation_id = self.correlation_id.or(Some(self.id));
        m.reply_to = Some(self.id);
        m
    }
}
```
Add a small helper `impl From<ParticipantId> for Address { fn from(p: ParticipantId) -> Self { Address::Direct(p) } }` and use it in `reply` (replace the placeholder call). Add `pub mod message;` and re-exports.
**Step 4: Pass** — `cargo test -p chan-core message`.
**Step 5: Commit** — `chan-core: message envelope and kinds`.

---

## Phase 2 — `chan-core`: `FileBackend` (the durable log)

Layout under `.chan/`: `participants/<id>.jsonl` (direct inbox, append-only),
`rooms/<id>.jsonl` (room log), `state/cursors.json` (`{"<scope>": <u64>}`),
`state/acks.json` (`{"<id>": [<messageId>...]}`).

### Task 2.1: `Channel` trait + `FileBackend::init`

**Files:** Create `src/backend.rs`, `src/file_backend.rs`; Modify `src/lib.rs`.

**Step 1: Failing test**
```rust
#[test]
fn init_creates_layout() {
    let dir = tempfile::tempdir().unwrap();
    let b = FileBackend::init(dir.path()).unwrap();
    assert!(dir.path().join("participants").is_dir());
    assert!(dir.path().join("rooms").is_dir());
    assert!(dir.path().join("state").is_dir());
    let _ = b;
}
```
**Step 2: Run fails**.
**Step 3: Implement trait + init**
```rust
// backend.rs
use crate::message::{Message, MessageId};
use crate::address::{ParticipantId, RoomId};
use crate::{ChanError, Result};

pub type Cursor = u64;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChannelStats { pub participants: usize, pub rooms: usize }

pub trait Channel {
    fn send(&self, msg: &Message) -> Result<MessageId>;
    fn recv(&self, who: &ParticipantId, cursor: Cursor, limit: usize) -> Result<(Vec<Message>, Cursor)>;
    fn subscribe(&self, who: &ParticipantId, room: &RoomId) -> Result<Cursor>;
    fn ack(&self, who: &ParticipantId, id: &MessageId) -> Result<()>;
    fn list(&self) -> Result<ChannelStats>;
}
```
```rust
// file_backend.rs
use std::path::{Path, PathBuf};
pub struct FileBackend { root: PathBuf }
impl FileBackend {
    pub fn init(root: impl AsRef<Path>) -> crate::Result<Self> {
        let root = root.as_ref().to_path_buf();
        for sub in ["participants", "rooms", "state"] { std::fs::create_dir_all(root.join(sub))?; }
        Ok(Self { root })
    }
    pub fn root(&self) -> &Path { &self.root }
}
```
Add modules + re-exports.
**Step 4: Pass**; **Step 5: Commit**.

### Task 2.2: Cursor + fan-out semantics (`subscribe`/`recv`)

**Step 1: Failing tests** (put the fan-out test in `file_backend.rs`)
```rust
#[test]
fn room_fanout_delivers_each_message_once_per_subscriber() {
    let dir = tempfile::tempdir().unwrap();
    let b = FileBackend::init(dir.path()).unwrap();
    let room = RoomId::new("build");
    let (a, c) = (ParticipantId::new("a"), ParticipantId::new("c"));
    let ca = b.subscribe(&a, &room).unwrap();
    let cc = b.subscribe(&c, &room).unwrap();
    let m = Message::task(ParticipantId::new("orch"), Address::Room(room.clone()), serde_json::json!(1));
    b.send(&m).unwrap();
    let (ra, ca2) = b.recv_room(&a, &room, ca, 10).unwrap();
    let (rc, _cc2) = b.recv_room(&c, &room, cc, 10).unwrap();
    assert_eq!(ra.len(), 1);
    assert_eq!(rc.len(), 1);
    let (ra2, _) = b.recv_room(&a, &room, ca2, 10).unwrap();
    assert!(ra2.is_empty(), "second read must not re-deliver");
}
```
**Step 2: Run fails**.
**Step 3: Implement** cursor state + room reads.
- `scope_key(participant, room) -> String` = `format!("sub:{}:{}", p, room)`.
- `read_cursor(scope)` / `write_cursor(scope, v)` with the **atomic write** helper:
```rust
fn write_json_atomic(path: &Path, v: &serde_json::Value) -> crate::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(v)?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}
```
- `read_json(path)` → `Value::Object(default)` if missing.
- `read_lines(path, cursor, limit)` → open, `BufReader::lines()`, skip `cursor`, take `limit`, parse each; **tolerate a malformed final line** (see Task 2.6).
- `send` picks the file by `msg.to`: `Direct(p) → participants/<p>.jsonl`, `Room(r) → rooms/<r>.jsonl`, `Broadcast → rooms/all.jsonl`.
- `subscribe(participant, room)` sets cursor 0 if absent, returns it.
- `recv_room(participant, room, cursor, limit)` → read room log from cursor, persist new cursor, return `(msgs, new_cursor)`.
- `recv(participant, cursor, limit)` → the participant's **direct inbox** log.
**Step 4: Pass** → `cargo test -p chan-core file_backend`.
**Step 5: Commit**.

### Task 2.3: `send` — flock + fsync append

**Step 1: Failing test**
```rust
#[test]
fn send_appends_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let b = FileBackend::init(dir.path()).unwrap();
    let p = ParticipantId::new("orch");
    let m = Message::task(ParticipantId::new("w1"), Address::Direct(p.clone()), serde_json::json!("go"));
    let id = b.send(&m).unwrap();
    let b2 = FileBackend::init(dir.path()).unwrap();
    let (msgs, _) = b2.recv(&p, 0, 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].id, id);
}
```
**Step 2: Run fails**.
**Step 3: Implement append with an exclusive `fs4` lock and `fsync`**
```rust
use fs4::fs_std::FileExt;
use std::io::Write;

fn append_line(path: &Path, line: &str) -> crate::Result<()> {
    let f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    FileExt::lock_exclusive(&f).map_err(|e| crate::ChanError::Lock(e.to_string()))?;
    let mut w = std::io::BufWriter::new(&f);
    writeln!(w, "{line}")?;
    w.flush()?;
    f.sync_all()?; // fsync durability
    let _ = FileExt::unlock(&f);
    Ok(())
}
```
`send` serializes the `Message` (`serde_json::to_string`), appends to the resolved file, returns `msg.id`.
**Step 4: Pass**; **Step 5: Commit**.

### Task 2.4: `ack` + `list`

**Step 1: Failing test**
```rust
#[test]
fn ack_records_receipt_idempotently() {
    let dir = tempfile::tempdir().unwrap();
    let b = FileBackend::init(dir.path()).unwrap();
    let p = ParticipantId::new("orch");
    let id = b.send(&Message::task(ParticipantId::new("w1"), Address::Direct(p.clone()), serde_json::json!(1))).unwrap();
    b.ack(&p, &id).unwrap();
    b.ack(&p, &id).unwrap(); // idempotent
    assert!(b.is_acked(&p, &id).unwrap());
}
```
**Step 2: Run fails**. **Step 3: Implement** `ack`/`is_acked` over `state/acks.json` (atomic write, dedup in a `Vec` or `BTreeSet`); `list` counts files. **Step 4: Pass**. **Step 5: Commit**.

### Task 2.5: Partial-line tolerance (crash safety)

**Step 1: Failing test**
```rust
#[test]
fn partial_trailing_line_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let b = FileBackend::init(dir.path()).unwrap();
    let p = ParticipantId::new("orch");
    b.send(&Message::task(ParticipantId::new("w1"), Address::Direct(p.clone()), serde_json::json!(1))).unwrap();
    // simulate a crash mid-write
    let f = dir.path().join("participants/orch.jsonl");
    let mut s = std::fs::read_to_string(&f).unwrap();
    s.push_str("{\"id\":\"trunc");
    std::fs::write(&f, s).unwrap();
    let (msgs, cur) = b.recv(&p, 0, 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(cur, 1); // truncated line not counted
}
```
**Step 2: Run fails** (parse error on the last line). **Step 3: Implement** in `read_lines`: if a line fails to parse **and it is the last line** (no trailing newline / EOF), skip it and do not advance the cursor past it. **Step 4: Pass**. **Step 5: Commit**.

### Task 2.6: TTL expiry

**Step 1: Failing test**
```rust
#[test]
fn expired_messages_are_filtered_on_recv() {
    let dir = tempfile::tempdir().unwrap();
    let b = FileBackend::init(dir.path()).unwrap();
    let p = ParticipantId::new("orch");
    let mut m = Message::task(ParticipantId::new("w1"), Address::Direct(p.clone()), serde_json::json!("stale"));
    m.ttl_secs = Some(0);
    m.ts = chrono::Utc::now() - chrono::Duration::seconds(5);
    b.send(&m).unwrap();
    let (msgs, _) = b.recv(&p, 0, 10).unwrap();
    assert!(msgs.is_empty());
}
```
**Step 2: Run fails**. **Step 3: Implement** TTL filter in `recv`: drop messages where `ts + ttl < now`. **Step 4: Pass**. **Step 5: Commit**.

### Task 2.7: Concurrent writers

**Step 1: Failing test**
```rust
#[test]
fn concurrent_writers_do_not_corrupt_log() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let mut hs = vec![];
    for w in 0..8 {
        let path = path.clone();
        hs.push(std::thread::spawn(move || {
            let b = FileBackend::init(&path).unwrap();
            let p = ParticipantId::new("orch");
            for i in 0..25 {
                b.send(&Message::task(ParticipantId::new(format!("w{w}")), Address::Direct(p.clone()), serde_json::json!(i))).unwrap();
            }
        }));
    }
    for h in hs { h.join().unwrap(); }
    let b = FileBackend::init(&path).unwrap();
    let (msgs, _) = b.recv(&ParticipantId::new("orch"), 0, 1000).unwrap();
    assert_eq!(msgs.len(), 200);
}
```
**Step 2: Run fails or races**. **Step 3:** confirm the exclusive lock in `append_line` serializes; if needed, lock a sibling `.lock` file (not the log itself) to avoid read/write interference. **Step 4: Pass** (run it 20× to be sure). **Step 5: Commit**.

---

## Phase 3 — `chan-core`: `Router`

### Task 3.1: Address resolution

**Files:** Create `src/router.rs`.

**Step 1: Failing test**
```rust
#[test]
fn router_broadcast_hits_the_all_room() {
    let dir = tempfile::tempdir().unwrap();
    let b = FileBackend::init(dir.path()).unwrap();
    let r = Router::new(b);
    let p = ParticipantId::new("w1");
    let cur = r.subscribe(&p, &RoomId::new("all")).unwrap();
    r.send(&Message::progress(ParticipantId::new("w1"), Address::Broadcast, serde_json::json!("halfway"))).unwrap();
    let (msgs, _) = r.recv_room(&p, &RoomId::new("all"), cur, 10).unwrap();
    assert_eq!(msgs.len(), 1);
}
```
(Add `Message::progress`, `Message::result`, `Message::ack` convenience constructors alongside `task`/`question`/`answer`.)
**Step 2: Run fails**. **Step 3: Implement** `Router<C: Channel>` that maps `Broadcast → Room("all")` and delegates `send`/`recv`/`subscribe`/`ack`/`list`. **Step 4: Pass**. **Step 5: Commit**.

### Task 3.2: `Channel` conformance suite

**Files:** Create `src/conformance.rs` (a `#[cfg(test)]` reusable module) with a
`pub fn run_channel_conformance<C: Channel>(make: impl Fn() -> C)` that exercises every
invariant (direct send/recv, room fan-out exactly-once, ack idempotence, TTL). Run it
against `FileBackend`.
**Step 1–5:** TDD as above; commit `chan-core: Channel conformance suite`.

**Phase 3 gate:** `cargo test -p chan-core` green, clippy/fmt clean, then commit.

---

## Phase 4 — `chan-cli`

### Task 4.1: CLI skeleton + `init`/`send`/`recv`

**Files:** Modify `chan-cli/src/main.rs`; add `chan-cli/tests/cli.rs`.

**Step 1: Failing integration test** (`tests/cli.rs`, using `assert_cmd` + `tempfile`)
```rust
use assert_cmd::Command;
#[test]
fn send_then_recv_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let chan = dir.path().join(".chan");
    Command::cargo_bin("chan").unwrap().args(["init","--dir"]).arg(&chan).assert().success();
    Command::cargo_bin("chan").unwrap()
        .env("CHAN_DIR", &chan)
        .args(["send","--from","orch","--to","direct:w1","--kind","task","--payload",r#"{"x":1}"#])
        .assert().success();
    let out = Command::cargo_bin("chan").unwrap()
        .env("CHAN_DIR", &chan)
        .args(["recv","--as","w1","--format","json"]).output().unwrap();
    assert!(out.status.success());
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 1);
}
```
**Step 2: Run fails**.
**Step 3: Implement** `clap` derive:
```rust
#[derive(clap::Parser)]
#[command(name = "chan", version)]
struct Cli { #[arg(long, global = true, env = "CHAN_DIR", default_value = ".chan")] dir: PathBuf, #[command(subcommand)] cmd: Cmd }

#[derive(clap::Subcommand)]
enum Cmd {
    Init,
    Send { #[arg(long)] from: String, #[arg(long)] to: String, #[arg(long)] kind: String, #[arg(long)] payload: String },
    Recv { #[arg(long)] as_: String /* value_name "as" */, #[arg(long, default_value_t = 0)] cursor: u64, #[arg(long, default_value_t = 100)] limit: usize, #[arg(long, default_value = "json")] format: String },
    Await { #[arg(long)] as_: String, #[arg(long)] kind: Option<String>, #[arg(long, default_value = "30s")] timeout: String },
    Publish { #[arg(long)] room: String, #[arg(long)] from: String, #[arg(long)] kind: String, #[arg(long)] payload: String },
    Subscribe { #[arg(long)] as_: String, #[arg(long)] room: String },
    Ack { #[arg(long)] as_: String, #[arg(long)] id: String },
    Inbox { #[arg(long)] as_: String },
    Rooms, List,
    Attach { #[arg(long)] participant: String, #[arg(long)] session: String },
    Pump { #[arg(long)] as_: String, #[arg(long)] r#follow: bool },
}
```
`--to` parsing: `direct:<id>` / `room:<id>` / `broadcast`. `--payload` accepts raw JSON or
`@file`. Wire to `chan_core::Router<FileBackend>`. Map errors → exit codes (3 for
`await` timeout).
**Step 4: Pass**; **Step 5: Commit** `chan-cli: init/send/recv/publish/subscribe/ack/inbox/rooms/list`.

### Task 4.2: `await` (block until a matching message)

**Failing test:** send after a delay on a thread; `chan await --as w1 --kind task --timeout 5s` succeeds; with no message it exits `3` at timeout. Implement by polling `recv` with a short interval (`std::thread::sleep`) — file-only, no async. Commit.

### Task 4.3: `pump --follow`

**Failing test:** append two messages while a `pump --follow` child runs; assert both lines land on stdout, then SIGINT exits cleanly. Implement as a tail loop (poll cursor). `attach`/`pump` live modes print a clear "opencode attach not built yet" note until Phase 5. Commit.

---

## Phase 5 — `chan-opencode`: live attach

### Task 5.1: Server client

**Files:** Create `chan-opencode/src/client.rs`.

**Step 1: Failing test** (wiremock)
```rust
#[tokio::test]
async fn prompt_posts_to_session_prompt() {
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::*};
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/api/session/ses_x/prompt"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok":true})))
        .mount(&server).await;
    let c = OpencodeClient::new(server.uri()).unwrap();
    c.prompt("ses_x", "hello").await.unwrap();
}
```
**Step 2: Run fails**. **Step 3: Implement** `OpencodeClient` with `reqwest`:
`prompt` (`POST /api/session/{id}/prompt`), `synthetic`, `interrupt`
(`POST …/interrupt`), `messages` (`GET …/message`), `active` (`GET /api/session/active`),
`event_stream` (`GET /api/event`, SSE via `eventsource-stream`).
**Step 4: Pass**; **Step 5: Commit**.

### Task 5.2: `AttachBackend` + `Pump` with degrade

**Step 1: Failing test** — with the server up (wiremock), an up-message goes through
`prompt` **and** is appended to the file log; with the server refusing connections, the
`Pump` still tails the file log (no error, no loss).
**Step 2: Run fails**. **Step 3: Implement**
- `AttachBackend { client, file: FileBackend }` whose `send` = file-append first, then
  best-effort `prompt`; on `reqwest` connect error → `AttachError::ServerUnavailable`
  swallowed (logged) → durable path only.
- `Pump::stream(participant)` = `tokio::select!` of the SSE `event_stream` and a file-tail
  poll of `recv`; emits a unified `Stream<Item = Message>`.
**Step 4: Pass**; **Step 5: Commit**.

---

## Phase 6 — `chan-mcp`

### Task 6.1: MCP server with `send`/`recv`/`await`

**Files:** `chan-mcp/src/main.rs`, `chan-mcp/src/tools.rs`.

**Step 1: Failing test** — spawn the built binary over stdio, send an MCP `initialize`,
`tools/list`, assert `chan_send`/`chan_recv`/`chan_await` are present; then call
`chan_send` and assert a file lands in `.chan/`.
**Step 2: Run fails**. **Step 3: Implement** with `rmcp`'s server + stdio transport; identity
from `--as`/`CHAN_PARTICIPANT`; each tool is a typed handler calling the shared
`chan-core`/`chan-opencode` layer. **Step 4: Pass**; **Step 5: Commit**.

### Task 6.2: Remaining tools + resources + prompt

Add `chan_publish`, `chan_subscribe`, `chan_ack`, `chan_inbox`, `chan_attach`,
`chan_interrupt`; resources `chan://rooms`, `chan://rooms/{id}/log`,
`chan://participants/{id}/inbox`; `coordinate` prompt. Schema snapshot tests. Commit.

---

## Phase 7 — End-to-end + docs

### Task 7.1: E2E orchestrator ⇄ two children

**Files:** `chan/tests/e2e.rs` (or a `scripts/e2e.sh`).
Drive: orchestrator sends a `Task` to `w1` and `w2`; each child `chan await`s it, sends
back a `Question`, the orchestrator `chan await`s and replies `Answer`, each child emits
`Result`. Assert all in the room log. Commit.

### Task 7.2: Wire the skill to `chan`

Add a short "channel" subsection to
`orchestrating-with-opencode-run/SKILL.md` pointing at `chan send/await/pump` and
`chan-mcp --as <participant>` as the native-tool attach. Commit.

### Task 7.3: Final gate + README

`cargo test --workspace && cargo clippy --all-targets -- -D warnings && cargo fmt --check`
must be green; add `chan/README.md` (usage + MCP config example). Commit.

---

## Phase 8 (later) — hardening, not in first cut

SSE MCP transport, a `chan daemon`, remote/multi-host, file-backend auth, opencode
`session.form` request/response integration. Explicitly out of scope for v0.
