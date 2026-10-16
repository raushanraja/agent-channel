//! Durable, append-only file backend — the channel's source of truth.
//!
//! Layout under the channel root:
//! ```text
//! participants/<id>.jsonl   # direct inbox, append-only
//! rooms/<id>.jsonl          # room log, append-only
//! state/cursors.json        # {"<scope>": <u64>}
//! state/acks.json           # {"<participant>": ["<messageId>", ...]}
//! ```

use crate::Result;
use crate::address::{Address, ParticipantId, RoomId};
use crate::backend::{Channel, ChannelStats, Cursor};
use crate::message::{Message, MessageId};
use fs4::FileExt;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Process-local counter for unique temp filenames.
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// A filesystem-backed channel.
pub struct FileBackend {
    root: PathBuf,
}

impl FileBackend {
    /// Create (or open) a channel rooted at `root`.
    pub fn init(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        for sub in ["participants", "rooms", "state"] {
            std::fs::create_dir_all(root.join(sub))?;
        }
        Ok(Self { root })
    }

    /// The root directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn dest_path(&self, to: &Address) -> PathBuf {
        match to {
            Address::Direct(p) => self.root.join("participants").join(format!("{p}.jsonl")),
            Address::Room(r) => self.root.join("rooms").join(format!("{r}.jsonl")),
            Address::Broadcast => self.root.join("rooms").join("all.jsonl"),
        }
    }

    fn state_path(&self, name: &str) -> PathBuf {
        self.root.join("state").join(name)
    }

    fn read_state(&self, name: &str) -> Result<serde_json::Value> {
        let path = self.state_path(name);
        match std::fs::read(&path) {
            Ok(bytes) if !bytes.is_empty() => Ok(serde_json::from_slice(&bytes)?),
            _ => Ok(serde_json::json!({})),
        }
    }

    fn write_state(&self, name: &str, value: &serde_json::Value) -> Result<()> {
        let path = self.state_path(name);
        // Unique temp name: concurrent writers must never `rename` the same temp away.
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp = self.state_path(&format!(".{name}.{}.{}.tmp", std::process::id(), seq));
        std::fs::write(&tmp, serde_json::to_vec_pretty(value)?)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Run `f` under an exclusive lock on the state directory, so a concurrent
    /// read-modify-write of a shared state file cannot lose an update.
    fn with_state_lock<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let lock_path = self.root.join("state").join(".lock");
        let fh = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)?;
        FileExt::lock(&fh).map_err(|e| crate::ChanError::Lock(e.to_string()))?;
        let out = f();
        let _ = FileExt::unlock(&fh);
        out
    }

    /// Current cursor for `scope` (0 if unseen).
    pub fn cursor(&self, scope: &str) -> Result<Cursor> {
        let v = self.read_state("cursors.json")?;
        Ok(v.get(scope)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0))
    }

    /// Persist a cursor for `scope`.
    pub fn set_cursor(&self, scope: &str, c: Cursor) -> Result<()> {
        self.with_state_lock(|| {
            let mut v = self.read_state("cursors.json")?;
            if !v.is_object() {
                v = serde_json::json!({});
            }
            v[scope] = serde_json::json!(c);
            self.write_state("cursors.json", &v)
        })
    }

    /// Whether `who` has acknowledged `id`.
    pub fn is_acked(&self, who: &ParticipantId, id: &MessageId) -> Result<bool> {
        let v = self.read_state("acks.json")?;
        let sid = id.to_string();
        Ok(v.get(who.as_str())
            .and_then(serde_json::Value::as_array)
            .is_some_and(|a| a.iter().any(|x| x.as_str() == Some(sid.as_str()))))
    }

    fn read_messages(
        &self,
        path: &Path,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Message>, Cursor)> {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), cursor)),
            Err(e) => return Err(e.into()),
        };
        // `split` yields a trailing "" for content ending in '\n'; a file NOT ending
        // in '\n' has a partial final line (a crash mid-write). Drop the last element
        // either way: a complete line is always written with a newline.
        let mut lines: Vec<&str> = content.split('\n').collect();
        lines.pop();
        let total = lines.len() as u64;
        let start = cursor.min(total);
        let end = start.saturating_add(limit as u64).min(total);
        let now = chrono::Utc::now();
        let mut out = Vec::new();
        for line in &lines[start as usize..end as usize] {
            let msg: Message = serde_json::from_str(line)?;
            if !msg.is_expired(now) {
                out.push(msg);
            }
        }
        Ok((out, end))
    }

    /// Read `who`'s direct inbox from `cursor`.
    pub fn recv_direct(
        &self,
        who: &ParticipantId,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Message>, Cursor)> {
        self.read_messages(
            &self.root.join("participants").join(format!("{who}.jsonl")),
            cursor,
            limit,
        )
    }
}

impl Channel for FileBackend {
    fn send(&self, msg: &Message) -> Result<MessageId> {
        let path = self.dest_path(&msg.to);
        let line = serde_json::to_string(msg)?;
        append_line(&path, &line)?;
        Ok(msg.id)
    }

    fn recv(
        &self,
        who: &ParticipantId,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Message>, Cursor)> {
        self.recv_direct(who, cursor, limit)
    }

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
        let filtered = all_msgs.into_iter().filter(|m| m.from != *who).collect();
        Ok((filtered, next))
    }

    fn subscribe(&self, who: &ParticipantId, room: &RoomId) -> Result<Cursor> {
        let scope = format!("sub:{who}:{room}");
        let c = self.cursor(&scope)?;
        self.set_cursor(&scope, c)?;
        Ok(c)
    }

    fn ack(&self, who: &ParticipantId, id: &MessageId) -> Result<()> {
        self.with_state_lock(|| {
            let mut v = self.read_state("acks.json")?;
            if !v.is_object() {
                v = serde_json::json!({});
            }
            let sid = id.to_string();
            let mut arr: Vec<serde_json::Value> = v
                .get(who.as_str())
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            if !arr.iter().any(|x| x.as_str() == Some(sid.as_str())) {
                arr.push(serde_json::json!(sid));
            }
            v[who.as_str()] = serde_json::Value::Array(arr);
            self.write_state("acks.json", &v)
        })
    }

    fn list(&self) -> Result<ChannelStats> {
        let count = |dir: &str| -> Result<usize> {
            Ok(std::fs::read_dir(self.root.join(dir))?
                .filter_map(std::result::Result::ok)
                .filter(|e| e.path().extension().and_then(std::ffi::OsStr::to_str) == Some("jsonl"))
                .count())
        };
        Ok(ChannelStats {
            participants: count("participants")?,
            rooms: count("rooms")?,
        })
    }
}

/// Append one line under an exclusive lock, then `fsync`.
fn append_line(path: &Path, line: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    FileExt::lock(&f).map_err(|e| crate::ChanError::Lock(e.to_string()))?;
    let result = (|| -> std::io::Result<()> {
        let mut w = std::io::BufWriter::new(&f);
        writeln!(w, "{line}")?;
        w.flush()?;
        f.sync_all()
    })();
    let _ = FileExt::unlock(&f);
    result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg_to(to: Address, body: serde_json::Value) -> Message {
        Message::task(ParticipantId::new("orch"), to, body)
    }

    #[test]
    fn init_creates_layout() {
        let dir = tempfile::tempdir().unwrap();
        let _b = FileBackend::init(dir.path()).unwrap();
        assert!(dir.path().join("participants").is_dir());
        assert!(dir.path().join("rooms").is_dir());
        assert!(dir.path().join("state").is_dir());
    }

    #[test]
    fn send_appends_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let b = FileBackend::init(dir.path()).unwrap();
        let p = ParticipantId::new("orch");
        let id = b
            .send(&msg_to(Address::Direct(p.clone()), serde_json::json!("go")))
            .unwrap();
        let b2 = FileBackend::init(dir.path()).unwrap();
        let (msgs, cur) = b2.recv(&p, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].id, id);
        assert_eq!(cur, 1);
    }

    #[test]
    fn room_fanout_delivers_each_message_once_per_subscriber() {
        let dir = tempfile::tempdir().unwrap();
        let b = FileBackend::init(dir.path()).unwrap();
        let room = RoomId::new("build");
        let (a, c) = (ParticipantId::new("a"), ParticipantId::new("c"));
        let ca = b.subscribe(&a, &room).unwrap();
        let cc = b.subscribe(&c, &room).unwrap();
        b.send(&msg_to(Address::Room(room.clone()), serde_json::json!(1)))
            .unwrap();
        let (ra, ca2) = b.recv_room(&a, &room, ca, 10).unwrap();
        let (rc, _) = b.recv_room(&c, &room, cc, 10).unwrap();
        assert_eq!(ra.len(), 1);
        assert_eq!(rc.len(), 1);
        let (ra2, _) = b.recv_room(&a, &room, ca2, 10).unwrap();
        assert!(ra2.is_empty(), "second read must not re-deliver");
    }

    #[test]
    fn ack_records_receipt_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let b = FileBackend::init(dir.path()).unwrap();
        let p = ParticipantId::new("orch");
        let id = b
            .send(&msg_to(Address::Direct(p.clone()), serde_json::json!(1)))
            .unwrap();
        b.ack(&p, &id).unwrap();
        b.ack(&p, &id).unwrap();
        assert!(b.is_acked(&p, &id).unwrap());
    }

    #[test]
    fn partial_trailing_line_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let b = FileBackend::init(dir.path()).unwrap();
        let p = ParticipantId::new("orch");
        b.send(&msg_to(Address::Direct(p.clone()), serde_json::json!(1)))
            .unwrap();
        let f = dir.path().join("participants/orch.jsonl");
        let mut s = std::fs::read_to_string(&f).unwrap();
        s.push_str("{\"id\":\"trunc");
        std::fs::write(&f, s).unwrap();
        let (msgs, cur) = b.recv(&p, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(cur, 1);
    }

    #[test]
    fn expired_messages_are_filtered_on_recv() {
        let dir = tempfile::tempdir().unwrap();
        let b = FileBackend::init(dir.path()).unwrap();
        let p = ParticipantId::new("orch");
        let mut m = msg_to(Address::Direct(p.clone()), serde_json::json!("stale"));
        m.ttl_secs = Some(0);
        m.ts = chrono::Utc::now() - chrono::Duration::seconds(5);
        b.send(&m).unwrap();
        let (msgs, _) = b.recv(&p, 0, 10).unwrap();
        assert!(msgs.is_empty());
    }

    #[test]
    fn concurrent_state_writes_do_not_race_or_lose_updates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let mut hs = Vec::new();
        for w in 0..8 {
            let path = path.clone();
            hs.push(std::thread::spawn(move || {
                let b = FileBackend::init(&path).unwrap();
                for i in 0..20 {
                    b.set_cursor(&format!("scope:w{w}:{i}"), i).unwrap();
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        let b = FileBackend::init(&path).unwrap();
        let v = b.read_state("cursors.json").unwrap();
        assert_eq!(
            v.as_object().unwrap().len(),
            160,
            "every concurrent cursor update must be retained"
        );
    }

    #[test]
    fn concurrent_writers_do_not_corrupt_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let mut hs = Vec::new();
        for w in 0..8 {
            let path = path.clone();
            hs.push(std::thread::spawn(move || {
                let b = FileBackend::init(&path).unwrap();
                let p = ParticipantId::new("orch");
                for i in 0..25 {
                    b.send(&msg_to(
                        Address::Direct(p.clone()),
                        serde_json::json!({ "w": w, "i": i }),
                    ))
                    .unwrap();
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        let b = FileBackend::init(&path).unwrap();
        let (msgs, _) = b.recv(&ParticipantId::new("orch"), 0, 1000).unwrap();
        assert_eq!(msgs.len(), 200);
    }
}
