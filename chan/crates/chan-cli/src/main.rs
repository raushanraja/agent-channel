//! `chan` — the channel message-passing CLI.
//!
//! Works against the local file log by default, or a `chan-serve` daemon when
//! `--server` is given.

use anyhow::{Context, Result, anyhow};
use chan_client::RemoteBackend;
use chan_core::{
    Address, Channel, ChannelStats, Cursor, FileBackend, Message, MessageId, MessageKind,
    ParticipantId, RoomId, resolve_participant,
};
use chan_live::{Backend, SessionRef, SessionStatus};
use chan_opencode::connect;
use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(name = "chan", version, about = "channel message passing")]
struct Cli {
    /// Channel directory (local mode).
    #[arg(long, global = true, env = "CHAN_DIR", default_value = ".chan")]
    dir: PathBuf,
    /// Remote daemon base URL (e.g. http://127.0.0.1:27400); enables remote mode.
    #[arg(long, global = true, env = "CHAN_SERVER")]
    server: Option<String>,
    /// Bearer token for the daemon.
    #[arg(long, global = true, env = "CHAN_TOKEN")]
    token: Option<String>,
    /// Client-side cursor store (remote mode).
    #[arg(long, global = true, default_value = ".chan.client")]
    state: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create the local channel layout.
    Init,
    /// Run the HTTP+SSE daemon.
    Serve {
        #[arg(long, default_value = "127.0.0.1:27400")]
        bind: std::net::SocketAddr,
    },
    /// Print a fresh random token.
    Token,
    /// Send a message (direct / room / broadcast).
    Send {
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: String,
        #[arg(long, default_value = "task")]
        kind: String,
        #[arg(long)]
        payload: String,
        #[arg(long)]
        reply_to: Option<String>,
        #[arg(long)]
        corr: Option<String>,
        #[arg(long)]
        ttl: Option<u64>,
    },
    /// Publish to a room.
    Publish {
        #[arg(long)]
        room: String,
        #[arg(long)]
        from: Option<String>,
        #[arg(long, default_value = "event")]
        kind: String,
        #[arg(long)]
        payload: String,
    },
    /// Read your direct inbox and advance your cursor.
    Recv {
        #[arg(long = "as")]
        as_: Option<String>,
        #[arg(long)]
        cursor: Option<u64>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Read a room and advance your subscription cursor.
    Room {
        #[arg(long = "as")]
        as_: Option<String>,
        #[arg(long)]
        room: String,
        #[arg(long)]
        cursor: Option<u64>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long)]
        include_self: bool,
    },
    /// Block until a matching message arrives (exit 3 on timeout).
    Await {
        #[arg(long = "as")]
        as_: Option<String>,
        #[arg(long)]
        kind: Option<String>,
        #[arg(long, default_value = "30s")]
        timeout: String,
    },
    /// Subscribe to a room (creates your cursor).
    Subscribe {
        #[arg(long = "as")]
        as_: Option<String>,
        #[arg(long)]
        room: String,
    },
    /// Acknowledge a message id.
    Ack {
        #[arg(long = "as")]
        as_: Option<String>,
        #[arg(long)]
        id: String,
    },
    /// Show an inbox's cursor and unread count.
    Inbox {
        #[arg(long = "as")]
        as_: Option<String>,
    },
    /// List rooms.
    Rooms,
    /// Summary counts.
    List,
    /// Bridge a participant's inbox into a live opencode session (durable-first).
    Attach {
        #[arg(long)]
        participant: Option<String>,
        #[arg(long)]
        session: String,
        /// Root URL of the opencode server (the API is mounted at /api).
        /// Omit to auto-discover a running background service.
        #[arg(long, env = "CHAN_OPENCODE")]
        opencode: Option<String>,
        /// Server password (`OPENCODE_PASSWORD`). Auto-discovered when omitted.
        #[arg(long, env = "OPENCODE_PASSWORD")]
        opencode_password: Option<String>,
        /// Drain the inbox once and exit instead of following.
        #[arg(long)]
        once: bool,
        /// Poll interval while following.
        #[arg(long, default_value_t = 250)]
        poll_ms: u64,
    },
    /// Interrupt a live opencode session's active execution.
    Interrupt {
        #[arg(long)]
        session: String,
        /// Root URL of the opencode server (the API is mounted at /api).
        /// Omit to auto-discover a running background service.
        #[arg(long, env = "CHAN_OPENCODE")]
        opencode: Option<String>,
        /// Server password (`OPENCODE_PASSWORD`). Auto-discovered when omitted.
        #[arg(long, env = "OPENCODE_PASSWORD")]
        opencode_password: Option<String>,
    },
    /// Stream messages from your inbox.
    Pump {
        #[arg(long = "as")]
        as_: Option<String>,
        #[arg(long)]
        follow: bool,
    },
    /// Show status of a live opencode session.
    Status {
        #[arg(long)]
        session: String,
        /// Root URL of the opencode server (the API is mounted at /api).
        /// Omit to auto-discover a running background service.
        #[arg(long, env = "CHAN_OPENCODE")]
        opencode: Option<String>,
        /// Server password (`OPENCODE_PASSWORD`). Auto-discovered when omitted.
        #[arg(long, env = "OPENCODE_PASSWORD")]
        opencode_password: Option<String>,
    },
    /// Block until a live opencode session becomes idle.
    Wait {
        #[arg(long)]
        session: String,
        #[arg(long, default_value = "60s")]
        timeout: String,
        /// Root URL of the opencode server (the API is mounted at /api).
        /// Omit to auto-discover a running background service.
        #[arg(long, env = "CHAN_OPENCODE")]
        opencode: Option<String>,
        /// Server password (`OPENCODE_PASSWORD`). Auto-discovered when omitted.
        #[arg(long, env = "OPENCODE_PASSWORD")]
        opencode_password: Option<String>,
    },
    /// Set environment variables on a live opencode session.
    Env {
        #[arg(long)]
        session: String,
        #[arg(long = "set", value_name = "KEY=VAL")]
        set: Vec<String>,
        /// Root URL of the opencode server (the API is mounted at /api).
        /// Omit to auto-discover a running background service.
        #[arg(long, env = "CHAN_OPENCODE")]
        opencode: Option<String>,
        /// Server password (`OPENCODE_PASSWORD`). Auto-discovered when omitted.
        #[arg(long, env = "OPENCODE_PASSWORD")]
        opencode_password: Option<String>,
    },
}

/// A tiny JSON cursor store used in remote mode.
struct Cursors(PathBuf);

impl Cursors {
    fn get(&self, scope: &str) -> Cursor {
        self.load().get(scope).copied().unwrap_or(0)
    }

    fn set(&self, scope: &str, v: Cursor) -> Result<()> {
        let mut m = self.load();
        m.insert(scope.to_string(), v);
        if let Some(parent) = self.0.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.0, serde_json::to_vec_pretty(&m)?)?;
        Ok(())
    }

    fn load(&self) -> HashMap<String, Cursor> {
        std::fs::read(&self.0)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default()
    }
}

/// The backend the CLI is talking to: local files or a remote daemon.
enum Chan {
    Local(FileBackend),
    Remote {
        backend: RemoteBackend,
        cursors: Cursors,
    },
}

impl Chan {
    fn open(dir: &Path, server: Option<&str>, token: Option<String>, state: &Path) -> Result<Self> {
        match server {
            Some(base) => Ok(Chan::Remote {
                backend: RemoteBackend::new(base, token),
                cursors: Cursors(state.to_path_buf()),
            }),
            None => Ok(Chan::Local(FileBackend::init(dir)?)),
        }
    }

    fn send(&self, msg: &Message) -> Result<MessageId> {
        Ok(match self {
            Chan::Local(b) => b.send(msg)?,
            Chan::Remote { backend, .. } => backend.send(msg)?,
        })
    }

    fn subscribe(&self, who: &ParticipantId, room: &RoomId) -> Result<Cursor> {
        Ok(match self {
            Chan::Local(b) => b.subscribe(who, room)?,
            Chan::Remote { backend, .. } => backend.subscribe(who, room)?,
        })
    }

    fn ack(&self, who: &ParticipantId, id: &MessageId) -> Result<()> {
        match self {
            Chan::Local(b) => b.ack(who, id)?,
            Chan::Remote { backend, .. } => backend.ack(who, id)?,
        }
        Ok(())
    }

    fn list(&self) -> Result<ChannelStats> {
        Ok(match self {
            Chan::Local(b) => b.list()?,
            Chan::Remote { backend, .. } => backend.list()?,
        })
    }

    fn cursor(&self, scope: &str) -> Result<Cursor> {
        Ok(match self {
            Chan::Local(b) => b.cursor(scope)?,
            Chan::Remote { cursors, .. } => cursors.get(scope),
        })
    }

    fn set_cursor(&self, scope: &str, c: Cursor) -> Result<()> {
        match self {
            Chan::Local(b) => b.set_cursor(scope, c)?,
            Chan::Remote { cursors, .. } => cursors.set(scope, c)?,
        }
        Ok(())
    }

    fn recv_direct(
        &self,
        who: &ParticipantId,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Message>, Cursor)> {
        Ok(match self {
            Chan::Local(b) => b.recv_direct(who, cursor, limit)?,
            Chan::Remote { backend, .. } => backend.recv(who, cursor, limit)?,
        })
    }

    fn recv_room(
        &self,
        who: &ParticipantId,
        room: &RoomId,
        cursor: Cursor,
        limit: usize,
    ) -> Result<(Vec<Message>, Cursor)> {
        Ok(match self {
            Chan::Local(b) => b.recv_room(who, room, cursor, limit)?,
            Chan::Remote { backend, .. } => backend.recv_room(who, room, cursor, limit)?,
        })
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let Cli {
        dir,
        server,
        token,
        state,
        cmd,
    } = cli;

    match cmd {
        Cmd::Serve { bind } => {
            let backend = FileBackend::init(&dir)?;
            let rt = tokio::runtime::Runtime::new()?;
            return rt.block_on(chan_server::serve(backend, bind, token));
        }
        Cmd::Token => {
            println!("{}", uuid::Uuid::new_v4());
            return Ok(());
        }
        _ => {}
    }

    let chan = Chan::open(&dir, server.as_deref(), token, &state)?;
    match cmd {
        Cmd::Init => {
            let b = FileBackend::init(&dir)?;
            print_json(&serde_json::json!({ "root": b.root() }));
        }
        Cmd::Send {
            from,
            to,
            kind,
            payload,
            reply_to,
            corr,
            ttl,
        } => {
            let from = resolve_participant(from.as_deref(), "orchestrator");
            let mut m = Message::task(from, parse_addr(&to)?, parse_payload(&payload)?);
            m.kind = parse_kind(&kind)?;
            m.ttl_secs = ttl;
            if let Some(r) = reply_to {
                m.reply_to = Some(MessageId::parse(&r)?);
            }
            if let Some(c) = corr {
                m.correlation_id = Some(MessageId::parse(&c)?);
            }
            m.session_id = std::env::var("OPENCODE_SESSION_ID").ok();
            let id = chan.send(&m)?;
            print_json(&serde_json::json!({ "id": id.to_string() }));
        }
        Cmd::Publish {
            room,
            from,
            kind,
            payload,
        } => {
            let from = resolve_participant(from.as_deref(), "orchestrator");
            let mut m = Message::task(
                from,
                Address::Room(RoomId::new(room)),
                parse_payload(&payload)?,
            );
            m.kind = parse_kind(&kind)?;
            m.session_id = std::env::var("OPENCODE_SESSION_ID").ok();
            let id = chan.send(&m)?;
            print_json(&serde_json::json!({ "id": id.to_string() }));
        }
        Cmd::Recv { as_, cursor, limit } => {
            let who = resolve_participant(as_.as_deref(), "orchestrator");
            let scope = format!("inbox:{who}");
            let from = cursor.unwrap_or(chan.cursor(&scope)?);
            let (msgs, next) = chan.recv_direct(&who, from, limit)?;
            chan.set_cursor(&scope, next)?;
            print_json(&serde_json::json!(msgs));
        }
        Cmd::Room {
            as_,
            room,
            cursor,
            limit,
            include_self,
        } => {
            let who = resolve_participant(as_.as_deref(), "orchestrator");
            let room = RoomId::new(room);
            let scope = format!("sub:{who}:{room}");
            let from = cursor.unwrap_or(chan.cursor(&scope)?);
            let read_who = if include_self {
                ParticipantId::new("_")
            } else {
                who.clone()
            };
            let (msgs, next) = chan.recv_room(&read_who, &room, from, limit)?;
            chan.set_cursor(&scope, next)?;
            print_json(&serde_json::json!(msgs));
        }
        Cmd::Await { as_, kind, timeout } => {
            let who = resolve_participant(as_.as_deref(), "orchestrator");
            let scope = format!("inbox:{who}");
            let want = kind.as_deref().map(parse_kind).transpose()?;
            let deadline = Instant::now() + parse_duration(&timeout)?;
            let mut cursor = chan.cursor(&scope)?;
            loop {
                let (msgs, next) = chan.recv_direct(&who, cursor, 1000)?;
                if let Some((idx, m)) = msgs
                    .iter()
                    .enumerate()
                    .find(|(_, m)| want.is_none_or(|k| m.kind == k))
                {
                    chan.set_cursor(&scope, cursor + idx as u64 + 1)?;
                    print_json(&serde_json::to_value(m)?);
                    return Ok(());
                }
                cursor = next;
                chan.set_cursor(&scope, cursor)?;
                if Instant::now() >= deadline {
                    print_json(&serde_json::json!([]));
                    std::process::exit(3);
                }
                std::thread::sleep(Duration::from_millis(150));
            }
        }
        Cmd::Subscribe { as_, room } => {
            let who = resolve_participant(as_.as_deref(), "orchestrator");
            let cursor = chan.subscribe(&who, &RoomId::new(room))?;
            print_json(&serde_json::json!({ "cursor": cursor }));
        }
        Cmd::Ack { as_, id } => {
            let who = resolve_participant(as_.as_deref(), "orchestrator");
            chan.ack(&who, &MessageId::parse(&id)?)?;
            print_json(&serde_json::json!({ "acked": id }));
        }
        Cmd::Inbox { as_ } => {
            let who = resolve_participant(as_.as_deref(), "orchestrator");
            let scope = format!("inbox:{who}");
            let cursor = chan.cursor(&scope)?;
            let (all, _) = chan.recv_direct(&who, 0, usize::MAX)?;
            print_json(&serde_json::json!({
                "participant": who.as_str(),
                "cursor": cursor,
                "unread": (all.len() as u64).saturating_sub(cursor),
            }));
        }
        Cmd::Rooms => {
            let names: Vec<String> = if server.is_some() {
                Vec::new()
            } else {
                list_json(&dir.join("rooms"))
            };
            print_json(&serde_json::json!(names));
        }
        Cmd::List => {
            print_json(&serde_json::json!(chan.list()?));
        }
        Cmd::Attach {
            participant,
            session,
            opencode,
            opencode_password,
            once,
            poll_ms,
        } => {
            let live = connect(opencode, opencode_password);
            let who = resolve_participant(participant.as_deref(), "orchestrator");
            let participant = who.as_str().to_string();
            let session = SessionRef::new(session);
            let scope = format!("inbox:{who}");
            let rt = tokio::runtime::Runtime::new()?;
            let mut cursor = chan.cursor(&scope)?;
            loop {
                let (msgs, next) = chan.recv_direct(&who, cursor, 1000)?;
                for m in &msgs {
                    let is_echo = m.session_id.as_deref() == Some(session.as_str())
                        || m.from.as_str() == participant.as_str();
                    if is_echo {
                        continue;
                    }
                    let text = serde_json::to_string(m)?;
                    match rt.block_on(live.inject(&session, &text)) {
                        Ok(()) => eprintln!("chan: injected {} -> {session}", m.id),
                        Err(e) => eprintln!("chan: inject failed for {}: {e}", m.id),
                    }
                }
                cursor = next;
                chan.set_cursor(&scope, cursor)?;
                if once {
                    break;
                }
                std::thread::sleep(Duration::from_millis(poll_ms));
            }
        }
        Cmd::Interrupt {
            session,
            opencode,
            opencode_password,
        } => {
            let live = connect(opencode, opencode_password);
            let rt = tokio::runtime::Runtime::new()?;
            let interrupted = rt.block_on(live.interrupt(&SessionRef::new(session)))?;
            print_json(&serde_json::json!({ "interrupted": interrupted }));
        }
        Cmd::Pump { as_, follow } => {
            let who = resolve_participant(as_.as_deref(), "orchestrator");
            let scope = format!("inbox:{who}");
            let mut cursor = chan.cursor(&scope)?;
            loop {
                let (msgs, next) = chan.recv_direct(&who, cursor, 1000)?;
                for m in &msgs {
                    println!("{}", serde_json::to_string(m)?);
                }
                cursor = next;
                chan.set_cursor(&scope, cursor)?;
                if !follow {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(150));
            }
        }
        Cmd::Status {
            session,
            opencode,
            opencode_password,
        } => {
            let live = connect(opencode, opencode_password);
            let rt = tokio::runtime::Runtime::new()?;
            let st = rt.block_on(live.status(&session))?;
            print_json(&serde_json::to_value(st)?);
        }
        Cmd::Wait {
            session,
            timeout,
            opencode,
            opencode_password,
        } => {
            let live = connect(opencode, opencode_password);
            let rt = tokio::runtime::Runtime::new()?;
            let st = rt.block_on(live.wait_idle(&session, parse_duration(&timeout)?))?;
            print_json(&serde_json::to_value(st.clone())?);
            if matches!(st, SessionStatus::Running) {
                std::process::exit(3);
            }
        }
        Cmd::Env {
            session,
            set,
            opencode,
            opencode_password,
        } => {
            let mut vars = HashMap::new();
            for kv in set {
                let (k, v) = kv
                    .split_once('=')
                    .ok_or_else(|| anyhow!("bad KEY=VAL: {kv}"))?;
                vars.insert(k.to_string(), v.to_string());
            }
            let live = connect(opencode, opencode_password);
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(live.set_environment(&session, &vars))?;
            print_json(&serde_json::json!({ "updated": vars.len() }));
        }
        Cmd::Serve { .. } | Cmd::Token => unreachable!("handled above"),
    }
    Ok(())
}

fn parse_kind(s: &str) -> Result<MessageKind> {
    Ok(match s {
        "task" => MessageKind::Task,
        "progress" => MessageKind::Progress,
        "question" => MessageKind::Question,
        "answer" => MessageKind::Answer,
        "result" => MessageKind::Result,
        "ack" => MessageKind::Ack,
        "control" => MessageKind::Control,
        "event" => MessageKind::Event,
        other => return Err(anyhow!("unknown kind: {other}")),
    })
}

fn parse_addr(s: &str) -> Result<Address> {
    if s == "broadcast" {
        return Ok(Address::Broadcast);
    }
    if let Some(id) = s.strip_prefix("direct:") {
        return Ok(Address::Direct(ParticipantId::new(id)));
    }
    if let Some(id) = s.strip_prefix("room:") {
        return Ok(Address::Room(RoomId::new(id)));
    }
    Err(anyhow!(
        "bad address `{s}` (use direct:<id> | room:<id> | broadcast)"
    ))
}

fn parse_payload(s: &str) -> Result<serde_json::Value> {
    if let Some(path) = s.strip_prefix('@') {
        let bytes = std::fs::read(path).with_context(|| format!("reading payload file {path}"))?;
        Ok(serde_json::from_slice(&bytes)?)
    } else {
        Ok(serde_json::from_str(s)?)
    }
}

fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: u64 = num.parse().with_context(|| format!("bad duration: {s}"))?;
    let secs = match unit {
        "" | "s" => n,
        "ms" => return Ok(Duration::from_millis(n)),
        "m" => n * 60,
        other => return Err(anyhow!("bad duration unit: {other}")),
    };
    Ok(Duration::from_secs(secs))
}

fn list_json(dir: &Path) -> Vec<String> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut rooms: Vec<String> = rd
        .filter_map(std::result::Result::ok)
        .filter_map(|e| {
            let p = e.path();
            (p.extension().and_then(std::ffi::OsStr::to_str) == Some("jsonl"))
                .then(|| {
                    p.file_stem()
                        .and_then(std::ffi::OsStr::to_str)
                        .map(str::to_string)
                })
                .flatten()
        })
        .collect();
    rooms.sort();
    rooms
}

fn print_json(v: &serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string(v).unwrap_or_else(|_| "null".to_string())
    );
}
