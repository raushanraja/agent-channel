//! `chan` — the channel message-passing CLI.

use anyhow::{Context, Result, anyhow};
use chan_core::{
    Address, Channel, FileBackend, Message, MessageId, MessageKind, ParticipantId, RoomId,
};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(name = "chan", version, about = "channel message passing")]
struct Cli {
    /// Channel directory.
    #[arg(long, global = true, env = "CHAN_DIR", default_value = ".chan")]
    dir: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create the channel layout.
    Init,
    /// Send a message (direct / room / broadcast).
    Send {
        #[arg(long)]
        from: String,
        /// `direct:<id>` | `room:<id>` | `broadcast`.
        #[arg(long)]
        to: String,
        #[arg(long, default_value = "task")]
        kind: String,
        /// Inline JSON, or `@path` to read a JSON file.
        #[arg(long)]
        payload: String,
        #[arg(long)]
        reply_to: Option<String>,
        #[arg(long)]
        corr: Option<String>,
        #[arg(long)]
        ttl: Option<u64>,
    },
    /// Publish to a room (sugar for `send --to room:<id>`).
    Publish {
        #[arg(long)]
        room: String,
        #[arg(long)]
        from: String,
        #[arg(long, default_value = "event")]
        kind: String,
        #[arg(long)]
        payload: String,
    },
    /// Read your direct inbox and advance your cursor.
    Recv {
        #[arg(long = "as")]
        as_: String,
        #[arg(long)]
        cursor: Option<u64>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Read a room and advance your subscription cursor.
    Room {
        #[arg(long = "as")]
        as_: String,
        #[arg(long)]
        room: String,
        #[arg(long)]
        cursor: Option<u64>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Block until a matching message arrives (exit 3 on timeout).
    Await {
        #[arg(long = "as")]
        as_: String,
        #[arg(long)]
        kind: Option<String>,
        #[arg(long, default_value = "30s")]
        timeout: String,
    },
    /// Subscribe to a room (creates your cursor).
    Subscribe {
        #[arg(long = "as")]
        as_: String,
        #[arg(long)]
        room: String,
    },
    /// Acknowledge a message id.
    Ack {
        #[arg(long = "as")]
        as_: String,
        #[arg(long)]
        id: String,
    },
    /// Show an inbox's cursor and unread count.
    Inbox {
        #[arg(long = "as")]
        as_: String,
    },
    /// List rooms.
    Rooms,
    /// Summary counts.
    List,
    /// Attach a participant to an opencode session (live; Phase 5).
    Attach {
        #[arg(long)]
        participant: String,
        #[arg(long)]
        session: String,
    },
    /// Stream messages from your inbox (durable tail).
    Pump {
        #[arg(long = "as")]
        as_: String,
        #[arg(long)]
        follow: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let dir = cli.dir;
    match cli.cmd {
        Cmd::Init => {
            let b = FileBackend::init(&dir)?;
            println!("{}", json_str(serde_json::json!({ "root": b.root() })));
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
            let b = FileBackend::init(&dir)?;
            let mut m = Message::task(
                ParticipantId::new(from),
                parse_addr(&to)?,
                parse_payload(&payload)?,
            );
            m.kind = parse_kind(&kind)?;
            if let Some(t) = ttl {
                m.ttl_secs = Some(t);
            }
            if let Some(r) = reply_to {
                m.reply_to = Some(MessageId::parse(&r)?);
            }
            if let Some(c) = corr {
                m.correlation_id = Some(MessageId::parse(&c)?);
            }
            let id = b.send(&m)?;
            print_json(&serde_json::json!({ "id": id.to_string() }));
        }
        Cmd::Publish {
            room,
            from,
            kind,
            payload,
        } => {
            let b = FileBackend::init(&dir)?;
            let mut m = Message::task(
                ParticipantId::new(from),
                Address::Room(RoomId::new(room)),
                parse_payload(&payload)?,
            );
            m.kind = parse_kind(&kind)?;
            let id = b.send(&m)?;
            print_json(&serde_json::json!({ "id": id.to_string() }));
        }
        Cmd::Recv { as_, cursor, limit } => {
            let b = FileBackend::init(&dir)?;
            let who = ParticipantId::new(as_);
            let scope = format!("inbox:{who}");
            let from = cursor.unwrap_or(b.cursor(&scope)?);
            let (msgs, next) = b.recv_direct(&who, from, limit)?;
            b.set_cursor(&scope, next)?;
            print_json(&serde_json::json!(msgs));
        }
        Cmd::Room {
            as_,
            room,
            cursor,
            limit,
        } => {
            let b = FileBackend::init(&dir)?;
            let who = ParticipantId::new(as_);
            let room = RoomId::new(room);
            let scope = format!("sub:{who}:{room}");
            let from = cursor.unwrap_or(b.cursor(&scope)?);
            let (msgs, next) = b.recv_room(&who, &room, from, limit)?;
            b.set_cursor(&scope, next)?;
            print_json(&serde_json::json!(msgs));
        }
        Cmd::Await { as_, kind, timeout } => {
            let b = FileBackend::init(&dir)?;
            let who = ParticipantId::new(as_);
            let scope = format!("inbox:{who}");
            let want = kind.as_deref().map(parse_kind).transpose()?;
            let deadline = Instant::now() + parse_duration(&timeout)?;
            let mut cursor = b.cursor(&scope)?;
            loop {
                let (msgs, next) = b.recv_direct(&who, cursor, 1000)?;
                if let Some((idx, m)) = msgs
                    .iter()
                    .enumerate()
                    .find(|(_, m)| want.is_none_or(|k| m.kind == k))
                {
                    b.set_cursor(&scope, cursor + idx as u64 + 1)?;
                    print_json(&serde_json::to_value(m)?);
                    return Ok(());
                }
                cursor = next;
                b.set_cursor(&scope, cursor)?;
                if Instant::now() >= deadline {
                    print_json(&serde_json::json!([]));
                    std::process::exit(3);
                }
                std::thread::sleep(Duration::from_millis(150));
            }
        }
        Cmd::Subscribe { as_, room } => {
            let b = FileBackend::init(&dir)?;
            let cursor = b.subscribe(&ParticipantId::new(as_), &RoomId::new(room))?;
            print_json(&serde_json::json!({ "cursor": cursor }));
        }
        Cmd::Ack { as_, id } => {
            let b = FileBackend::init(&dir)?;
            b.ack(&ParticipantId::new(as_), &MessageId::parse(&id)?)?;
            print_json(&serde_json::json!({ "acked": id }));
        }
        Cmd::Inbox { as_ } => {
            let b = FileBackend::init(&dir)?;
            let who = ParticipantId::new(as_);
            let scope = format!("inbox:{who}");
            let cursor = b.cursor(&scope)?;
            let (all, _) = b.recv_direct(&who, 0, usize::MAX)?;
            print_json(&serde_json::json!({
                "participant": who.as_str(),
                "cursor": cursor,
                "unread": (all.len() as u64).saturating_sub(cursor),
            }));
        }
        Cmd::Rooms => {
            let b = FileBackend::init(&dir)?;
            print_json(&serde_json::json!(list_json(&dir.join("rooms"))));
            let _ = b;
        }
        Cmd::List => {
            let b = FileBackend::init(&dir)?;
            print_json(&serde_json::json!(b.list()?));
        }
        Cmd::Attach {
            participant,
            session,
        } => {
            eprintln!(
                "chan: live attach for {participant} -> {session} is not built yet (Phase 5). \
                 Durable file messaging is unaffected."
            );
        }
        Cmd::Pump { as_, follow } => {
            let b = FileBackend::init(&dir)?;
            let who = ParticipantId::new(as_);
            let scope = format!("inbox:{who}");
            let mut cursor = b.cursor(&scope)?;
            loop {
                let (msgs, next) = b.recv_direct(&who, cursor, 1000)?;
                for m in &msgs {
                    println!("{}", serde_json::to_string(m)?);
                }
                cursor = next;
                b.set_cursor(&scope, cursor)?;
                if !follow {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(150));
            }
        }
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

fn json_str(v: serde_json::Value) -> String {
    serde_json::to_string(&v).unwrap_or_else(|_| "null".to_string())
}

fn print_json(v: &serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string(v).unwrap_or_else(|_| "null".to_string())
    );
}
