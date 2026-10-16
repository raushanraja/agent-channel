//! `chan-mcp` — the channel as an MCP server, so agents attach with native tools.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use chan_core::{
    Address, Channel, FileBackend, Message, MessageId, MessageKind, ParticipantId, RoomId,
    resolve_participant,
};
use chan_live::{Backend, SessionRef};
use chan_opencode::{OpencodeClient, connect};
use clap::Parser;
use rmcp::ErrorData as McpError;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::transport::stdio;
use rmcp::{ServerHandler, ServiceExt, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Parser)]
#[command(name = "chan-mcp", version, about = "chan channel MCP server")]
struct Cli {
    /// Channel directory.
    #[arg(long, env = "CHAN_DIR", default_value = ".chan")]
    dir: PathBuf,
    /// This server's participant identity.
    #[arg(long = "as", env = "CHAN_PARTICIPANT")]
    as_: Option<String>,
    /// Root URL of the opencode server used by the live tools (API at /api).
    /// Omit to auto-discover a running background service.
    #[arg(long, env = "CHAN_OPENCODE")]
    opencode: Option<String>,
    /// Server password (`OPENCODE_PASSWORD`). Auto-discovered when omitted.
    #[arg(long, env = "OPENCODE_PASSWORD")]
    opencode_password: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SendArgs {
    /// Sender (defaults to this server's identity).
    #[serde(default)]
    from: Option<String>,
    /// `direct:<id>` | `room:<id>` | `broadcast`.
    to: String,
    /// One of task|progress|question|answer|result|ack|control|event.
    #[serde(default)]
    kind: Option<String>,
    /// JSON body.
    payload: serde_json::Value,
    #[serde(default)]
    reply_to: Option<String>,
    #[serde(default)]
    correlation_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct PublishArgs {
    room: String,
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ParticipantArgs {
    /// Participant (defaults to this server's identity).
    #[serde(default)]
    participant: Option<String>,
    #[serde(default)]
    cursor: Option<u64>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct AwaitArgs {
    #[serde(default)]
    participant: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SubscribeArgs {
    #[serde(default)]
    participant: Option<String>,
    room: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct RoomArgs {
    #[serde(default)]
    participant: Option<String>,
    room: String,
    #[serde(default)]
    include_self: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct AckArgs {
    #[serde(default)]
    participant: Option<String>,
    id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct HistoryArgs {
    /// Author of the messages to list.
    from: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    to: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SessionArgs {
    /// Live session id.
    session: String,
    /// Override the opencode server base URL.
    #[serde(default)]
    opencode: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct InjectArgs {
    /// Live session id.
    session: String,
    /// Text to deliver into the session.
    text: String,
    /// Override the opencode server base URL.
    #[serde(default)]
    opencode: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ActiveArgs {
    /// Override the opencode server base URL.
    #[serde(default)]
    opencode: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct StatusArgs {
    /// Live session id.
    session: String,
    /// Override the opencode server base URL.
    #[serde(default)]
    opencode: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct WaitArgs {
    /// Live session id.
    session: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
    /// Override the opencode server base URL.
    #[serde(default)]
    opencode: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct EnvArgs {
    /// Live session id.
    session: String,
    /// Variables to set.
    variables: HashMap<String, String>,
    /// Override the opencode server base URL.
    #[serde(default)]
    opencode: Option<String>,
}

#[derive(Clone)]
struct ChanServer {
    dir: PathBuf,
    identity: ParticipantId,
    session_id: Option<String>,
    opencode: Option<String>,
    opencode_password: Option<String>,
    tool_router: ToolRouter<Self>,
}

fn invalid(msg: impl Into<String>) -> McpError {
    McpError::invalid_params(msg.into(), None)
}

fn internal(e: impl std::fmt::Display) -> McpError {
    McpError::internal_error(e.to_string(), None)
}

fn parse_kind(s: &str) -> Result<MessageKind, McpError> {
    Ok(match s {
        "task" => MessageKind::Task,
        "progress" => MessageKind::Progress,
        "question" => MessageKind::Question,
        "answer" => MessageKind::Answer,
        "result" => MessageKind::Result,
        "ack" => MessageKind::Ack,
        "control" => MessageKind::Control,
        "event" => MessageKind::Event,
        other => return Err(invalid(format!("unknown kind: {other}"))),
    })
}

fn parse_addr(s: &str) -> Result<Address, McpError> {
    if s == "broadcast" {
        return Ok(Address::Broadcast);
    }
    if let Some(id) = s.strip_prefix("direct:") {
        return Ok(Address::Direct(ParticipantId::new(id)));
    }
    if let Some(id) = s.strip_prefix("room:") {
        return Ok(Address::Room(RoomId::new(id)));
    }
    Err(invalid(format!(
        "bad address `{s}` (use direct:<id> | room:<id> | broadcast)"
    )))
}

#[tool_router]
impl ChanServer {
    fn new(
        dir: PathBuf,
        identity: ParticipantId,
        session_id: Option<String>,
        opencode: Option<String>,
        opencode_password: Option<String>,
    ) -> Self {
        Self {
            dir,
            identity,
            session_id,
            opencode,
            opencode_password,
            tool_router: Self::tool_router(),
        }
    }

    fn backend(&self) -> Result<FileBackend, McpError> {
        FileBackend::init(&self.dir).map_err(internal)
    }

    fn live_client(&self, opencode: &Option<String>) -> OpencodeClient {
        let base = opencode.clone().or_else(|| self.opencode.clone());
        connect(base, self.opencode_password.clone())
    }

    fn participant(&self, p: &Option<String>) -> ParticipantId {
        p.as_ref()
            .map_or_else(|| self.identity.clone(), ParticipantId::new)
    }

    #[tool(description = "Send a message to a participant, room, or everyone")]
    fn chan_send(&self, Parameters(a): Parameters<SendArgs>) -> Result<String, McpError> {
        let b = self.backend()?;
        let from = a
            .from
            .map_or_else(|| self.identity.clone(), ParticipantId::new);
        let mut m = Message::task(from, parse_addr(&a.to)?, a.payload);
        if let Some(k) = a.kind {
            m.kind = parse_kind(&k)?;
        }
        if let Some(r) = a.reply_to {
            m.reply_to = Some(MessageId::parse(&r).map_err(|e| invalid(e.to_string()))?);
        }
        if let Some(c) = a.correlation_id {
            m.correlation_id = Some(MessageId::parse(&c).map_err(|e| invalid(e.to_string()))?);
        }
        m.session_id = self.session_id.clone();
        let id = b.send(&m).map_err(internal)?;
        Ok(serde_json::json!({ "id": id.to_string() }).to_string())
    }

    #[tool(description = "Publish a message to a room")]
    fn chan_publish(&self, Parameters(a): Parameters<PublishArgs>) -> Result<String, McpError> {
        let b = self.backend()?;
        let from = a
            .from
            .map_or_else(|| self.identity.clone(), ParticipantId::new);
        let mut m = Message::task(from, Address::Room(RoomId::new(a.room)), a.payload);
        if let Some(k) = a.kind {
            m.kind = parse_kind(&k)?;
        }
        m.session_id = self.session_id.clone();
        let id = b.send(&m).map_err(internal)?;
        Ok(serde_json::json!({ "id": id.to_string() }).to_string())
    }

    #[tool(description = "Read your direct inbox and advance your cursor")]
    fn chan_recv(&self, Parameters(a): Parameters<ParticipantArgs>) -> Result<String, McpError> {
        let b = self.backend()?;
        let who = self.participant(&a.participant);
        let scope = format!("inbox:{who}");
        let from = match a.cursor {
            Some(c) => c,
            None => b.cursor(&scope).map_err(internal)?,
        };
        let limit = a.limit.unwrap_or(100);
        let (msgs, next) = b.recv_direct(&who, from, limit).map_err(internal)?;
        b.set_cursor(&scope, next).map_err(internal)?;
        serde_json::to_string(&msgs).map_err(internal)
    }

    #[tool(
        description = "Read a room and advance your subscription cursor (include_self to read your own publishes)"
    )]
    fn chan_room(&self, Parameters(a): Parameters<RoomArgs>) -> Result<String, McpError> {
        let b = self.backend()?;
        let who = self.participant(&a.participant);
        let room = RoomId::new(a.room);
        let read_who = if a.include_self == Some(true) {
            ParticipantId::new("_")
        } else {
            who.clone()
        };
        let scope = format!("sub:{who}:{room}");
        let from = b.cursor(&scope).map_err(internal)?;
        let (msgs, next) = b
            .recv_room(&read_who, &room, from, 1000)
            .map_err(internal)?;
        b.set_cursor(&scope, next).map_err(internal)?;
        serde_json::to_string(&msgs).map_err(internal)
    }

    #[tool(description = "Block until a matching message arrives (or time out)")]
    async fn chan_await(&self, Parameters(a): Parameters<AwaitArgs>) -> Result<String, McpError> {
        let b = self.backend()?;
        let who = self.participant(&a.participant);
        let scope = format!("inbox:{who}");
        let want = a.kind.as_deref().map(parse_kind).transpose()?;
        let deadline = Instant::now() + Duration::from_millis(a.timeout_ms.unwrap_or(30_000));
        let mut cursor = b.cursor(&scope).map_err(internal)?;
        loop {
            let (msgs, next) = b.recv_direct(&who, cursor, 1000).map_err(internal)?;
            if let Some((idx, m)) = msgs
                .iter()
                .enumerate()
                .find(|(_, m)| want.is_none_or(|k| m.kind == k))
            {
                b.set_cursor(&scope, cursor + idx as u64 + 1)
                    .map_err(internal)?;
                return serde_json::to_string(m).map_err(internal);
            }
            cursor = next;
            b.set_cursor(&scope, cursor).map_err(internal)?;
            if Instant::now() >= deadline {
                return Ok(serde_json::json!({ "timeout": true }).to_string());
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }

    #[tool(description = "Subscribe to a room (creates your cursor)")]
    fn chan_subscribe(&self, Parameters(a): Parameters<SubscribeArgs>) -> Result<String, McpError> {
        let b = self.backend()?;
        let who = self.participant(&a.participant);
        let cursor = b.subscribe(&who, &RoomId::new(a.room)).map_err(internal)?;
        Ok(serde_json::json!({ "cursor": cursor }).to_string())
    }

    #[tool(description = "Acknowledge a message id")]
    fn chan_ack(&self, Parameters(a): Parameters<AckArgs>) -> Result<String, McpError> {
        let b = self.backend()?;
        let who = self.participant(&a.participant);
        b.ack(
            &who,
            &MessageId::parse(&a.id).map_err(|e| invalid(e.to_string()))?,
        )
        .map_err(internal)?;
        Ok(serde_json::json!({ "acked": a.id }).to_string())
    }

    #[tool(description = "List all messages authored by a participant (any destination)")]
    fn chan_history(&self, Parameters(a): Parameters<HistoryArgs>) -> Result<String, McpError> {
        let b = self.backend()?;
        let mut msgs = b
            .messages_from(&ParticipantId::new(&a.from))
            .map_err(internal)?;
        if let Some(k) = a.kind {
            let want = parse_kind(&k)?;
            msgs.retain(|m| m.kind == want);
        }
        if let Some(to) = a.to {
            let want_addr = parse_addr(&to)?;
            msgs.retain(|m| m.to == want_addr);
        }
        if let Some(limit) = a.limit {
            if msgs.len() > limit {
                msgs = msgs.split_off(msgs.len() - limit);
            }
        }
        serde_json::to_string(&msgs).map_err(internal)
    }

    #[tool(description = "Show an inbox's cursor and unread count")]
    fn chan_inbox(&self, Parameters(a): Parameters<ParticipantArgs>) -> Result<String, McpError> {
        let b = self.backend()?;
        let who = self.participant(&a.participant);
        let scope = format!("inbox:{who}");
        let cursor = b.cursor(&scope).map_err(internal)?;
        let (all, _) = b.recv_direct(&who, 0, usize::MAX).map_err(internal)?;
        Ok(serde_json::json!({
            "participant": who.as_str(),
            "cursor": cursor,
            "unread": (all.len() as u64).saturating_sub(cursor),
        })
        .to_string())
    }

    #[tool(description = "Inject text into a running session (send more data)")]
    async fn chan_inject(&self, Parameters(a): Parameters<InjectArgs>) -> Result<String, McpError> {
        let c = self.live_client(&a.opencode);
        c.inject(&SessionRef::new(a.session), &a.text)
            .await
            .map_err(internal)?;
        Ok(serde_json::json!({ "injected": true }).to_string())
    }

    #[tool(description = "Interrupt (abort) a running session's active execution")]
    async fn chan_interrupt(
        &self,
        Parameters(a): Parameters<SessionArgs>,
    ) -> Result<String, McpError> {
        let c = self.live_client(&a.opencode);
        let interrupted = c
            .interrupt(&SessionRef::new(a.session))
            .await
            .map_err(internal)?;
        Ok(serde_json::json!({ "interrupted": interrupted }).to_string())
    }

    #[tool(description = "List live sessions")]
    async fn chan_active(&self, Parameters(a): Parameters<ActiveArgs>) -> Result<String, McpError> {
        let c = self.live_client(&a.opencode);
        let sessions: Vec<String> = c
            .active()
            .await
            .map_err(internal)?
            .into_iter()
            .map(|s| s.as_str().to_string())
            .collect();
        serde_json::to_string(&sessions).map_err(internal)
    }

    #[tool(description = "Snapshot a live session status")]
    async fn chan_status(&self, Parameters(a): Parameters<StatusArgs>) -> Result<String, McpError> {
        let c = self.live_client(&a.opencode);
        let s = c.status(&a.session).await.map_err(internal)?;
        serde_json::to_string(&s).map_err(internal)
    }

    #[tool(description = "Block until a session becomes idle or timeout")]
    async fn chan_wait(&self, Parameters(a): Parameters<WaitArgs>) -> Result<String, McpError> {
        let c = self.live_client(&a.opencode);
        let timeout = Duration::from_millis(a.timeout_ms.unwrap_or(30_000));
        let s = c.wait_idle(&a.session, timeout).await.map_err(internal)?;
        if matches!(s, chan_live::SessionStatus::Running) {
            return Ok(serde_json::json!({"timeout": true}).to_string());
        }
        serde_json::to_string(&s).map_err(internal)
    }

    #[tool(description = "Set environment variables for a live session")]
    async fn chan_env(&self, Parameters(a): Parameters<EnvArgs>) -> Result<String, McpError> {
        let c = self.live_client(&a.opencode);
        c.set_environment(&a.session, &a.variables)
            .await
            .map_err(internal)?;
        Ok(serde_json::json!({ "updated": true }).to_string())
    }
}

#[tool_handler(router = self.tool_router.clone())]
impl ServerHandler for ChanServer {
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::default()
            .with_server_info(Implementation::new("chan", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "Durable channel for orchestrating agents: chan_send / chan_publish / \
                 chan_recv / chan_await / chan_room / chan_subscribe / chan_ack / chan_inbox. \
                 Live control of running sessions: chan_inject / chan_interrupt / chan_active.",
            );
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let identity = resolve_participant(cli.as_.as_deref(), "orchestrator");
    let session_id = std::env::var("OPENCODE_SESSION_ID").ok();
    let server = ChanServer::new(
        cli.dir,
        identity,
        session_id,
        cli.opencode,
        cli.opencode_password,
    );
    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}
