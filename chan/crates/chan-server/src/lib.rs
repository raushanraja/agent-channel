#![forbid(unsafe_code)]
//! `chan-server` — an HTTP + SSE daemon that exposes a `chan-core` file backend over
//! the network, so participants on different hosts can exchange messages. The file log
//! remains the source of truth.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chan_core::{Address, Channel, FileBackend, Message, MessageId, ParticipantId, RoomId};
use serde::Deserialize;
use serde_json::{Value, json};

/// Shared daemon state.
pub struct ServerState {
    /// The durable backend this daemon owns.
    pub backend: FileBackend,
    /// Required bearer token, or `None` for open access.
    pub token: Option<String>,
}

/// Bind `addr` and serve until shutdown.
pub async fn serve(
    backend: FileBackend,
    addr: SocketAddr,
    token: Option<String>,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, auth = token.is_some(), "chan-serve listening");
    axum::serve(listener, router(Arc::new(ServerState { backend, token }))).await?;
    Ok(())
}

/// Build the axum router (exposed for tests).
pub fn router(state: Arc<ServerState>) -> Router {
    let protected = Router::new()
        .route("/v1/send", post(send))
        .route("/v1/inbox/{participant}", get(inbox))
        .route("/v1/room/{room}", get(room))
        .route("/v1/subscribe", post(subscribe))
        .route("/v1/ack", post(ack))
        .route("/v1/rooms", get(rooms))
        .route("/v1/inbox/{participant}/stream", get(stream))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth));
    Router::new()
        .route("/v1/health", get(health))
        .merge(protected)
        .with_state(state)
}

/// Whether a request carries the correct bearer token.
fn authorized(state: &ServerState, headers: &HeaderMap, query: Option<&str>) -> bool {
    let Some(expected) = state.token.as_deref() else {
        return true;
    };
    let header_ok = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .is_some_and(|t| t == expected);
    let query_ok = query.is_some_and(|q| {
        q.split('&')
            .any(|kv| kv.strip_prefix("token=") == Some(expected))
    });
    header_ok || query_ok
}

async fn auth(
    State(state): State<Arc<ServerState>>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let query = req.uri().query().map(str::to_owned);
    if authorized(&state, req.headers(), query.as_deref()) {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// An error that maps to an HTTP response.
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<chan_core::ChanError> for ApiError {
    fn from(e: chan_core::ChanError) -> Self {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}

fn bad(msg: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, msg.into())
}

async fn health(State(state): State<Arc<ServerState>>) -> Result<Json<Value>, ApiError> {
    let stats = state.backend.list()?;
    Ok(Json(json!({
        "status": "ok",
        "participants": stats.participants,
        "rooms": stats.rooms,
    })))
}

#[derive(Deserialize)]
struct SendBody {
    message: Message,
}

async fn send(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<SendBody>,
) -> Result<Json<Value>, ApiError> {
    let id = state.backend.send(&body.message)?;
    Ok(Json(json!({ "id": id.to_string() })))
}

#[derive(Deserialize)]
struct PageQuery {
    cursor: Option<u64>,
    limit: Option<usize>,
    participant: Option<String>,
}

async fn inbox(
    State(state): State<Arc<ServerState>>,
    Path(participant): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, ApiError> {
    let who = ParticipantId::new(participant);
    let (msgs, next) = state
        .backend
        .recv(&who, q.cursor.unwrap_or(0), q.limit.unwrap_or(100))?;
    Ok(Json(json!({ "messages": msgs, "cursor": next })))
}

async fn room(
    State(state): State<Arc<ServerState>>,
    Path(room): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, ApiError> {
    let who = q
        .participant
        .map_or_else(|| ParticipantId::new("_"), ParticipantId::new);
    let (msgs, next) = state.backend.recv_room(
        &who,
        &RoomId::new(room),
        q.cursor.unwrap_or(0),
        q.limit.unwrap_or(1000),
    )?;
    Ok(Json(json!({ "messages": msgs, "cursor": next })))
}

#[derive(Deserialize)]
struct SubscribeBody {
    participant: String,
    room: String,
}

async fn subscribe(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<SubscribeBody>,
) -> Result<Json<Value>, ApiError> {
    let cursor = state.backend.subscribe(
        &ParticipantId::new(body.participant),
        &RoomId::new(body.room),
    )?;
    Ok(Json(json!({ "cursor": cursor })))
}

#[derive(Deserialize)]
struct AckBody {
    participant: String,
    id: String,
}

async fn ack(
    State(state): State<Arc<ServerState>>,
    Json(body): Json<AckBody>,
) -> Result<Json<Value>, ApiError> {
    let id = MessageId::parse(&body.id).map_err(|e| bad(e.to_string()))?;
    state
        .backend
        .ack(&ParticipantId::new(body.participant), &id)?;
    Ok(Json(json!({ "acked": body.id })))
}

async fn rooms(State(state): State<Arc<ServerState>>) -> Result<Json<Value>, ApiError> {
    let dir = state.backend.root().join("rooms");
    let mut names: Vec<String> = match std::fs::read_dir(&dir) {
        Ok(rd) => rd
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
            .collect(),
        Err(_) => Vec::new(),
    };
    names.sort();
    Ok(Json(json!({ "rooms": names })))
}

async fn stream(
    State(state): State<Arc<ServerState>>,
    Path(participant): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let who = ParticipantId::new(participant);
    let backend = Arc::new(FileBackend::init(state.backend.root())?);
    let start = q.cursor.unwrap_or(0);
    let stream = futures::stream::unfold(
        (backend, who, start, VecDeque::new()),
        |(b, who, mut cursor, mut buf)| async move {
            loop {
                if let Some(m) = buf.pop_front() {
                    let event = Event::default()
                        .event("message")
                        .data(serde_json::to_string(&m).unwrap_or_default());
                    return Some((Ok::<_, Infallible>(event), (b, who, cursor, buf)));
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
                if let Ok((msgs, next)) = b.recv_direct(&who, cursor, 100) {
                    cursor = next;
                    buf.extend(msgs);
                }
            }
        },
    );
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// Convenience for callers that only need a message destination.
pub fn parse_address(s: &str) -> Option<Address> {
    if s == "broadcast" {
        return Some(Address::Broadcast);
    }
    if let Some(id) = s.strip_prefix("direct:") {
        return Some(Address::Direct(ParticipantId::new(id)));
    }
    s.strip_prefix("room:")
        .map(|id| Address::Room(RoomId::new(id)))
}
