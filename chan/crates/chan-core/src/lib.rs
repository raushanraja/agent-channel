#![forbid(unsafe_code)]
//! `chan-core` — the channel protocol and durable file backend.
//!
//! Messages travel over a durable, append-only file log (the source of truth); a live
//! transport (see `chan-opencode`) is layered on top and may be absent without loss.

pub mod address;
pub mod backend;
pub mod error;
pub mod file_backend;
pub mod message;

pub use address::{Address, ParticipantId, RoomId};
pub use backend::{Channel, ChannelStats, Cursor};
pub use error::{ChanError, Result};
pub use file_backend::FileBackend;
pub use message::{Message, MessageId, MessageKind};
