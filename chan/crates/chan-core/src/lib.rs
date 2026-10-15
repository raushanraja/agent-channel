#![forbid(unsafe_code)]
//! `chan-core` — the channel protocol and durable file backend.
//!
//! Messages travel over a durable, append-only file log (the source of truth); a live
//! transport (see `chan-opencode`) is layered on top and may be absent without loss.

pub mod address;
pub mod error;
pub mod message;

pub use address::{Address, ParticipantId, RoomId};
pub use error::{ChanError, Result};
pub use message::{Message, MessageId, MessageKind};
