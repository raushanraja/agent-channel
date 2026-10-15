#![forbid(unsafe_code)]
//! `chan-opencode` — live attach to a running `opencode serve` instance.
//!
//! Durable-first, live-second: messages are persisted to the `chan-core` file log
//! before any network call, so a rate-limited or dead child never loses a message.

pub mod attach;
pub mod client;

pub use attach::AttachBackend;
pub use client::{AttachError, OpencodeClient};
