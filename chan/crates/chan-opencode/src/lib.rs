#![forbid(unsafe_code)]
//! `chan-opencode` — a **live-session adapter** for opencode (v2 HTTP API).
//!
//! This crate is deliberately thin: it implements [`chan_live::Backend`] for opencode and
//! supplies a durable-first [`AttachBackend`]. Call sites depend on the `chan-live` *port*,
//! never on this crate directly, so the engine is replaceable.

pub mod adapter;
pub mod attach;
pub mod client;

pub use attach::AttachBackend;
pub use client::{EventStream, OpencodeClient};
