#![forbid(unsafe_code)]
//! `chan-live` — the product-agnostic port for controlling a *running* agent session.
//!
//! This crate is the stable seam between the durable channel ([`chan_core`]) and whatever
//! engine actually runs a child (opencode today, another engine tomorrow). Call sites
//! depend on [`Backend`]; concrete engines are *adapters* (e.g. `chan-opencode`). The
//! engine's HTTP/RPC shape may change without touching a single call site.

pub mod noop;
pub mod port;

pub use noop::NoopBackend;
pub use port::{Backend, BoxFuture, LiveError, SessionRef};
