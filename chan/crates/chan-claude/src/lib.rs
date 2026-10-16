#![forbid(unsafe_code)]
//! `chan-claude` — Claude Code (live) adapter for chan.
//!
//! Claude Code does not expose a documented local control API equivalent to opencode's v2
//! daemon, so live control degrades gracefully to the durable channel: this crate returns
//! a [`chan_live::NoopBackend`]. Agents still exchange messages over chan through the MCP
//! server (`chan-mcp`) or the CLI (`chan`), which is the supported integration path.

use chan_live::Backend;
use std::path::{Path, PathBuf};

/// Best-effort discovery of the Claude configuration root.
pub fn config_dir() -> Option<PathBuf> {
    std::env::var_os("CLAUDE_CONFIG_DIR")
        .map(PathBuf::from)
        .or_else(|| dirs_home().map(|h| h.join(".claude")))
        .filter(|p| p.is_dir())
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

/// Claude's settings file, if present.
pub fn settings_path() -> Option<PathBuf> {
    config_dir().map(|d| d.join("settings.json"))
}

/// Whether Claude config is present on this machine.
pub fn discovered() -> bool {
    config_dir().is_some()
}

/// Live backend for Claude. There is no live control API, so a NoopBackend is returned
/// and callers degrade to the durable log.
pub fn connect() -> Box<dyn Backend> {
    Box::new(chan_live::NoopBackend)
}

/// Hint path where a project should put `chan` MCP registration for Claude Code
/// (Claude reads `.mcp.json` at the project root).
pub fn project_mcp_path(root: impl AsRef<Path>) -> PathBuf {
    root.as_ref().join(".mcp.json")
}

/// Convenience MCP registration payload for chan-mcp for Claude Code.
pub fn mcp_config(participant: &str) -> serde_json::Value {
    serde_json::json!({
        "mcpServers": {
            "chan": {
                "command": "chan-mcp",
                "args": ["--as", participant],
                "env": {}
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mcp_config_has_chan() {
        let v = mcp_config("orch");
        let root = v.as_object().unwrap();
        assert!(root["mcpServers"]["chan"].is_object());
    }
}
