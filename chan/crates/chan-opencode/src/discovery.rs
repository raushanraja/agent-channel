//! Zero-config discovery of a locally-registered `opencode serve` service.
//!
//! A background server (`opencode serve --service`) registers itself in
//! `$XDG_STATE_HOME/opencode/service.json` (default `~/.local/state/opencode/service.json`)
//! as `{ "url", "password", "pid", … }`. The server *always* requires its password, so when
//! no server is configured explicitly, `chan` reads that file — callers never have to pass
//! the password by hand. To pin a known password for a private server instead, start it with
//! `OPENCODE_SERVER_PASSWORD=<known>`.

use crate::client::OpencodeClient;
use std::path::{Path, PathBuf};

/// Path of the runtime service registration, if it can be determined.
#[must_use]
pub fn service_state_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_STATE_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")))?;
    Some(base.join("opencode").join("service.json"))
}

/// Read a registration file at `path`, returning `(connect_url, password)`.
///
/// Wildcard binds (`0.0.0.0`, `::`) are rewritten to loopback so the URL is dialable.
#[must_use]
pub fn discover_from(path: &Path) -> Option<(String, String)> {
    let text = std::fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    let url = v.get("url")?.as_str()?;
    let password = v.get("password")?.as_str()?;
    Some((normalize_url(url), password.to_owned()))
}

/// Discover the local background service, if one is registered.
#[must_use]
pub fn discover_local() -> Option<(String, String)> {
    discover_from(&service_state_path()?)
}

/// Build a client from optional explicit configuration, falling back to the local service.
///
/// Precedence: an explicit `base` wins (with `password` if given); otherwise the discovered
/// local service (using its password unless one is given); otherwise `http://127.0.0.1:4096`.
#[must_use]
pub fn connect(base: Option<String>, password: Option<String>) -> OpencodeClient {
    match base {
        Some(base) => OpencodeClient::with_password(base, password),
        None => match discover_local() {
            Some((url, found)) => OpencodeClient::with_password(url, password.or(Some(found))),
            None => OpencodeClient::with_password("http://127.0.0.1:4096", password),
        },
    }
}

/// Rewrite wildcard bind addresses to loopback so the URL can be dialed.
fn normalize_url(url: &str) -> String {
    url.replace("://0.0.0.0", "://127.0.0.1")
        .replace("://[::]", "://127.0.0.1")
        .replace("://::", "://127.0.0.1")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_file(name: &str, contents: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("chan-discover-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn reads_url_and_password() {
        let p = tmp_file(
            "service.json",
            r#"{"url":"http://0.0.0.0:49374","password":"secret","pid":1}"#,
        );
        assert_eq!(
            discover_from(&p),
            Some(("http://127.0.0.1:49374".to_string(), "secret".to_string()))
        );
    }

    #[test]
    fn missing_or_malformed_is_none() {
        assert_eq!(discover_from(Path::new("/no/such/file.json")), None);
        let p = tmp_file("bad.json", "not json");
        assert_eq!(discover_from(&p), None);
    }

    #[test]
    fn normalizes_wildcard_hosts() {
        assert_eq!(normalize_url("http://0.0.0.0:1"), "http://127.0.0.1:1");
        assert_eq!(normalize_url("http://[::]:2"), "http://127.0.0.1:2");
        assert_eq!(normalize_url("http://127.0.0.1:3"), "http://127.0.0.1:3");
    }

    #[test]
    fn explicit_base_makes_client_without_touching_disk() {
        let c = connect(Some("http://example:9".to_string()), Some("pw".to_string()));
        assert_eq!(c.api_base(), "http://example:9/api");
    }
}
