//! `chan-serve` — the channel daemon.

use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "chan-serve", version, about = "chan HTTP+SSE daemon")]
struct Cli {
    /// Channel directory.
    #[arg(long, env = "CHAN_DIR", default_value = ".chan")]
    dir: PathBuf,
    /// Address to bind.
    #[arg(long, default_value = "127.0.0.1:27400")]
    bind: SocketAddr,
    /// Require this bearer token (also `CHAN_TOKEN`).
    #[arg(long, env = "CHAN_TOKEN")]
    token: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let backend = chan_core::FileBackend::init(&cli.dir)?;
    chan_server::serve(backend, cli.bind, cli.token).await
}
