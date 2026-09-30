use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail};
use orchestrator::api::{self, AppState};
use orchestrator::config::Config;
use orchestrator::models::Ollama;
use orchestrator::store::Store;
use tracing_subscriber::EnvFilter;

/// The API token comes from `ORCH_TOKEN`, or from the file named by
/// `ORCH_TOKEN_FILE` (e.g. a Docker secret).
fn load_token() -> anyhow::Result<Option<String>> {
    let raw = if let Ok(t) = std::env::var("ORCH_TOKEN") {
        t
    } else if let Ok(path) = std::env::var("ORCH_TOKEN_FILE") {
        std::fs::read_to_string(&path).with_context(|| format!("reading {path}"))?
    } else {
        return Ok(None);
    };
    Ok(Some(raw.trim().to_string()).filter(|t| !t.is_empty()))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cfg_path = std::env::var_os("ORCH_CONFIG")
        .filter(|p| !p.is_empty())
        .map(PathBuf::from);
    let cfg = Config::load(cfg_path.as_deref())?;
    let addr: SocketAddr = cfg
        .listen
        .parse()
        .with_context(|| format!("invalid listen address {:?}", cfg.listen))?;
    let token = load_token()?;

    // Fail closed: without a token, only loopback binds are allowed.
    if token.is_none() && !addr.ip().is_loopback() {
        bail!("refusing to listen on {addr} without ORCH_TOKEN; set a token or bind to 127.0.0.1");
    }

    let store = Store::open(&cfg.database).with_context(|| format!("opening {}", cfg.database))?;
    let ollama = Some(Ollama::new(&cfg.models.ollama_url));
    let tick_every = Duration::from_secs(cfg.tick_secs.max(1));
    let state = AppState::new(cfg, store, ollama, token);

    let ticker = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick_every);
        loop {
            interval.tick().await;
            api::tick(&ticker).await;
        }
    });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "orchestrator listening");
    axum::serve(listener, api::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}
