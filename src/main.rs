mod app;
mod config;
mod download;
mod progress;

use anyhow::{Context, Result};
use app::run_app;
use config::{configure_logging, load_runtime_env, AppConfig};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let config_base_dir = load_runtime_env()?
        .unwrap_or(env::current_dir().context("failed to resolve current working directory")?);
    configure_logging();

    let config = AppConfig::from_env(&config_base_dir)?;
    run_app(config).await
}
