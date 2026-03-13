mod common;

use common::unique_test_dir;
use std::env;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use telegram_downloader_rust::config::AppConfig;

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[test]
fn from_env_loads_and_creates_destination_directories() {
    let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
    let base = unique_test_dir("config-load");

    set_required_env(&base);
    env::set_var("DOWNLOAD_DIR", "downloads");
    env::set_var("MOVIE_DIR", "movies");
    env::set_var("TV_SHOW_DIR", "shows");
    env::set_var("ANIME_DIR", "anime");

    let config = AppConfig::from_env(&base).expect("config should load");
    assert_eq!(config.download_dir, base.join("downloads"));
    assert_eq!(config.movie_dir, base.join("movies"));
    assert_eq!(config.tv_show_dir, base.join("shows"));
    assert_eq!(config.anime_dir, base.join("anime"));
    assert!(config.download_dir.is_dir());
    assert!(config.movie_dir.is_dir());
    assert!(config.tv_show_dir.is_dir());
    assert!(config.anime_dir.is_dir());

    clear_test_env();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn from_env_requires_media_destination_paths() {
    let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
    let base = unique_test_dir("config-missing");

    set_required_env(&base);
    env::remove_var("MOVIE_DIR");
    env::remove_var("TV_SHOW_DIR");
    env::remove_var("ANIME_DIR");

    let error = AppConfig::from_env(&base).expect_err("missing dirs should fail");
    assert!(error.to_string().contains("MOVIE_DIR"));

    clear_test_env();
    let _ = std::fs::remove_dir_all(&base);
}

fn set_required_env(base: &Path) {
    env::set_var("TELEGRAM_API_ID", "12345");
    env::set_var("TELEGRAM_API_HASH", "hash");
    env::set_var("TELEGRAM_BOT_TOKEN", "token");
    env::set_var(
        "TELEGRAM_SESSION_NAME",
        base.join("session").to_string_lossy().to_string(),
    );
    env::remove_var("MAX_CONCURRENT_DOWNLOADS");
    env::remove_var("PARALLEL_CHUNK_DOWNLOADS");
    env::remove_var("DOWNLOAD_CHUNK_SIZE_MB");
    env::remove_var("ARIA2C_PATH");
    env::remove_var("ARIA2C_POLL_INTERVAL_MS");
    env::remove_var("MEGADL_PATH");
    env::remove_var("LOG_LEVEL");
    env::remove_var("REPLY_ON_DUPLICATE");
}

fn clear_test_env() {
    for key in [
        "TELEGRAM_API_ID",
        "TELEGRAM_API_HASH",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_SESSION_NAME",
        "DOWNLOAD_DIR",
        "MOVIE_DIR",
        "TV_SHOW_DIR",
        "ANIME_DIR",
        "MAX_CONCURRENT_DOWNLOADS",
        "PARALLEL_CHUNK_DOWNLOADS",
        "DOWNLOAD_CHUNK_SIZE_MB",
        "ARIA2C_PATH",
        "ARIA2C_POLL_INTERVAL_MS",
        "MEGADL_PATH",
        "LOG_LEVEL",
        "REPLY_ON_DUPLICATE",
    ] {
        env::remove_var(key);
    }
}
