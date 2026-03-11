use anyhow::{Context, Result};
use dotenvy::from_path;
use grammers_client::client::files::{MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// Runtime configuration loaded from environment variables.
#[derive(Clone, Debug)]
pub struct AppConfig {
    pub api_id: i32,
    pub api_hash: String,
    pub bot_token: String,
    pub session_path: PathBuf,
    pub download_dir: PathBuf,
    pub movie_dir: PathBuf,
    pub tv_show_dir: PathBuf,
    pub anime_dir: PathBuf,
    pub max_concurrent_downloads: usize,
    pub parallel_chunk_downloads: usize,
    pub chunk_size: usize,
    pub aria2c_path: String,
    pub aria2c_poll_interval_ms: u64,
    pub reply_on_duplicate: bool,
}

impl AppConfig {
    /// Build configuration from environment variables and create the download directory.
    pub fn from_env(base_dir: &Path) -> Result<Self> {
        let api_id = env::var("TELEGRAM_API_ID")
            .context("TELEGRAM_API_ID is required")?
            .parse::<i32>()
            .context("TELEGRAM_API_ID must be an integer")?;
        let api_hash = env::var("TELEGRAM_API_HASH").context("TELEGRAM_API_HASH is required")?;
        let bot_token = env::var("TELEGRAM_BOT_TOKEN").context("TELEGRAM_BOT_TOKEN is required")?;
        let session_path = resolve_config_path(
            base_dir,
            &env::var("TELEGRAM_SESSION_NAME")
                .unwrap_or_else(|_| "telegram_downloader.session".to_string()),
        );
        let download_dir = resolve_config_path(
            base_dir,
            &env::var("DOWNLOAD_DIR").unwrap_or_else(|_| "./downloads-mtproto".to_string()),
        );
        let movie_dir = resolve_config_path(
            base_dir,
            &env::var("MOVIE_DIR").context("MOVIE_DIR is required")?,
        );
        let tv_show_dir = resolve_config_path(
            base_dir,
            &env::var("TV_SHOW_DIR").context("TV_SHOW_DIR is required")?,
        );
        let anime_dir = resolve_config_path(
            base_dir,
            &env::var("ANIME_DIR").context("ANIME_DIR is required")?,
        );
        fs::create_dir_all(&download_dir)
            .with_context(|| format!("failed to create download dir {}", download_dir.display()))?;
        fs::create_dir_all(&movie_dir)
            .with_context(|| format!("failed to create movie dir {}", movie_dir.display()))?;
        fs::create_dir_all(&tv_show_dir)
            .with_context(|| format!("failed to create TV show dir {}", tv_show_dir.display()))?;
        fs::create_dir_all(&anime_dir)
            .with_context(|| format!("failed to create anime dir {}", anime_dir.display()))?;

        Ok(Self {
            api_id,
            api_hash,
            bot_token,
            session_path,
            download_dir,
            movie_dir,
            tv_show_dir,
            anime_dir,
            max_concurrent_downloads: env_usize("MAX_CONCURRENT_DOWNLOADS", 2).max(1),
            parallel_chunk_downloads: env_usize("PARALLEL_CHUNK_DOWNLOADS", 4).max(1),
            chunk_size: chunk_size_from_env(),
            aria2c_path: env::var("ARIA2C_PATH").unwrap_or_else(|_| "aria2c".to_string()),
            aria2c_poll_interval_ms: env_u64("ARIA2C_POLL_INTERVAL_MS", 1000).max(100),
            reply_on_duplicate: env_bool("REPLY_ON_DUPLICATE", true),
        })
    }
}

/// Load `.env` from the current directory first, then from the executable directory.
pub fn load_runtime_env() -> Result<Option<PathBuf>> {
    let current_dir = env::current_dir().context("failed to resolve current working directory")?;
    let current_env_path = current_dir.join(".env");
    if current_env_path.exists() {
        from_path(&current_env_path).with_context(|| {
            format!(
                "failed to load environment from {}",
                current_env_path.display()
            )
        })?;
        return Ok(Some(current_dir));
    }

    let exe_dir = env::current_exe()
        .context("failed to resolve executable path")?
        .parent()
        .map(Path::to_path_buf);
    if let Some(exe_dir) = exe_dir {
        let exe_env_path = exe_dir.join(".env");
        if exe_env_path.exists() {
            from_path(&exe_env_path).with_context(|| {
                format!("failed to load environment from {}", exe_env_path.display())
            })?;
            return Ok(Some(exe_dir));
        }
    }

    Ok(None)
}

/// Configure `env_logger` so log level can be controlled with `LOG_LEVEL`.
pub fn configure_logging() {
    let mut builder = env_logger::Builder::from_env(
        env_logger::Env::default()
            .default_filter_or(env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string())),
    );
    builder.format_timestamp_secs();
    builder.init();
}

fn resolve_config_path(base_dir: &Path, raw_path: &str) -> PathBuf {
    let path = PathBuf::from(raw_path);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}

fn chunk_size_from_env() -> usize {
    let raw_mb = env::var("DOWNLOAD_CHUNK_SIZE_MB")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.5);
    let requested_bytes = ((raw_mb * 1024.0 * 1024.0) as usize).max(MIN_CHUNK_SIZE as usize);
    let clamped = requested_bytes.min(MAX_CHUNK_SIZE as usize);
    (clamped / MIN_CHUNK_SIZE as usize).max(1) * MIN_CHUNK_SIZE as usize
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::AppConfig;
    use std::env;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

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
            "LOG_LEVEL",
            "REPLY_ON_DUPLICATE",
        ] {
            env::remove_var(key);
        }
    }

    fn unique_test_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic enough for tests")
            .as_nanos();
        env::temp_dir().join(format!("telegram_downloader_{prefix}_{nanos}"))
    }
}
