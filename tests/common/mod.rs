#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use telegram_downloader_rust::config::AppConfig;

pub fn unique_test_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic enough for tests")
        .as_nanos();
    std::env::temp_dir().join(format!("telegram_downloader_{prefix}_{nanos}"))
}

pub fn test_config(base: &Path) -> AppConfig {
    AppConfig {
        api_id: 1,
        api_hash: "hash".to_string(),
        bot_token: "token".to_string(),
        session_path: base.join("session"),
        download_dir: base.join("downloads"),
        movie_dir: base.join("movies"),
        tv_show_dir: base.join("shows"),
        anime_dir: base.join("anime"),
        max_concurrent_downloads: 1,
        parallel_chunk_downloads: 1,
        chunk_size: 512 * 1024,
        aria2c_path: "aria2c".to_string(),
        aria2c_poll_interval_ms: 1000,
        megadl_path: "megadl".to_string(),
        reply_on_duplicate: true,
    }
}

pub fn copy_dir(source: &Path, destination: &Path) -> Result<(), std::io::Error> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let entry_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry_path, &destination_path)?;
        } else {
            fs::copy(&entry_path, &destination_path)?;
        }
    }
    Ok(())
}

pub fn remove_source_files(path: &Path) -> Result<(), std::io::Error> {
    if !path.is_dir() {
        return Ok(());
    }

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        if entry.file_type()?.is_dir() {
            remove_source_files(&entry_path)?;
        } else {
            fs::remove_file(&entry_path)?;
        }
    }

    Ok(())
}
