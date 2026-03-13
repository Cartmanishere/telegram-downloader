use crate::config::AppConfig;
use crate::progress::ProgressTracker;
use anyhow::{anyhow, bail, Context, Result};
use futures_util::future::join_all;
use grammers_client::client::files::DownloadIter;
use grammers_client::types::{Media, Message};
use grammers_client::Client as TelegramClient;
use regex::Regex;
use reqwest::{Client as HttpClient, Url};
use serde::Deserialize;
use serde_json::json;
use std::collections::VecDeque;
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::{self as tokio_fs, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::process::Command as TokioCommand;
use tokio::sync::Notify;

/// Identifies a download already in progress so duplicate updates can be ignored.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ActiveDownloadKey {
    Telegram { path: PathBuf, size: Option<u64> },
    External { identity: String },
}

/// Information extracted from a Telegram message before starting a download.
#[derive(Clone, Debug)]
pub struct DownloadCandidate {
    pub filename: String,
    pub file_size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalSource {
    DirectUrl { url: String },
    Magnet { uri: String, info_hash: String },
    Mega { url: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LibraryKind {
    Movie,
    TvShow,
    Anime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassificationTarget {
    pub source_path: PathBuf,
    pub cleanup_dir: Option<PathBuf>,
}

#[derive(Clone)]
pub enum DownloadRequest {
    TelegramMedia {
        media: Media,
        candidate: DownloadCandidate,
        target_path: PathBuf,
    },
    ExternalLink {
        source: ExternalSource,
        label: String,
        target_path: PathBuf,
        expected_size: Option<u64>,
    },
}

#[derive(Clone, Default)]
pub struct DownloadCancellation {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl DownloadCancellation {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        self.notify.notified().await;
    }
}

#[derive(Debug)]
pub struct DownloadCancelled;

impl fmt::Display for DownloadCancelled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "download cancelled")
    }
}

impl std::error::Error for DownloadCancelled {}

pub fn is_download_cancelled(err: &anyhow::Error) -> bool {
    err.downcast_ref::<DownloadCancelled>().is_some()
}

#[derive(Clone)]
struct SegmentPlan {
    index: usize,
    start: u64,
    end: u64,
    part_path: PathBuf,
}

impl SegmentPlan {
    fn size(&self) -> u64 {
        self.end - self.start
    }
}

impl DownloadRequest {
    pub fn active_key(&self) -> ActiveDownloadKey {
        match self {
            Self::TelegramMedia {
                target_path,
                candidate,
                ..
            } => ActiveDownloadKey::Telegram {
                path: target_path.clone(),
                size: candidate.file_size,
            },
            Self::ExternalLink { source, .. } => ActiveDownloadKey::External {
                identity: external_identity(source),
            },
        }
    }

    pub fn display_name(&self) -> String {
        match self {
            Self::TelegramMedia { candidate, .. } => candidate.filename.clone(),
            Self::ExternalLink { label, .. } => label.clone(),
        }
    }

    pub fn target_path(&self) -> &Path {
        match self {
            Self::TelegramMedia { target_path, .. } | Self::ExternalLink { target_path, .. } => {
                target_path
            }
        }
    }

    pub fn expected_size(&self) -> Option<u64> {
        match self {
            Self::TelegramMedia { candidate, .. } => candidate.file_size,
            Self::ExternalLink { expected_size, .. } => *expected_size,
        }
    }

    pub fn already_exists(&self) -> Result<bool> {
        match self {
            Self::TelegramMedia {
                target_path,
                candidate,
                ..
            } => target_matches(target_path, candidate.file_size),
            Self::ExternalLink {
                source,
                target_path,
                ..
            } => match source {
                ExternalSource::DirectUrl { .. } => target_matches(target_path, None),
                ExternalSource::Magnet { .. } => contains_payload_file(target_path),
                ExternalSource::Mega { .. } => Ok(false),
            },
        }
    }

    pub fn resolve_classification_target(&self) -> Result<ClassificationTarget> {
        match self {
            Self::TelegramMedia { target_path, .. }
            | Self::ExternalLink {
                source: ExternalSource::DirectUrl { .. },
                target_path,
                ..
            } => Ok(ClassificationTarget {
                source_path: target_path.clone(),
                cleanup_dir: None,
            }),
            Self::ExternalLink {
                source: ExternalSource::Magnet { .. },
                target_path,
                ..
            } => resolve_magnet_payload(target_path),
            Self::ExternalLink {
                source: ExternalSource::Mega { .. },
                target_path,
                ..
            } => resolve_mega_payload(target_path),
        }
    }
}

#[derive(Deserialize)]
struct AriaRpcResponse<T> {
    result: T,
}

#[derive(Deserialize)]
struct AriaStatus {
    status: String,
    #[serde(rename = "completedLength")]
    completed_length: String,
    #[serde(rename = "totalLength")]
    total_length: String,
}

struct AriaSnapshot {
    active: Vec<AriaStatus>,
    waiting: Vec<AriaStatus>,
    stopped: Vec<AriaStatus>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MegaProgressSnapshot {
    pub current_item_label: String,
    pub downloaded_bytes: u64,
    pub total_bytes: Option<u64>,
    pub speed_mb_s: Option<f64>,
}

struct ProcessOutputCapture {
    tail: Vec<String>,
}

impl AriaSnapshot {
    fn is_terminal(&self) -> bool {
        self.active.is_empty()
            && self.waiting.is_empty()
            && !self.stopped.is_empty()
            && self
                .stopped
                .iter()
                .all(|status| matches!(status.status.as_str(), "complete" | "error" | "removed"))
    }
}

static MAGNET_LINK_RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(r"(?i)magnet:\?[^\s<>]+").expect("magnet regex should compile")
});
static DIRECT_LINK_RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(r"(?i)\b(?:https?|ftp)://[^\s<>]+").expect("direct-link regex should compile")
});
static MEGA_LINK_RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(r"(?i)\bhttps://mega\.nz/[^\s<>]+").expect("mega regex should compile")
});
static BTIH_RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(r"(?i)(?:^|&)xt=urn:btih:([^&]+)").expect("btih regex should compile")
});
static MEGA_PROGRESS_RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(
        r"^(?P<name>.+?):\s+(?P<percent>\d+(?:\.\d+)?)%\s+-\s+(?P<human_downloaded>.+?)\s+\((?P<downloaded_bytes>[\d,]+)\s+bytes\)\s+of\s+(?P<human_total>.+?)\s+\((?P<speed>.+?)/s\)\s*$",
    )
    .expect("mega progress regex should compile")
});

/// Build a human-friendly filename and expected size from Telegram media metadata.
pub fn build_candidate(message: &Message, media: &Media) -> DownloadCandidate {
    match media {
        Media::Document(document) => {
            let filename = if document.name().is_empty() {
                format!("document_{}", message.id())
            } else {
                document.name().to_string()
            };
            DownloadCandidate {
                filename: sanitize_filename(&filename),
                file_size: Some(document.size() as u64),
            }
        }
        Media::Photo(photo) => DownloadCandidate {
            filename: format!("photo_{}.jpg", message.id()),
            file_size: Some(photo.size() as u64),
        },
        _ => DownloadCandidate {
            filename: format!("download_{}", message.id()),
            file_size: None,
        },
    }
}

pub fn classify_download_request(
    config: &AppConfig,
    message: &Message,
) -> Result<Option<DownloadRequest>> {
    if let Some(media) = message.media() {
        if matches!(media, Media::Photo(_) | Media::Document(_)) {
            let candidate = build_candidate(message, &media);
            let target_path = choose_target_path(
                &config.download_dir,
                &candidate.filename,
                candidate.file_size,
            )?;
            return Ok(Some(DownloadRequest::TelegramMedia {
                media,
                candidate,
                target_path,
            }));
        }
    }

    let Some(source) = extract_external_link(message.text()) else {
        return Ok(None);
    };
    let (label, target_path, expected_size) =
        build_external_target(&config.download_dir, message.id(), &source)?;
    Ok(Some(DownloadRequest::ExternalLink {
        source,
        label,
        target_path,
        expected_size,
    }))
}

pub fn extract_external_link(text: &str) -> Option<ExternalSource> {
    if let Some(mat) = MAGNET_LINK_RE.find(text) {
        let uri = trim_link_punctuation(mat.as_str());
        let query = uri.split_once('?')?.1;
        let captures = BTIH_RE.captures(query)?;
        let info_hash = captures.get(1)?.as_str().to_ascii_lowercase();
        return Some(ExternalSource::Magnet {
            uri: uri.to_string(),
            info_hash,
        });
    }

    if let Some(mat) = MEGA_LINK_RE.find(text) {
        let url = trim_link_punctuation(mat.as_str());
        return Some(ExternalSource::Mega {
            url: url.to_string(),
        });
    }

    let direct = DIRECT_LINK_RE.find(text)?;
    let url = trim_link_punctuation(direct.as_str());
    let parsed = Url::parse(url).ok()?;
    match parsed.scheme() {
        "http" | "https" | "ftp" => Some(ExternalSource::DirectUrl {
            url: parsed.to_string(),
        }),
        _ => None,
    }
}

pub fn build_external_target(
    download_dir: &Path,
    message_id: i32,
    source: &ExternalSource,
) -> Result<(String, PathBuf, Option<u64>)> {
    match source {
        ExternalSource::DirectUrl { url } => {
            let filename =
                filename_from_url(url).unwrap_or_else(|| format!("download_{message_id}"));
            let filename = sanitize_filename(&filename);
            let target_path = choose_target_path(download_dir, &filename, None)?;
            Ok((filename, target_path, None))
        }
        ExternalSource::Magnet { info_hash, .. } => {
            let label = format!("torrent_{info_hash}");
            let target_path = download_dir.join("torrents").join(info_hash);
            Ok((label, target_path, None))
        }
        ExternalSource::Mega { .. } => {
            let label = format!("mega_{message_id}");
            let target_path = download_dir.join("mega").join(&label);
            Ok((label, target_path, None))
        }
    }
}

/// Pick the final target path, reusing an existing exact match or generating a numbered name.
pub fn choose_target_path(
    download_dir: &Path,
    filename: &str,
    expected_size: Option<u64>,
) -> Result<PathBuf> {
    let candidate = download_dir.join(filename);
    if !candidate.exists() {
        return Ok(candidate);
    }

    if target_matches(&candidate, expected_size)? {
        return Ok(candidate);
    }

    let stem = candidate
        .file_stem()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| "downloaded_file".to_string());
    let suffix = candidate
        .extension()
        .map(|value| format!(".{}", value.to_string_lossy()))
        .unwrap_or_default();

    let mut counter = 1;
    loop {
        let numbered = download_dir.join(format!("{stem}_{counter}{suffix}"));
        if !numbered.exists() || target_matches(&numbered, expected_size)? {
            return Ok(numbered);
        }
        counter += 1;
    }
}

pub fn target_matches(path: &Path, expected_size: Option<u64>) -> Result<bool> {
    if !path.is_file() {
        return Ok(false);
    }
    match expected_size {
        Some(size) => Ok(fs::metadata(path)?.len() == size),
        None => Ok(true),
    }
}

pub fn choose_available_path(destination_dir: &Path, name: &OsStr) -> PathBuf {
    let candidate = destination_dir.join(name);
    if !candidate.exists() {
        return candidate;
    }

    let path = Path::new(name);
    let stem = path
        .file_stem()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| "downloaded_file".to_string());
    let suffix = path
        .extension()
        .map(|value| format!(".{}", value.to_string_lossy()))
        .unwrap_or_default();

    let mut counter = 1;
    loop {
        let numbered = destination_dir.join(format!("{stem}_{counter}{suffix}"));
        if !numbered.exists() {
            return numbered;
        }
        counter += 1;
    }
}

fn choose_available_path_ignoring(
    destination_dir: &Path,
    name: &OsStr,
    ignored_existing_path: &Path,
) -> PathBuf {
    let candidate = destination_dir.join(name);
    if candidate == ignored_existing_path || !candidate.exists() {
        return candidate;
    }

    let path = Path::new(name);
    let stem = path
        .file_stem()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| "downloaded_file".to_string());
    let suffix = path
        .extension()
        .map(|value| format!(".{}", value.to_string_lossy()))
        .unwrap_or_default();

    let mut counter = 1;
    loop {
        let numbered = destination_dir.join(format!("{stem}_{counter}{suffix}"));
        if numbered == ignored_existing_path || !numbered.exists() {
            return numbered;
        }
        counter += 1;
    }
}

pub fn move_classified_target(
    config: &AppConfig,
    target: &ClassificationTarget,
    kind: LibraryKind,
) -> Result<PathBuf> {
    move_classified_target_with_operations(config, target, kind, &|source, destination| {
        move_with_rsync(source, destination)
    })
}

pub fn move_classified_target_with_operations<Mover>(
    config: &AppConfig,
    target: &ClassificationTarget,
    kind: LibraryKind,
    mover: &Mover,
) -> Result<PathBuf>
where
    Mover: Fn(&Path, &Path) -> Result<()>,
{
    let destination_dir = match kind {
        LibraryKind::Movie => &config.movie_dir,
        LibraryKind::TvShow => &config.tv_show_dir,
        LibraryKind::Anime => &config.anime_dir,
    };
    let file_name = target.source_path.file_name().ok_or_else(|| {
        anyhow!(
            "source path has no filename: {}",
            target.source_path.display()
        )
    })?;
    let destination_path = choose_available_path(destination_dir, file_name);
    mover(&target.source_path, &destination_path).with_context(|| {
        format!(
            "failed to move {} to {}",
            target.source_path.display(),
            destination_path.display()
        )
    })?;
    remove_residual_source_path(&target.source_path)?;

    if let Some(cleanup_dir) = &target.cleanup_dir {
        remove_empty_directory(cleanup_dir)?;
    }

    Ok(destination_path)
}

fn move_with_rsync(source_path: &Path, destination_path: &Path) -> Result<()> {
    let output = StdCommand::new("rsync")
        .arg("-avh")
        .arg("--progress")
        .arg("--remove-source-files")
        .arg("--")
        .arg(source_path)
        .arg(destination_path)
        .stdout(Stdio::null())
        .output()
        .context("failed to spawn rsync")?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        bail!("rsync exited with status {}", output.status);
    }

    bail!("rsync exited with status {}: {stderr}", output.status);
}

fn remove_residual_source_path(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    if path.is_dir() {
        fs::remove_dir_all(path).with_context(|| {
            format!(
                "failed to remove residual source directory {}",
                path.display()
            )
        })?;
    }

    Ok(())
}

/// Download a file, resuming partial work and using parallel segments when the total size is known.
pub async fn download_with_resume(
    client: TelegramClient,
    config: AppConfig,
    media: Media,
    target_path: &Path,
    expected_size: Option<u64>,
    tracker: ProgressTracker,
    cancellation: DownloadCancellation,
) -> Result<()> {
    let Some(total_size) = expected_size.filter(|size| *size > 0) else {
        let result = download_single_stream(
            client,
            config.chunk_size,
            media,
            target_path,
            expected_size,
            tracker,
            cancellation.clone(),
        )
        .await;
        if cancellation.is_cancelled() {
            cleanup_single_stream_partial(target_path).await?;
        }
        return result;
    };

    let work_dir = partial_work_dir(&config.download_dir, target_path);
    tokio_fs::create_dir_all(&work_dir).await?;
    let segment_count = config
        .parallel_chunk_downloads
        .min(((total_size + config.chunk_size as u64 - 1) / config.chunk_size as u64) as usize)
        .max(1);
    let plans = build_segment_plans(&work_dir, total_size, segment_count);

    let mut completed_bytes = 0;
    for plan in &plans {
        if let Ok(metadata) = fs::metadata(&plan.part_path) {
            completed_bytes += metadata.len().min(plan.size());
        }
    }
    tracker.set_completed_bytes(completed_bytes).await;
    tracker.log_snapshot().await?;

    let mut handles = Vec::with_capacity(plans.len());
    for plan in plans.clone() {
        let tracker = tracker.clone();
        let client = client.clone();
        let media = media.clone();
        let chunk_size = config.chunk_size;
        let cancellation = cancellation.clone();
        handles.push(tokio::spawn(async move {
            download_segment(client, media, plan, chunk_size, tracker, cancellation).await
        }));
    }

    let result = join_all(handles).await;
    let mut first_err = None;
    for handle in result {
        match handle {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                cancellation.cancel();
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
            Err(err) => {
                cancellation.cancel();
                if first_err.is_none() {
                    first_err = Some(err.into());
                }
            }
        }
    }
    if let Some(err) = first_err {
        let _ = tokio_fs::remove_dir_all(&work_dir).await;
        let _ = tokio_fs::remove_file(target_path).await;
        let assembling_path = assembling_target_path(target_path);
        let _ = tokio_fs::remove_file(assembling_path).await;
        return Err(err);
    }

    merge_segments(&plans, target_path, &cancellation).await?;
    let _ = tokio_fs::remove_dir_all(&work_dir).await;
    Ok(())
}

pub async fn download_external(
    config: AppConfig,
    source: ExternalSource,
    target_path: &Path,
    tracker: ProgressTracker,
    cancellation: DownloadCancellation,
) -> Result<PathBuf> {
    match source {
        ExternalSource::Mega { url } => {
            download_external_with_megadl(config, url, target_path, tracker, cancellation).await
        }
        other => {
            download_external_with_aria2(config, other, target_path, tracker, cancellation).await?;
            Ok(target_path.to_path_buf())
        }
    }
}

pub async fn download_external_with_aria2(
    config: AppConfig,
    source: ExternalSource,
    target_path: &Path,
    tracker: ProgressTracker,
    cancellation: DownloadCancellation,
) -> Result<()> {
    let (rpc_url, rpc_secret, rpc_port) = allocate_rpc_endpoint(&config).await?;
    let mut command = TokioCommand::new(&config.aria2c_path);
    command
        .arg("--enable-rpc=true")
        .arg("--rpc-listen-all=false")
        .arg(format!("--rpc-listen-port={rpc_port}"))
        .arg(format!("--rpc-secret={rpc_secret}"))
        .arg("--file-allocation=none")
        .arg("--summary-interval=0")
        .arg("--console-log-level=warn")
        .arg("--seed-time=0")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    match &source {
        ExternalSource::DirectUrl { url } => {
            let parent = target_path
                .parent()
                .ok_or_else(|| anyhow!("target path has no parent: {}", target_path.display()))?;
            tokio_fs::create_dir_all(parent).await?;
            let output_name = target_path
                .file_name()
                .ok_or_else(|| anyhow!("target path has no filename: {}", target_path.display()))?
                .to_string_lossy()
                .into_owned();
            command
                .arg(format!("--dir={}", parent.display()))
                .arg(format!("--out={output_name}"))
                .arg("--continue=true")
                .arg(url);
        }
        ExternalSource::Magnet { uri, .. } => {
            tokio_fs::create_dir_all(target_path).await?;
            command
                .arg(format!("--dir={}", target_path.display()))
                .arg(uri);
        }
        ExternalSource::Mega { .. } => unreachable!("MEGA downloads do not use aria2c"),
    }

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn {}", config.aria2c_path))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("failed to capture aria2c stderr"))?;
    let stderr_task = tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(stderr);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;
        Ok::<String, std::io::Error>(String::from_utf8_lossy(&buffer).trim().to_string())
    });

    let http = HttpClient::builder().build()?;
    let poll_interval = Duration::from_millis(config.aria2c_poll_interval_ms);
    let mut shutdown_requested = false;
    let mut cancelled = false;

    let exit_status = loop {
        tokio::select! {
            status = child.wait() => break status?,
            _ = cancellation.cancelled(), if !cancelled => {
                cancelled = true;
                let _ = child.start_kill();
            }
            _ = tokio::time::sleep(poll_interval) => {
                if let Ok(snapshot) = refresh_tracker_from_aria2(&http, &rpc_url, &rpc_secret, &tracker).await {
                    if snapshot.is_terminal() && !shutdown_requested {
                        let _ = rpc_call::<String>(
                            &http,
                            &rpc_url,
                            &rpc_secret,
                            "aria2.forceShutdown",
                            json!([]),
                        )
                        .await;
                        shutdown_requested = true;
                    }
                }
            }
        }
    };

    let _ = refresh_tracker_from_aria2(&http, &rpc_url, &rpc_secret, &tracker).await;
    let stderr_text = stderr_task.await??;
    if cancelled {
        cleanup_external_partial(&source, target_path).await?;
        return Err(anyhow::Error::new(DownloadCancelled));
    }
    if !exit_status.success() {
        if stderr_text.is_empty() {
            bail!("aria2c exited with status {exit_status}");
        }
        bail!("aria2c exited with status {exit_status}: {stderr_text}");
    }

    match source {
        ExternalSource::DirectUrl { .. } => {
            if !target_path.is_file() {
                bail!(
                    "aria2c completed without creating {}",
                    target_path.display()
                );
            }
        }
        ExternalSource::Magnet { .. } => {
            if !contains_payload_file(target_path)? {
                bail!(
                    "aria2c completed without downloaded payload files in {}",
                    target_path.display()
                );
            }
        }
        ExternalSource::Mega { .. } => unreachable!("MEGA downloads do not use aria2c"),
    }

    Ok(())
}

async fn download_external_with_megadl(
    config: AppConfig,
    url: String,
    target_path: &Path,
    tracker: ProgressTracker,
    cancellation: DownloadCancellation,
) -> Result<PathBuf> {
    if config.megadl_path.trim().is_empty() {
        bail!("MEGADL_PATH cannot be empty");
    }

    if target_path.exists() {
        let _ = tokio_fs::remove_dir_all(target_path).await;
    }
    tokio_fs::create_dir_all(target_path).await?;

    let mut command = TokioCommand::new(&config.megadl_path);
    command
        .arg("--path")
        .arg(target_path)
        .arg(&url)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn {}", config.megadl_path))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("failed to capture megadl stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("failed to capture megadl stderr"))?;

    let stdout_task =
        tokio::spawn(async move { collect_megadl_output(stdout, Some(tracker)).await });
    let stderr_task = tokio::spawn(async move { collect_megadl_output(stderr, None).await });

    let mut cancelled = false;
    let exit_status = loop {
        tokio::select! {
            status = child.wait() => break status?,
            _ = cancellation.cancelled(), if !cancelled => {
                cancelled = true;
                let _ = child.start_kill();
            }
        }
    };

    let stdout_capture = stdout_task.await??;
    let stderr_capture = stderr_task.await??;
    if cancelled {
        cleanup_external_partial(&ExternalSource::Mega { url }, target_path).await?;
        return Err(anyhow::Error::new(DownloadCancelled));
    }
    if !exit_status.success() {
        let details = render_process_failure_details(&stdout_capture.tail, &stderr_capture.tail);
        if details.is_empty() {
            bail!("megadl exited with status {exit_status}");
        }
        bail!("megadl exited with status {exit_status}: {details}");
    }
    if !contains_payload_file(target_path)? {
        bail!(
            "megadl completed without downloaded payload files in {}",
            target_path.display()
        );
    }

    finalize_mega_target(target_path).await
}

async fn download_single_stream(
    client: TelegramClient,
    chunk_size: usize,
    media: Media,
    target_path: &Path,
    expected_size: Option<u64>,
    tracker: ProgressTracker,
    cancellation: DownloadCancellation,
) -> Result<()> {
    let partial_path = partial_file_path(target_path);
    if let Some(parent) = partial_path.parent() {
        tokio_fs::create_dir_all(parent).await?;
    }

    let existing_size = aligned_existing_size(&partial_path, chunk_size)?;
    tracker.set_completed_bytes(0).await;
    tracker.update(0, existing_size).await?;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&partial_path)
        .await?;

    let mut downloaded = existing_size;
    let mut stream = build_download_stream(&client, &media, chunk_size, existing_size);
    loop {
        if cancellation.is_cancelled() {
            return Err(anyhow::Error::new(DownloadCancelled));
        }
        let chunk = tokio::select! {
            _ = cancellation.cancelled() => return Err(anyhow::Error::new(DownloadCancelled)),
            chunk = stream.next() => chunk?,
        };
        let Some(chunk) = chunk else {
            break;
        };
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        tracker.update(0, downloaded).await?;
    }
    file.flush().await?;
    if cancellation.is_cancelled() {
        return Err(anyhow::Error::new(DownloadCancelled));
    }

    if let Some(expected_size) = expected_size {
        let actual = fs::metadata(&partial_path)?.len();
        if actual != expected_size {
            return Err(anyhow!(
                "single stream download incomplete: expected={expected_size} actual={actual}"
            ));
        }
    }

    tokio_fs::rename(&partial_path, target_path).await?;
    Ok(())
}

async fn download_segment(
    client: TelegramClient,
    media: Media,
    plan: SegmentPlan,
    chunk_size: usize,
    tracker: ProgressTracker,
    cancellation: DownloadCancellation,
) -> Result<()> {
    if let Some(parent) = plan.part_path.parent() {
        tokio_fs::create_dir_all(parent).await?;
    }

    let existing_size = aligned_existing_size(&plan.part_path, chunk_size)?.min(plan.size());
    if existing_size == plan.size() {
        tracker
            .mark_segment_complete(plan.index, plan.size())
            .await?;
        return Ok(());
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&plan.part_path)
        .await?;

    let offset = plan.start + existing_size;
    let mut remaining = plan.size() - existing_size;
    let mut downloaded = existing_size;
    tracker.update(plan.index, downloaded).await?;

    let mut stream = build_download_stream(&client, &media, chunk_size, offset);
    while remaining > 0 {
        if cancellation.is_cancelled() {
            return Err(anyhow::Error::new(DownloadCancelled));
        }
        let chunk = tokio::select! {
            _ = cancellation.cancelled() => return Err(anyhow::Error::new(DownloadCancelled)),
            chunk = stream.next() => chunk?,
        };
        let Some(chunk) = chunk else {
            break;
        };
        if remaining == 0 {
            break;
        }

        let take = remaining.min(chunk.len() as u64) as usize;
        file.write_all(&chunk[..take]).await?;
        remaining -= take as u64;
        downloaded += take as u64;
        tracker.update(plan.index, downloaded).await?;
        if remaining == 0 {
            break;
        }
    }
    file.flush().await?;

    let actual = fs::metadata(&plan.part_path)?.len();
    if actual != plan.size() {
        return Err(anyhow!(
            "segment {} incomplete: expected={} actual={actual}",
            plan.index,
            plan.size()
        ));
    }

    tracker
        .mark_segment_complete(plan.index, plan.size())
        .await?;
    Ok(())
}

async fn merge_segments(
    plans: &[SegmentPlan],
    target_path: &Path,
    cancellation: &DownloadCancellation,
) -> Result<()> {
    let temp_target = assembling_target_path(target_path);
    let mut out = File::create(&temp_target).await?;
    for plan in plans {
        if cancellation.is_cancelled() {
            let _ = tokio_fs::remove_file(&temp_target).await;
            return Err(anyhow::Error::new(DownloadCancelled));
        }
        let bytes = tokio_fs::read(&plan.part_path).await?;
        out.write_all(&bytes).await?;
    }
    out.flush().await?;
    if cancellation.is_cancelled() {
        let _ = tokio_fs::remove_file(&temp_target).await;
        return Err(anyhow::Error::new(DownloadCancelled));
    }
    tokio_fs::rename(temp_target, target_path).await?;
    Ok(())
}

fn assembling_target_path(target_path: &Path) -> PathBuf {
    target_path.with_file_name(format!(
        "{}.assembling",
        target_path
            .file_name()
            .map(|value| value.to_string_lossy())
            .unwrap_or_default()
    ))
}

async fn cleanup_single_stream_partial(target_path: &Path) -> Result<()> {
    let partial_path = partial_file_path(target_path);
    let _ = tokio_fs::remove_file(partial_path).await;
    Ok(())
}

async fn cleanup_external_partial(source: &ExternalSource, target_path: &Path) -> Result<()> {
    match source {
        ExternalSource::DirectUrl { .. } => {
            let _ = tokio_fs::remove_file(target_path).await;
            let control_file = target_path.with_file_name(format!(
                "{}.aria2",
                target_path
                    .file_name()
                    .map(|value| value.to_string_lossy())
                    .unwrap_or_default()
            ));
            let _ = tokio_fs::remove_file(control_file).await;
        }
        ExternalSource::Magnet { .. } => {
            let _ = tokio_fs::remove_dir_all(target_path).await;
        }
        ExternalSource::Mega { .. } => {
            let _ = tokio_fs::remove_dir_all(target_path).await;
        }
    }
    Ok(())
}

fn build_download_stream<'a>(
    client: &TelegramClient,
    media: &Media,
    chunk_size: usize,
    offset: u64,
) -> DownloadIter {
    let mut stream = client.iter_download(media);
    stream = stream.chunk_size(chunk_size as i32);
    if offset > 0 {
        stream = stream.skip_chunks((offset / chunk_size as u64) as i32);
    }
    stream
}

fn build_segment_plans(base_dir: &Path, total_size: u64, segment_count: usize) -> Vec<SegmentPlan> {
    let segment_size = (total_size + segment_count as u64 - 1) / segment_count as u64;
    let mut plans = Vec::new();
    for index in 0..segment_count {
        let start = index as u64 * segment_size;
        let end = (start + segment_size).min(total_size);
        if start >= end {
            break;
        }
        plans.push(SegmentPlan {
            index,
            start,
            end,
            part_path: base_dir.join(format!("segment_{index:03}.part")),
        });
    }
    plans
}

fn partial_work_dir(download_dir: &Path, target_path: &Path) -> PathBuf {
    download_dir
        .join(".partial")
        .join(target_path.file_name().unwrap_or_default())
}

fn partial_file_path(target_path: &Path) -> PathBuf {
    target_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".partial")
        .join(format!(
            "{}.part",
            target_path
                .file_name()
                .map(|value| value.to_string_lossy())
                .unwrap_or_default()
        ))
}

fn external_identity(source: &ExternalSource) -> String {
    match source {
        ExternalSource::DirectUrl { url } => format!("url:{url}"),
        ExternalSource::Magnet { info_hash, .. } => format!("magnet:{info_hash}"),
        ExternalSource::Mega { url } => format!("mega:{url}"),
    }
}

fn filename_from_url(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    if parsed.path().ends_with('/') {
        return None;
    }
    let segment = parsed
        .path_segments()?
        .filter(|value| !value.is_empty())
        .next_back()?;
    Some(segment.to_string())
}

fn trim_link_punctuation(link: &str) -> &str {
    link.trim_end_matches(|ch: char| matches!(ch, ')' | ']' | '}' | '.' | ',' | ';' | ':' | '!'))
}

fn sanitize_filename(name: &str) -> String {
    let mut output = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            output.push(ch);
        } else {
            output.push('_');
        }
    }
    let trimmed = output.trim_matches(['.', '_']);
    if trimmed.is_empty() {
        "downloaded_file".to_string()
    } else {
        trimmed.to_string()
    }
}

fn aligned_existing_size(path: &Path, chunk_size: usize) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }

    let actual = fs::metadata(path)?.len();
    let aligned = actual - (actual % chunk_size as u64);
    if aligned != actual {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .with_context(|| format!("failed to reopen {} for truncation", path.display()))?;
        file.set_len(aligned)?;
    }
    Ok(aligned)
}

pub fn contains_payload_file(path: &Path) -> Result<bool> {
    if path.is_file() {
        return Ok(!is_aria2_artifact(path));
    }
    if !path.is_dir() {
        return Ok(false);
    }

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        if entry_path.is_dir() && contains_payload_file(&entry_path)? {
            return Ok(true);
        }
        if entry_path.is_file() && !is_aria2_artifact(&entry_path) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn resolve_magnet_payload(container_dir: &Path) -> Result<ClassificationTarget> {
    if !container_dir.is_dir() {
        bail!(
            "magnet download did not create a torrent directory at {}",
            container_dir.display()
        );
    }

    let entries = fs::read_dir(container_dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| !is_aria2_artifact(path))
        .collect::<Vec<_>>();

    match entries.as_slice() {
        [] => bail!(
            "torrent payload directory is empty: {}",
            container_dir.display()
        ),
        [payload] => Ok(ClassificationTarget {
            source_path: payload.clone(),
            cleanup_dir: Some(container_dir.to_path_buf()),
        }),
        _ => bail!(
            "torrent payload is ambiguous in {}; expected exactly one top-level file or folder",
            container_dir.display()
        ),
    }
}

pub fn resolve_mega_payload(container_dir: &Path) -> Result<ClassificationTarget> {
    if !container_dir.is_dir() {
        bail!(
            "MEGA download did not create a wrapper directory at {}",
            container_dir.display()
        );
    }

    let entries = payload_entries(container_dir)?;
    match entries.as_slice() {
        [] => bail!(
            "MEGA payload directory is empty: {}",
            container_dir.display()
        ),
        [payload] => Ok(ClassificationTarget {
            source_path: payload.clone(),
            cleanup_dir: Some(container_dir.to_path_buf()),
        }),
        _ => Ok(ClassificationTarget {
            source_path: container_dir.to_path_buf(),
            cleanup_dir: None,
        }),
    }
}

fn remove_empty_directory(path: &Path) -> Result<()> {
    if !path.is_dir() {
        return Ok(());
    }
    if fs::read_dir(path)?.next().is_none() {
        fs::remove_dir(path)
            .with_context(|| format!("failed to remove empty directory {}", path.display()))?;
    }
    Ok(())
}

fn is_aria2_artifact(path: &Path) -> bool {
    path.file_name()
        .map(|name| {
            let name = name.to_string_lossy();
            name.starts_with('.') || name.ends_with(".aria2")
        })
        .unwrap_or(false)
}

async fn allocate_rpc_endpoint(config: &AppConfig) -> Result<(String, String, u16)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr: SocketAddr = listener.local_addr()?;
    let port = addr.port();
    drop(listener);
    let secret = format!(
        "aria2-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let rpc_url = format!("http://127.0.0.1:{port}/jsonrpc");
    if config.aria2c_path.trim().is_empty() {
        bail!("ARIA2C_PATH cannot be empty");
    }
    Ok((rpc_url, secret, port))
}

async fn refresh_tracker_from_aria2(
    http: &HttpClient,
    rpc_url: &str,
    rpc_secret: &str,
    tracker: &ProgressTracker,
) -> Result<AriaSnapshot> {
    let requests = [
        rpc_call::<Vec<AriaStatus>>(
            http,
            rpc_url,
            rpc_secret,
            "aria2.tellActive",
            json!([["status", "completedLength", "totalLength"]]),
        ),
        rpc_call::<Vec<AriaStatus>>(
            http,
            rpc_url,
            rpc_secret,
            "aria2.tellWaiting",
            json!([0, 1000, ["status", "completedLength", "totalLength"]]),
        ),
        rpc_call::<Vec<AriaStatus>>(
            http,
            rpc_url,
            rpc_secret,
            "aria2.tellStopped",
            json!([0, 1000, ["status", "completedLength", "totalLength"]]),
        ),
    ];

    let responses = join_all(requests).await;
    let mut response_groups = Vec::with_capacity(3);
    let mut downloaded = 0_u64;
    let mut total = 0_u64;
    for response in responses {
        let group = response?;
        for status in &group {
            downloaded += status.completed_length.parse::<u64>().unwrap_or(0);
            total += status.total_length.parse::<u64>().unwrap_or(0);
        }
        response_groups.push(group);
    }

    tracker
        .set_absolute_progress(downloaded, (total > 0).then_some(total))
        .await?;
    let mut groups = response_groups.into_iter();
    Ok(AriaSnapshot {
        active: groups.next().unwrap_or_default(),
        waiting: groups.next().unwrap_or_default(),
        stopped: groups.next().unwrap_or_default(),
    })
}

async fn collect_megadl_output<R>(
    mut reader: R,
    tracker: Option<ProgressTracker>,
) -> Result<ProcessOutputCapture, std::io::Error>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = [0_u8; 4096];
    let mut pending = Vec::new();
    let mut tail = VecDeque::with_capacity(10);

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            if !pending.is_empty() {
                process_megadl_line(&pending, tracker.as_ref(), &mut tail).await;
            }
            break;
        }

        for byte in &buffer[..read] {
            if matches!(*byte, b'\n' | b'\r') {
                if !pending.is_empty() {
                    process_megadl_line(&pending, tracker.as_ref(), &mut tail).await;
                    pending.clear();
                }
            } else {
                pending.push(*byte);
            }
        }
    }

    Ok(ProcessOutputCapture {
        tail: tail.into_iter().collect(),
    })
}

async fn process_megadl_line(
    raw_line: &[u8],
    tracker: Option<&ProgressTracker>,
    tail: &mut VecDeque<String>,
) {
    let line = String::from_utf8_lossy(raw_line).trim().to_string();
    if line.is_empty() {
        return;
    }
    push_tail_line(tail, line.clone());

    if let (Some(tracker), Some(snapshot)) = (tracker, parse_megadl_progress_line(&line)) {
        let _ = tracker
            .set_external_progress(
                snapshot.current_item_label,
                snapshot.downloaded_bytes,
                snapshot.total_bytes,
                snapshot.speed_mb_s,
            )
            .await;
    }
}

fn push_tail_line(tail: &mut VecDeque<String>, line: String) {
    if tail.len() == 10 {
        tail.pop_front();
    }
    tail.push_back(line);
}

fn render_process_failure_details(stdout_tail: &[String], stderr_tail: &[String]) -> String {
    let stderr_text = stderr_tail.join(" | ");
    if !stderr_text.is_empty() {
        return stderr_text;
    }

    stdout_tail.join(" | ")
}

pub fn parse_megadl_progress_line(line: &str) -> Option<MegaProgressSnapshot> {
    let captures = MEGA_PROGRESS_RE.captures(line)?;
    let downloaded_bytes = parse_decimal_bytes(captures.name("downloaded_bytes")?.as_str())?;
    let total_bytes = parse_human_size(captures.name("human_total")?.as_str());
    let speed_mb_s = parse_speed_to_mb_s(captures.name("speed")?.as_str());

    Some(MegaProgressSnapshot {
        current_item_label: captures.name("name")?.as_str().trim().to_string(),
        downloaded_bytes,
        total_bytes,
        speed_mb_s,
    })
}

fn parse_decimal_bytes(input: &str) -> Option<u64> {
    let digits = input
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect::<String>();
    digits.parse().ok()
}

pub fn parse_human_size(input: &str) -> Option<u64> {
    let normalized_input = input.replace(['\u{a0}', '\u{202f}'], " ");
    let normalized = normalized_input.split_whitespace().collect::<Vec<_>>();
    let [value, unit] = normalized.as_slice() else {
        return None;
    };

    let value = value.replace(',', "");
    let numeric = value.parse::<f64>().ok()?;
    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "b" => 1_f64,
        "kib" => 1024_f64,
        "mib" => 1024_f64.powi(2),
        "gib" => 1024_f64.powi(3),
        "tib" => 1024_f64.powi(4),
        "kb" => 1000_f64,
        "mb" => 1000_f64.powi(2),
        "gb" => 1000_f64.powi(3),
        "tb" => 1000_f64.powi(4),
        _ => return None,
    };

    Some((numeric * multiplier).round() as u64)
}

fn parse_speed_to_mb_s(input: &str) -> Option<f64> {
    parse_human_size(input).map(|bytes| bytes as f64 / (1024.0 * 1024.0))
}

pub async fn finalize_mega_target(target_path: &Path) -> Result<PathBuf> {
    let entries = payload_entries(target_path)?;
    if let [payload] = entries.as_slice() {
        if payload.is_dir() {
            return collapse_single_nested_mega_directory(target_path, payload).await;
        }
    }

    let parent = target_path
        .parent()
        .ok_or_else(|| anyhow!("MEGA target path has no parent: {}", target_path.display()))?;
    let fallback_name = target_path.file_name().ok_or_else(|| {
        anyhow!(
            "MEGA target path has no filename: {}",
            target_path.display()
        )
    })?;
    let inferred_name = infer_mega_wrapper_name(target_path)
        .unwrap_or_else(|| fallback_name.to_string_lossy().into_owned());
    let sanitized = sanitize_filename(&inferred_name);
    let destination = choose_available_path(parent, OsStr::new(&sanitized));
    if destination == target_path {
        return Ok(target_path.to_path_buf());
    }
    tokio_fs::rename(target_path, &destination)
        .await
        .with_context(|| {
            format!(
                "failed to rename MEGA download directory {} to {}",
                target_path.display(),
                destination.display()
            )
        })?;
    Ok(destination)
}

async fn collapse_single_nested_mega_directory(
    target_path: &Path,
    payload_dir: &Path,
) -> Result<PathBuf> {
    let parent = target_path
        .parent()
        .ok_or_else(|| anyhow!("MEGA target path has no parent: {}", target_path.display()))?;
    let desired_name = payload_dir
        .file_name()
        .map(|value| sanitize_filename(&value.to_string_lossy()))
        .unwrap_or_else(|| {
            target_path
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_else(|| "mega_download".to_string())
        });
    let current_name = target_path
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_default();

    if desired_name == current_name {
        move_directory_contents(payload_dir, target_path).await?;
        tokio_fs::remove_dir(payload_dir).await.with_context(|| {
            format!(
                "failed to remove collapsed MEGA wrapper directory {}",
                payload_dir.display()
            )
        })?;
        return Ok(target_path.to_path_buf());
    }

    let destination =
        choose_available_path_ignoring(parent, OsStr::new(&desired_name), target_path);
    tokio_fs::rename(payload_dir, &destination)
        .await
        .with_context(|| {
            format!(
                "failed to move nested MEGA payload {} to {}",
                payload_dir.display(),
                destination.display()
            )
        })?;
    tokio_fs::remove_dir(target_path).await.with_context(|| {
        format!(
            "failed to remove empty MEGA wrapper directory {}",
            target_path.display()
        )
    })?;
    Ok(destination)
}

async fn move_directory_contents(source_dir: &Path, destination_dir: &Path) -> Result<()> {
    let mut entries = tokio_fs::read_dir(source_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let source_path = entry.path();
        let destination_path = destination_dir.join(entry.file_name());
        tokio_fs::rename(&source_path, &destination_path)
            .await
            .with_context(|| {
                format!(
                    "failed to move {} into {}",
                    source_path.display(),
                    destination_dir.display()
                )
            })?;
    }
    Ok(())
}

pub fn infer_mega_wrapper_name(target_path: &Path) -> Option<String> {
    let entries = payload_entries(target_path).ok()?;
    match entries.as_slice() {
        [payload] if payload.is_file() => payload
            .file_stem()
            .map(|value| sanitize_filename(&value.to_string_lossy())),
        [payload] if payload.is_dir() => payload
            .file_name()
            .map(|value| sanitize_filename(&value.to_string_lossy())),
        _ => infer_common_payload_root(target_path),
    }
}

fn payload_entries(target_path: &Path) -> Result<Vec<PathBuf>> {
    let mut entries = fs::read_dir(target_path)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| !is_aria2_artifact(path))
        .collect::<Vec<_>>();
    entries.sort();
    Ok(entries)
}

pub fn infer_common_payload_root(target_path: &Path) -> Option<String> {
    let mut common_root: Option<String> = None;
    let mut saw_payload = false;
    collect_payload_files(target_path)
        .ok()?
        .into_iter()
        .try_for_each(|path| {
            let relative = path.strip_prefix(target_path).ok()?;
            let root = relative
                .components()
                .find_map(|component| match component {
                    Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
                    _ => None,
                })?;
            saw_payload = true;
            match &common_root {
                Some(existing) if existing != &root => None,
                Some(_) => Some(()),
                None => {
                    common_root = Some(root);
                    Some(())
                }
            }
        })?;

    if saw_payload {
        common_root.map(|value| sanitize_filename(&value))
    } else {
        None
    }
}

fn collect_payload_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_payload_files_inner(root, &mut files)?;
    Ok(files)
}

fn collect_payload_files_inner(root: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if is_aria2_artifact(&path) {
            continue;
        }
        if entry.file_type()?.is_dir() {
            collect_payload_files_inner(&path, files)?;
        } else {
            files.push(path);
        }
    }
    Ok(())
}

async fn rpc_call<T: for<'de> Deserialize<'de>>(
    http: &HttpClient,
    rpc_url: &str,
    rpc_secret: &str,
    method: &str,
    extra_params: serde_json::Value,
) -> Result<T> {
    let mut params = vec![json!(format!("token:{rpc_secret}"))];
    match extra_params {
        serde_json::Value::Array(values) => params.extend(values),
        value => params.push(value),
    }

    let response = http
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": method,
            "method": method,
            "params": params,
        }))
        .send()
        .await?
        .error_for_status()?;

    let payload = response.json::<AriaRpcResponse<T>>().await?;
    Ok(payload.result)
}
