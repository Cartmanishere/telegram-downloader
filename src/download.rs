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
use std::ffi::OsStr;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::{self as tokio_fs, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::process::Command as TokioCommand;

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
static BTIH_RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(r"(?i)(?:^|&)xt=urn:btih:([^&]+)").expect("btih regex should compile")
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

fn build_external_target(
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

pub fn move_classified_target(
    config: &AppConfig,
    target: &ClassificationTarget,
    kind: LibraryKind,
) -> Result<PathBuf> {
    move_classified_target_with_operations(config, target, kind, &|source, destination| {
        move_with_rsync(source, destination)
    })
}

fn move_classified_target_with_operations<Mover>(
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
        fs::remove_dir_all(path)
            .with_context(|| format!("failed to remove residual source directory {}", path.display()))?;
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
) -> Result<()> {
    let Some(total_size) = expected_size.filter(|size| *size > 0) else {
        return download_single_stream(
            client,
            config.chunk_size,
            media,
            target_path,
            expected_size,
            tracker,
        )
        .await;
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
        handles.push(tokio::spawn(async move {
            download_segment(client, media, plan, chunk_size, tracker).await
        }));
    }

    for handle in handles {
        handle.await??;
    }

    merge_segments(&plans, target_path).await?;
    let _ = tokio_fs::remove_dir_all(&work_dir).await;
    Ok(())
}

pub async fn download_external_with_aria2(
    config: AppConfig,
    source: ExternalSource,
    target_path: &Path,
    tracker: ProgressTracker,
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

    let exit_status = loop {
        tokio::select! {
            status = child.wait() => break status?,
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
    }

    Ok(())
}

async fn download_single_stream(
    client: TelegramClient,
    chunk_size: usize,
    media: Media,
    target_path: &Path,
    expected_size: Option<u64>,
    tracker: ProgressTracker,
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
    while let Some(chunk) = stream.next().await? {
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        tracker.update(0, downloaded).await?;
    }
    file.flush().await?;

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
    while let Some(chunk) = stream.next().await? {
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

async fn merge_segments(plans: &[SegmentPlan], target_path: &Path) -> Result<()> {
    let temp_target = target_path.with_file_name(format!(
        "{}.assembling",
        target_path
            .file_name()
            .map(|value| value.to_string_lossy())
            .unwrap_or_default()
    ));
    let mut out = File::create(&temp_target).await?;
    for plan in plans {
        let bytes = tokio_fs::read(&plan.part_path).await?;
        out.write_all(&bytes).await?;
    }
    out.flush().await?;
    tokio_fs::rename(temp_target, target_path).await?;
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

fn contains_payload_file(path: &Path) -> Result<bool> {
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

fn resolve_magnet_payload(container_dir: &Path) -> Result<ClassificationTarget> {
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

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use super::{
        build_external_target, choose_available_path, contains_payload_file, extract_external_link,
        move_classified_target, move_classified_target_with_operations, resolve_magnet_payload,
        ActiveDownloadKey, ClassificationTarget, ExternalSource, LibraryKind,
    };
    use crate::config::AppConfig;
    use std::ffi::OsStr;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn extracts_direct_http_link() {
        let source = extract_external_link("download https://example.com/files/archive.zip")
            .expect("http link should be found");
        assert_eq!(
            source,
            ExternalSource::DirectUrl {
                url: "https://example.com/files/archive.zip".to_string()
            }
        );
    }

    #[test]
    fn extracts_https_link_with_query() {
        let source = extract_external_link("mirror: https://example.com/file.iso?token=abc123")
            .expect("https link should be found");
        assert_eq!(
            source,
            ExternalSource::DirectUrl {
                url: "https://example.com/file.iso?token=abc123".to_string()
            }
        );
    }

    #[test]
    fn prefers_magnet_over_direct_link() {
        let source = extract_external_link(
            "http://example.com/file.iso magnet:?xt=urn:btih:ABCDEF1234567890&dn=test",
        )
        .expect("magnet should be found");
        assert_eq!(
            source,
            ExternalSource::Magnet {
                uri: "magnet:?xt=urn:btih:ABCDEF1234567890&dn=test".to_string(),
                info_hash: "abcdef1234567890".to_string(),
            }
        );
    }

    #[test]
    fn returns_none_when_no_supported_link_exists() {
        assert!(extract_external_link("nothing to download here").is_none());
    }

    #[test]
    fn builds_direct_target_from_url_filename() {
        let base = unique_test_dir("direct-target");
        fs::create_dir_all(&base).expect("test dir should be created");
        let (label, target, expected_size) = build_external_target(
            &base,
            42,
            &ExternalSource::DirectUrl {
                url: "https://example.com/release.tar.gz".to_string(),
            },
        )
        .expect("target should build");
        assert_eq!(label, "release.tar.gz");
        assert_eq!(target, base.join("release.tar.gz"));
        assert_eq!(expected_size, None);
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn falls_back_to_message_id_when_url_has_no_filename() {
        let base = unique_test_dir("direct-fallback");
        fs::create_dir_all(&base).expect("test dir should be created");
        let (label, target, _) = build_external_target(
            &base,
            99,
            &ExternalSource::DirectUrl {
                url: "https://example.com/downloads/".to_string(),
            },
        )
        .expect("fallback target should build");
        assert_eq!(label, "download_99");
        assert_eq!(target, base.join("download_99"));
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn builds_magnet_directory_from_info_hash() {
        let base = PathBuf::from("/tmp/test-downloads");
        let (label, target, expected_size) = build_external_target(
            &base,
            1,
            &ExternalSource::Magnet {
                uri: "magnet:?xt=urn:btih:001122".to_string(),
                info_hash: "001122".to_string(),
            },
        )
        .expect("magnet target should build");
        assert_eq!(label, "torrent_001122");
        assert_eq!(target, base.join("torrents").join("001122"));
        assert_eq!(expected_size, None);
    }

    #[test]
    fn active_keys_cover_telegram_and_external_downloads() {
        let telegram = ActiveDownloadKey::Telegram {
            path: PathBuf::from("/tmp/photo.jpg"),
            size: Some(10),
        };
        let direct = ActiveDownloadKey::External {
            identity: "url:https://example.com/file.bin".to_string(),
        };
        let magnet = ActiveDownloadKey::External {
            identity: "magnet:001122".to_string(),
        };

        assert_eq!(
            telegram,
            ActiveDownloadKey::Telegram {
                path: PathBuf::from("/tmp/photo.jpg"),
                size: Some(10),
            }
        );
        assert_eq!(
            direct,
            ActiveDownloadKey::External {
                identity: "url:https://example.com/file.bin".to_string(),
            }
        );
        assert_eq!(
            magnet,
            ActiveDownloadKey::External {
                identity: "magnet:001122".to_string(),
            }
        );
    }

    #[test]
    fn payload_detection_ignores_aria2_artifacts() {
        let base = unique_test_dir("payload-check");
        fs::create_dir_all(&base).expect("test dir should be created");
        fs::write(base.join("partial.iso.aria2"), b"state").expect("artifact should be written");
        assert!(!contains_payload_file(&base).expect("artifact-only dir should be empty"));

        fs::write(base.join("real.iso"), b"payload").expect("payload should be written");
        assert!(contains_payload_file(&base).expect("payload should be detected"));
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn resolve_magnet_payload_returns_single_file() {
        let base = unique_test_dir("magnet-payload-file");
        let container = base.join("hash");
        fs::create_dir_all(&container).expect("container should exist");
        let payload = container.join("episode.mkv");
        fs::write(&payload, b"payload").expect("payload should be written");

        let target = resolve_magnet_payload(&container).expect("payload should resolve");
        assert_eq!(
            target,
            ClassificationTarget {
                source_path: payload,
                cleanup_dir: Some(container),
            }
        );
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn resolve_magnet_payload_returns_single_directory() {
        let base = unique_test_dir("magnet-payload-dir");
        let container = base.join("hash");
        let payload = container.join("Season 1");
        fs::create_dir_all(&payload).expect("payload dir should exist");

        let target = resolve_magnet_payload(&container).expect("payload should resolve");
        assert_eq!(target.source_path, payload);
        assert_eq!(target.cleanup_dir, Some(container));
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn resolve_magnet_payload_rejects_empty_directory() {
        let base = unique_test_dir("magnet-empty");
        let container = base.join("hash");
        fs::create_dir_all(&container).expect("container should exist");

        let error = resolve_magnet_payload(&container).expect_err("empty container should fail");
        assert!(error.to_string().contains("empty"));
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn resolve_magnet_payload_rejects_multiple_top_level_entries() {
        let base = unique_test_dir("magnet-many");
        let container = base.join("hash");
        fs::create_dir_all(&container).expect("container should exist");
        fs::write(container.join("a.mkv"), b"a").expect("first payload should be written");
        fs::write(container.join("b.mkv"), b"b").expect("second payload should be written");

        let error =
            resolve_magnet_payload(&container).expect_err("multi-entry container should fail");
        assert!(error.to_string().contains("ambiguous"));
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn choose_available_path_adds_numeric_suffix() {
        let base = unique_test_dir("available-path");
        fs::create_dir_all(&base).expect("base should exist");
        fs::write(base.join("movie.mkv"), b"payload").expect("existing file should be written");

        let chosen = choose_available_path(&base, OsStr::new("movie.mkv"));
        assert_eq!(chosen, base.join("movie_1.mkv"));
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn move_classified_target_moves_file_to_movie_dir() {
        let base = unique_test_dir("move-file");
        let config = test_config(&base);
        fs::create_dir_all(&config.movie_dir).expect("movie dir should exist");
        fs::create_dir_all(&config.tv_show_dir).expect("tv dir should exist");
        fs::create_dir_all(&config.anime_dir).expect("anime dir should exist");
        let source = base.join("downloads").join("movie.mkv");
        fs::create_dir_all(source.parent().unwrap()).expect("downloads dir should exist");
        fs::write(&source, b"payload").expect("source should exist");

        let destination = move_classified_target(
            &config,
            &ClassificationTarget {
                source_path: source.clone(),
                cleanup_dir: None,
            },
            LibraryKind::Movie,
        )
        .expect("move should succeed");

        assert_eq!(destination, config.movie_dir.join("movie.mkv"));
        assert!(destination.exists());
        assert!(!source.exists());
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn move_classified_target_removes_empty_magnet_container() {
        let base = unique_test_dir("move-magnet");
        let config = test_config(&base);
        fs::create_dir_all(&config.movie_dir).expect("movie dir should exist");
        fs::create_dir_all(&config.tv_show_dir).expect("tv dir should exist");
        fs::create_dir_all(&config.anime_dir).expect("anime dir should exist");
        let container = base.join("downloads").join("torrents").join("abc");
        fs::create_dir_all(&container).expect("container should exist");
        let source = container.join("Show");
        fs::create_dir_all(&source).expect("payload dir should exist");

        let destination = move_classified_target(
            &config,
            &ClassificationTarget {
                source_path: source.clone(),
                cleanup_dir: Some(container.clone()),
            },
            LibraryKind::TvShow,
        )
        .expect("move should succeed");

        assert_eq!(destination, config.tv_show_dir.join("Show"));
        assert!(destination.exists());
        assert!(!container.exists());
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn move_classified_target_avoids_overwrite() {
        let base = unique_test_dir("move-collision");
        let config = test_config(&base);
        fs::create_dir_all(&config.movie_dir).expect("movie dir should exist");
        fs::create_dir_all(&config.tv_show_dir).expect("tv dir should exist");
        fs::create_dir_all(&config.anime_dir).expect("anime dir should exist");
        fs::write(config.movie_dir.join("movie.mkv"), b"existing").expect("existing file");
        let source = base.join("downloads").join("movie.mkv");
        fs::create_dir_all(source.parent().unwrap()).expect("downloads dir should exist");
        fs::write(&source, b"payload").expect("source should exist");

        let destination = move_classified_target(
            &config,
            &ClassificationTarget {
                source_path: source,
                cleanup_dir: None,
            },
            LibraryKind::Movie,
        )
        .expect("move should succeed");

        assert_eq!(destination, config.movie_dir.join("movie_1.mkv"));
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn move_classified_target_moves_directory_to_anime_dir() {
        let base = unique_test_dir("move-anime");
        let config = test_config(&base);
        fs::create_dir_all(&config.movie_dir).expect("movie dir should exist");
        fs::create_dir_all(&config.tv_show_dir).expect("tv dir should exist");
        fs::create_dir_all(&config.anime_dir).expect("anime dir should exist");
        let source = base.join("downloads").join("Anime Series");
        fs::create_dir_all(&source).expect("anime source should exist");

        let destination = move_classified_target(
            &config,
            &ClassificationTarget {
                source_path: source.clone(),
                cleanup_dir: None,
            },
            LibraryKind::Anime,
        )
        .expect("move should succeed");

        assert_eq!(destination, config.anime_dir.join("Anime Series"));
        assert!(destination.exists());
        assert!(!source.exists());
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn move_classified_target_uses_external_mover_for_successful_transfer() {
        let base = unique_test_dir("move-rsync-success");
        let config = test_config(&base);
        fs::create_dir_all(&config.movie_dir).expect("movie dir should exist");
        fs::create_dir_all(&config.tv_show_dir).expect("tv dir should exist");
        fs::create_dir_all(&config.anime_dir).expect("anime dir should exist");
        let container = base.join("downloads").join("torrents").join("abc");
        fs::create_dir_all(&container).expect("container should exist");
        let source = container.join("Show");
        fs::create_dir_all(&source).expect("payload dir should exist");
        fs::write(source.join("episode.mkv"), b"payload").expect("payload file should exist");

        let destination = move_classified_target_with_operations(
            &config,
            &ClassificationTarget {
                source_path: source.clone(),
                cleanup_dir: Some(container.clone()),
            },
            LibraryKind::Movie,
            &|source, destination| {
                copy_dir(source, destination)?;
                remove_source_files(source)?;
                Ok(())
            },
        )
        .expect("rsync-style move should succeed");

        assert_eq!(destination, config.movie_dir.join("Show"));
        assert!(destination.join("episode.mkv").exists());
        assert!(!source.exists());
        assert!(!container.exists());
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn move_classified_target_keeps_cleanup_dir_when_external_mover_fails() {
        let base = unique_test_dir("move-rsync-failure");
        let config = test_config(&base);
        fs::create_dir_all(&config.movie_dir).expect("movie dir should exist");
        fs::create_dir_all(&config.tv_show_dir).expect("tv dir should exist");
        fs::create_dir_all(&config.anime_dir).expect("anime dir should exist");
        let container = base.join("downloads").join("torrents").join("abc");
        fs::create_dir_all(&container).expect("container should exist");
        let source = container.join("Show");
        fs::create_dir_all(&source).expect("payload dir should exist");

        let error = move_classified_target_with_operations(
            &config,
            &ClassificationTarget {
                source_path: source.clone(),
                cleanup_dir: Some(container.clone()),
            },
            LibraryKind::Movie,
            &|_, _| bail!("rsync exited with status 23: permission denied"),
        )
        .expect_err("external mover should fail");

        let rendered = format!("{error:#}");
        assert!(rendered.contains("failed to move"));
        assert!(rendered.contains("permission denied"));
        assert!(source.exists());
        assert!(container.exists());
        let _ = fs::remove_dir_all(&base);
    }

    fn unique_test_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic enough for tests")
            .as_nanos();
        std::env::temp_dir().join(format!("telegram_downloader_{prefix}_{nanos}"))
    }

    fn test_config(base: &Path) -> AppConfig {
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
            reply_on_duplicate: true,
        }
    }

    fn copy_dir(source: &Path, destination: &Path) -> Result<(), std::io::Error> {
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

    fn remove_source_files(path: &Path) -> Result<(), std::io::Error> {
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
}
