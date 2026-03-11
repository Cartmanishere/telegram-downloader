use crate::config::AppConfig;
use crate::progress::ProgressTracker;
use anyhow::{anyhow, Context, Result};
use grammers_client::client::files::DownloadIter;
use grammers_client::types::{Media, Message};
use grammers_client::Client;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::fs::{self as tokio_fs, File, OpenOptions};
use tokio::io::AsyncWriteExt;

/// Identifies a download already in progress so duplicate updates can be ignored.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ActiveDownloadKey {
    pub path: PathBuf,
    pub size: Option<u64>,
}

/// Information extracted from a Telegram message before starting a download.
#[derive(Clone)]
pub struct DownloadCandidate {
    pub filename: String,
    pub file_size: Option<u64>,
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

/// Download a file, resuming partial work and using parallel segments when the total size is known.
pub async fn download_with_resume(
    client: Client,
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

async fn download_single_stream(
    client: Client,
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
    client: Client,
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
    client: &Client,
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
