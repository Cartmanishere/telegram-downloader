use anyhow::{anyhow, Context, Result};
use dotenvy::from_path;
use grammers_client::client::files::{DownloadIter, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use grammers_client::types::update::Message as UpdateMessage;
use grammers_client::types::{Media, Message};
use grammers_client::{Client, Update, UpdatesConfiguration};
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;
use log::{error, info};
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::{self as tokio_fs, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Semaphore};

const PROGRESS_EDIT_INTERVAL_SECONDS: u64 = 10;
const MIN_PROGRESS_LOG_INCREMENT_MB: f64 = 1.0;
const TERMINAL_LINE_WIDTH: usize = 140;
const PROGRESS_BAR_WIDTH: usize = 10;

static TERMINAL_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[derive(Clone)]
struct AppConfig {
    api_id: i32,
    api_hash: String,
    bot_token: String,
    session_path: PathBuf,
    download_dir: PathBuf,
    max_concurrent_downloads: usize,
    parallel_chunk_downloads: usize,
    chunk_size: usize,
    reply_on_duplicate: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ActiveDownloadKey {
    path: PathBuf,
    size: Option<u64>,
}

#[derive(Clone)]
struct DownloadCandidate {
    filename: String,
    file_size: Option<u64>,
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

struct AppState {
    config: AppConfig,
    client: Client,
    semaphore: Arc<Semaphore>,
    active_downloads: Arc<Mutex<HashSet<ActiveDownloadKey>>>,
}

#[derive(Default)]
struct ProgressState {
    completed_bytes: u64,
    segment_progress: Vec<u64>,
    last_logged_mb: f64,
    terminal_status_text: String,
    telegram_status_text: String,
}

#[derive(Clone)]
struct ProgressTracker {
    filename: String,
    total_size: Option<u64>,
    started_at: Instant,
    inner: Arc<Mutex<ProgressState>>,
}

impl ProgressTracker {
    fn new(filename: String, total_size: Option<u64>) -> Self {
        Self {
            filename,
            total_size,
            started_at: Instant::now(),
            inner: Arc::new(Mutex::new(ProgressState {
                completed_bytes: 0,
                segment_progress: Vec::new(),
                last_logged_mb: -1.0,
                terminal_status_text: String::new(),
                telegram_status_text: String::new(),
            })),
        }
    }

    async fn set_completed_bytes(&self, completed_bytes: u64) {
        let mut inner = self.inner.lock().await;
        inner.completed_bytes = completed_bytes;
        let downloaded = inner.completed_bytes + inner.segment_progress.iter().sum::<u64>();
        inner.terminal_status_text = self.format_terminal_progress_text(downloaded);
        inner.telegram_status_text = self.format_telegram_progress_text(downloaded);
    }

    async fn log_snapshot(&self) -> Result<()> {
        let text = {
            let mut inner = self.inner.lock().await;
            let downloaded = inner.completed_bytes + inner.segment_progress.iter().sum::<u64>();
            inner.terminal_status_text = self.format_terminal_progress_text(downloaded);
            inner.telegram_status_text = self.format_telegram_progress_text(downloaded);
            inner.terminal_status_text.clone()
        };
        render_terminal_line(&text).await
    }

    async fn update(&self, segment_index: usize, bytes_downloaded: u64) -> Result<()> {
        let maybe_text = {
            let mut inner = self.inner.lock().await;
            if inner.segment_progress.len() <= segment_index {
                inner.segment_progress.resize(segment_index + 1, 0);
            }
            inner.segment_progress[segment_index] = bytes_downloaded;

            let downloaded = inner.completed_bytes + inner.segment_progress.iter().sum::<u64>();
            inner.terminal_status_text = self.format_terminal_progress_text(downloaded);
            inner.telegram_status_text = self.format_telegram_progress_text(downloaded);
            let downloaded_mb = downloaded as f64 / (1024.0 * 1024.0);
            if inner.last_logged_mb >= 0.0
                && (downloaded_mb - inner.last_logged_mb) < MIN_PROGRESS_LOG_INCREMENT_MB
            {
                None
            } else {
                inner.last_logged_mb = downloaded_mb;
                Some(inner.terminal_status_text.clone())
            }
        };

        if let Some(text) = maybe_text {
            render_terminal_line(&text).await?;
        }
        Ok(())
    }

    async fn mark_segment_complete(&self, segment_index: usize, segment_size: u64) -> Result<()> {
        let text = {
            let mut inner = self.inner.lock().await;
            if inner.segment_progress.len() <= segment_index {
                inner.segment_progress.resize(segment_index + 1, 0);
            }
            inner.segment_progress[segment_index] = segment_size;
            let downloaded = inner.completed_bytes + inner.segment_progress.iter().sum::<u64>();
            inner.terminal_status_text = self.format_terminal_progress_text(downloaded);
            inner.telegram_status_text = self.format_telegram_progress_text(downloaded);
            inner.last_logged_mb = downloaded as f64 / (1024.0 * 1024.0);
            inner.terminal_status_text.clone()
        };
        render_terminal_line(&text).await
    }

    async fn current_status_text(&self) -> String {
        let inner = self.inner.lock().await;
        inner.telegram_status_text.clone()
    }

    fn format_terminal_progress_text(&self, downloaded: u64) -> String {
        let summary = self.format_progress_summary(downloaded);
        format!("{}: {}", self.filename, summary)
    }

    fn format_telegram_progress_text(&self, downloaded: u64) -> String {
        self.format_progress_summary(downloaded)
    }

    fn format_progress_summary(&self, downloaded: u64) -> String {
        let downloaded_mb = downloaded as f64 / (1024.0 * 1024.0);
        let elapsed_secs = self.started_at.elapsed().as_secs_f64().max(1.0);
        let speed_mb_s = downloaded_mb / elapsed_secs;
        if let Some(total_size) = self.total_size.filter(|size| *size > 0) {
            let total_mb = total_size as f64 / (1024.0 * 1024.0);
            let percent = (downloaded as f64 / total_size as f64 * 100.0).min(100.0);
            let filled = ((downloaded as f64 / total_size as f64) * PROGRESS_BAR_WIDTH as f64)
                .round() as usize;
            let filled = filled.min(PROGRESS_BAR_WIDTH);
            let progress_bar = format!(
                "{}{}",
                "█".repeat(filled),
                "░".repeat(PROGRESS_BAR_WIDTH.saturating_sub(filled))
            );
            let eta = if speed_mb_s > 0.0 && downloaded < total_size {
                let remaining_mb = (total_size - downloaded) as f64 / (1024.0 * 1024.0);
                format_duration(Duration::from_secs_f64(remaining_mb / speed_mb_s))
            } else {
                "0s".to_string()
            };
            format!(
                "[{}] {:.2}/{:.2} MB ({:.1}%) | {:.2} MB/s | ETA {}",
                progress_bar, downloaded_mb, total_mb, percent, speed_mb_s, eta
            )
        } else {
            format!("{:.2} MB | {:.2} MB/s", downloaded_mb, speed_mb_s)
        }
    }
}

fn format_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m {seconds}s")
    } else {
        format!("{seconds}s")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config_base_dir = load_runtime_env()?.unwrap_or(env::current_dir().context("failed to resolve current working directory")?);
    configure_logging();

    let config = AppConfig::from_env(&config_base_dir)?;
    let session = Arc::new(
        SqliteSession::open(&config.session_path)
            .with_context(|| format!("failed to open session {}", config.session_path.display()))?,
    );
    let pool = SenderPool::new(Arc::clone(&session), config.api_id);
    let client = Client::new(&pool);
    let SenderPool {
        runner, updates, ..
    } = pool;
    let _runner_handle = tokio::spawn(runner.run());
    authorize_client(&client, &config).await?;
    let mut update_stream = client.stream_updates(
        updates,
        UpdatesConfiguration {
            catch_up: true,
            ..Default::default()
        },
    );

    let me = client.get_me().await?;
    let login_label = me
        .username()
        .map(str::to_owned)
        .unwrap_or_else(|| me.raw.id().to_string());
    info!("Logged in as {}", login_label);
    info!(
        "Download settings: workers={} segment_workers={} chunk_size={:.2} MB",
        config.max_concurrent_downloads,
        config.parallel_chunk_downloads,
        config.chunk_size as f64 / (1024.0 * 1024.0)
    );
    info!("Listening for incoming Telegram messages with downloadable media");

    let state = Arc::new(AppState {
        semaphore: Arc::new(Semaphore::new(config.max_concurrent_downloads)),
        active_downloads: Arc::new(Mutex::new(HashSet::new())),
        config,
        client: client.clone(),
    });

    loop {
        let update = update_stream.next().await?;
        if let Update::NewMessage(message) = update {
            if message.outgoing() {
                continue;
            }
            if !matches!(message.media(), Some(Media::Photo(_)) | Some(Media::Document(_))) {
                continue;
            }

            let state = Arc::clone(&state);
            tokio::spawn(async move {
                if let Err(err) = process_message(state, message).await {
                    error!("message processing failed: {err:#}");
                }
            });
        }
    }
}

fn load_runtime_env() -> Result<Option<PathBuf>> {
    let current_dir = env::current_dir().context("failed to resolve current working directory")?;
    let current_env_path = current_dir.join(".env");
    if current_env_path.exists() {
        from_path(&current_env_path)
            .with_context(|| format!("failed to load environment from {}", current_env_path.display()))?;
        return Ok(Some(current_dir));
    }

    let exe_dir = env::current_exe()
        .context("failed to resolve executable path")?
        .parent()
        .map(Path::to_path_buf);
    if let Some(exe_dir) = exe_dir {
        let exe_env_path = exe_dir.join(".env");
        if exe_env_path.exists() {
            from_path(&exe_env_path)
                .with_context(|| format!("failed to load environment from {}", exe_env_path.display()))?;
            return Ok(Some(exe_dir));
        }
    }

    Ok(None)
}

impl AppConfig {
    fn from_env(script_dir: &Path) -> Result<Self> {
        let api_id = env::var("TELEGRAM_API_ID")
            .context("TELEGRAM_API_ID is required")?
            .parse::<i32>()
            .context("TELEGRAM_API_ID must be an integer")?;
        let api_hash = env::var("TELEGRAM_API_HASH").context("TELEGRAM_API_HASH is required")?;
        let bot_token =
            env::var("TELEGRAM_BOT_TOKEN").context("TELEGRAM_BOT_TOKEN is required")?;
        let session_path = resolve_config_path(
            script_dir,
            &env::var("TELEGRAM_SESSION_NAME").unwrap_or_else(|_| "telegram_downloader.session".to_string()),
        );
        let download_dir = resolve_config_path(
            script_dir,
            &env::var("DOWNLOAD_DIR").unwrap_or_else(|_| "./downloads-mtproto".to_string()),
        );
        fs::create_dir_all(&download_dir)
            .with_context(|| format!("failed to create download dir {}", download_dir.display()))?;

        Ok(Self {
            api_id,
            api_hash,
            bot_token,
            session_path,
            download_dir,
            max_concurrent_downloads: env_usize("MAX_CONCURRENT_DOWNLOADS", 2).max(1),
            parallel_chunk_downloads: env_usize("PARALLEL_CHUNK_DOWNLOADS", 4).max(1),
            chunk_size: chunk_size_from_env(),
            reply_on_duplicate: env_bool("REPLY_ON_DUPLICATE", true),
        })
    }
}

async fn authorize_client(client: &Client, config: &AppConfig) -> Result<()> {
    if !client.is_authorized().await? {
        client
            .bot_sign_in(&config.bot_token, &config.api_hash)
            .await
            .context("bot sign-in failed")?;
    }
    Ok(())
}

async fn process_message(state: Arc<AppState>, message: UpdateMessage) -> Result<()> {
    let media = message
        .media()
        .ok_or_else(|| anyhow!("message did not contain downloadable media"))?;
    let candidate = build_candidate(&message, &media);
    let target_path = choose_target_path(&state.config.download_dir, &candidate.filename, candidate.file_size)?;
    let key = ActiveDownloadKey {
        path: target_path.clone(),
        size: candidate.file_size,
    };

    {
        let mut active = state.active_downloads.lock().await;
        if active.contains(&key) {
            if state.config.reply_on_duplicate {
                let _ = message
                    .reply(format!("Download is already in progress for {}.", target_path.file_name().unwrap().to_string_lossy()))
                    .await;
            }
            return Ok(());
        }

        if target_path.exists() && target_matches(&target_path, candidate.file_size)? {
            if state.config.reply_on_duplicate {
                let _ = message
                    .reply(format!("Download already exists for {}.", target_path.file_name().unwrap().to_string_lossy()))
                    .await;
            }
            return Ok(());
        }

        active.insert(key.clone());
    }

    let permit = state.semaphore.clone().acquire_owned().await?;
    let filename = target_path
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| candidate.filename.clone());
    let tracker = ProgressTracker::new(filename.clone(), candidate.file_size);
    let progress_message = message.reply(format!("Starting download: {filename}")).await.ok();
    let updater_handle = tokio::spawn(progress_message_updater(
        progress_message.clone(),
        tracker.clone(),
    ));

    let result = download_with_resume(
        state.client.clone(),
        state.config.clone(),
        media,
        &target_path,
        candidate.file_size,
        tracker.clone(),
    )
    .await;

    drop(permit);
    clear_terminal_line().await?;
    updater_handle.abort();

    match result {
        Ok(()) => {
            if !target_path.exists() {
                return Err(anyhow!("download did not create the target file"));
            }
            if let Some(expected_size) = candidate.file_size {
                let actual = fs::metadata(&target_path)?.len();
                if actual != expected_size {
                    return Err(anyhow!(
                        "size mismatch after transfer: expected={expected_size} actual={actual}"
                    ));
                }
            }
            info!("Download complete for {}", filename);
            if let Some(progress_message) = progress_message.clone() {
                let _ = progress_message.edit(format!("Download complete: {filename}")).await;
            }
            let _ = message.reply(format!("Download complete: {filename}")).await;
        }
        Err(err) => {
            error!("Download failed for {}: {err:#}", filename);
            if let Some(progress_message) = progress_message.clone() {
                let _ = progress_message
                    .edit(format!("Download failed for {filename}: {err}"))
                    .await;
            }
            let _ = message
                .reply(format!("Download failed for {filename}: {err}"))
                .await;
        }
    }

    let mut active = state.active_downloads.lock().await;
    active.remove(&key);
    Ok(())
}

async fn progress_message_updater(progress_message: Option<Message>, tracker: ProgressTracker) {
    let Some(progress_message) = progress_message else {
        return;
    };

    let mut last_sent_text = String::new();
    loop {
        tokio::time::sleep(Duration::from_secs(PROGRESS_EDIT_INTERVAL_SECONDS)).await;
        let current_text = format!("Download in progress: {}", tracker.current_status_text().await);
        if current_text == last_sent_text {
            continue;
        }
        if progress_message.edit(current_text.clone()).await.is_err() {
            return;
        }
        last_sent_text = current_text;
    }
}

async fn download_with_resume(
    client: Client,
    config: AppConfig,
    media: Media,
    target_path: &Path,
    expected_size: Option<u64>,
    tracker: ProgressTracker,
) -> Result<()> {
    let Some(total_size) = expected_size.filter(|size| *size > 0) else {
        return download_single_stream(client, config.chunk_size, media, target_path, expected_size, tracker).await;
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
        tracker.mark_segment_complete(plan.index, plan.size()).await?;
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

    tracker.mark_segment_complete(plan.index, plan.size()).await?;
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

fn build_candidate(message: &Message, media: &Media) -> DownloadCandidate {
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

fn choose_target_path(download_dir: &Path, filename: &str, expected_size: Option<u64>) -> Result<PathBuf> {
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

fn target_matches(path: &Path, expected_size: Option<u64>) -> Result<bool> {
    if !path.is_file() {
        return Ok(false);
    }
    match expected_size {
        Some(size) => Ok(fs::metadata(path)?.len() == size),
        None => Ok(true),
    }
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

fn resolve_config_path(script_dir: &Path, raw_path: &str) -> PathBuf {
    let path = PathBuf::from(raw_path);
    if path.is_absolute() {
        path
    } else {
        script_dir.join(path)
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

async fn render_terminal_line(text: &str) -> Result<()> {
    let _lock = TERMINAL_LOCK.lock().await;
    let trimmed: String = text.chars().take(TERMINAL_LINE_WIDTH).collect();
    print!("\r{trimmed:width$}", width = TERMINAL_LINE_WIDTH);
    io::stdout().flush()?;
    Ok(())
}

async fn clear_terminal_line() -> Result<()> {
    let _lock = TERMINAL_LOCK.lock().await;
    print!("\r{space:width$}\r", space = "", width = TERMINAL_LINE_WIDTH);
    io::stdout().flush()?;
    Ok(())
}

fn configure_logging() {
    let mut builder = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string())),
    );
    builder.format_timestamp_secs();
    builder.init();
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}
