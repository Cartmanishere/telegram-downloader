use crate::config::AppConfig;
use crate::download::{
    classify_download_request, download_external_with_aria2, download_with_resume,
    ActiveDownloadKey, DownloadRequest,
};
use crate::progress::{clear_terminal_line, ProgressTracker};
use anyhow::{anyhow, Context, Result};
use grammers_client::types::update::Message as UpdateMessage;
use grammers_client::types::Message;
use grammers_client::{Client, Update, UpdatesConfiguration};
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;
use log::{error, info};
use std::collections::HashSet;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

const PROGRESS_EDIT_INTERVAL_SECONDS: u64 = 10;

/// Shared runtime state used by every spawned message handler.
struct AppState {
    config: AppConfig,
    client: Client,
    semaphore: Arc<Semaphore>,
    active_downloads: Arc<Mutex<HashSet<ActiveDownloadKey>>>,
}

/// Authorize the Telegram client and start processing media messages forever.
pub async fn run_app(config: AppConfig) -> Result<()> {
    let session =
        Arc::new(SqliteSession::open(&config.session_path).with_context(|| {
            format!("failed to open session {}", config.session_path.display())
        })?);
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
            catch_up: false,
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
        "Download settings: workers={} segment_workers={} chunk_size={:.2} MB aria2_poll={}ms",
        config.max_concurrent_downloads,
        config.parallel_chunk_downloads,
        config.chunk_size as f64 / (1024.0 * 1024.0),
        config.aria2c_poll_interval_ms,
    );
    info!("Listening for incoming Telegram messages with media or supported links");

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

            let state = Arc::clone(&state);
            tokio::spawn(async move {
                if let Err(err) = process_message(state, message).await {
                    error!("message processing failed: {err:#}");
                }
            });
        }
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
    let Some(request) = classify_download_request(&state.config, &message)? else {
        return Ok(());
    };
    let key = request.active_key();
    let target_path = request.target_path().to_path_buf();
    let expected_size = request.expected_size();

    {
        let mut active = state.active_downloads.lock().await;
        if active.contains(&key) {
            if state.config.reply_on_duplicate {
                let _ = message
                    .reply(format!(
                        "Download is already in progress for {}.",
                        target_path.file_name().unwrap().to_string_lossy()
                    ))
                    .await;
            }
            return Ok(());
        }

        if request.already_exists()? {
            if state.config.reply_on_duplicate {
                let _ = message
                    .reply(format!(
                        "Download already exists for {}.",
                        target_path.file_name().unwrap().to_string_lossy()
                    ))
                    .await;
            }
            return Ok(());
        }

        active.insert(key.clone());
    }

    let permit = state.semaphore.clone().acquire_owned().await?;
    let filename = request.display_name();
    let tracker = ProgressTracker::new(filename.clone(), expected_size);
    let progress_message = message
        .reply(format!("Starting download: {filename}"))
        .await
        .ok();
    let updater_handle = tokio::spawn(progress_message_updater(
        progress_message.clone(),
        tracker.clone(),
    ));

    let result = execute_download_request(
        state.client.clone(),
        state.config.clone(),
        request,
        tracker.clone(),
    )
    .await;

    drop(permit);
    clear_terminal_line().await?;
    updater_handle.abort();

    match result {
        Ok(()) => {
            if !target_path.exists() {
                return Err(anyhow!("download did not create the target path"));
            }
            if let Some(expected_size) = expected_size {
                let actual = fs::metadata(&target_path)?.len();
                if actual != expected_size {
                    return Err(anyhow!(
                        "size mismatch after transfer: expected={expected_size} actual={actual}"
                    ));
                }
            }
            info!("Download complete for {}", filename);
            if let Some(progress_message) = progress_message.clone() {
                let _ = progress_message
                    .edit(format!("Download complete: {filename}"))
                    .await;
            }
            let _ = message
                .reply(format!("Download complete: {filename}"))
                .await;
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

async fn execute_download_request(
    client: Client,
    config: AppConfig,
    request: DownloadRequest,
    tracker: ProgressTracker,
) -> Result<()> {
    match request {
        DownloadRequest::TelegramMedia {
            media,
            target_path,
            candidate,
        } => {
            download_with_resume(
                client,
                config,
                media,
                &target_path,
                candidate.file_size,
                tracker,
            )
            .await
        }
        DownloadRequest::ExternalLink {
            source,
            target_path,
            ..
        } => download_external_with_aria2(config, source, &target_path, tracker).await,
    }
}

async fn progress_message_updater(progress_message: Option<Message>, tracker: ProgressTracker) {
    let Some(progress_message) = progress_message else {
        return;
    };

    let mut last_sent_text = String::new();
    loop {
        tokio::time::sleep(Duration::from_secs(PROGRESS_EDIT_INTERVAL_SECONDS)).await;
        let current_text = format!(
            "Download in progress: {}",
            tracker.current_status_text().await
        );
        if current_text == last_sent_text {
            continue;
        }
        if progress_message.edit(current_text.clone()).await.is_err() {
            return;
        }
        last_sent_text = current_text;
    }
}
