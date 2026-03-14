use crate::config::AppConfig;
use crate::download::{
    classify_download_request, download_external, download_with_resume, is_download_cancelled,
    move_classified_target, resolve_mega_payload, ActiveDownloadKey, ClassificationTarget,
    DownloadCancellation, DownloadRequest, LibraryKind,
};
use crate::progress::{clear_terminal_line, ProgressTracker};
use anyhow::{anyhow, Context, Result};
use grammers_client::grammers_tl_types as tl;
use grammers_client::types::update::CallbackQuery;
use grammers_client::types::update::Message as UpdateMessage;
use grammers_client::types::Message;
use grammers_client::{button, reply_markup, Client, InputMessage, Update, UpdatesConfiguration};
use grammers_mtsender::InvocationError;
use grammers_mtsender::SenderPool;
use grammers_session::defs::PeerId;
use grammers_session::storages::SqliteSession;
use log::{error, info, warn};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

const PROGRESS_EDIT_INTERVAL_SECONDS: u64 = 10;
const UPDATE_STREAM_RETRY_DELAY_SECONDS: u64 = 3;

/// Shared runtime state used by every spawned message handler.
struct AppState {
    config: AppConfig,
    client: Client,
    semaphore: Arc<Semaphore>,
    active_downloads: Arc<Mutex<HashSet<ActiveDownloadKey>>>,
    active_download_controls: Arc<Mutex<HashMap<u64, ActiveDownloadControl>>>,
    pending_classifications: Arc<Mutex<HashMap<u64, PendingClassification>>>,
    next_download_id: AtomicU64,
    next_classification_id: AtomicU64,
}

#[derive(Clone)]
struct ActiveDownloadControl {
    original_sender_id: PeerId,
    display_name: String,
    cancellation: DownloadCancellation,
}

#[derive(Clone)]
struct PendingClassification {
    original_sender_id: PeerId,
    display_name: String,
    target: ClassificationTarget,
    prompt_message: Message,
    prompt_message_id: i32,
}

#[derive(Clone, Copy)]
enum ClassificationChoice {
    Movie,
    TvShow,
    Anime,
}

enum CallbackAction {
    CancelDownload(u64),
    Classification(u64, ClassificationChoice),
}

impl ClassificationChoice {
    fn from_callback_data(data: &[u8]) -> Option<(u64, Self)> {
        let raw = std::str::from_utf8(data).ok()?;
        let (prefix, job_id, kind) = raw.split_once(':').and_then(|(prefix, rest)| {
            let (job_id, kind) = rest.split_once(':')?;
            Some((prefix, job_id, kind))
        })?;
        if prefix != "classify" {
            return None;
        }

        let job_id = job_id.parse().ok()?;
        let kind = match kind {
            "movie" => Self::Movie,
            "tv" => Self::TvShow,
            "anime" => Self::Anime,
            _ => return None,
        };
        Some((job_id, kind))
    }

    fn from_text(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "movie" => Some(Self::Movie),
            "tv show" | "tv" | "show" => Some(Self::TvShow),
            "anime" => Some(Self::Anime),
            _ => None,
        }
    }

    fn to_library_kind(self) -> LibraryKind {
        match self {
            Self::Movie => LibraryKind::Movie,
            Self::TvShow => LibraryKind::TvShow,
            Self::Anime => LibraryKind::Anime,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Movie => "Movie",
            Self::TvShow => "TV Show",
            Self::Anime => "Anime",
        }
    }
}

fn utf16_len(text: &str) -> i32 {
    text.encode_utf16().count() as i32
}

fn code_message(prefix: &str, name: &str, suffix: &str) -> InputMessage {
    InputMessage::new()
        .text(format!("{prefix}{name}{suffix}"))
        .fmt_entities(vec![tl::types::MessageEntityCode {
            offset: utf16_len(prefix),
            length: utf16_len(name),
        }
        .into()])
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
        active_download_controls: Arc::new(Mutex::new(HashMap::new())),
        pending_classifications: Arc::new(Mutex::new(HashMap::new())),
        next_download_id: AtomicU64::new(1),
        next_classification_id: AtomicU64::new(1),
        config,
        client: client.clone(),
    });

    loop {
        let update = match update_stream.next().await {
            Ok(update) => update,
            Err(err) => {
                error!("update stream failed, retrying in {UPDATE_STREAM_RETRY_DELAY_SECONDS}s: {err:#}");
                tokio::time::sleep(Duration::from_secs(UPDATE_STREAM_RETRY_DELAY_SECONDS)).await;
                continue;
            }
        };
        match update {
            Update::NewMessage(message) => {
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
            Update::CallbackQuery(query) => {
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(err) = process_callback_query(state, query).await {
                        error!("callback query processing failed: {err:#}");
                    }
                });
            }
            _ => {}
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
    if try_handle_text_classification_reply(&state, &message).await? {
        return Ok(());
    }

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
                    .reply(code_message(
                        "⏳ Download already in progress for ",
                        &target_path.file_name().unwrap().to_string_lossy(),
                        ".",
                    ))
                    .await;
            }
            return Ok(());
        }

        if request.already_exists()? {
            if state.config.reply_on_duplicate {
                let _ = message
                    .reply(code_message(
                        "📁 Download already exists for ",
                        &target_path.file_name().unwrap().to_string_lossy(),
                        ".",
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
    let request_for_classification = request.clone();
    let cancellation = DownloadCancellation::default();
    let download_id = state.next_download_id.fetch_add(1, Ordering::Relaxed);
    let original_sender_id = message.sender().map(|sender| sender.id());
    let progress_message = message
        .reply(progress_status_message(
            &format!("⬇️ Starting download: {filename}"),
            Some(download_id),
        ))
        .await
        .ok();
    if let Some(original_sender_id) = original_sender_id {
        state.active_download_controls.lock().await.insert(
            download_id,
            ActiveDownloadControl {
                original_sender_id,
                display_name: filename.clone(),
                cancellation: cancellation.clone(),
            },
        );
    }
    let updater_handle = tokio::spawn(progress_message_updater(
        progress_message.clone(),
        tracker.clone(),
        download_id,
        cancellation.clone(),
    ));

    let result = execute_download_request(
        state.client.clone(),
        state.config.clone(),
        request,
        tracker.clone(),
        cancellation.clone(),
    )
    .await;

    drop(permit);
    clear_terminal_line().await?;
    updater_handle.abort();
    state
        .active_download_controls
        .lock()
        .await
        .remove(&download_id);

    match result {
        Ok(final_target_path) => {
            if !final_target_path.exists() {
                return Err(anyhow!("download did not create the target path"));
            }
            if let Some(expected_size) = expected_size {
                let actual = fs::metadata(&final_target_path)?.len();
                if actual != expected_size {
                    return Err(anyhow!(
                        "size mismatch after transfer: expected={expected_size} actual={actual}"
                    ));
                }
            }
            info!("Download complete for {}", filename);
            if let Some(progress_message) = progress_message.clone() {
                let _ = progress_message
                    .edit(code_message("✅ Download complete: ", &filename, ""))
                    .await;
            }
            let completion_message = message
                .reply(code_message("✅ Download complete: ", &filename, ""))
                .await;
            let classification_target = match request_for_classification {
                DownloadRequest::ExternalLink {
                    source: crate::download::ExternalSource::Mega { .. },
                    ..
                } => resolve_mega_payload(&final_target_path),
                _ => request_for_classification.resolve_classification_target(),
            };
            match classification_target {
                Ok(target) => {
                    let prompt_target_name = target
                        .source_path
                        .file_name()
                        .map(|value| value.to_string_lossy().into_owned())
                        .unwrap_or_else(|| filename.clone());
                    let prompt_reply_target = completion_message.as_ref().unwrap_or(&message);
                    if let Some(original_sender_id) = message.sender().map(|sender| sender.id()) {
                        if let Err(err) = queue_classification_prompt(
                            &state,
                            prompt_reply_target,
                            original_sender_id,
                            prompt_target_name,
                            target,
                        )
                        .await
                        {
                            error!(
                                "failed to send classification prompt for {}: {err:#}",
                                filename
                            );
                            let _ = message
                                .reply(code_message(
                                    "⚠️ Download complete, but classification prompt failed for ",
                                    &filename,
                                    &format!(": {err}"),
                                ))
                                .await;
                        }
                    } else {
                        error!("unable to determine original sender for {}", filename);
                        let _ = message
                            .reply(code_message(
                                "⚠️ Download complete, but classification is unavailable for ",
                                &filename,
                                ": unable to determine sender",
                            ))
                            .await;
                    }
                }
                Err(err) => {
                    error!(
                        "failed to resolve classification target for {}: {err:#}",
                        filename
                    );
                    let _ = message
                        .reply(code_message(
                            "⚠️ Download complete, but classification is unavailable for ",
                            &filename,
                            &format!(": {err}"),
                        ))
                        .await;
                }
            }
        }
        Err(err) => {
            if is_download_cancelled(&err) {
                info!("Download cancelled for {}", filename);
                if let Some(progress_message) = progress_message.clone() {
                    let _ = progress_message
                        .edit(code_message("🛑 Download cancelled: ", &filename, ""))
                        .await;
                }
                let _ = message
                    .reply(code_message("🛑 Download cancelled: ", &filename, ""))
                    .await;
            } else {
                error!("Download failed for {}: {err:#}", filename);
                if let Some(progress_message) = progress_message.clone() {
                    let _ = progress_message
                        .edit(code_message(
                            "❌ Download failed for ",
                            &filename,
                            &format!(": {err}"),
                        ))
                        .await;
                }
                let _ = message
                    .reply(code_message(
                        "❌ Download failed for ",
                        &filename,
                        &format!(": {err}"),
                    ))
                    .await;
            }
        }
    }

    let mut active = state.active_downloads.lock().await;
    active.remove(&key);
    Ok(())
}

async fn queue_classification_prompt(
    state: &Arc<AppState>,
    reply_target: &Message,
    original_sender_id: PeerId,
    display_name: String,
    target: ClassificationTarget,
) -> Result<()> {
    let job_id = state.next_classification_id.fetch_add(1, Ordering::Relaxed);
    let markup = reply_markup::inline(vec![vec![
        button::inline("TV Show", format!("classify:{job_id}:tv").into_bytes()),
        button::inline("Movie", format!("classify:{job_id}:movie").into_bytes()),
        button::inline("Anime", format!("classify:{job_id}:anime").into_bytes()),
    ]]);
    let prompt_message = reply_target
        .reply(
            code_message(
                "🗂️ Classify ",
                &display_name,
                " as TV Show, Movie, or Anime.\nYou can also reply with `TV Show`, `Movie`, or `Anime`.",
            )
                .reply_markup(&markup),
        )
        .await?;

    let pending = PendingClassification {
        original_sender_id,
        display_name,
        target,
        prompt_message: prompt_message.clone(),
        prompt_message_id: prompt_message.id(),
    };
    state
        .pending_classifications
        .lock()
        .await
        .insert(job_id, pending);
    Ok(())
}

async fn try_handle_text_classification_reply(
    state: &Arc<AppState>,
    message: &UpdateMessage,
) -> Result<bool> {
    let Some(choice) = ClassificationChoice::from_text(message.text()) else {
        return Ok(false);
    };
    let Some(reply_to_message_id) = message.reply_to_message_id() else {
        return Ok(false);
    };
    let Some(sender_id) = message.sender().map(|sender| sender.id()) else {
        return Ok(false);
    };

    let job_id = {
        let pending = state.pending_classifications.lock().await;
        pending.iter().find_map(|(job_id, job)| {
            (job.prompt_message_id == reply_to_message_id).then_some(*job_id)
        })
    };
    let Some(job_id) = job_id else {
        return Ok(false);
    };

    complete_classification_job(state, job_id, choice, sender_id, Some(message)).await?;
    Ok(true)
}

async fn process_callback_query(state: Arc<AppState>, query: CallbackQuery) -> Result<()> {
    let Some(action) = callback_action_from_data(query.data()) else {
        handle_callback_answer_result(query.answer().text("Unknown action.").send().await)?;
        return Ok(());
    };
    match action {
        CallbackAction::CancelDownload(download_id) => {
            let response =
                request_download_cancellation(&state, download_id, query.sender().id()).await?;
            if response.toast == "🛑 Cancellation requested." {
                handle_callback_answer_result(
                    query.answer().text(response.toast).edit(progress_status_message(
                        &format!("🛑 Cancelling download: {}", response.display_name),
                        Some(download_id),
                    ))
                    .await,
                )
                ?;
            } else {
                handle_callback_answer_result(query.answer().text(response.toast).send().await)?;
            }
        }
        CallbackAction::Classification(job_id, choice) => {
            let sender_id = query.sender().id();
            handle_callback_answer_result(query.answer().text("🚚 Starting move...").send().await)?;
            complete_classification_job(&state, job_id, choice, sender_id, None).await?;
        }
    }
    Ok(())
}

fn handle_callback_answer_result(result: std::result::Result<(), InvocationError>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(err) if is_stale_callback_answer_error(&err) => {
            warn!("ignoring stale callback answer: {err:#}");
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}

struct CancelRequestResponse {
    toast: &'static str,
    display_name: String,
}

async fn request_download_cancellation(
    state: &Arc<AppState>,
    download_id: u64,
    sender_id: PeerId,
) -> Result<CancelRequestResponse> {
    let control = {
        let active = state.active_download_controls.lock().await;
        active.get(&download_id).cloned()
    };
    let Some(control) = control else {
        return Ok(CancelRequestResponse {
            toast: "⌛ That download is no longer active.",
            display_name: "download".to_string(),
        });
    };
    if control.original_sender_id != sender_id {
        return Ok(CancelRequestResponse {
            toast: "🔒 Only the original sender can cancel this download.",
            display_name: control.display_name,
        });
    }
    control.cancellation.cancel();
    Ok(CancelRequestResponse {
        toast: "🛑 Cancellation requested.",
        display_name: control.display_name,
    })
}

async fn complete_classification_job(
    state: &Arc<AppState>,
    job_id: u64,
    choice: ClassificationChoice,
    sender_id: PeerId,
    reply_message: Option<&UpdateMessage>,
) -> Result<&'static str> {
    let pending = {
        let pending_map = state.pending_classifications.lock().await;
        pending_map.get(&job_id).cloned()
    };
    let Some(pending) = pending else {
        if let Some(message) = reply_message {
            let _ = message
                .reply("⌛ That classification request is no longer active.")
                .await;
        }
        return Ok("⌛ That request is no longer active.");
    };

    if pending.original_sender_id != sender_id {
        if let Some(message) = reply_message {
            let _ = message
                .reply("🔒 Only the original sender can classify this download.")
                .await;
        }
        return Ok("🔒 Only the original sender can classify this download.");
    }

    let starting_message = code_message(
        "🚚 Starting move: ",
        &pending.display_name,
        &format!(" -> {}", choice.label()),
    );
    info!(
        "Starting move for {} to {} from {}",
        pending.display_name,
        choice.label(),
        pending.target.source_path.display()
    );
    send_classification_status(state, &pending, starting_message).await;

    let config = state.config.clone();
    let target = pending.target.clone();
    let library_kind = choice.to_library_kind();
    match tokio::task::spawn_blocking(move || {
        move_classified_target(&config, &target, library_kind)
    })
    .await? {
        Ok(destination_path) => {
            state.pending_classifications.lock().await.remove(&job_id);
            info!(
                "Move complete for {} to {}",
                pending.display_name,
                destination_path.display()
            );
            let completion_message = code_message(
                "📦 Move complete: ",
                &pending.display_name,
                &format!(" -> {}", destination_path.display()),
            );
            send_classification_status(state, &pending, completion_message).await;
            hide_classification_prompt(state, &pending).await;
            Ok("📦 Move completed.")
        }
        Err(err) => {
            state.pending_classifications.lock().await.remove(&job_id);
            error!("Move failed for {}: {err:#}", pending.display_name);
            let failure_message = code_message(
                "❌ Move failed for ",
                &pending.display_name,
                &format!(": {err}"),
            );
            send_classification_status(state, &pending, failure_message).await;
            hide_classification_prompt(state, &pending).await;
            Ok("❌ Move failed.")
        }
    }
}

fn is_stale_callback_answer_error(err: &InvocationError) -> bool {
    matches!(err, InvocationError::Rpc(rpc_error) if rpc_error.code == 400 && rpc_error.name == "QUERY_ID_INVALID")
}

async fn send_classification_status(
    _state: &Arc<AppState>,
    pending: &PendingClassification,
    message: InputMessage,
) {
    let _ = pending.prompt_message.reply(message).await;
}

async fn hide_classification_prompt(_state: &Arc<AppState>, pending: &PendingClassification) {
    let _ = pending
        .prompt_message
        .edit(code_message(
            "✅ Classification handled for ",
            &pending.display_name,
            ".",
        ))
        .await;
}

async fn execute_download_request(
    client: Client,
    config: AppConfig,
    request: DownloadRequest,
    tracker: ProgressTracker,
    cancellation: DownloadCancellation,
) -> Result<std::path::PathBuf> {
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
                cancellation,
            )
            .await?;
            Ok(target_path)
        }
        DownloadRequest::ExternalLink {
            source,
            target_path,
            ..
        } => download_external(config, source, &target_path, tracker, cancellation).await,
    }
}

async fn progress_message_updater(
    progress_message: Option<Message>,
    tracker: ProgressTracker,
    download_id: u64,
    cancellation: DownloadCancellation,
) {
    let Some(progress_message) = progress_message else {
        return;
    };

    let mut last_sent_text = String::new();
    loop {
        tokio::time::sleep(Duration::from_secs(PROGRESS_EDIT_INTERVAL_SECONDS)).await;
        if cancellation.is_cancelled() {
            return;
        }
        let current_text = format!(
            "⏳ Download in progress: {}",
            tracker.current_status_text().await
        );
        if current_text == last_sent_text {
            continue;
        }
        if progress_message
            .edit(progress_status_message(&current_text, Some(download_id)))
            .await
            .is_err()
        {
            return;
        }
        last_sent_text = current_text;
    }
}

fn callback_action_from_data(data: &[u8]) -> Option<CallbackAction> {
    let raw = std::str::from_utf8(data).ok()?;
    if let Some(id) = raw.strip_prefix("cancel:") {
        return Some(CallbackAction::CancelDownload(id.parse().ok()?));
    }
    let (job_id, choice) = ClassificationChoice::from_callback_data(data)?;
    Some(CallbackAction::Classification(job_id, choice))
}

fn progress_status_message(text: &str, download_id: Option<u64>) -> InputMessage {
    let message = InputMessage::new().text(text.to_string());
    match download_id {
        Some(download_id) => {
            message.reply_markup(&reply_markup::inline(vec![vec![button::inline(
                "Cancel",
                format!("cancel:{download_id}").into_bytes(),
            )]]))
        }
        None => message,
    }
}

#[cfg(test)]
mod tests {
    use super::is_stale_callback_answer_error;
    use grammers_mtsender::{InvocationError, RpcError};

    #[test]
    fn identifies_query_id_invalid_callback_errors() {
        let err = InvocationError::Rpc(RpcError {
            code: 400,
            name: "QUERY_ID_INVALID".into(),
            value: None,
            caused_by: None,
        });

        assert!(is_stale_callback_answer_error(&err));
    }

    #[test]
    fn ignores_other_rpc_errors() {
        let err = InvocationError::Rpc(RpcError {
            code: 400,
            name: "MESSAGE_NOT_MODIFIED".into(),
            value: None,
            caused_by: None,
        });

        assert!(!is_stale_callback_answer_error(&err));
    }
}
