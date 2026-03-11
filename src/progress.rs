use anyhow::Result;
use once_cell::sync::Lazy;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const MIN_PROGRESS_LOG_INCREMENT_MB: f64 = 1.0;
const TERMINAL_LINE_WIDTH: usize = 140;
const PROGRESS_BAR_WIDTH: usize = 10;

static TERMINAL_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[derive(Default)]
struct ProgressState {
    completed_bytes: u64,
    segment_progress: Vec<u64>,
    last_logged_mb: f64,
    terminal_status_text: String,
    telegram_status_text: String,
}

/// Tracks progress for one file and formats that progress for both terminal and Telegram updates.
#[derive(Clone)]
pub struct ProgressTracker {
    filename: String,
    total_size: Option<u64>,
    started_at: Instant,
    inner: Arc<Mutex<ProgressState>>,
}

impl ProgressTracker {
    pub fn new(filename: String, total_size: Option<u64>) -> Self {
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

    pub async fn set_completed_bytes(&self, completed_bytes: u64) {
        let mut inner = self.inner.lock().await;
        inner.completed_bytes = completed_bytes;
        let downloaded = inner.completed_bytes + inner.segment_progress.iter().sum::<u64>();
        inner.terminal_status_text = self.format_terminal_progress_text(downloaded);
        inner.telegram_status_text = self.format_telegram_progress_text(downloaded);
    }

    pub async fn log_snapshot(&self) -> Result<()> {
        let text = {
            let mut inner = self.inner.lock().await;
            let downloaded = inner.completed_bytes + inner.segment_progress.iter().sum::<u64>();
            inner.terminal_status_text = self.format_terminal_progress_text(downloaded);
            inner.telegram_status_text = self.format_telegram_progress_text(downloaded);
            inner.terminal_status_text.clone()
        };
        render_terminal_line(&text).await
    }

    pub async fn update(&self, segment_index: usize, bytes_downloaded: u64) -> Result<()> {
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

    pub async fn mark_segment_complete(
        &self,
        segment_index: usize,
        segment_size: u64,
    ) -> Result<()> {
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

    pub async fn current_status_text(&self) -> String {
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

pub async fn render_terminal_line(text: &str) -> Result<()> {
    let _lock = TERMINAL_LOCK.lock().await;
    let trimmed: String = text.chars().take(TERMINAL_LINE_WIDTH).collect();
    print!("\r{trimmed:width$}", width = TERMINAL_LINE_WIDTH);
    io::stdout().flush()?;
    Ok(())
}

pub async fn clear_terminal_line() -> Result<()> {
    let _lock = TERMINAL_LOCK.lock().await;
    print!("\r{space:width$}\r", space = "", width = TERMINAL_LINE_WIDTH);
    io::stdout().flush()?;
    Ok(())
}
