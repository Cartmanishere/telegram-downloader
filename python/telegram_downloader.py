#!/usr/bin/env python3
import asyncio
import logging
import math
import os
import re
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.client.downloads import MAX_CHUNK_SIZE
from telethon.tl.custom.message import Message


LOGGER = logging.getLogger("mtproto_downloader")
INVALID_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
SCRIPT_DIR = Path(__file__).resolve().parent
ENV_FILE = SCRIPT_DIR / ".env"
SUPPORTED_MEDIA_TYPES = ("photo", "document")
MIN_PROGRESS_LOG_INCREMENT_MB = 1.0
PROGRESS_EDIT_INTERVAL_SECONDS = 10
TERMINAL_LINE_WIDTH = 140


@dataclass(frozen=True)
class DownloadCandidate:
    message_id: int
    chat_id: int
    filename: str
    file_size: Optional[int]


@dataclass(frozen=True)
class SegmentPlan:
    index: int
    start: int
    end: int
    part_path: Path

    @property
    def size(self) -> int:
        return self.end - self.start


class ProgressTracker:
    terminal_lock = asyncio.Lock()

    def __init__(self, filename: str, total_size: Optional[int], completed_bytes: int = 0) -> None:
        self.filename = filename
        self.total_size = total_size
        self.completed_bytes = completed_bytes
        self.segment_progress: dict[int, int] = {}
        self.lock = asyncio.Lock()
        self.last_logged_mb = -1.0
        self.status_text = self._format_progress_text(completed_bytes)

    async def log_snapshot(self) -> None:
        async with self.lock:
            self.status_text = self._format_progress_text(
                self.completed_bytes + sum(self.segment_progress.values())
            )
            await self._render_terminal_line(self.status_text)

    async def update(self, segment_index: int, bytes_downloaded: int) -> None:
        async with self.lock:
            self.segment_progress[segment_index] = bytes_downloaded
            await self._log_progress_if_needed()

    async def mark_segment_complete(self, segment_index: int, segment_size: int) -> None:
        async with self.lock:
            self.segment_progress[segment_index] = segment_size
            await self._log_progress_if_needed(force=True)

    async def _log_progress_if_needed(self, force: bool = False) -> None:
        downloaded = self.completed_bytes + sum(self.segment_progress.values())
        self.status_text = self._format_progress_text(downloaded)
        downloaded_mb = downloaded / (1024 * 1024)

        if not force and self.last_logged_mb >= 0:
            if downloaded_mb - self.last_logged_mb < MIN_PROGRESS_LOG_INCREMENT_MB:
                return

        self.last_logged_mb = downloaded_mb
        await self._render_terminal_line(self.status_text)

    def current_status_text(self) -> str:
        return self.status_text

    def _format_progress_text(self, downloaded: int) -> str:
        downloaded_mb = downloaded / (1024 * 1024)
        if self.total_size and self.total_size > 0:
            total_mb = self.total_size / (1024 * 1024)
            percent = min(100.0, (downloaded / self.total_size) * 100)
            return f"{self.filename}: {downloaded_mb:.2f}/{total_mb:.2f} MB ({percent:.1f}%)"
        return f"{self.filename}: {downloaded_mb:.2f} MB"

    @classmethod
    async def _render_terminal_line(cls, text: str) -> None:
        async with cls.terminal_lock:
            trimmed = text[:TERMINAL_LINE_WIDTH]
            sys.stdout.write("\r" + trimmed.ljust(TERMINAL_LINE_WIDTH))
            sys.stdout.flush()

    @classmethod
    async def clear_terminal_line(cls) -> None:
        async with cls.terminal_lock:
            sys.stdout.write("\r" + (" " * TERMINAL_LINE_WIDTH) + "\r")
            sys.stdout.flush()


def configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def resolve_config_path(raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = SCRIPT_DIR / path
    return path.resolve()


def sanitize_filename(name: str) -> str:
    normalized = INVALID_FILENAME_CHARS.sub("_", name).strip("._")
    return normalized or "downloaded_file"


def detect_media_type(message: Message) -> Optional[str]:
    if message.photo:
        return "photo"
    if message.document:
        return "document"
    return None


def derive_filename(message: Message, media_type: str) -> str:
    if media_type == "document" and message.file and message.file.name:
        return sanitize_filename(message.file.name)

    extension = ""
    if message.file and message.file.ext:
        extension = message.file.ext

    base_name = f"{media_type}_{message.id}"
    return sanitize_filename(base_name) + extension


def choose_target_path(download_dir: Path, filename: str, expected_size: Optional[int]) -> Path:
    candidate = download_dir / filename
    if not candidate.exists():
        return candidate

    if candidate.is_file() and expected_size is not None and candidate.stat().st_size == expected_size:
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    counter = 1
    while True:
        numbered = download_dir / f"{stem}_{counter}{suffix}"
        if not numbered.exists():
            return numbered
        if expected_size is not None and numbered.stat().st_size == expected_size:
            return numbered
        counter += 1


def chunk_size_from_env() -> int:
    raw_mb = float(os.getenv("DOWNLOAD_CHUNK_SIZE_MB", "0.5"))
    requested_bytes = max(64 * 1024, int(raw_mb * 1024 * 1024))
    clamped = min(requested_bytes, MAX_CHUNK_SIZE)
    return max(4096, (clamped // 4096) * 4096)


def segment_count_from_env() -> int:
    return max(1, int(os.getenv("PARALLEL_CHUNK_DOWNLOADS", "4")))


def build_segment_plans(
    *,
    base_dir: Path,
    total_size: int,
    segment_count: int,
) -> list[SegmentPlan]:
    segment_size = math.ceil(total_size / segment_count)
    plans: list[SegmentPlan] = []
    for index in range(segment_count):
        start = index * segment_size
        end = min(total_size, start + segment_size)
        if start >= end:
            break
        plans.append(
            SegmentPlan(
                index=index,
                start=start,
                end=end,
                part_path=base_dir / f"segment_{index:03d}.part",
            )
        )
    return plans


class MTProtoDownloader:
    def __init__(self) -> None:
        api_id_raw = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        if not api_id_raw or not api_hash:
            raise RuntimeError("TELEGRAM_API_ID and TELEGRAM_API_HASH are required")

        self.api_id = int(api_id_raw)
        self.api_hash = api_hash
        self.session_name = os.getenv("TELEGRAM_SESSION_NAME", "mtproto_downloader")
        self.session_path = str(resolve_config_path(self.session_name))
        self.download_dir = resolve_config_path(os.getenv("DOWNLOAD_DIR", "./downloads-mtproto"))
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.max_concurrent_downloads = max(1, int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2")))
        self.parallel_chunk_downloads = segment_count_from_env()
        self.chunk_size = chunk_size_from_env()
        self.reply_on_duplicate = os.getenv("REPLY_ON_DUPLICATE", "true").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.client = TelegramClient(self.session_path, self.api_id, self.api_hash)
        self.semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        self.active_downloads: set[tuple[Path, Optional[int]]] = set()
        self.active_lock = asyncio.Lock()

    async def run(self) -> None:
        await self.client.start()
        me = await self.client.get_me()
        LOGGER.info(
            "Logged in as %s",
            getattr(me, "username", None) or getattr(me, "id", "unknown"),
        )
        LOGGER.info(
            "Download settings: workers=%s segment_workers=%s chunk_size=%.2f MB",
            self.max_concurrent_downloads,
            self.parallel_chunk_downloads,
            self.chunk_size / (1024 * 1024),
        )

        @self.client.on(events.NewMessage(incoming=True))
        async def on_new_message(event: events.NewMessage.Event) -> None:
            message = event.message
            media_type = detect_media_type(message)
            if media_type not in SUPPORTED_MEDIA_TYPES:
                return

            asyncio.create_task(self.process_message(message, media_type))

        LOGGER.info("Listening for incoming Telegram messages with downloadable media")
        await self.client.run_until_disconnected()

    async def process_message(self, message: Message, media_type: str) -> None:
        candidate = self.build_candidate(message, media_type)
        target_path = choose_target_path(self.download_dir, candidate.filename, candidate.file_size)
        key = (target_path, candidate.file_size)

        async with self.active_lock:
            if key in self.active_downloads:
                if self.reply_on_duplicate:
                    await message.reply(f"Download is already in progress for {target_path.name}.")
                return

            if target_path.exists() and target_path.is_file():
                if candidate.file_size is None or target_path.stat().st_size == candidate.file_size:
                    if self.reply_on_duplicate:
                        await message.reply(f"Download already exists for {target_path.name}.")
                    return

            self.active_downloads.add(key)

        progress_message: Optional[Message] = None
        progress_updater: Optional[asyncio.Task[None]] = None
        try:
            async with self.semaphore:
                LOGGER.info("Starting download for %s", target_path.name)
                progress_message = await message.reply(f"Starting download: {target_path.name}")
                tracker = ProgressTracker(target_path.name, candidate.file_size, completed_bytes=0)
                progress_updater = asyncio.create_task(
                    self.progress_message_updater(progress_message, tracker)
                )
                await self.download_with_resume(message, target_path, candidate.file_size, tracker)

            if not target_path.exists():
                raise RuntimeError("download did not create the target file")

            if candidate.file_size is not None and target_path.stat().st_size != candidate.file_size:
                raise RuntimeError(
                    f"size mismatch after transfer: expected={candidate.file_size} actual={target_path.stat().st_size}"
                )

            await ProgressTracker.clear_terminal_line()
            LOGGER.info("Download complete for %s", target_path.name)
            if progress_updater is not None:
                progress_updater.cancel()
                await self.await_cancel(progress_updater)
            if progress_message is not None:
                await progress_message.edit(f"Download complete: {target_path.name}")
            await message.reply(f"Download complete: {target_path.name}")
        except Exception as exc:
            await ProgressTracker.clear_terminal_line()
            LOGGER.exception("Download failed for %s", target_path.name)
            if progress_updater is not None:
                progress_updater.cancel()
                await self.await_cancel(progress_updater)
            if progress_message is not None:
                await progress_message.edit(f"Download failed for {target_path.name}: {exc}")
            await message.reply(f"Download failed for {target_path.name}: {exc}")
        finally:
            async with self.active_lock:
                self.active_downloads.discard(key)

    async def download_with_resume(
        self,
        message: Message,
        target_path: Path,
        expected_size: Optional[int],
        tracker: ProgressTracker,
    ) -> None:
        if not expected_size or expected_size <= 0:
            await self.download_single_stream(message, target_path, expected_size, tracker)
            return

        work_dir = self.partial_work_dir(target_path)
        work_dir.mkdir(parents=True, exist_ok=True)
        plans = build_segment_plans(
            base_dir=work_dir,
            total_size=expected_size,
            segment_count=min(self.parallel_chunk_downloads, max(1, math.ceil(expected_size / self.chunk_size))),
        )

        completed_bytes = 0
        for plan in plans:
            if plan.part_path.exists():
                existing_size = min(plan.part_path.stat().st_size, plan.size)
                completed_bytes += existing_size

        tracker.completed_bytes = completed_bytes
        await tracker.log_snapshot()

        tasks = [
            asyncio.create_task(self.download_segment(message, plan, tracker))
            for plan in plans
        ]
        await asyncio.gather(*tasks)
        await self.merge_segments(plans, target_path)
        shutil.rmtree(work_dir, ignore_errors=True)

    async def download_single_stream(
        self,
        message: Message,
        target_path: Path,
        expected_size: Optional[int],
        tracker: ProgressTracker,
    ) -> None:
        partial_path = self.partial_file_path(target_path)
        existing_size = partial_path.stat().st_size if partial_path.exists() else 0
        tracker.completed_bytes = 0
        await tracker.update(segment_index=0, bytes_downloaded=existing_size)

        offset = existing_size
        partial_path.parent.mkdir(parents=True, exist_ok=True)
        with partial_path.open("ab") as handle:
            async for chunk in self.client.iter_download(
                message,
                offset=offset,
                chunk_size=self.chunk_size,
                request_size=self.chunk_size,
                file_size=expected_size,
            ):
                handle.write(chunk)
                offset += len(chunk)
                await tracker.update(segment_index=0, bytes_downloaded=offset)

        partial_path.replace(target_path)

    async def download_segment(
        self,
        message: Message,
        plan: SegmentPlan,
        tracker: ProgressTracker,
    ) -> None:
        part_path = plan.part_path
        existing_size = part_path.stat().st_size if part_path.exists() else 0
        existing_size = min(existing_size, plan.size)

        if existing_size == plan.size:
            await tracker.mark_segment_complete(plan.index, plan.size)
            return

        offset = plan.start + existing_size
        remaining = plan.size - existing_size
        await tracker.update(plan.index, existing_size)
        part_path.parent.mkdir(parents=True, exist_ok=True)

        with part_path.open("ab") as handle:
            async for chunk in self.client.iter_download(
                message,
                offset=offset,
                chunk_size=self.chunk_size,
                request_size=self.chunk_size,
                file_size=message.file.size if message.file else None,
            ):
                if not chunk or remaining <= 0:
                    break

                to_write = bytes(chunk[:remaining])
                handle.write(to_write)
                existing_size += len(to_write)
                remaining -= len(to_write)
                offset += len(to_write)
                await tracker.update(plan.index, existing_size)

                if remaining <= 0:
                    break

        if part_path.stat().st_size != plan.size:
            raise RuntimeError(
                f"segment {plan.index} incomplete: expected={plan.size} actual={part_path.stat().st_size}"
            )

        await tracker.mark_segment_complete(plan.index, plan.size)

    async def merge_segments(self, plans: list[SegmentPlan], target_path: Path) -> None:
        temp_target = target_path.with_name(target_path.name + ".assembling")
        with temp_target.open("wb") as out_handle:
            for plan in plans:
                with plan.part_path.open("rb") as in_handle:
                    shutil.copyfileobj(in_handle, out_handle)
        temp_target.replace(target_path)

    def partial_work_dir(self, target_path: Path) -> Path:
        return self.download_dir / ".partial" / target_path.name

    def partial_file_path(self, target_path: Path) -> Path:
        return self.download_dir / ".partial" / f"{target_path.name}.part"

    def build_candidate(self, message: Message, media_type: str) -> DownloadCandidate:
        filename = derive_filename(message, media_type)
        file_size = message.file.size if message.file else None
        return DownloadCandidate(
            message_id=message.id,
            chat_id=message.chat_id or 0,
            filename=filename,
            file_size=file_size,
        )

    async def progress_message_updater(self, progress_message: Message, tracker: ProgressTracker) -> None:
        last_sent_text = ""
        while True:
            await asyncio.sleep(PROGRESS_EDIT_INTERVAL_SECONDS)
            current_text = f"Download in progress: {tracker.current_status_text()}"
            if current_text == last_sent_text:
                continue
            await progress_message.edit(current_text)
            last_sent_text = current_text

    @staticmethod
    async def await_cancel(task: asyncio.Task[None]) -> None:
        try:
            await task
        except asyncio.CancelledError:
            pass


def main() -> None:
    load_dotenv(ENV_FILE)
    configure_logging()
    downloader = MTProtoDownloader()
    asyncio.run(downloader.run())


if __name__ == "__main__":
    main()
