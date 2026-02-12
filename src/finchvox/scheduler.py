import time
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from finchvox.audio_compressor import AudioCompressor
from finchvox.collector.config import get_sessions_base_dir

_scheduler: AsyncIOScheduler | None = None


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler()
    return _scheduler


def _session_needs_compression(
    session_dir: Path, min_inactive_threshold: float, max_inactive_threshold: float
) -> bool:
    audio_dir = session_dir / "audio"
    audio_opus = session_dir / "audio.opus"

    if audio_opus.exists() or not audio_dir.exists():
        return False

    chunks = list(audio_dir.glob("chunk_*.wav"))
    if not chunks:
        return False

    latest_chunk_mtime = max(chunk.stat().st_mtime for chunk in chunks)
    return max_inactive_threshold < latest_chunk_mtime < min_inactive_threshold


def find_sessions_to_compress(
    sessions_dir: Path, min_inactive_minutes: int = 1, max_inactive_minutes: int = 60
) -> list[str]:
    if not sessions_dir.exists():
        return []

    now = time.time()
    min_inactive_threshold = now - (min_inactive_minutes * 60)
    max_inactive_threshold = now - (max_inactive_minutes * 60)

    return [
        session_dir.name
        for session_dir in sessions_dir.iterdir()
        if session_dir.is_dir()
        and _session_needs_compression(
            session_dir, min_inactive_threshold, max_inactive_threshold
        )
    ]


def compress_pending_sessions(
    data_dir: Path, min_inactive_minutes: int = 1, max_inactive_minutes: int = 60
) -> int:
    logger.debug("Running scheduled audio compression check")
    sessions_dir = get_sessions_base_dir(data_dir)
    sessions = find_sessions_to_compress(
        sessions_dir, min_inactive_minutes, max_inactive_minutes
    )
    if not sessions:
        logger.debug("No sessions require compression")
        return 0

    logger.info(f"Found {len(sessions)} session(s) to compress")
    compressor = AudioCompressor(data_dir)
    compressed_count = 0

    for session_id in sessions:
        if compressor.compress(session_id):
            compressed_count += 1

    return compressed_count


def start_scheduler(
    data_dir: Path,
    interval_minutes: int = 1,
    min_inactive_minutes: int = 1,
    max_inactive_minutes: int = 60,
):
    scheduler = get_scheduler()

    def job():
        compress_pending_sessions(data_dir, min_inactive_minutes, max_inactive_minutes)

    scheduler.add_job(
        job,
        trigger=IntervalTrigger(minutes=interval_minutes),
        id="compress_audio",
        replace_existing=True,
        next_run_time=datetime.now(),
    )
    scheduler.start()
    logger.info(
        f"Audio compression scheduler started (interval: {interval_minutes}m, inactive: {min_inactive_minutes}m-{max_inactive_minutes}m)"
    )


def stop_scheduler():
    global _scheduler
    scheduler = _scheduler
    _scheduler = None
    if scheduler is not None and scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Audio compression scheduler stopped")
