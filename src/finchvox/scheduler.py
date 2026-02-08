import time
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from finchvox.audio_compressor import AudioCompressor

_scheduler: AsyncIOScheduler | None = None


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler()
    return _scheduler


def find_sessions_to_compress(
    sessions_dir: Path, inactive_minutes: int = 5
) -> list[str]:
    sessions_to_compress = []
    if not sessions_dir.exists():
        return sessions_to_compress

    inactive_threshold = time.time() - (inactive_minutes * 60)

    for session_dir in sessions_dir.iterdir():
        if not session_dir.is_dir():
            continue

        session_id = session_dir.name
        audio_dir = session_dir / "audio"
        audio_opus = session_dir / "audio.opus"

        if audio_opus.exists():
            continue

        if not audio_dir.exists():
            continue

        chunks = list(audio_dir.glob("chunk_*.wav"))
        if not chunks:
            continue

        latest_chunk_mtime = max(chunk.stat().st_mtime for chunk in chunks)
        if latest_chunk_mtime < inactive_threshold:
            sessions_to_compress.append(session_id)

    return sessions_to_compress


def compress_pending_sessions(sessions_dir: Path, inactive_minutes: int = 5) -> int:
    logger.debug("Running scheduled audio compression check")
    sessions = find_sessions_to_compress(sessions_dir, inactive_minutes)
    if not sessions:
        logger.debug("No sessions require compression")
        return 0

    logger.info(f"Found {len(sessions)} session(s) to compress")
    compressor = AudioCompressor(sessions_dir)
    compressed_count = 0

    for session_id in sessions:
        if compressor.compress(session_id):
            compressed_count += 1

    return compressed_count


def start_scheduler(
    sessions_dir: Path, interval_minutes: int = 5, inactive_minutes: int = 5
):
    scheduler = get_scheduler()

    def job():
        compress_pending_sessions(sessions_dir, inactive_minutes)

    scheduler.add_job(
        job,
        trigger=IntervalTrigger(minutes=interval_minutes),
        id="compress_audio",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        f"Audio compression scheduler started (interval: {interval_minutes}m, inactive threshold: {inactive_minutes}m)"
    )


def stop_scheduler():
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Audio compression scheduler stopped")
    _scheduler = None
