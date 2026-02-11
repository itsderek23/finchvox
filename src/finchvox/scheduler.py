import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from finchvox.collector.config import get_sessions_base_dir
from finchvox.session import Session
from finchvox.session_finalizer import SessionFinalizer
from finchvox.storage.backend import StorageBackend

_scheduler: AsyncIOScheduler | None = None
_storage_backend: Optional[StorageBackend] = None


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler()
    return _scheduler


def _get_last_activity_time(session_dir: Path) -> float:
    session_id = session_dir.name
    mtimes = []

    trace_file = session_dir / f"trace_{session_id}.jsonl"
    if trace_file.exists():
        mtimes.append(trace_file.stat().st_mtime)

    logs_file = session_dir / f"logs_{session_id}.jsonl"
    if logs_file.exists():
        mtimes.append(logs_file.stat().st_mtime)

    audio_dir = session_dir / "audio"
    if audio_dir.exists():
        for chunk in audio_dir.glob("chunk_*.wav"):
            mtimes.append(chunk.stat().st_mtime)

    return max(mtimes) if mtimes else 0.0


def _session_needs_finalization(
    session_dir: Path,
    min_inactive_seconds: int = 60,
    max_inactive_seconds: int = 3600,
) -> bool:
    manifest_path = session_dir / "manifest.json"
    if manifest_path.exists():
        return False

    trace_file = session_dir / f"trace_{session_dir.name}.jsonl"
    if not trace_file.exists():
        return False

    last_activity = _get_last_activity_time(session_dir)
    inactive_seconds = time.time() - last_activity

    if inactive_seconds > max_inactive_seconds:
        return False

    if inactive_seconds >= min_inactive_seconds:
        return True

    session = Session(session_dir)
    return session.is_root_span_ended()


def find_sessions_to_finalize(
    sessions_dir: Path,
    min_inactive_seconds: int = 60,
    max_inactive_seconds: int = 3600,
) -> list[str]:
    if not sessions_dir.exists():
        return []

    return [
        d.name
        for d in sessions_dir.iterdir()
        if d.is_dir()
        and _session_needs_finalization(d, min_inactive_seconds, max_inactive_seconds)
    ]


def finalize_pending_sessions(
    data_dir: Path,
    min_inactive_seconds: int = 60,
    max_inactive_seconds: int = 3600,
    storage_backend: Optional[StorageBackend] = None,
) -> int:
    logger.debug("Running scheduled session finalization check")
    sessions_dir = get_sessions_base_dir(data_dir)
    sessions = find_sessions_to_finalize(
        sessions_dir, min_inactive_seconds, max_inactive_seconds
    )
    if not sessions:
        logger.debug("No sessions require finalization")
        return 0

    logger.info(f"Found {len(sessions)} session(s) to finalize")
    finalizer = SessionFinalizer(sessions_dir, storage_backend=storage_backend)
    finalized_count = 0

    for session_id in sessions:
        if finalizer.finalize(session_id):
            finalized_count += 1

    return finalized_count


def start_scheduler(
    data_dir: Path,
    interval_minutes: int = 1,
    min_inactive_minutes: int = 1,
    max_inactive_minutes: int = 60,
    storage_backend: Optional[StorageBackend] = None,
):
    global _storage_backend
    _storage_backend = storage_backend

    scheduler = get_scheduler()
    min_inactive_seconds = min_inactive_minutes * 60
    max_inactive_seconds = max_inactive_minutes * 60

    def job():
        finalize_pending_sessions(
            data_dir, min_inactive_seconds, max_inactive_seconds, _storage_backend
        )

    scheduler.add_job(
        job,
        trigger=IntervalTrigger(minutes=interval_minutes),
        id="finalize_sessions",
        replace_existing=True,
        next_run_time=datetime.now(),
    )
    scheduler.start()
    logger.info(
        f"Session finalization scheduler started (interval: {interval_minutes}m, inactive: {min_inactive_minutes}m-{max_inactive_minutes}m)"
    )


def stop_scheduler():
    global _scheduler
    scheduler = _scheduler
    _scheduler = None
    if scheduler is not None and scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Session finalization scheduler stopped")
