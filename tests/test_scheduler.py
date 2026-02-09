import json
import os
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from finchvox.scheduler import (
    _get_last_activity_time,
    _session_needs_finalization,
    finalize_pending_sessions,
    find_sessions_to_finalize,
    get_scheduler,
    start_scheduler,
    stop_scheduler,
)


def create_trace_file(
    session_dir: Path, session_id: str, with_root_end_time: bool = False
):
    trace_file = session_dir / f"trace_{session_id}.jsonl"
    if with_root_end_time:
        span = {
            "name": "root",
            "start_time_unix_nano": "1000000000000",
            "end_time_unix_nano": "2000000000000",
        }
    else:
        span = {
            "name": "root",
            "parent_span_id_hex": "abc123",
            "start_time_unix_nano": "1000000000000",
        }
    with open(trace_file, "w") as f:
        f.write(json.dumps(span) + "\n")


def make_session_with_trace(
    sessions_dir: Path, session_id: str, with_root_end_time: bool = False
) -> str:
    session_dir = sessions_dir / session_id
    session_dir.mkdir(parents=True)
    create_trace_file(session_dir, session_id, with_root_end_time)
    return session_id


def make_session_with_chunks(
    sessions_dir: Path, session_id: str, create_wav_file, mtime: float | None = None
) -> str:
    session_dir = sessions_dir / session_id
    session_dir.mkdir(parents=True, exist_ok=True)
    create_trace_file(session_dir, session_id)
    audio_dir = session_dir / "audio"
    audio_dir.mkdir(parents=True, exist_ok=True)
    for i in range(3):
        chunk_path = audio_dir / f"chunk_{i:04d}.wav"
        create_wav_file(chunk_path, duration_seconds=0.5)
        if mtime is not None:
            os.utime(chunk_path, (mtime, mtime))
    return session_id


@pytest.fixture
def session_with_old_chunks(temp_data_dir, create_wav_file):
    sessions_dir = temp_data_dir / "sessions"
    session_id = "abc123def456789012345678901234"
    old_time = time.time() - 120
    make_session_with_chunks(sessions_dir, session_id, create_wav_file, old_time)
    trace_file = sessions_dir / session_id / f"trace_{session_id}.jsonl"
    os.utime(trace_file, (old_time, old_time))
    return session_id


@pytest.fixture
def session_with_recent_chunks(temp_data_dir, create_wav_file):
    sessions_dir = temp_data_dir / "sessions"
    return make_session_with_chunks(
        sessions_dir, "recent123456789012345678901234", create_wav_file
    )


class TestGetLastActivityTime:
    def test_returns_trace_file_mtime(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "trace_only_123456789012345678"
        session_dir = sessions_dir / session_id
        session_dir.mkdir(parents=True)
        create_trace_file(session_dir, session_id)

        result = _get_last_activity_time(session_dir)
        expected = (session_dir / f"trace_{session_id}.jsonl").stat().st_mtime
        assert result == expected

    def test_returns_max_of_trace_and_logs(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "trace_logs_123456789012345678"
        session_dir = sessions_dir / session_id
        session_dir.mkdir(parents=True)

        trace_file = session_dir / f"trace_{session_id}.jsonl"
        logs_file = session_dir / f"logs_{session_id}.jsonl"

        trace_file.write_text('{"name": "test"}\n')
        old_time = time.time() - 100
        os.utime(trace_file, (old_time, old_time))

        logs_file.write_text('{"body": "log"}\n')

        result = _get_last_activity_time(session_dir)
        assert result == logs_file.stat().st_mtime

    def test_returns_max_of_all_files(self, temp_data_dir, create_wav_file):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "all_files_1234567890123456789"
        session_dir = sessions_dir / session_id
        audio_dir = session_dir / "audio"
        audio_dir.mkdir(parents=True)

        trace_file = session_dir / f"trace_{session_id}.jsonl"
        trace_file.write_text('{"name": "test"}\n')

        old_time = time.time() - 200
        os.utime(trace_file, (old_time, old_time))

        chunk = audio_dir / "chunk_0000.wav"
        create_wav_file(chunk, duration_seconds=0.5)

        result = _get_last_activity_time(session_dir)
        assert result == chunk.stat().st_mtime

    def test_returns_zero_for_empty_dir(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_dir = sessions_dir / "empty_session_123456789012"
        session_dir.mkdir(parents=True)

        result = _get_last_activity_time(session_dir)
        assert result == 0.0


class TestSessionNeedsFinalization:
    def test_returns_false_if_manifest_exists(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "manifest_exists_12345678901234"
        session_dir = sessions_dir / session_id
        session_dir.mkdir(parents=True)
        create_trace_file(session_dir, session_id)
        (session_dir / "manifest.json").write_text("{}")

        result = _session_needs_finalization(session_dir)
        assert result is False

    def test_returns_false_if_no_trace_file(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "no_trace_123456789012345678"
        session_dir = sessions_dir / session_id
        session_dir.mkdir(parents=True)

        result = _session_needs_finalization(session_dir)
        assert result is False

    def test_returns_true_if_root_span_ended(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "root_ended_12345678901234567"
        make_session_with_trace(sessions_dir, session_id, with_root_end_time=True)

        result = _session_needs_finalization(sessions_dir / session_id)
        assert result is True

    def test_returns_true_if_inactive_past_threshold(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "inactive_1234567890123456789"
        session_dir = sessions_dir / session_id
        session_dir.mkdir(parents=True)
        create_trace_file(session_dir, session_id)

        old_time = time.time() - 120
        trace_file = session_dir / f"trace_{session_id}.jsonl"
        os.utime(trace_file, (old_time, old_time))

        result = _session_needs_finalization(session_dir, min_inactive_seconds=60)
        assert result is True

    def test_returns_false_if_not_inactive_enough(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "active_session_1234567890123"
        session_dir = sessions_dir / session_id
        session_dir.mkdir(parents=True)
        create_trace_file(session_dir, session_id)

        result = _session_needs_finalization(session_dir, min_inactive_seconds=60)
        assert result is False

    def test_returns_false_if_older_than_max_threshold(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "very_old_session_1234567890"
        session_dir = sessions_dir / session_id
        session_dir.mkdir(parents=True)
        create_trace_file(session_dir, session_id, with_root_end_time=True)

        old_time = time.time() - 7200
        trace_file = session_dir / f"trace_{session_id}.jsonl"
        os.utime(trace_file, (old_time, old_time))

        result = _session_needs_finalization(
            session_dir, min_inactive_seconds=60, max_inactive_seconds=3600
        )
        assert result is False


class TestFindSessionsToFinalize:
    def test_finds_session_with_ended_root_span(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = make_session_with_trace(
            sessions_dir, "ended_root_12345678901234567", with_root_end_time=True
        )

        result = find_sessions_to_finalize(sessions_dir)
        assert session_id in result

    def test_finds_inactive_session(self, temp_data_dir, session_with_old_chunks):
        sessions_dir = temp_data_dir / "sessions"
        result = find_sessions_to_finalize(sessions_dir, min_inactive_seconds=60)
        assert session_with_old_chunks in result

    def test_excludes_recently_active_session(
        self, temp_data_dir, session_with_recent_chunks
    ):
        sessions_dir = temp_data_dir / "sessions"
        result = find_sessions_to_finalize(sessions_dir, min_inactive_seconds=60)
        assert session_with_recent_chunks not in result

    def test_excludes_already_finalized_session(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = make_session_with_trace(
            sessions_dir, "finalized_12345678901234567", with_root_end_time=True
        )
        (sessions_dir / session_id / "manifest.json").write_text("{}")

        result = find_sessions_to_finalize(sessions_dir)
        assert session_id not in result

    def test_excludes_session_older_than_max_threshold(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "very_old_session_1234567890"
        session_dir = sessions_dir / session_id
        session_dir.mkdir(parents=True)
        create_trace_file(session_dir, session_id, with_root_end_time=True)

        old_time = time.time() - 7200
        trace_file = session_dir / f"trace_{session_id}.jsonl"
        os.utime(trace_file, (old_time, old_time))

        result = find_sessions_to_finalize(
            sessions_dir, min_inactive_seconds=60, max_inactive_seconds=3600
        )
        assert session_id not in result

    def test_returns_empty_for_nonexistent_dir(self):
        result = find_sessions_to_finalize(Path("/nonexistent/path"))
        assert result == []


class TestFinalizePendingSessions:
    def test_finalizes_eligible_sessions(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = make_session_with_trace(
            sessions_dir, "eligible_123456789012345678", with_root_end_time=True
        )

        with patch("finchvox.scheduler.SessionFinalizer") as mock_class:
            mock_finalizer = mock_class.return_value
            mock_finalizer.finalize.return_value = True

            count = finalize_pending_sessions(
                temp_data_dir, min_inactive_seconds=60, max_inactive_seconds=3600
            )

            assert count == 1
            mock_finalizer.finalize.assert_called_once_with(session_id)

    def test_returns_zero_when_no_sessions(self, temp_data_dir):
        count = finalize_pending_sessions(
            temp_data_dir, min_inactive_seconds=60, max_inactive_seconds=3600
        )
        assert count == 0

    def test_counts_successful_finalizations(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        make_session_with_trace(
            sessions_dir, "success1_12345678901234567", with_root_end_time=True
        )
        make_session_with_trace(
            sessions_dir, "failure1_12345678901234567", with_root_end_time=True
        )

        with patch("finchvox.scheduler.SessionFinalizer") as mock_class:
            mock_finalizer = mock_class.return_value
            mock_finalizer.finalize.side_effect = [True, False]

            count = finalize_pending_sessions(
                temp_data_dir, min_inactive_seconds=60, max_inactive_seconds=3600
            )

            assert count == 1
            assert mock_finalizer.finalize.call_count == 2


class TestSchedulerLifecycle:
    def test_get_scheduler_returns_scheduler(self):
        import finchvox.scheduler as sched_module

        sched_module._scheduler = None
        scheduler = get_scheduler()
        assert scheduler is not None
        sched_module._scheduler = None

    @pytest.mark.asyncio
    async def test_start_and_stop_scheduler(self, temp_data_dir):
        import finchvox.scheduler as sched_module

        sched_module._scheduler = None

        start_scheduler(temp_data_dir, interval_minutes=1, min_inactive_minutes=1)
        scheduler = get_scheduler()
        assert scheduler.running

        stop_scheduler()
        assert sched_module._scheduler is None

    def test_stop_scheduler_when_not_running(self):
        import finchvox.scheduler as sched_module

        sched_module._scheduler = None
        stop_scheduler()
        assert sched_module._scheduler is None

    @pytest.mark.asyncio
    async def test_scheduler_runs_immediately_on_start(self, temp_data_dir):
        import asyncio

        import finchvox.scheduler as sched_module

        sched_module._scheduler = None

        with patch("finchvox.scheduler.finalize_pending_sessions") as mock_finalize:
            start_scheduler(temp_data_dir, interval_minutes=1, min_inactive_minutes=1)

            await asyncio.sleep(0.1)

            mock_finalize.assert_called_once()

            stop_scheduler()
