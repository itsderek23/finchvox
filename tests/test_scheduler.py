import struct
import tempfile
import time
import wave
from pathlib import Path
from unittest.mock import patch

import pytest

from finchvox.scheduler import (
    compress_pending_sessions,
    find_sessions_to_compress,
    get_scheduler,
    start_scheduler,
    stop_scheduler,
)


def create_wav_file(
    path: Path, duration_seconds: float = 1.0, sample_rate: int = 16000
):
    num_samples = int(duration_seconds * sample_rate)
    with wave.open(str(path), "wb") as wf:
        wf.setnchannels(2)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        samples = [0] * (num_samples * 2)
        wf.writeframes(struct.pack(f"{len(samples)}h", *samples))


@pytest.fixture
def temp_sessions_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def session_with_old_chunks(temp_sessions_dir):
    session_id = "abc123def456789012345678901234"
    session_dir = temp_sessions_dir / session_id
    audio_dir = session_dir / "audio"
    audio_dir.mkdir(parents=True)

    old_time = time.time() - 600
    for i in range(3):
        chunk_path = audio_dir / f"chunk_{i:04d}.wav"
        create_wav_file(chunk_path, duration_seconds=0.5)
        import os

        os.utime(chunk_path, (old_time, old_time))

    return session_id


@pytest.fixture
def session_with_recent_chunks(temp_sessions_dir):
    session_id = "recent123456789012345678901234"
    session_dir = temp_sessions_dir / session_id
    audio_dir = session_dir / "audio"
    audio_dir.mkdir(parents=True)

    for i in range(3):
        chunk_path = audio_dir / f"chunk_{i:04d}.wav"
        create_wav_file(chunk_path, duration_seconds=0.5)

    return session_id


class TestFindSessionsToCompress:
    def test_finds_session_with_old_chunks(
        self, temp_sessions_dir, session_with_old_chunks
    ):
        result = find_sessions_to_compress(temp_sessions_dir, inactive_minutes=5)
        assert session_with_old_chunks in result

    def test_excludes_recently_active_session(
        self, temp_sessions_dir, session_with_recent_chunks
    ):
        result = find_sessions_to_compress(temp_sessions_dir, inactive_minutes=5)
        assert session_with_recent_chunks not in result

    def test_excludes_already_compressed_session(
        self, temp_sessions_dir, session_with_old_chunks
    ):
        session_dir = temp_sessions_dir / session_with_old_chunks
        (session_dir / "audio.opus").touch()

        result = find_sessions_to_compress(temp_sessions_dir, inactive_minutes=5)
        assert session_with_old_chunks not in result

    def test_excludes_session_without_chunks(self, temp_sessions_dir):
        session_id = "empty123456789012345678901234"
        session_dir = temp_sessions_dir / session_id
        audio_dir = session_dir / "audio"
        audio_dir.mkdir(parents=True)

        result = find_sessions_to_compress(temp_sessions_dir, inactive_minutes=5)
        assert session_id not in result

    def test_returns_empty_for_nonexistent_dir(self):
        result = find_sessions_to_compress(Path("/nonexistent/path"))
        assert result == []

    def test_handles_custom_inactive_minutes(
        self, temp_sessions_dir, session_with_old_chunks
    ):
        result = find_sessions_to_compress(temp_sessions_dir, inactive_minutes=20)
        assert session_with_old_chunks not in result


class TestCompressPendingSessions:
    def test_compresses_eligible_sessions(
        self, temp_sessions_dir, session_with_old_chunks
    ):
        with patch("finchvox.scheduler.AudioCompressor") as mock_compressor_class:
            mock_compressor = mock_compressor_class.return_value
            mock_compressor.compress.return_value = True

            count = compress_pending_sessions(temp_sessions_dir, inactive_minutes=5)

            assert count == 1
            mock_compressor.compress.assert_called_once_with(session_with_old_chunks)

    def test_returns_zero_when_no_sessions(self, temp_sessions_dir):
        count = compress_pending_sessions(temp_sessions_dir, inactive_minutes=5)
        assert count == 0

    def test_counts_successful_compressions(
        self, temp_sessions_dir, session_with_old_chunks
    ):
        session_id_2 = "second23456789012345678901234"
        session_dir_2 = temp_sessions_dir / session_id_2
        audio_dir_2 = session_dir_2 / "audio"
        audio_dir_2.mkdir(parents=True)

        old_time = time.time() - 600
        chunk_path = audio_dir_2 / "chunk_0000.wav"
        create_wav_file(chunk_path)
        import os

        os.utime(chunk_path, (old_time, old_time))

        with patch("finchvox.scheduler.AudioCompressor") as mock_compressor_class:
            mock_compressor = mock_compressor_class.return_value
            mock_compressor.compress.side_effect = [True, False]

            count = compress_pending_sessions(temp_sessions_dir, inactive_minutes=5)

            assert count == 1
            assert mock_compressor.compress.call_count == 2


class TestSchedulerLifecycle:
    def test_get_scheduler_returns_scheduler(self):
        import finchvox.scheduler as sched_module

        sched_module._scheduler = None
        scheduler = get_scheduler()
        assert scheduler is not None
        sched_module._scheduler = None

    @pytest.mark.asyncio
    async def test_start_and_stop_scheduler(self, temp_sessions_dir):
        import finchvox.scheduler as sched_module

        sched_module._scheduler = None

        start_scheduler(temp_sessions_dir, interval_minutes=1, inactive_minutes=5)
        scheduler = get_scheduler()
        assert scheduler.running

        stop_scheduler()
        assert sched_module._scheduler is None

    def test_stop_scheduler_when_not_running(self):
        import finchvox.scheduler as sched_module

        sched_module._scheduler = None
        stop_scheduler()
        assert sched_module._scheduler is None
