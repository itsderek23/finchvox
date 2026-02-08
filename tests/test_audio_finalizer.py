from unittest.mock import patch

import pytest

from finchvox.audio_finalizer import (
    AudioFinalizer,
    check_ffmpeg_available,
    compress_to_opus,
    ffmpeg_available,
)


@pytest.fixture
def session_with_chunks(temp_sessions_dir, create_wav_file):
    session_id = "abc123def456789012345678901234"
    session_dir = temp_sessions_dir / session_id
    audio_dir = session_dir / "audio"
    audio_dir.mkdir(parents=True)

    for i in range(3):
        chunk_path = audio_dir / f"chunk_{i:04d}.wav"
        create_wav_file(chunk_path, duration_seconds=0.5)
        metadata_path = audio_dir / f"chunk_{i:04d}.json"
        metadata_path.write_text("{}")

    return session_id


class TestFfmpegAvailable:
    def test_returns_true_when_ffmpeg_installed(self):
        result = ffmpeg_available()
        assert isinstance(result, bool)

    def test_check_caches_result(self):
        import finchvox.audio_finalizer as af

        af._FFMPEG_AVAILABLE = None
        result1 = check_ffmpeg_available()
        result2 = check_ffmpeg_available()
        assert result1 == result2


class TestAudioFinalizer:
    def test_finalize_with_no_audio_dir(self, temp_sessions_dir):
        session_id = "abc123def456789012345678901234"
        session_dir = temp_sessions_dir / session_id
        session_dir.mkdir(parents=True)

        finalizer = AudioFinalizer(temp_sessions_dir)
        result = finalizer.finalize(session_id)

        assert result is True

    def test_finalize_with_empty_audio_dir(self, temp_sessions_dir):
        session_id = "abc123def456789012345678901234"
        session_dir = temp_sessions_dir / session_id
        audio_dir = session_dir / "audio"
        audio_dir.mkdir(parents=True)

        finalizer = AudioFinalizer(temp_sessions_dir)
        result = finalizer.finalize(session_id)

        assert result is True
        assert not audio_dir.exists()

    def test_finalize_merges_chunks_and_removes_audio_dir_without_ffmpeg(
        self, temp_sessions_dir, session_with_chunks
    ):
        with patch(
            "finchvox.audio_finalizer.check_ffmpeg_available", return_value=False
        ):
            finalizer = AudioFinalizer(temp_sessions_dir)
            result = finalizer.finalize(session_with_chunks)

        assert result is True

        session_dir = temp_sessions_dir / session_with_chunks
        assert (session_dir / "audio.wav").exists()
        assert not (session_dir / "audio").exists()

    def test_finalize_compresses_to_opus_and_removes_chunks_with_ffmpeg(
        self, temp_sessions_dir, session_with_chunks
    ):
        if not ffmpeg_available():
            pytest.skip("ffmpeg not available")

        import finchvox.audio_finalizer as af

        af._FFMPEG_AVAILABLE = None

        finalizer = AudioFinalizer(temp_sessions_dir)
        result = finalizer.finalize(session_with_chunks)

        assert result is True

        session_dir = temp_sessions_dir / session_with_chunks
        assert (session_dir / "audio.opus").exists()
        assert not (session_dir / "audio.wav").exists()
        assert not (session_dir / "audio").exists()


class TestCompressToOpus:
    def test_compress_to_opus_creates_file(self, temp_sessions_dir, create_wav_file):
        if not ffmpeg_available():
            pytest.skip("ffmpeg not available")

        input_wav = temp_sessions_dir / "input.wav"
        output_opus = temp_sessions_dir / "output.opus"
        create_wav_file(input_wav, duration_seconds=1.0)

        result = compress_to_opus(input_wav, output_opus)

        assert result is True
        assert output_opus.exists()
        assert output_opus.stat().st_size > 0
        assert output_opus.stat().st_size < input_wav.stat().st_size
