import struct
import tempfile
import wave
from pathlib import Path

import pytest


def _create_wav_file(
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
def create_wav_file():
    return _create_wav_file


@pytest.fixture
def temp_sessions_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_data_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir)
        sessions_dir = data_dir / "sessions"
        sessions_dir.mkdir()
        yield data_dir
