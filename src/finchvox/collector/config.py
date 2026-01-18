from pathlib import Path

GRPC_PORT = 4317
HTTP_PORT = 3000
MAX_WORKERS = 10

LOG_LEVEL = "INFO"

MAX_AUDIO_FILE_SIZE = 10 * 1024 * 1024
ALLOWED_AUDIO_FORMATS = {".wav", ".mp3", ".ogg", ".flac"}

MAX_LOG_BATCH_SIZE = 100
LOG_FLUSH_INTERVAL = 5.0


def get_default_data_dir() -> Path:
    return Path.home() / ".finchvox"


def get_sessions_base_dir(data_dir: Path) -> Path:
    return data_dir / "sessions"


def get_session_dir(data_dir: Path, session_id: str) -> Path:
    return get_sessions_base_dir(data_dir) / session_id


def get_session_audio_dir(data_dir: Path, session_id: str) -> Path:
    return get_session_dir(data_dir, session_id) / "audio"


def get_session_exceptions_dir(data_dir: Path, session_id: str) -> Path:
    return get_session_dir(data_dir, session_id) / "exceptions"
