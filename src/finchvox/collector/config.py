from pathlib import Path

# Server configuration
GRPC_PORT = 4317  # Standard OTLP gRPC port
HTTP_PORT = 3000  # Unified HTTP server (collector + UI)
MAX_WORKERS = 10  # Thread pool size for concurrent requests

# Logging configuration
LOG_LEVEL = "INFO"  # Can be overridden via LOGURU_LEVEL env var

# Audio upload configuration
MAX_AUDIO_FILE_SIZE = 10 * 1024 * 1024  # 10MB max per chunk
ALLOWED_AUDIO_FORMATS = {".wav", ".mp3", ".ogg", ".flac"}

# Log batching configuration
MAX_LOG_BATCH_SIZE = 100  # Max logs per HTTP request
LOG_FLUSH_INTERVAL = 5.0  # Seconds between batched uploads


def get_default_data_dir() -> Path:
    """Get the default data directory (~/.finchvox)."""
    return Path.home() / ".finchvox"


def get_traces_base_dir(data_dir: Path) -> Path:
    """
    Get the base traces directory.

    Args:
        data_dir: Base data directory (e.g., ~/.finchvox)

    Returns:
        Path to traces directory (e.g., ~/.finchvox/traces)
    """
    return data_dir / "traces"


def get_trace_dir(data_dir: Path, trace_id: str) -> Path:
    """
    Get the directory for a specific trace.

    Args:
        data_dir: Base data directory
        trace_id: Hex string trace ID

    Returns:
        Path to trace-specific directory (e.g., ~/.finchvox/traces/<trace_id>)
    """
    return get_traces_base_dir(data_dir) / trace_id


def get_trace_logs_dir(data_dir: Path, trace_id: str) -> Path:
    """Get the logs directory for a specific trace."""
    return get_trace_dir(data_dir, trace_id) / "logs"


def get_trace_audio_dir(data_dir: Path, trace_id: str) -> Path:
    """Get the audio directory for a specific trace."""
    return get_trace_dir(data_dir, trace_id) / "audio"


def get_trace_exceptions_dir(data_dir: Path, trace_id: str) -> Path:
    """Get the exceptions directory for a specific trace."""
    return get_trace_dir(data_dir, trace_id) / "exceptions"
