import json
from pathlib import Path
from typing import Optional

from finchvox.audio_utils import find_chunks


class Trace:
    def __init__(self, turn_count: int):
        self.turn_count = turn_count

    def to_dict(self) -> dict:
        return {
            "turn_count": self.turn_count,
        }


class Session:
    def __init__(self, session_dir: Path):
        self.session_dir = session_dir
        self.session_id = session_dir.name
        self._span_count: Optional[int] = None
        self._turn_count: Optional[int] = None
        self._log_count: Optional[int] = None
        self._min_start_nano: Optional[int] = None
        self._max_end_nano: Optional[int] = None
        self._service_name: Optional[str] = None
        self._load_metadata()
        self._load_log_count()

    @property
    def trace_file(self) -> Path:
        return self.session_dir / f"trace_{self.session_id}.jsonl"

    def _extract_service_name(self, span: dict) -> Optional[str]:
        resource = span.get("resource", {})
        for attr in resource.get("attributes", []):
            if attr.get("key") == "service.name":
                return attr.get("value", {}).get("string_value")
        return None

    def _update_min_start(self, current: Optional[int], span: dict) -> Optional[int]:
        if "start_time_unix_nano" not in span:
            return current
        start_nano = int(span["start_time_unix_nano"])
        if current is None or start_nano < current:
            return start_nano
        return current

    def _update_max_end(self, current: Optional[int], span: dict) -> Optional[int]:
        if "end_time_unix_nano" not in span:
            return current
        end_nano = int(span["end_time_unix_nano"])
        if current is None or end_nano > current:
            return end_nano
        return current

    def _load_metadata(self):
        span_count = 0
        turn_count = 0
        min_start = None
        max_end = None
        service_name = None

        try:
            with open(self.trace_file, 'r') as f:
                for line in f:
                    if line.strip():
                        span = json.loads(line)
                        span_count += 1
                        if span.get("name") == "turn":
                            turn_count += 1
                        min_start = self._update_min_start(min_start, span)
                        max_end = self._update_max_end(max_end, span)
                        if service_name is None:
                            service_name = self._extract_service_name(span)
        except Exception as e:
            print(f"Error loading session {self.trace_file}: {e}")

        self._span_count = span_count
        self._turn_count = turn_count
        self._min_start_nano = min_start
        self._max_end_nano = max_end
        self._service_name = service_name

    def _load_log_count(self):
        log_file = self.session_dir / f"logs_{self.session_id}.jsonl"
        if not log_file.exists():
            self._log_count = 0
            return

        try:
            count = 0
            with open(log_file, 'r') as f:
                for line in f:
                    if line.strip():
                        count += 1
            self._log_count = count
        except Exception as e:
            print(f"Error loading log count for session {self.session_id}: {e}")
            self._log_count = 0

    @property
    def span_count(self) -> int:
        return self._span_count or 0

    @property
    def turn_count(self) -> int:
        return self._turn_count or 0

    @property
    def log_count(self) -> int:
        return self._log_count or 0

    @property
    def start_time(self) -> Optional[float]:
        if self._min_start_nano:
            return self._min_start_nano / 1_000_000_000
        return None

    @property
    def end_time(self) -> Optional[float]:
        if self._max_end_nano:
            return self._max_end_nano / 1_000_000_000
        return None

    @property
    def duration_ms(self) -> Optional[float]:
        if self._min_start_nano and self._max_end_nano:
            return (self._max_end_nano - self._min_start_nano) / 1_000_000
        return None

    @property
    def service_name(self) -> Optional[str]:
        return self._service_name

    @property
    def trace(self) -> Trace:
        return Trace(turn_count=self.turn_count)

    def get_audio_size_bytes(self) -> Optional[int]:
        sessions_base_dir = self.session_dir.parent
        chunks = find_chunks(sessions_base_dir, self.session_id)
        if not chunks:
            return None
        return sum(chunk_path.stat().st_size for _, chunk_path in chunks)

    def to_dict(self) -> dict:
        audio_bytes = self.get_audio_size_bytes()
        audio_size_mb = audio_bytes // (1024 * 1024) if audio_bytes is not None else None
        return {
            "session_id": self.session_id,
            "service_name": self.service_name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "audio_size_mb": audio_size_mb,
            "trace": self.trace.to_dict(),
            "log_count": self.log_count,
        }
