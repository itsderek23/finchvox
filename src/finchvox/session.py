import io
import json
import shutil
import zipfile
from pathlib import Path
from typing import Optional

from finchvox.audio_utils import find_chunks


def _read_jsonl_file(file_path: Path) -> list[dict]:
    records = []
    with open(file_path, "r") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


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
            with open(self.trace_file, "r") as f:
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
            with open(log_file, "r") as f:
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

    def is_root_span_ended(self) -> bool:
        try:
            with open(self.trace_file, "r") as f:
                for line in f:
                    if line.strip():
                        span = json.loads(line)
                        if not span.get("parent_span_id_hex") and span.get(
                            "end_time_unix_nano"
                        ):
                            return True
        except Exception:
            pass
        return False

    def get_audio_size_bytes(self) -> Optional[int]:
        opus_file = self.session_dir / "audio.opus"
        if opus_file.exists():
            return opus_file.stat().st_size

        wav_file = self.session_dir / "audio.wav"
        if wav_file.exists():
            return wav_file.stat().st_size

        sessions_base_dir = self.session_dir.parent
        chunks = find_chunks(sessions_base_dir, self.session_id)
        if not chunks:
            return None
        return sum(chunk_path.stat().st_size for _, chunk_path in chunks)

    def to_dict(self) -> dict:
        audio_bytes = self.get_audio_size_bytes()
        if audio_bytes is None:
            audio_size_mb = None
        elif audio_bytes < 1024 * 1024:
            audio_size_mb = round(audio_bytes / (1024 * 1024), 1)
        else:
            audio_size_mb = audio_bytes // (1024 * 1024)
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

    def to_zip(self) -> io.BytesIO:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in self.session_dir.rglob("*"):
                if file_path.is_file():
                    arcname = (
                        f"{self.session_id}/{file_path.relative_to(self.session_dir)}"
                    )
                    zf.write(file_path, arcname)

        zip_buffer.seek(0)
        return zip_buffer

    @property
    def start_time_nano(self) -> Optional[int]:
        return self._min_start_nano

    @property
    def logs_file(self) -> Path:
        return self.session_dir / f"logs_{self.session_id}.jsonl"

    @property
    def exceptions_file(self) -> Path:
        return self.session_dir / f"exceptions_{self.session_id}.jsonl"

    @property
    def environment_file(self) -> Path:
        return self.session_dir / f"environment_{self.session_id}.json"

    @property
    def has_environment(self) -> bool:
        return self.environment_file.exists()

    def get_spans(self) -> list[dict]:
        if not self.trace_file.exists():
            return []
        return _read_jsonl_file(self.trace_file)

    def get_logs(self) -> list[dict]:
        if not self.logs_file.exists():
            return []
        logs = []
        with open(self.logs_file, "r") as f:
            for line in f:
                if line.strip():
                    try:
                        logs.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return logs

    def get_exceptions(self) -> list[dict]:
        if not self.exceptions_file.exists():
            return []
        return _read_jsonl_file(self.exceptions_file)

    @staticmethod
    def _validate_jsonl_file(zf: zipfile.ZipFile, jsonl_file: str) -> str | None:
        with zf.open(jsonl_file) as f:
            for line_num, line in enumerate(f, 1):
                line = line.decode("utf-8").strip()
                if not line:
                    continue
                try:
                    json.loads(line)
                except json.JSONDecodeError:
                    return f"Invalid JSON on line {line_num} of {jsonl_file}"
        return None

    @staticmethod
    def validate_zip(zip_bytes: bytes) -> tuple[bool, str | None]:
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
                jsonl_files = [f for f in zf.namelist() if f.endswith(".jsonl")]
                if not jsonl_files:
                    return False, "Zip must contain at least one .jsonl file"

                for jsonl_file in jsonl_files:
                    error = Session._validate_jsonl_file(zf, jsonl_file)
                    if error:
                        return False, error

                return True, None
        except zipfile.BadZipFile:
            return False, "Invalid zip file"

    @staticmethod
    def extract_id_from_zip(zip_bytes: bytes) -> str | None:
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            file_list = zf.namelist()
            if not file_list:
                return None

            first_path = file_list[0]
            parts = first_path.split("/")
            if parts and parts[0]:
                return parts[0]

        return None

    @staticmethod
    def load_dict_from_dir(session_dir: Path) -> dict | None:
        manifest_path = session_dir / "manifest.json"
        if manifest_path.exists():
            return json.loads(manifest_path.read_text())

        trace_file = session_dir / f"trace_{session_dir.name}.jsonl"
        if not trace_file.exists():
            return None
        return Session(session_dir).to_dict()

    @staticmethod
    def from_zip(
        zip_bytes: bytes, sessions_base_dir: Path
    ) -> tuple[Optional["Session"], str | None]:
        is_valid, error_msg = Session.validate_zip(zip_bytes)
        if not is_valid:
            return None, error_msg

        session_id = Session.extract_id_from_zip(zip_bytes)
        if not session_id:
            return None, "Could not determine session ID from zip structure"

        session_dir = sessions_base_dir / session_id
        if session_dir.exists():
            shutil.rmtree(session_dir)

        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            zf.extractall(sessions_base_dir)

        return Session(session_dir), None
