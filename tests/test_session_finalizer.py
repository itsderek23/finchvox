import json
from pathlib import Path
from unittest.mock import patch


from finchvox.session_finalizer import SessionFinalizer


def create_trace_file(session_dir: Path, session_id: str, spans: list[dict]):
    trace_file = session_dir / f"trace_{session_id}.jsonl"
    with open(trace_file, "w") as f:
        for span in spans:
            f.write(json.dumps(span) + "\n")


def create_logs_file(session_dir: Path, session_id: str, log_count: int = 5):
    logs_file = session_dir / f"logs_{session_id}.jsonl"
    with open(logs_file, "w") as f:
        for i in range(log_count):
            f.write(json.dumps({"time_unix_nano": str(i), "body": f"log {i}"}) + "\n")


class TestSessionFinalizer:
    def test_finalize_generates_manifest(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "test123456789012345678901234"
        session_dir = sessions_dir / session_id
        session_dir.mkdir(parents=True)

        spans = [
            {
                "name": "root",
                "start_time_unix_nano": "1000000000000",
                "end_time_unix_nano": "2000000000000",
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"string_value": "test-service"},
                        }
                    ]
                },
            },
            {
                "name": "turn",
                "parent_span_id_hex": "abc123",
                "start_time_unix_nano": "1100000000000",
                "end_time_unix_nano": "1200000000000",
            },
        ]
        create_trace_file(session_dir, session_id, spans)
        create_logs_file(session_dir, session_id, log_count=3)

        finalizer = SessionFinalizer(sessions_dir)
        result = finalizer.finalize(session_id)

        assert result is True
        manifest_path = session_dir / "manifest.json"
        assert manifest_path.exists()

        manifest = json.loads(manifest_path.read_text())
        assert manifest["session_id"] == session_id
        assert manifest["service_name"] == "test-service"
        assert manifest["turn_count"] == 1
        assert manifest["log_count"] == 3
        assert manifest["duration_ms"] == 1000000.0

    def test_finalize_generates_manifest_without_ffmpeg(
        self, temp_data_dir, create_wav_file
    ):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "nocodec12345678901234567890"
        session_dir = sessions_dir / session_id
        audio_dir = session_dir / "audio"
        audio_dir.mkdir(parents=True)

        spans = [
            {
                "name": "root",
                "start_time_unix_nano": "1000000000000",
                "end_time_unix_nano": "2000000000000",
            }
        ]
        create_trace_file(session_dir, session_id, spans)

        chunk_path = audio_dir / "chunk_0000.wav"
        create_wav_file(chunk_path, duration_seconds=0.5)

        with patch(
            "finchvox.audio_compressor.check_ffmpeg_available", return_value=False
        ):
            finalizer = SessionFinalizer(sessions_dir)
            result = finalizer.finalize(session_id)

        assert result is True
        manifest_path = session_dir / "manifest.json"
        assert manifest_path.exists()
        assert audio_dir.exists()

    def test_finalize_compresses_audio_when_ffmpeg_available(
        self, temp_data_dir, create_wav_file
    ):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "withcodec1234567890123456789"
        session_dir = sessions_dir / session_id
        audio_dir = session_dir / "audio"
        audio_dir.mkdir(parents=True)

        spans = [
            {
                "name": "root",
                "start_time_unix_nano": "1000000000000",
                "end_time_unix_nano": "2000000000000",
            }
        ]
        create_trace_file(session_dir, session_id, spans)

        chunk_path = audio_dir / "chunk_0000.wav"
        create_wav_file(chunk_path, duration_seconds=0.5)

        with patch(
            "finchvox.audio_compressor.check_ffmpeg_available", return_value=True
        ):
            with patch("finchvox.audio_compressor.compress_to_opus") as mock_compress:
                mock_compress.return_value = True

                finalizer = SessionFinalizer(sessions_dir)
                result = finalizer.finalize(session_id)

        assert result is True
        manifest_path = session_dir / "manifest.json"
        assert manifest_path.exists()
        mock_compress.assert_called_once()

    def test_finalize_returns_false_for_nonexistent_session(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        finalizer = SessionFinalizer(sessions_dir)
        result = finalizer.finalize("nonexistent123456789012345")
        assert result is False

    def test_manifest_contains_expected_fields(self, temp_data_dir):
        sessions_dir = temp_data_dir / "sessions"
        session_id = "fields123456789012345678901"
        session_dir = sessions_dir / session_id
        session_dir.mkdir(parents=True)

        spans = [
            {
                "name": "root",
                "start_time_unix_nano": "1707300600000000000",
                "end_time_unix_nano": "1707301500000000000",
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"string_value": "my-voice-agent"},
                        }
                    ]
                },
            },
            {"name": "turn", "parent_span_id_hex": "abc"},
            {"name": "turn", "parent_span_id_hex": "def"},
        ]
        create_trace_file(session_dir, session_id, spans)
        create_logs_file(session_dir, session_id, log_count=10)

        finalizer = SessionFinalizer(sessions_dir)
        finalizer.finalize(session_id)

        manifest = json.loads((session_dir / "manifest.json").read_text())

        assert manifest["session_id"] == session_id
        assert manifest["service_name"] == "my-voice-agent"
        assert manifest["start_time"] == 1707300600.0
        assert manifest["end_time"] == 1707301500.0
        assert manifest["duration_ms"] == 900000.0
        assert manifest["audio_size_bytes"] is None
        assert manifest["turn_count"] == 2
        assert manifest["log_count"] == 10
