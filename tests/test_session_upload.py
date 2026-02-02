import io
import json
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from finchvox.ui_routes import register_ui_routes


@pytest.fixture
def temp_data_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def app_with_temp_dir(temp_data_dir):
    app = FastAPI()
    register_ui_routes(app, temp_data_dir)
    return app


@pytest.fixture
def client(app_with_temp_dir):
    return TestClient(app_with_temp_dir)


@dataclass
class SessionContent:
    spans: list
    logs: list = field(default_factory=list)
    exceptions: list = field(default_factory=list)
    audio_files: list = field(default_factory=list)


def _write_jsonl(path: Path, records: list):
    with path.open("w") as f:
        for record in records:
            json.dump(record, f)
            f.write("\n")


def create_session_files(data_dir: Path, session_id: str, content: SessionContent):
    session_dir = data_dir / "sessions" / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    _write_jsonl(session_dir / f"trace_{session_id}.jsonl", content.spans)

    if content.logs:
        _write_jsonl(session_dir / f"logs_{session_id}.jsonl", content.logs)

    if content.exceptions:
        _write_jsonl(session_dir / f"exceptions_{session_id}.jsonl", content.exceptions)

    if content.audio_files:
        audio_dir = session_dir / "audio"
        audio_dir.mkdir(exist_ok=True)
        for audio_filename, audio_content in content.audio_files:
            (audio_dir / audio_filename).write_bytes(audio_content)


def create_valid_zip(session_id: str, spans: list, logs: list = None) -> bytes:
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        trace_content = "\n".join(json.dumps(span) for span in spans)
        zf.writestr(f"{session_id}/trace_{session_id}.jsonl", trace_content)

        if logs:
            log_content = "\n".join(json.dumps(log) for log in logs)
            zf.writestr(f"{session_id}/logs_{session_id}.jsonl", log_content)

    zip_buffer.seek(0)
    return zip_buffer.read()


def upload_zip(client, zip_buffer: io.BytesIO):
    zip_buffer.seek(0)
    return client.post(
        "/api/sessions/upload",
        files={"file": ("test.zip", zip_buffer, "application/zip")}
    )


def download_and_get_zip_files(client, session_id: str) -> list[str]:
    response = client.get(f"/api/sessions/{session_id}/download")
    assert response.status_code == 200
    zip_buffer = io.BytesIO(response.content)
    with zipfile.ZipFile(zip_buffer, 'r') as zf:
        return zf.namelist()


class TestSessionDownload:

    def test_download_returns_valid_zip_with_trace(self, client, temp_data_dir):
        session_id = "download123"
        spans = [{"name": "test-span", "start_time_unix_nano": 1000000000}]
        create_session_files(temp_data_dir, session_id, SessionContent(spans=spans))

        response = client.get(f"/api/sessions/{session_id}/download")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/zip"
        assert f"finchvox_session_{session_id}.zip" in response.headers["content-disposition"]

        zip_buffer = io.BytesIO(response.content)
        with zipfile.ZipFile(zip_buffer, 'r') as zf:
            file_list = zf.namelist()
            assert f"{session_id}/trace_{session_id}.jsonl" in file_list

    def test_download_includes_logs_if_present(self, client, temp_data_dir):
        session_id = "download_logs"
        spans = [{"name": "test-span", "start_time_unix_nano": 1000000000}]
        logs = [{"time_unix_nano": 1500000000, "severity_text": "INFO", "body": "Test log"}]
        create_session_files(temp_data_dir, session_id, SessionContent(spans=spans, logs=logs))

        file_list = download_and_get_zip_files(client, session_id)

        assert f"{session_id}/trace_{session_id}.jsonl" in file_list
        assert f"{session_id}/logs_{session_id}.jsonl" in file_list

    def test_download_includes_audio_files(self, client, temp_data_dir):
        session_id = "download_audio"
        spans = [{"name": "test-span", "start_time_unix_nano": 1000000000}]
        audio_files = [("chunk_001.wav", b"fake audio data")]
        create_session_files(temp_data_dir, session_id, SessionContent(spans=spans, audio_files=audio_files))

        file_list = download_and_get_zip_files(client, session_id)

        assert f"{session_id}/audio/chunk_001.wav" in file_list

    def test_download_returns_404_for_nonexistent_session(self, client):
        response = client.get("/api/sessions/nonexistent_session/download")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()


class TestSessionUpload:

    def test_valid_zip_upload_extracts_successfully(self, client, temp_data_dir):
        session_id = "uploaded_session"
        spans = [{"name": "test-span", "start_time_unix_nano": 1000000000}]
        zip_content = create_valid_zip(session_id, spans)

        response = client.post(
            "/api/sessions/upload",
            files={"file": ("test.zip", io.BytesIO(zip_content), "application/zip")}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["session_id"] == session_id

        session_dir = temp_data_dir / "sessions" / session_id
        assert session_dir.exists()
        trace_file = session_dir / f"trace_{session_id}.jsonl"
        assert trace_file.exists()

    def test_invalid_zip_file_returns_400(self, client):
        invalid_content = b"this is not a zip file"

        response = client.post(
            "/api/sessions/upload",
            files={"file": ("test.zip", io.BytesIO(invalid_content), "application/zip")}
        )

        assert response.status_code == 400
        assert "invalid zip" in response.json()["detail"].lower()

    def test_zip_missing_jsonl_returns_400(self, client):
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zf:
            zf.writestr("session123/readme.txt", "No JSONL files here")

        response = upload_zip(client, zip_buffer)

        assert response.status_code == 400
        assert "jsonl" in response.json()["detail"].lower()

    def test_jsonl_with_invalid_json_returns_400(self, client):
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zf:
            zf.writestr("session123/trace_session123.jsonl", "not valid json\n{bad json}")

        response = upload_zip(client, zip_buffer)

        assert response.status_code == 400
        assert "invalid json" in response.json()["detail"].lower()

    def test_upload_overwrites_existing_session(self, client, temp_data_dir):
        session_id = "existing_session"
        original_spans = [{"name": "original-span", "version": 1}]
        create_session_files(temp_data_dir, session_id, SessionContent(spans=original_spans))

        new_spans = [{"name": "new-span", "version": 2}]
        zip_content = create_valid_zip(session_id, new_spans)

        response = client.post(
            "/api/sessions/upload",
            files={"file": ("test.zip", io.BytesIO(zip_content), "application/zip")}
        )

        assert response.status_code == 200

        trace_file = temp_data_dir / "sessions" / session_id / f"trace_{session_id}.jsonl"
        content = trace_file.read_text()
        assert "new-span" in content
        assert "original-span" not in content


class TestDownloadUploadRoundTrip:

    def test_downloaded_session_can_be_uploaded(self, client, temp_data_dir):
        session_id = "roundtrip_session"
        spans = [{"name": "test-span", "start_time_unix_nano": 1000000000, "end_time_unix_nano": 2000000000}]
        logs = [{"time_unix_nano": 1500000000, "severity_text": "INFO", "body": "Test message"}]
        create_session_files(temp_data_dir, session_id, SessionContent(spans=spans, logs=logs))

        download_response = client.get(f"/api/sessions/{session_id}/download")
        assert download_response.status_code == 200

        session_dir = temp_data_dir / "sessions" / session_id
        import shutil
        shutil.rmtree(session_dir)

        upload_response = client.post(
            "/api/sessions/upload",
            files={"file": ("test.zip", io.BytesIO(download_response.content), "application/zip")}
        )

        assert upload_response.status_code == 200
        assert upload_response.json()["session_id"] == session_id

        assert session_dir.exists()
        trace_file = session_dir / f"trace_{session_id}.jsonl"
        assert trace_file.exists()
        log_file = session_dir / f"logs_{session_id}.jsonl"
        assert log_file.exists()
