import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from finchvox.ui_routes import register_ui_routes


@pytest.fixture
def temp_data_dir():
    """Create a temporary data directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def app_with_temp_dir(temp_data_dir):
    """Create a FastAPI app with routes registered to temp directory."""
    app = FastAPI()
    register_ui_routes(app, temp_data_dir)
    return app


@pytest.fixture
def client(app_with_temp_dir):
    """Create a test client for the app."""
    return TestClient(app_with_temp_dir)


def create_trace_with_logs(data_dir: Path, trace_id: str, spans: list, logs: list):
    """Helper to create trace and log files for testing."""
    trace_dir = data_dir / "traces" / trace_id
    trace_dir.mkdir(parents=True, exist_ok=True)

    trace_file = trace_dir / f"trace_{trace_id}.jsonl"
    with trace_file.open("w") as f:
        for span in spans:
            json.dump(span, f)
            f.write("\n")

    if logs:
        log_file = trace_dir / f"logs_{trace_id}.jsonl"
        with log_file.open("w") as f:
            for log in logs:
                json.dump(log, f)
                f.write("\n")


class TestGetLogsEndpoint:

    def test_returns_logs_for_valid_trace(self, client, temp_data_dir):
        trace_id = "abc123def456"
        spans = [
            {"name": "test-span", "start_time_unix_nano": 1000000000, "end_time_unix_nano": 2000000000}
        ]
        logs = [
            {"time_unix_nano": 1500000000, "severity_text": "INFO", "body": "Test message 1"},
            {"time_unix_nano": 1200000000, "severity_text": "DEBUG", "body": "Test message 2"},
        ]
        create_trace_with_logs(temp_data_dir, trace_id, spans, logs)

        response = client.get(f"/api/logs/{trace_id}")

        assert response.status_code == 200
        data = response.json()
        assert "logs" in data
        assert len(data["logs"]) == 2
        assert data["total_count"] == 2
        assert data["limit"] == 1000
        assert data["trace_start_time"] == 1000000000

    def test_returns_empty_array_for_trace_with_no_logs_file(self, client, temp_data_dir):
        trace_id = "nologs123"
        spans = [
            {"name": "test-span", "start_time_unix_nano": 5000000000, "end_time_unix_nano": 6000000000}
        ]
        create_trace_with_logs(temp_data_dir, trace_id, spans, logs=[])

        response = client.get(f"/api/logs/{trace_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["logs"] == []
        assert data["total_count"] == 0
        assert data["trace_start_time"] == 5000000000

    def test_returns_404_for_nonexistent_trace(self, client):
        response = client.get("/api/logs/nonexistent_trace_id")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_respects_limit_parameter(self, client, temp_data_dir):
        trace_id = "limittest123"
        spans = [{"name": "span", "start_time_unix_nano": 1000000000}]
        logs = [
            {"time_unix_nano": i * 1000000, "severity_text": "INFO", "body": f"Log {i}"}
            for i in range(10)
        ]
        create_trace_with_logs(temp_data_dir, trace_id, spans, logs)

        response = client.get(f"/api/logs/{trace_id}?limit=5")

        assert response.status_code == 200
        data = response.json()
        assert len(data["logs"]) == 5
        assert data["total_count"] == 10
        assert data["limit"] == 5

    def test_logs_sorted_by_timestamp_ascending(self, client, temp_data_dir):
        trace_id = "sorttest123"
        spans = [{"name": "span", "start_time_unix_nano": 1000000000}]
        logs = [
            {"time_unix_nano": 3000000000, "severity_text": "INFO", "body": "Third"},
            {"time_unix_nano": 1000000000, "severity_text": "INFO", "body": "First"},
            {"time_unix_nano": 2000000000, "severity_text": "INFO", "body": "Second"},
        ]
        create_trace_with_logs(temp_data_dir, trace_id, spans, logs)

        response = client.get(f"/api/logs/{trace_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["logs"][0]["body"] == "First"
        assert data["logs"][1]["body"] == "Second"
        assert data["logs"][2]["body"] == "Third"

    def test_trace_start_time_is_minimum_span_start(self, client, temp_data_dir):
        trace_id = "starttime123"
        spans = [
            {"name": "span1", "start_time_unix_nano": 5000000000},
            {"name": "span2", "start_time_unix_nano": 3000000000},
            {"name": "span3", "start_time_unix_nano": 7000000000},
        ]
        logs = [{"time_unix_nano": 4000000000, "severity_text": "INFO", "body": "Test"}]
        create_trace_with_logs(temp_data_dir, trace_id, spans, logs)

        response = client.get(f"/api/logs/{trace_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["trace_start_time"] == 3000000000
