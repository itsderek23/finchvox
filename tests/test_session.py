import json

import pytest

from finchvox.session import Session


@pytest.fixture
def create_session(temp_sessions_dir):
    def _create_session(session_id: str, spans: list[dict]) -> Session:
        session_dir = temp_sessions_dir / session_id
        session_dir.mkdir(parents=True)
        trace_file = session_dir / f"trace_{session_id}.jsonl"
        with open(trace_file, "w") as f:
            for span in spans:
                f.write(json.dumps(span) + "\n")
        return Session(session_dir)

    return _create_session


class TestIsRootSpanEnded:
    def test_returns_true_when_root_span_has_end_time(self, create_session):
        spans = [{"name": "root", "end_time_unix_nano": "2000000000000"}]
        session = create_session("root_ended_12345678901234567", spans)
        assert session.is_root_span_ended() is True

    def test_returns_false_when_root_span_has_no_end_time(self, create_session):
        spans = [{"name": "root", "start_time_unix_nano": "1000000000000"}]
        session = create_session("root_not_ended_123456789012", spans)
        assert session.is_root_span_ended() is False

    def test_returns_false_when_span_has_parent(self, create_session):
        spans = [
            {"parent_span_id_hex": "abc123", "end_time_unix_nano": "2000000000000"}
        ]
        session = create_session("has_parent_123456789012345", spans)
        assert session.is_root_span_ended() is False

    def test_returns_true_when_root_span_among_many(self, create_session):
        spans = [
            {"parent_span_id_hex": "abc123", "end_time_unix_nano": "1200000000000"},
            {"name": "root", "end_time_unix_nano": "2000000000000"},
            {"parent_span_id_hex": "abc123", "start_time_unix_nano": "1300000000000"},
        ]
        session = create_session("many_spans_123456789012345", spans)
        assert session.is_root_span_ended() is True

    def test_returns_false_for_empty_trace(self, create_session):
        session = create_session("empty_trace_123456789012345", [])
        assert session.is_root_span_ended() is False

    def test_returns_false_when_trace_file_missing(self, temp_sessions_dir):
        session_id = "no_trace_12345678901234567"
        session_dir = temp_sessions_dir / session_id
        session_dir.mkdir(parents=True)
        session = Session(session_dir)
        assert session.is_root_span_ended() is False


class TestLoadDictFromDir:
    def test_returns_manifest_when_exists(self, temp_sessions_dir):
        session_id = "manifest_exists_12345678901"
        session_dir = temp_sessions_dir / session_id
        session_dir.mkdir(parents=True)
        manifest = {"session_id": session_id, "service_name": "test"}
        (session_dir / "manifest.json").write_text(json.dumps(manifest))

        result = Session.load_dict_from_dir(session_dir)
        assert result == manifest

    def test_falls_back_to_trace_when_no_manifest(self, temp_sessions_dir):
        session_id = "no_manifest_1234567890123456"
        session_dir = temp_sessions_dir / session_id
        session_dir.mkdir(parents=True)
        spans = [
            {
                "name": "root",
                "start_time_unix_nano": "1000000000000",
                "end_time_unix_nano": "2000000000000",
            }
        ]
        trace_file = session_dir / f"trace_{session_id}.jsonl"
        with open(trace_file, "w") as f:
            for span in spans:
                f.write(json.dumps(span) + "\n")

        result = Session.load_dict_from_dir(session_dir)
        assert result["session_id"] == session_id
        assert result["duration_ms"] == 1000000.0

    def test_returns_none_when_no_trace_or_manifest(self, temp_sessions_dir):
        session_id = "empty_session_12345678901234"
        session_dir = temp_sessions_dir / session_id
        session_dir.mkdir(parents=True)

        result = Session.load_dict_from_dir(session_dir)
        assert result is None
