import json
import pytest

from finchvox.storage.local import LocalStorage


@pytest.fixture
def local_storage(temp_data_dir):
    return LocalStorage(temp_data_dir)


@pytest.fixture
def create_local_session(temp_data_dir):
    def _create(session_id: str, spans: list[dict]) -> None:
        sessions_dir = temp_data_dir / "sessions"
        session_dir = sessions_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        trace_file = session_dir / f"trace_{session_id}.jsonl"
        with open(trace_file, "w") as f:
            for span in spans:
                f.write(json.dumps(span) + "\n")

    return _create


class TestLocalStorageWriteFile:
    @pytest.mark.asyncio
    async def test_writes_file_to_session_dir(self, local_storage, temp_data_dir):
        session_id = "abc123def456789012345678"
        content = b"test content"

        await local_storage.write_file(session_id, "test.txt", content)

        file_path = temp_data_dir / "sessions" / session_id / "test.txt"
        assert file_path.exists()
        assert file_path.read_bytes() == content

    @pytest.mark.asyncio
    async def test_creates_session_directory(self, local_storage, temp_data_dir):
        session_id = "new123session45678901234"

        await local_storage.write_file(session_id, "data.bin", b"data")

        session_dir = temp_data_dir / "sessions" / session_id
        assert session_dir.exists()
        assert session_dir.is_dir()


class TestLocalStorageReadFile:
    @pytest.mark.asyncio
    async def test_reads_existing_file(self, local_storage, temp_data_dir):
        session_id = "read123test456789012345"
        session_dir = temp_data_dir / "sessions" / session_id
        session_dir.mkdir(parents=True)
        (session_dir / "content.txt").write_bytes(b"hello world")

        result = await local_storage.read_file(session_id, "content.txt")
        assert result == b"hello world"


class TestLocalStorageFileExists:
    @pytest.mark.asyncio
    async def test_returns_true_for_existing_file(self, local_storage, temp_data_dir):
        session_id = "exists123test45678901234"
        session_dir = temp_data_dir / "sessions" / session_id
        session_dir.mkdir(parents=True)
        (session_dir / "exists.txt").write_text("content")

        assert await local_storage.file_exists(session_id, "exists.txt") is True

    @pytest.mark.asyncio
    async def test_returns_false_for_missing_file(self, local_storage):
        session_id = "missing123test4567890123"
        assert await local_storage.file_exists(session_id, "missing.txt") is False


class TestLocalStorageListSessions:
    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_sessions(self, local_storage):
        sessions = await local_storage.list_sessions()
        assert sessions == []

    @pytest.mark.asyncio
    async def test_returns_sessions_with_manifest(self, local_storage, temp_data_dir):
        session_id = "manifest123session456789"
        session_dir = temp_data_dir / "sessions" / session_id
        session_dir.mkdir(parents=True)
        manifest = {
            "session_id": session_id,
            "service_name": "test",
            "start_time": 1000,
        }
        (session_dir / "manifest.json").write_text(json.dumps(manifest))

        sessions = await local_storage.list_sessions()
        assert len(sessions) == 1
        assert sessions[0]["session_id"] == session_id

    @pytest.mark.asyncio
    async def test_returns_sessions_from_trace_file(
        self, local_storage, create_local_session
    ):
        session_id = "trace123session456789012"
        spans = [
            {
                "name": "root",
                "start_time_unix_nano": "1000000000000",
                "end_time_unix_nano": "2000000000000",
            }
        ]
        create_local_session(session_id, spans)

        sessions = await local_storage.list_sessions()
        assert len(sessions) == 1
        assert sessions[0]["session_id"] == session_id

    @pytest.mark.asyncio
    async def test_respects_limit(self, local_storage, create_local_session):
        for i in range(5):
            session_id = f"session{i:02d}12345678901234567"
            create_local_session(
                session_id,
                [{"name": "root", "start_time_unix_nano": str(i * 1000000000000)}],
            )

        sessions = await local_storage.list_sessions(limit=3)
        assert len(sessions) == 3


class TestLocalStorageDeleteSession:
    @pytest.mark.asyncio
    async def test_deletes_session_directory(self, local_storage, temp_data_dir):
        session_id = "delete123test45678901234"
        session_dir = temp_data_dir / "sessions" / session_id
        session_dir.mkdir(parents=True)
        (session_dir / "test.txt").write_text("content")

        await local_storage.delete_session(session_id)

        assert not session_dir.exists()

    @pytest.mark.asyncio
    async def test_does_not_error_for_missing_session(self, local_storage):
        await local_storage.delete_session("nonexistent123456789012")


class TestLocalStorageGetSessionManifest:
    @pytest.mark.asyncio
    async def test_returns_manifest_when_exists(self, local_storage, temp_data_dir):
        session_id = "manifest123get4567890123"
        session_dir = temp_data_dir / "sessions" / session_id
        session_dir.mkdir(parents=True)
        manifest = {"session_id": session_id, "service_name": "test"}
        (session_dir / "manifest.json").write_text(json.dumps(manifest))

        result = await local_storage.get_session_manifest(session_id)
        assert result == manifest

    @pytest.mark.asyncio
    async def test_builds_manifest_from_trace(
        self, local_storage, create_local_session
    ):
        session_id = "notrace123manifest456789"
        spans = [{"name": "root", "start_time_unix_nano": "1000000000000"}]
        create_local_session(session_id, spans)

        result = await local_storage.get_session_manifest(session_id)
        assert result["session_id"] == session_id

    @pytest.mark.asyncio
    async def test_returns_none_when_no_trace(self, local_storage, temp_data_dir):
        session_id = "nodata123session45678901"
        session_dir = temp_data_dir / "sessions" / session_id
        session_dir.mkdir(parents=True)

        result = await local_storage.get_session_manifest(session_id)
        assert result is None
