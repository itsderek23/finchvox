import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from finchvox.storage.s3 import S3Storage


@pytest.fixture
def s3_storage():
    return S3Storage(
        bucket="test-bucket",
        region="us-east-1",
        prefix="sessions",
    )


class TestS3StorageInit:
    def test_creates_with_required_params(self):
        storage = S3Storage(bucket="mybucket")
        assert storage.bucket == "mybucket"
        assert storage.region == "us-east-1"
        assert storage.prefix == "sessions"
        assert storage.endpoint_url is None

    def test_creates_with_all_params(self):
        storage = S3Storage(
            bucket="mybucket",
            region="eu-west-1",
            prefix="data",
            endpoint_url="http://localhost:4566",
        )
        assert storage.bucket == "mybucket"
        assert storage.region == "eu-west-1"
        assert storage.prefix == "data"
        assert storage.endpoint_url == "http://localhost:4566"


class TestS3StorageGetSessionPrefix:
    def test_generates_date_partitioned_prefix(self, s3_storage):
        from datetime import datetime

        date = datetime(2024, 2, 7, 12, 0, 0)
        session_id = "abc123def456789012345678"

        prefix = s3_storage._get_session_prefix(session_id, date)

        assert prefix == "sessions/2024/02/07/abc123def456789012345678"

    def test_uses_current_date_when_not_specified(self, s3_storage):
        from datetime import datetime, timezone

        session_id = "abc123def456789012345678"
        prefix = s3_storage._get_session_prefix(session_id)

        today = datetime.now(timezone.utc)
        expected_date = today.strftime("%Y/%m/%d")
        assert prefix.startswith(f"sessions/{expected_date}/")


class TestS3StorageWriteFile:
    @pytest.mark.asyncio
    async def test_writes_to_s3(self, s3_storage):
        mock_s3 = AsyncMock()
        mock_s3.put_object = AsyncMock()

        with patch.object(
            s3_storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            await s3_storage.write_file("session123", "test.txt", b"content")

        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "test-bucket"
        assert "session123/test.txt" in call_kwargs["Key"]
        assert call_kwargs["Body"] == b"content"


class TestS3StorageListSessions:
    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_sessions(self, s3_storage):
        mock_s3 = AsyncMock()

        async def mock_paginate(*args, **kwargs):
            yield {"CommonPrefixes": []}

        mock_paginator = MagicMock()
        mock_paginator.paginate = mock_paginate
        mock_s3.get_paginator = MagicMock(return_value=mock_paginator)

        with patch.object(
            s3_storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            sessions = await s3_storage.list_sessions()

        assert sessions == []


class TestS3StorageUploadSession:
    @pytest.mark.asyncio
    async def test_uploads_all_files(self, s3_storage, tmp_path):
        local_dir = tmp_path / "session123"
        local_dir.mkdir()
        (local_dir / "trace_session123.jsonl").write_text('{"name": "test"}\n')
        (local_dir / "manifest.json").write_text(
            json.dumps({"session_id": "session123", "start_time": 1000})
        )

        mock_s3 = AsyncMock()
        mock_s3.put_object = AsyncMock()

        with patch.object(
            s3_storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            await s3_storage.upload_session("session123", local_dir)

        assert mock_s3.put_object.call_count == 2


class TestS3StorageDownloadSession:
    @pytest.mark.asyncio
    async def test_downloads_session_files(self, s3_storage, tmp_path):
        local_dir = tmp_path / "downloaded_session"

        mock_body = AsyncMock()
        mock_body.read = AsyncMock(return_value=b'{"session_id": "abc123"}')

        mock_s3 = AsyncMock()

        async def mock_paginate(*args, **kwargs):
            yield {
                "Contents": [
                    {"Key": "sessions/2024/02/07/abc123/manifest.json"},
                    {"Key": "sessions/2024/02/07/abc123/trace_abc123.jsonl"},
                ]
            }

        mock_paginator = MagicMock()
        mock_paginator.paginate = mock_paginate
        mock_s3.get_paginator = MagicMock(return_value=mock_paginator)
        mock_s3.get_object = AsyncMock(
            return_value={"Body": AsyncContextManager(mock_body)}
        )

        with patch.object(
            s3_storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            result = await s3_storage.download_session("abc123", local_dir)

        assert result is True
        assert (local_dir / "manifest.json").exists()


class TestS3StorageDeleteSession:
    @pytest.mark.asyncio
    async def test_deletes_all_session_objects(self, s3_storage):
        mock_s3 = AsyncMock()

        async def mock_paginate(*args, **kwargs):
            yield {
                "Contents": [
                    {"Key": "sessions/2024/02/07/abc123/manifest.json"},
                    {"Key": "sessions/2024/02/07/abc123/trace_abc123.jsonl"},
                ]
            }

        mock_paginator = MagicMock()
        mock_paginator.paginate = mock_paginate
        mock_s3.get_paginator = MagicMock(return_value=mock_paginator)
        mock_s3.delete_objects = AsyncMock()

        with patch.object(
            s3_storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            await s3_storage.delete_session("abc123")

        mock_s3.delete_objects.assert_called_once()
        call_kwargs = mock_s3.delete_objects.call_args.kwargs
        assert len(call_kwargs["Delete"]["Objects"]) == 2


class AsyncContextManager:
    def __init__(self, mock_obj):
        self.mock_obj = mock_obj

    async def __aenter__(self):
        return self.mock_obj

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
