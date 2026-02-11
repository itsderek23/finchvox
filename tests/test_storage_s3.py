import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from finchvox.storage.backend import SessionFile
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
        file = SessionFile(session_id="session123", filename="test.txt")

        with patch.object(
            s3_storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            await s3_storage.write_file(file, b"content")

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


class TestS3StorageValidateConnection:
    @pytest.mark.asyncio
    async def test_validates_existing_bucket(self, s3_storage):
        mock_s3 = AsyncMock()
        mock_s3.head_bucket = AsyncMock()

        with patch.object(
            s3_storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            await s3_storage.validate_connection()

        mock_s3.head_bucket.assert_called_once_with(Bucket="test-bucket")

    @pytest.mark.asyncio
    async def test_creates_bucket_when_missing(self, s3_storage):
        mock_s3 = AsyncMock()
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_bucket = AsyncMock(
            side_effect=ClientError(error_response, "HeadBucket")
        )
        mock_s3.create_bucket = AsyncMock()

        with patch.object(
            s3_storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            await s3_storage.validate_connection()

        mock_s3.create_bucket.assert_called_once_with(Bucket="test-bucket")

    @pytest.mark.asyncio
    async def test_creates_bucket_with_location_for_non_us_east_1(self):
        storage = S3Storage(bucket="test-bucket", region="eu-west-1")
        mock_s3 = AsyncMock()
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_bucket = AsyncMock(
            side_effect=ClientError(error_response, "HeadBucket")
        )
        mock_s3.create_bucket = AsyncMock()

        with patch.object(
            storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            await storage.validate_connection()

        mock_s3.create_bucket.assert_called_once_with(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )

    @pytest.mark.asyncio
    async def test_exits_on_auth_failure(self, s3_storage, capsys):
        mock_s3 = AsyncMock()
        error_response = {"Error": {"Code": "403", "Message": "Access Denied"}}
        mock_s3.head_bucket = AsyncMock(
            side_effect=ClientError(error_response, "HeadBucket")
        )

        with patch.object(
            s3_storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            with pytest.raises(SystemExit) as exc_info:
                await s3_storage.validate_connection()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "S3 Authentication Failed" in captured.out
        assert "AWS_ACCESS_KEY_ID" in captured.out

    @pytest.mark.asyncio
    async def test_exits_on_connection_error_with_endpoint(self, capsys):
        storage = S3Storage(
            bucket="test-bucket",
            endpoint_url="http://localhost:4566",
        )
        mock_s3 = AsyncMock()
        mock_s3.head_bucket = AsyncMock(
            side_effect=EndpointConnectionError(endpoint_url="http://localhost:4566")
        )

        with patch.object(
            storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            with pytest.raises(SystemExit) as exc_info:
                await storage.validate_connection()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "S3 Connection Failed" in captured.out
        assert "LocalStack" in captured.out

    @pytest.mark.asyncio
    async def test_exits_on_connection_error_without_endpoint(self, s3_storage, capsys):
        mock_s3 = AsyncMock()
        mock_s3.head_bucket = AsyncMock(
            side_effect=EndpointConnectionError(endpoint_url="https://s3.amazonaws.com")
        )

        with patch.object(
            s3_storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            with pytest.raises(SystemExit) as exc_info:
                await s3_storage.validate_connection()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "S3 Connection Failed" in captured.out
        assert "only needed for LocalStack/MinIO" in captured.out

    @pytest.mark.asyncio
    async def test_exits_on_bucket_creation_failure(self, s3_storage, capsys):
        mock_s3 = AsyncMock()
        head_error = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_bucket = AsyncMock(
            side_effect=ClientError(head_error, "HeadBucket")
        )
        create_error = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        mock_s3.create_bucket = AsyncMock(
            side_effect=ClientError(create_error, "CreateBucket")
        )

        with patch.object(
            s3_storage._session, "client", return_value=AsyncContextManager(mock_s3)
        ):
            with pytest.raises(SystemExit) as exc_info:
                await s3_storage.validate_connection()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "S3 Bucket Creation Failed" in captured.out
        assert "aws s3 mb" in captured.out


class TestS3StorageExitWithError:
    def test_prints_formatted_error(self, s3_storage, capsys):
        with pytest.raises(SystemExit) as exc_info:
            s3_storage._exit_with_error(
                "Test Error",
                "Something went wrong",
                ["Try this", "Or try that"],
            )

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Test Error" in captured.out
        assert "Something went wrong" in captured.out
        assert "Try this" in captured.out
        assert "Or try that" in captured.out
        assert "test-bucket" in captured.out

    def test_prints_endpoint_when_configured(self, capsys):
        storage = S3Storage(
            bucket="my-bucket",
            endpoint_url="http://localhost:4566",
        )

        with pytest.raises(SystemExit):
            storage._exit_with_error("Error", "Message", ["Hint"])

        captured = capsys.readouterr()
        assert "http://localhost:4566" in captured.out
