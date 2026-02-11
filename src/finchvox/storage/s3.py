import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aioboto3
from botocore.exceptions import ClientError, EndpointConnectionError
from loguru import logger

from finchvox.storage.backend import SessionFile


class S3Storage:
    def __init__(
        self,
        bucket: str,
        region: str = "us-east-1",
        prefix: str = "sessions",
        endpoint_url: Optional[str] = None,
    ):
        self.bucket = bucket
        self.region = region
        self.prefix = prefix
        self.endpoint_url = endpoint_url
        self._session = aioboto3.Session()

    def _get_session_prefix(
        self, session_id: str, date: Optional[datetime] = None
    ) -> str:
        if date is None:
            date = datetime.now(timezone.utc)
        date_path = date.strftime("%Y/%m/%d")
        return f"{self.prefix}/{date_path}/{session_id}"

    def _get_client_kwargs(self) -> dict:
        kwargs = {"region_name": self.region}
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        return kwargs

    async def validate_connection(self) -> None:
        async with self._session.client("s3", **self._get_client_kwargs()) as s3:
            try:
                await s3.head_bucket(Bucket=self.bucket)
                logger.info(f"S3 bucket validated: {self.bucket}")
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "404":
                    logger.info(f"Bucket {self.bucket} not found, creating...")
                    await self._create_bucket(s3)
                elif error_code == "403":
                    self._exit_with_error(
                        "S3 Authentication Failed",
                        "Invalid or missing AWS credentials.",
                        [
                            "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY",
                            "Or configure ~/.aws/credentials",
                            "If using LocalStack: credentials can be any non-empty value",
                        ],
                    )
                else:
                    self._exit_with_error(
                        "S3 Connection Failed",
                        f"Could not connect to S3: {e.response['Error']['Message']}",
                        [
                            "If using AWS: check your network/firewall settings",
                            "If using LocalStack/MinIO: verify --s3-endpoint URL is correct",
                        ],
                    )
            except EndpointConnectionError:
                if self.endpoint_url:
                    self._exit_with_error(
                        "S3 Connection Failed",
                        f"Could not connect to S3 endpoint: {self.endpoint_url}",
                        [
                            "If using LocalStack: docker run -d -p 4566:4566 localstack/localstack",
                            "Verify the endpoint URL is correct and the service is running",
                        ],
                    )
                else:
                    self._exit_with_error(
                        "S3 Connection Failed",
                        "Could not connect to AWS S3",
                        [
                            "Check your network connection and firewall settings",
                            "Verify AWS credentials are configured correctly",
                            "Note: --s3-endpoint is only needed for LocalStack/MinIO, not AWS",
                        ],
                    )

    async def _create_bucket(self, s3) -> None:
        try:
            create_args = {"Bucket": self.bucket}
            if self.region != "us-east-1":
                create_args["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.region
                }
            await s3.create_bucket(**create_args)
            logger.info(f"Created S3 bucket: {self.bucket}")
        except ClientError as e:
            self._exit_with_error(
                "S3 Bucket Creation Failed",
                f'Bucket "{self.bucket}" does not exist and could not be created.\n'
                f"Error: {e.response['Error']['Message']}",
                [
                    f"Create the bucket manually: aws s3 mb s3://{self.bucket}",
                    "Or grant s3:CreateBucket permission to your credentials",
                ],
            )

    def _exit_with_error(
        self, title: str, message: str, troubleshooting: list[str]
    ) -> None:
        print("=" * 50)
        print(title)
        print("=" * 50)
        print(message)
        print()
        print("Troubleshooting:")
        for item in troubleshooting:
            print(f"  - {item}")
        print()
        print("Configuration:")
        print(f"  Bucket:   {self.bucket}")
        print(f"  Region:   {self.region}")
        if self.endpoint_url:
            print(f"  Endpoint: {self.endpoint_url}")
        print("=" * 50)
        sys.exit(1)

    async def write_file(self, file: SessionFile, content: bytes) -> None:
        key = f"{self._get_session_prefix(file.session_id)}/{file.filename}"
        async with self._session.client("s3", **self._get_client_kwargs()) as s3:
            await s3.put_object(Bucket=self.bucket, Key=key, Body=content)
            logger.debug(f"Uploaded {file.filename} to s3://{self.bucket}/{key}")

    async def read_file(self, file: SessionFile) -> bytes:
        prefix_pattern = f"{self.prefix}/"
        async with self._session.client("s3", **self._get_client_kwargs()) as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=self.bucket, Prefix=prefix_pattern
            ):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(f"/{file.session_id}/{file.filename}"):
                        response = await s3.get_object(Bucket=self.bucket, Key=key)
                        async with response["Body"] as stream:
                            return await stream.read()
        raise FileNotFoundError(f"File not found: {file.session_id}/{file.filename}")

    async def file_exists(self, file: SessionFile) -> bool:
        try:
            await self.read_file(file)
            return True
        except FileNotFoundError:
            return False
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    async def _list_date_prefixes(self) -> list[str]:
        date_prefixes = set()
        async with self._session.client("s3", **self._get_client_kwargs()) as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=self.bucket, Prefix=f"{self.prefix}/", Delimiter="/"
            ):
                for prefix in page.get("CommonPrefixes", []):
                    year_prefix = prefix["Prefix"]
                    async for month_page in paginator.paginate(
                        Bucket=self.bucket, Prefix=year_prefix, Delimiter="/"
                    ):
                        for month_prefix in month_page.get("CommonPrefixes", []):
                            async for day_page in paginator.paginate(
                                Bucket=self.bucket,
                                Prefix=month_prefix["Prefix"],
                                Delimiter="/",
                            ):
                                for day_prefix in day_page.get("CommonPrefixes", []):
                                    date_prefixes.add(day_prefix["Prefix"])
        return list(date_prefixes)

    async def _list_sessions_in_prefix(self, date_prefix: str) -> list[str]:
        session_ids = []
        async with self._session.client("s3", **self._get_client_kwargs()) as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=self.bucket, Prefix=date_prefix, Delimiter="/"
            ):
                for prefix in page.get("CommonPrefixes", []):
                    parts = prefix["Prefix"].rstrip("/").split("/")
                    if parts:
                        session_ids.append(parts[-1])
        return session_ids

    async def _fetch_manifest(self, session_id: str) -> dict | None:
        try:
            file = SessionFile(session_id=session_id, filename="manifest.json")
            content = await self.read_file(file)
            return json.loads(content.decode("utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    async def _collect_manifests_from_prefix(self, date_prefix: str) -> list[dict]:
        session_ids = await self._list_sessions_in_prefix(date_prefix)
        manifests = await asyncio.gather(
            *[self._fetch_manifest(sid) for sid in session_ids]
        )
        return [m for m in manifests if m is not None]

    async def list_sessions(self, limit: int = 100) -> list[dict]:
        date_prefixes = await self._list_date_prefixes()
        date_prefixes.sort(reverse=True)

        all_sessions = []
        for date_prefix in date_prefixes:
            if len(all_sessions) >= limit:
                break
            all_sessions.extend(await self._collect_manifests_from_prefix(date_prefix))

        all_sessions.sort(key=lambda s: s.get("start_time") or 0, reverse=True)
        return all_sessions[:limit]

    async def delete_session(self, session_id: str) -> None:
        async with self._session.client("s3", **self._get_client_kwargs()) as s3:
            paginator = s3.get_paginator("list_objects_v2")
            objects_to_delete = []

            async for page in paginator.paginate(
                Bucket=self.bucket, Prefix=f"{self.prefix}/"
            ):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if f"/{session_id}/" in key:
                        objects_to_delete.append({"Key": key})

            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i : i + 1000]
                    await s3.delete_objects(
                        Bucket=self.bucket, Delete={"Objects": batch}
                    )
                logger.info(
                    f"Deleted {len(objects_to_delete)} objects for session {session_id[:8]}..."
                )

    async def upload_session(self, session_id: str, local_dir: Path) -> None:
        if not local_dir.exists():
            logger.warning(f"Local session directory not found: {local_dir}")
            return

        session_start = await self._get_session_start_time(local_dir, session_id)
        date = (
            datetime.fromtimestamp(session_start, tz=timezone.utc)
            if session_start
            else datetime.now(timezone.utc)
        )

        async with self._session.client("s3", **self._get_client_kwargs()) as s3:
            for file_path in local_dir.rglob("*"):
                if file_path.is_file():
                    relative_path = file_path.relative_to(local_dir)
                    key = (
                        f"{self._get_session_prefix(session_id, date)}/{relative_path}"
                    )

                    with open(file_path, "rb") as f:
                        content = f.read()

                    await s3.put_object(Bucket=self.bucket, Key=key, Body=content)
                    logger.debug(
                        f"Uploaded {relative_path} to s3://{self.bucket}/{key}"
                    )

        logger.info(f"Uploaded session {session_id[:8]}... to S3")

    async def _get_session_start_time(
        self, local_dir: Path, session_id: str
    ) -> Optional[float]:
        manifest_path = local_dir / "manifest.json"
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text())
                return manifest.get("start_time")
            except (json.JSONDecodeError, IOError):
                pass
        return None

    async def get_session_manifest(self, session_id: str) -> dict | None:
        return await self._fetch_manifest(session_id)

    async def download_session(self, session_id: str, local_dir: Path) -> bool:
        local_dir.mkdir(parents=True, exist_ok=True)
        downloaded = False

        async with self._session.client("s3", **self._get_client_kwargs()) as s3:
            paginator = s3.get_paginator("list_objects_v2")

            async for page in paginator.paginate(
                Bucket=self.bucket, Prefix=f"{self.prefix}/"
            ):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if f"/{session_id}/" in key:
                        filename = key.split(f"/{session_id}/")[-1]
                        local_path = local_dir / filename
                        local_path.parent.mkdir(parents=True, exist_ok=True)

                        response = await s3.get_object(Bucket=self.bucket, Key=key)
                        async with response["Body"] as stream:
                            content = await stream.read()
                            local_path.write_bytes(content)
                        downloaded = True

        return downloaded
