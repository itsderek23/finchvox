from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass
class SessionFile:
    session_id: str
    filename: str


@runtime_checkable
class StorageBackend(Protocol):
    async def write_file(self, file: SessionFile, content: bytes) -> None: ...

    async def read_file(self, file: SessionFile) -> bytes: ...

    async def file_exists(self, file: SessionFile) -> bool: ...

    async def list_sessions(self, limit: int = 100) -> list[dict]: ...

    async def delete_session(self, session_id: str) -> None: ...

    async def upload_session(self, session_id: str, local_dir: Path) -> None: ...

    async def get_session_manifest(self, session_id: str) -> dict | None: ...
