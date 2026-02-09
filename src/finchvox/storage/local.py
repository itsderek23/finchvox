import json
import shutil
from pathlib import Path

import aiofiles

from finchvox.collector.config import get_sessions_base_dir, get_session_dir
from finchvox.storage.backend import SessionFile


class LocalStorage:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.sessions_dir = get_sessions_base_dir(data_dir)

    def _get_session_dir(self, session_id: str) -> Path:
        return get_session_dir(self.data_dir, session_id)

    async def write_file(self, file: SessionFile, content: bytes) -> None:
        session_dir = self._get_session_dir(file.session_id)
        session_dir.mkdir(parents=True, exist_ok=True)
        file_path = session_dir / file.filename
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(content)

    async def read_file(self, file: SessionFile) -> bytes:
        file_path = self._get_session_dir(file.session_id) / file.filename
        async with aiofiles.open(file_path, "rb") as f:
            return await f.read()

    async def file_exists(self, file: SessionFile) -> bool:
        file_path = self._get_session_dir(file.session_id) / file.filename
        return file_path.exists()

    async def list_sessions(self, limit: int = 100) -> list[dict]:
        if not self.sessions_dir.exists():
            return []

        session_dirs = [d for d in self.sessions_dir.iterdir() if d.is_dir()]
        session_dirs.sort(key=lambda d: d.stat().st_mtime, reverse=True)
        session_dirs = session_dirs[:limit]

        sessions = []
        for session_dir in session_dirs:
            manifest = await self.get_session_manifest(session_dir.name)
            if manifest:
                sessions.append(manifest)

        sessions.sort(key=lambda s: s.get("start_time") or 0, reverse=True)
        return sessions

    async def delete_session(self, session_id: str) -> None:
        session_dir = self._get_session_dir(session_id)
        if session_dir.exists():
            shutil.rmtree(session_dir)

    async def upload_session(self, session_id: str, local_dir: Path) -> None:
        pass

    async def get_session_manifest(self, session_id: str) -> dict | None:
        session_dir = self._get_session_dir(session_id)
        manifest_path = session_dir / "manifest.json"

        if manifest_path.exists():
            async with aiofiles.open(manifest_path, "r") as f:
                content = await f.read()
                return json.loads(content)

        trace_file = session_dir / f"trace_{session_id}.jsonl"
        if not trace_file.exists():
            return None

        from finchvox.session import Session

        return Session(session_dir).to_dict()

    def get_local_session_dir(self, session_id: str) -> Path:
        return self._get_session_dir(session_id)
