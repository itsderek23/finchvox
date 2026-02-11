import asyncio
import json
import shutil
from pathlib import Path
from typing import Optional

from loguru import logger

from finchvox.audio_compressor import AudioCompressor
from finchvox.session import Session
from finchvox.storage.backend import StorageBackend


class SessionFinalizer:
    def __init__(
        self,
        sessions_dir: Path,
        storage_backend: Optional[StorageBackend] = None,
        delete_local_after_upload: bool = True,
    ):
        self.sessions_dir = sessions_dir
        self.audio_compressor = AudioCompressor(sessions_dir.parent)
        self.storage_backend = storage_backend
        self.delete_local_after_upload = delete_local_after_upload

    def finalize(self, session_id: str) -> bool:
        session_dir = self.sessions_dir / session_id

        if not session_dir.exists():
            logger.warning(f"Session directory not found: {session_id[:8]}...")
            return False

        self.audio_compressor.compress(session_id)

        self._generate_manifest(session_dir)

        if self.storage_backend is not None:
            self._upload_to_storage(session_id, session_dir)

        return True

    def _generate_manifest(self, session_dir: Path) -> None:
        session = Session(session_dir)
        manifest = session.to_dict()
        manifest_path = session_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))
        logger.info(f"Generated manifest for session {session.session_id[:8]}...")

    def _upload_to_storage(self, session_id: str, session_dir: Path) -> None:
        try:
            asyncio.run(self.storage_backend.upload_session(session_id, session_dir))
            logger.info(f"Uploaded session {session_id[:8]}... to remote storage")

            if self.delete_local_after_upload:
                shutil.rmtree(session_dir)
                logger.info(f"Deleted local session directory: {session_id[:8]}...")
        except Exception as e:
            logger.error(
                f"Failed to upload session {session_id[:8]}... to storage: {e}"
            )
