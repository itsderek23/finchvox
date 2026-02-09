import json
from pathlib import Path

from loguru import logger

from finchvox.audio_compressor import AudioCompressor
from finchvox.session import Session


class SessionFinalizer:
    def __init__(self, sessions_dir: Path):
        self.sessions_dir = sessions_dir
        self.audio_compressor = AudioCompressor(sessions_dir.parent)

    def finalize(self, session_id: str) -> bool:
        session_dir = self.sessions_dir / session_id

        if not session_dir.exists():
            logger.warning(f"Session directory not found: {session_id[:8]}...")
            return False

        self.audio_compressor.compress(session_id)

        self._generate_manifest(session_dir)
        return True

    def _generate_manifest(self, session_dir: Path) -> None:
        session = Session(session_dir)
        manifest = session.to_dict()
        manifest_path = session_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))
        logger.info(f"Generated manifest for session {session.session_id[:8]}...")
