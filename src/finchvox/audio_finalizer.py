import shutil
import subprocess
from pathlib import Path

from loguru import logger

from finchvox.audio_utils import combine_chunks, find_chunks


def ffmpeg_available() -> bool:
    try:
        subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True,
            check=True,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


_FFMPEG_AVAILABLE: bool | None = None


def check_ffmpeg_available() -> bool:
    global _FFMPEG_AVAILABLE
    if _FFMPEG_AVAILABLE is None:
        _FFMPEG_AVAILABLE = ffmpeg_available()
        if _FFMPEG_AVAILABLE:
            logger.info("ffmpeg detected - audio compression enabled")
        else:
            logger.warning("ffmpeg not found - audio will not be compressed")
    return _FFMPEG_AVAILABLE


def compress_to_opus(input_wav: Path, output_opus: Path) -> bool:
    try:
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(input_wav),
                "-c:a",
                "libopus",
                "-b:a",
                "32k",
                "-application",
                "voip",
                str(output_opus),
            ],
            check=True,
            capture_output=True,
        )
        logger.info(f"Compressed audio to {output_opus.name}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"ffmpeg compression failed: {e.stderr.decode()}")
        return False


class AudioFinalizer:
    def __init__(self, sessions_dir: Path):
        self.sessions_dir = sessions_dir
        self._ffmpeg_checked = False

    def finalize(self, session_id: str) -> bool:
        session_dir = self.sessions_dir / session_id
        audio_dir = session_dir / "audio"

        if not audio_dir.exists():
            logger.debug(f"No audio directory for session {session_id[:8]}...")
            return True

        chunks = find_chunks(self.sessions_dir, session_id)
        if not chunks:
            logger.debug(f"No audio chunks found for session {session_id[:8]}...")
            shutil.rmtree(audio_dir, ignore_errors=True)
            return True

        logger.info(
            f"Finalizing audio for session {session_id[:8]}... ({len(chunks)} chunks)"
        )

        merged_wav = session_dir / "audio_merged.wav"
        try:
            combine_chunks(chunks, merged_wav)
        except Exception as e:
            logger.error(f"Failed to merge audio chunks: {e}")
            return False

        if check_ffmpeg_available():
            output_opus = session_dir / "audio.opus"
            if compress_to_opus(merged_wav, output_opus):
                merged_wav.unlink()
            else:
                merged_wav.rename(session_dir / "audio.wav")
                logger.warning("Compression failed, keeping uncompressed audio.wav")
        else:
            merged_wav.rename(session_dir / "audio.wav")

        shutil.rmtree(audio_dir)
        logger.info(f"Audio finalized for session {session_id[:8]}...")
        return True
