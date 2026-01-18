"""
Audio utilities for FinchVox trace viewer.

This module provides functions for finding and combining audio chunks
from voice conversation traces.
"""

import wave
from pathlib import Path
from typing import List, Tuple

from loguru import logger


def find_chunks(sessions_dir: Path, session_id: str) -> List[Tuple[int, Path]]:
    """
    Find all audio chunks for a given session_id.

    Args:
        sessions_dir: Base sessions directory (e.g., ~/.finchvox/sessions)
        session_id: Session ID to search for

    Returns:
        List of (chunk_number, chunk_path) tuples, sorted by chunk number
    """
    chunks = []

    audio_dir = sessions_dir / session_id / "audio"
    if audio_dir.exists():
        for chunk_file in audio_dir.glob("chunk_*.wav"):
            try:
                chunk_num = int(chunk_file.stem.split("_")[1])
                chunks.append((chunk_num, chunk_file))
            except (IndexError, ValueError) as e:
                logger.warning(f"Could not parse chunk number from {chunk_file}: {e}")

    chunks = list(set(chunks))
    chunks.sort(key=lambda x: x[0])
    return chunks


def combine_chunks(chunks: List[Tuple[int, Path]], output_file: Path) -> None:
    """
    Combine audio chunks into a single WAV file.

    Args:
        chunks: List of (chunk_number, chunk_path) tuples
        output_file: Path to write combined WAV file
    """
    if not chunks:
        logger.error("No chunks to combine")
        return

    # Get audio parameters from first chunk
    first_chunk = chunks[0][1]
    with wave.open(str(first_chunk), "rb") as wf:
        sample_rate = wf.getframerate()
        num_channels = wf.getnchannels()
        sample_width = wf.getsampwidth()

    logger.info(
        f"Combining {len(chunks)} chunks: "
        f"{sample_rate}Hz, {num_channels} channels, {sample_width*8}-bit"
    )

    # Open output file
    with wave.open(str(output_file), "wb") as out_wf:
        out_wf.setnchannels(num_channels)
        out_wf.setsampwidth(sample_width)
        out_wf.setframerate(sample_rate)

        # Append each chunk
        total_frames = 0
        for chunk_num, chunk_path in chunks:
            logger.debug(f"Adding chunk {chunk_num}: {chunk_path.name}")

            with wave.open(str(chunk_path), "rb") as in_wf:
                # Verify parameters match
                if (
                    in_wf.getframerate() != sample_rate
                    or in_wf.getnchannels() != num_channels
                    or in_wf.getsampwidth() != sample_width
                ):
                    logger.warning(
                        f"Chunk {chunk_num} has different audio parameters, skipping"
                    )
                    continue

                # Read and write all frames
                frames = in_wf.readframes(in_wf.getnframes())
                out_wf.writeframes(frames)
                total_frames += in_wf.getnframes()

        duration_seconds = total_frames / sample_rate
        logger.info(
            f"Combined {len(chunks)} chunks into {output_file.name} "
            f"({duration_seconds:.1f} seconds)"
        )
