"""
Collector routes for audio chunk uploads, logs, and exceptions.

This module provides route registration functions for the collector endpoints,
which handle data ingestion from Pipecat applications.
"""

import json
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse
from loguru import logger

from .audio_handler import AudioHandler
from .config import ALLOWED_AUDIO_FORMATS, MAX_AUDIO_FILE_SIZE


def register_collector_routes(
    app: FastAPI,
    audio_handler: AudioHandler,
    prefix: str = "/collector"
):
    """
    Register collector routes on an existing FastAPI app with URL prefix.

    Args:
        app: Existing FastAPI application to register routes on
        audio_handler: AudioHandler instance for managing audio storage
        prefix: URL prefix for all collector routes (default: "/collector")
    """

    @app.post(f"{prefix}/audio/{{trace_id}}/chunk")
    async def upload_audio_chunk(
        trace_id: str,
        audio: UploadFile = File(..., description="Audio file (WAV format)"),
        metadata: str = Form(..., description="JSON metadata string"),
    ):
        """
        Upload audio chunk for a trace.

        Args:
            trace_id: Hex string trace ID (32 chars)
            audio: Audio file (WAV format)
            metadata: JSON string with:
                - chunk_number: int
                - timestamp: ISO format string
                - sample_rate: int
                - num_channels: int
                - timing_events: list[dict] (optional)

        Returns:
            JSON response with storage path and status

        Raises:
            HTTPException: For validation errors or server errors
        """
        try:
            # Validate trace_id format
            if len(trace_id) != 32 or not all(
                c in "0123456789abcdef" for c in trace_id
            ):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid trace_id format: must be 32 hex chars, got {trace_id}",
                )

            # Parse metadata
            try:
                metadata_dict = json.loads(metadata)
            except json.JSONDecodeError as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid JSON metadata: {e}",
                )

            # Validate required metadata fields
            required_fields = ["chunk_number", "timestamp", "sample_rate", "num_channels"]
            missing = [f for f in required_fields if f not in metadata_dict]
            if missing:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Missing required metadata fields: {missing}",
                )

            # Read audio data
            audio_data = await audio.read()

            # Validate file size
            if len(audio_data) > MAX_AUDIO_FILE_SIZE:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail=f"Audio file too large: {len(audio_data)} bytes (max {MAX_AUDIO_FILE_SIZE})",
                )

            # Validate file format (basic check)
            file_ext = Path(audio.filename or "unknown.wav").suffix.lower()
            if file_ext not in ALLOWED_AUDIO_FORMATS:
                raise HTTPException(
                    status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                    detail=f"Unsupported audio format: {file_ext} (allowed: {ALLOWED_AUDIO_FORMATS})",
                )

            # Check if this is a new trace or existing trace (for logging)
            existing_chunks = audio_handler.list_chunks(trace_id)
            is_new_trace = len(existing_chunks) == 0

            if is_new_trace:
                logger.info(f"New audio trace {trace_id[:8]}... - receiving chunk #{metadata_dict['chunk_number']}")
            else:
                logger.info(f"Audio trace {trace_id[:8]}... - receiving chunk #{metadata_dict['chunk_number']} (total: {len(existing_chunks) + 1})")

            # Save audio chunk
            saved_path = await audio_handler.save_audio_chunk(
                trace_id=trace_id,
                chunk_number=metadata_dict["chunk_number"],
                audio_data=audio_data,
                metadata=metadata_dict,
            )

            if saved_path is None:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to save audio chunk",
                )

            return JSONResponse(
                status_code=status.HTTP_201_CREATED,
                content={
                    "status": "success",
                    "trace_id": trace_id,
                    "chunk_number": metadata_dict["chunk_number"],
                    "file_path": str(saved_path),
                    "size_bytes": len(audio_data),
                },
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                f"Failed to process audio upload for trace {trace_id}: {e}",
                exc_info=True,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal server error processing audio upload",
            )

    @app.get(f"{prefix}/health")
    async def health_check():
        """
        Health check endpoint for monitoring.

        Returns:
            Status information
        """
        return {"status": "healthy", "service": "finchvox-collector"}

    @app.get(f"{prefix}/audio/{{trace_id}}/chunks")
    async def list_audio_chunks(trace_id: str):
        """
        List all audio chunks for a trace.

        Args:
            trace_id: OpenTelemetry trace ID

        Returns:
            JSON with list of chunks

        Raises:
            HTTPException: If listing fails
        """
        try:
            chunks = audio_handler.list_chunks(trace_id)
            return {
                "trace_id": trace_id,
                "chunk_count": len(chunks),
                "chunks": [{"path": str(p), "name": p.name} for p in chunks],
            }
        except Exception as e:
            logger.error(f"Failed to list chunks for trace {trace_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to list audio chunks",
            )
