"""
UI routes for FinchVox Trace Viewer.

Serves the web UI and provides REST APIs for trace data.
"""

import json
import tempfile
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

from finchvox.audio_utils import find_chunks, combine_chunks
from finchvox.trace import Trace
from finchvox.collector.config import (
    get_traces_base_dir,
    get_trace_dir,
    get_trace_logs_dir,
    get_trace_audio_dir,
    get_trace_exceptions_dir,
    get_default_data_dir
)


# UI directory - check package location first (when installed via pip/uv),
# then fall back to development location
UI_DIR = Path(__file__).parent / "ui"
if not UI_DIR.exists():
    # Fall back to development location (ui/ at project root)
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    UI_DIR = PROJECT_ROOT / "ui"


def register_ui_routes(app: FastAPI, data_dir: Path = None):
    """
    Register UI routes and static file serving on an existing FastAPI app.

    Args:
        app: Existing FastAPI application to register routes on
        data_dir: Base data directory (default: ~/.finchvox)
    """
    if data_dir is None:
        data_dir = get_default_data_dir()

    traces_base_dir = get_traces_base_dir(data_dir)
    # Mount static files FIRST (must be before route handlers)
    app.mount("/css", StaticFiles(directory=str(UI_DIR / "css")), name="css")
    app.mount("/js", StaticFiles(directory=str(UI_DIR / "js")), name="js")
    app.mount("/lib", StaticFiles(directory=str(UI_DIR / "lib")), name="lib")
    app.mount("/images", StaticFiles(directory=str(UI_DIR / "images")), name="images")

    @app.get("/favicon.ico")
    async def favicon():
        """Serve the favicon."""
        return FileResponse(str(UI_DIR / "images" / "favicon.ico"))

    @app.get("/")
    async def index():
        """Serve the traces list page."""
        return FileResponse(str(UI_DIR / "traces_list.html"))

    @app.get("/traces/{trace_id}")
    async def trace_detail_page(trace_id: str):
        """Serve the trace detail page."""
        return FileResponse(str(UI_DIR / "trace_detail.html"))

    @app.get("/api/traces")
    async def list_traces() -> JSONResponse:
        """
        List all available traces.

        Returns:
            List of trace metadata including trace_id, service_name, span_count,
            start_time, end_time, and duration_ms
        """
        traces = []

        if not traces_base_dir.exists():
            return JSONResponse({"traces": [], "data_dir": str(traces_base_dir)})

        # Scan trace directories (each directory is a trace)
        for trace_dir in traces_base_dir.iterdir():
            if not trace_dir.is_dir():
                continue

            trace_id = trace_dir.name
            trace_file = trace_dir / f"trace_{trace_id}.jsonl"

            if not trace_file.exists():
                continue

            try:
                # Use Trace class to load metadata
                trace = Trace(trace_file)
                traces.append(trace.to_dict())
            except Exception as e:
                print(f"Error reading trace file {trace_file}: {e}")
                continue

        # Sort by start_time descending (most recent first)
        traces.sort(key=lambda t: t.get("start_time") or 0, reverse=True)

        return JSONResponse({"traces": traces, "data_dir": str(traces_base_dir)})

    @app.get("/api/trace/{trace_id}")
    async def get_trace(trace_id: str) -> JSONResponse:
        """
        Get all spans for a specific trace.

        Args:
            trace_id: The trace ID

        Returns:
            JSON with spans array
        """
        trace_dir = get_trace_dir(data_dir, trace_id)
        trace_file = trace_dir / f"trace_{trace_id}.jsonl"

        if not trace_file.exists():
            raise HTTPException(status_code=404, detail=f"Trace {trace_id} not found")

        spans = []
        last_span_time = None
        try:
            with open(trace_file, 'r') as f:
                for line in f:
                    if line.strip():
                        span = json.loads(line)
                        spans.append(span)
                        # Track the last span's end time for abandonment detection
                        if "end_time_unix_nano" in span:
                            last_span_time = span["end_time_unix_nano"]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error reading trace: {str(e)}")

        return JSONResponse({
            "spans": spans,
            "last_span_time": last_span_time
        })

    @app.get("/api/trace/{trace_id}/raw")
    async def get_trace_raw(trace_id: str) -> JSONResponse:
        """
        Get raw trace data as a formatted JSON array.

        Reads the JSONL file and returns all spans as a single JSON array
        with indentation for easy reading in browser.

        Args:
            trace_id: The trace ID

        Returns:
            JSON array of all spans with formatting
        """
        trace_dir = get_trace_dir(data_dir, trace_id)
        trace_file = trace_dir / f"trace_{trace_id}.jsonl"

        if not trace_file.exists():
            raise HTTPException(status_code=404, detail=f"Trace {trace_id} not found")

        spans = []
        try:
            with open(trace_file, 'r') as f:
                for line in f:
                    if line.strip():
                        span = json.loads(line)
                        spans.append(span)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error reading trace: {str(e)}")

        # Return as formatted JSON with indentation
        return JSONResponse(
            content=spans,
            media_type="application/json",
            headers={
                "Content-Type": "application/json; charset=utf-8"
            }
        )

    @app.get("/api/logs/{trace_id}")
    async def get_logs(trace_id: str) -> JSONResponse:
        """
        Get all logs for a specific trace.

        Args:
            trace_id: The trace ID

        Returns:
            JSON with logs array
        """
        logs_dir = get_trace_logs_dir(data_dir, trace_id)
        log_file = logs_dir / f"log_{trace_id}.jsonl"

        if not log_file.exists():
            return JSONResponse({"logs": []})

        logs = []
        try:
            with open(log_file, 'r') as f:
                for line in f:
                    if line.strip():
                        log = json.loads(line)
                        logs.append(log)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error reading logs: {str(e)}")

        return JSONResponse({"logs": logs})

    @app.get("/api/exceptions/{trace_id}")
    async def get_exceptions(trace_id: str) -> JSONResponse:
        """
        Get all exceptions for a specific trace.

        Args:
            trace_id: The trace ID

        Returns:
            JSON with exceptions array
        """
        exceptions_dir = get_trace_exceptions_dir(data_dir, trace_id)
        exceptions_file = exceptions_dir / f"exceptions_{trace_id}.jsonl"

        if not exceptions_file.exists():
            return JSONResponse({"exceptions": []})

        exceptions = []
        try:
            with open(exceptions_file, 'r') as f:
                for line in f:
                    if line.strip():
                        exception = json.loads(line)
                        exceptions.append(exception)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error reading exceptions: {str(e)}")

        return JSONResponse({"exceptions": exceptions})

    @app.get("/api/audio/{trace_id}")
    async def get_audio(trace_id: str, background_tasks: BackgroundTasks):
        """
        Get combined audio for a specific trace.

        Combines all audio chunks into a single WAV file on-demand.

        Args:
            trace_id: The trace ID
            background_tasks: FastAPI background tasks for cleanup

        Returns:
            Combined WAV file with all audio chunks
        """
        audio_dir = get_trace_audio_dir(data_dir, trace_id)

        if not audio_dir.exists():
            raise HTTPException(status_code=404, detail=f"Audio for trace {trace_id} not found")

        # Find all chunks for this trace
        logger.info(f"Finding audio chunks for trace {trace_id}")
        # Pass the parent of audio_dir (trace dir) since find_chunks expects base and appends trace_id
        chunks = find_chunks(get_traces_base_dir(data_dir), trace_id)

        if not chunks:
            raise HTTPException(status_code=404, detail=f"No audio chunks found for trace {trace_id}")

        logger.info(f"Found {len(chunks)} chunks for trace {trace_id}")

        # Generate combined WAV in temp file
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp:
            tmp_path = Path(tmp.name)

        combine_chunks(chunks, tmp_path)

        # Schedule cleanup after response sent
        background_tasks.add_task(tmp_path.unlink)

        return FileResponse(
            str(tmp_path),
            media_type="audio/wav",
            headers={
                "Content-Disposition": f"inline; filename=trace_{trace_id}.wav"
            }
        )

    @app.get("/api/audio/{trace_id}/download")
    async def download_audio(trace_id: str, background_tasks: BackgroundTasks):
        """
        Download combined audio for a specific trace.

        Same as get_audio but with Content-Disposition: attachment to trigger download.

        Args:
            trace_id: The trace ID
            background_tasks: FastAPI background tasks for cleanup

        Returns:
            Combined WAV file with all audio chunks (as download)
        """
        audio_dir = get_trace_audio_dir(data_dir, trace_id)

        if not audio_dir.exists():
            raise HTTPException(status_code=404, detail=f"Audio for trace {trace_id} not found")

        # Find all chunks for this trace
        logger.info(f"Finding audio chunks for trace {trace_id}")
        chunks = find_chunks(get_traces_base_dir(data_dir), trace_id)

        if not chunks:
            raise HTTPException(status_code=404, detail=f"No audio chunks found for trace {trace_id}")

        logger.info(f"Found {len(chunks)} chunks for trace {trace_id}")

        # Generate combined WAV in temp file
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp:
            tmp_path = Path(tmp.name)

        combine_chunks(chunks, tmp_path)

        # Schedule cleanup after response sent
        background_tasks.add_task(tmp_path.unlink)

        return FileResponse(
            str(tmp_path),
            media_type="audio/wav",
            headers={
                "Content-Disposition": f"attachment; filename=trace_{trace_id}.wav"
            }
        )

    @app.get("/api/audio/{trace_id}/status")
    async def get_audio_status(trace_id: str) -> JSONResponse:
        """
        Get audio metadata without combining chunks.

        Returns chunk count and last modification time for detecting
        when new audio has been added to a trace.

        Args:
            trace_id: The trace ID

        Returns:
            JSON with chunk_count and last_modified timestamp
        """
        audio_dir = get_trace_audio_dir(data_dir, trace_id)

        if not audio_dir.exists():
            return JSONResponse({"chunk_count": 0, "last_modified": None})

        # Find all chunks for this trace
        chunks = find_chunks(get_traces_base_dir(data_dir), trace_id)

        last_modified = None
        if chunks:
            # Get most recent modification time
            last_modified = max(Path(c).stat().st_mtime for c in chunks)

        return JSONResponse({
            "chunk_count": len(chunks),
            "last_modified": last_modified
        })
