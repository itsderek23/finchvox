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


UI_DIR = Path(__file__).parent / "ui"
if not UI_DIR.exists():
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    UI_DIR = PROJECT_ROOT / "ui"


def _read_jsonl_file(file_path: Path) -> list[dict]:
    """Read a JSONL file and return list of parsed JSON objects."""
    records = []
    with open(file_path, 'r') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def _get_combined_audio_file(
    data_dir: Path,
    trace_id: str,
    background_tasks: BackgroundTasks
) -> Path:
    """
    Combine audio chunks for a trace into a temporary WAV file.

    Schedules cleanup of the temp file after response is sent.
    Raises HTTPException if audio not found.
    """
    audio_dir = get_trace_audio_dir(data_dir, trace_id)
    if not audio_dir.exists():
        raise HTTPException(status_code=404, detail=f"Audio for trace {trace_id} not found")

    logger.info(f"Finding audio chunks for trace {trace_id}")
    chunks = find_chunks(get_traces_base_dir(data_dir), trace_id)

    if not chunks:
        raise HTTPException(status_code=404, detail=f"No audio chunks found for trace {trace_id}")

    logger.info(f"Found {len(chunks)} chunks for trace {trace_id}")

    with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp:
        tmp_path = Path(tmp.name)

    combine_chunks(chunks, tmp_path)
    background_tasks.add_task(tmp_path.unlink)

    return tmp_path


async def _handle_list_traces(traces_base_dir: Path) -> JSONResponse:
    """List all available traces."""
    if not traces_base_dir.exists():
        return JSONResponse({"traces": [], "data_dir": str(traces_base_dir)})

    traces = []
    for trace_dir in traces_base_dir.iterdir():
        if not trace_dir.is_dir():
            continue

        trace_id = trace_dir.name
        trace_file = trace_dir / f"trace_{trace_id}.jsonl"

        if not trace_file.exists():
            continue

        try:
            trace = Trace(trace_file)
            traces.append(trace.to_dict())
        except Exception as e:
            print(f"Error reading trace file {trace_file}: {e}")
            continue

    traces.sort(key=lambda t: t.get("start_time") or 0, reverse=True)
    return JSONResponse({"traces": traces, "data_dir": str(traces_base_dir)})


async def _handle_get_trace(data_dir: Path, trace_id: str) -> JSONResponse:
    """Get all spans for a specific trace."""
    trace_dir = get_trace_dir(data_dir, trace_id)
    trace_file = trace_dir / f"trace_{trace_id}.jsonl"

    if not trace_file.exists():
        raise HTTPException(status_code=404, detail=f"Trace {trace_id} not found")

    try:
        spans = _read_jsonl_file(trace_file)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading trace: {str(e)}")

    last_span_time = None
    for span in spans:
        if "end_time_unix_nano" in span:
            last_span_time = span["end_time_unix_nano"]

    return JSONResponse({"spans": spans, "last_span_time": last_span_time})


async def _handle_get_trace_raw(data_dir: Path, trace_id: str) -> JSONResponse:
    """Get raw trace data as a formatted JSON array."""
    trace_dir = get_trace_dir(data_dir, trace_id)
    trace_file = trace_dir / f"trace_{trace_id}.jsonl"

    if not trace_file.exists():
        raise HTTPException(status_code=404, detail=f"Trace {trace_id} not found")

    try:
        spans = _read_jsonl_file(trace_file)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading trace: {str(e)}")

    return JSONResponse(
        content=spans,
        media_type="application/json",
        headers={"Content-Type": "application/json; charset=utf-8"}
    )


async def _handle_get_jsonl_records(
    file_path: Path,
    response_key: str
) -> JSONResponse:
    """Get records from a JSONL file for a specific trace."""
    if not file_path.exists():
        return JSONResponse({response_key: []})

    try:
        records = _read_jsonl_file(file_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading {response_key}: {str(e)}")

    return JSONResponse({response_key: records})


async def _handle_get_logs(data_dir: Path, trace_id: str, limit: int) -> JSONResponse:
    """Get logs for a specific trace with sorting and limiting."""
    trace_dir = get_trace_dir(data_dir, trace_id)
    trace_file = trace_dir / f"trace_{trace_id}.jsonl"

    if not trace_file.exists():
        raise HTTPException(status_code=404, detail=f"Trace {trace_id} not found")

    log_file = trace_dir / f"logs_{trace_id}.jsonl"

    if not log_file.exists():
        spans = _read_jsonl_file(trace_file)
        trace_start_time = None
        if spans:
            trace_start_time = min(s.get("start_time_unix_nano", float("inf")) for s in spans)
        return JSONResponse({
            "logs": [],
            "total_count": 0,
            "limit": limit,
            "trace_start_time": trace_start_time
        })

    try:
        logs = _read_jsonl_file(log_file)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading logs: {str(e)}")

    logs.sort(key=lambda l: int(l.get("time_unix_nano", 0)))

    total_count = len(logs)
    logs = logs[:limit]

    spans = _read_jsonl_file(trace_file)
    trace_start_time = None
    if spans:
        trace_start_time = min(s.get("start_time_unix_nano", float("inf")) for s in spans)

    return JSONResponse({
        "logs": logs,
        "total_count": total_count,
        "limit": limit,
        "trace_start_time": trace_start_time
    })


async def _handle_get_audio(
    data_dir: Path,
    trace_id: str,
    background_tasks: BackgroundTasks,
    as_download: bool = False
) -> FileResponse:
    """Get or download combined audio for a specific trace."""
    tmp_path = _get_combined_audio_file(data_dir, trace_id, background_tasks)
    disposition = "attachment" if as_download else "inline"

    return FileResponse(
        str(tmp_path),
        media_type="audio/wav",
        headers={"Content-Disposition": f"{disposition}; filename=trace_{trace_id}.wav"}
    )


async def _handle_get_audio_status(data_dir: Path, trace_id: str) -> JSONResponse:
    """Get audio metadata without combining chunks."""
    audio_dir = get_trace_audio_dir(data_dir, trace_id)

    if not audio_dir.exists():
        return JSONResponse({"chunk_count": 0, "last_modified": None})

    chunks = find_chunks(get_traces_base_dir(data_dir), trace_id)

    last_modified = None
    if chunks:
        last_modified = max(Path(c).stat().st_mtime for c in chunks)

    return JSONResponse({"chunk_count": len(chunks), "last_modified": last_modified})


def register_ui_routes(app: FastAPI, data_dir: Path = None):
    """Register UI routes and static file serving on an existing FastAPI app."""
    if data_dir is None:
        data_dir = get_default_data_dir()

    traces_base_dir = get_traces_base_dir(data_dir)

    app.mount("/css", StaticFiles(directory=str(UI_DIR / "css")), name="css")
    app.mount("/js", StaticFiles(directory=str(UI_DIR / "js")), name="js")
    app.mount("/lib", StaticFiles(directory=str(UI_DIR / "lib")), name="lib")
    app.mount("/images", StaticFiles(directory=str(UI_DIR / "images")), name="images")

    @app.get("/favicon.ico")
    async def favicon():
        return FileResponse(str(UI_DIR / "images" / "favicon.ico"))

    @app.get("/")
    async def index():
        return FileResponse(str(UI_DIR / "traces_list.html"))

    @app.get("/traces/{trace_id}")
    async def trace_detail_page(trace_id: str):
        return FileResponse(str(UI_DIR / "trace_detail.html"))

    @app.get("/api/traces")
    async def list_traces() -> JSONResponse:
        return await _handle_list_traces(traces_base_dir)

    @app.get("/api/trace/{trace_id}")
    async def get_trace(trace_id: str) -> JSONResponse:
        return await _handle_get_trace(data_dir, trace_id)

    @app.get("/api/trace/{trace_id}/raw")
    async def get_trace_raw(trace_id: str) -> JSONResponse:
        return await _handle_get_trace_raw(data_dir, trace_id)

    @app.get("/api/logs/{trace_id}")
    async def get_logs(trace_id: str, limit: int = 1000) -> JSONResponse:
        return await _handle_get_logs(data_dir, trace_id, limit)

    @app.get("/api/exceptions/{trace_id}")
    async def get_exceptions(trace_id: str) -> JSONResponse:
        exceptions_file = get_trace_exceptions_dir(data_dir, trace_id) / f"exceptions_{trace_id}.jsonl"
        return await _handle_get_jsonl_records(exceptions_file, "exceptions")

    @app.get("/api/audio/{trace_id}")
    async def get_audio(trace_id: str, background_tasks: BackgroundTasks):
        return await _handle_get_audio(data_dir, trace_id, background_tasks)

    @app.get("/api/audio/{trace_id}/download")
    async def download_audio(trace_id: str, background_tasks: BackgroundTasks):
        return await _handle_get_audio(data_dir, trace_id, background_tasks, as_download=True)

    @app.get("/api/audio/{trace_id}/status")
    async def get_audio_status(trace_id: str) -> JSONResponse:
        return await _handle_get_audio_status(data_dir, trace_id)
