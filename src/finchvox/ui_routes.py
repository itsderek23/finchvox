import json
import tempfile
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

from finchvox.audio_utils import find_chunks, combine_chunks
from finchvox.conversation import Conversation
from finchvox.metrics import Metrics
from finchvox.session import Session
from finchvox.collector.config import (
    get_sessions_base_dir,
    get_session_dir,
    get_session_audio_dir,
    get_session_exceptions_dir,
    get_default_data_dir
)
from finchvox import telemetry


UI_DIR = Path(__file__).parent / "ui"
if not UI_DIR.exists():
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    UI_DIR = PROJECT_ROOT / "ui"


def _read_jsonl_file(file_path: Path) -> list[dict]:
    records = []
    with open(file_path, 'r') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def _get_trace_start_time(trace_file: Path) -> int | None:
    spans = _read_jsonl_file(trace_file)
    if not spans:
        return None
    return min(s.get("start_time_unix_nano", float("inf")) for s in spans)


def _get_combined_audio_file(
    data_dir: Path,
    session_id: str,
    background_tasks: BackgroundTasks
) -> Path:
    audio_dir = get_session_audio_dir(data_dir, session_id)
    if not audio_dir.exists():
        raise HTTPException(status_code=404, detail=f"Audio for session {session_id} not found")

    logger.info(f"Finding audio chunks for session {session_id}")
    chunks = find_chunks(get_sessions_base_dir(data_dir), session_id)

    if not chunks:
        raise HTTPException(status_code=404, detail=f"No audio chunks found for session {session_id}")

    logger.info(f"Found {len(chunks)} chunks for session {session_id}")

    with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp:
        tmp_path = Path(tmp.name)

    combine_chunks(chunks, tmp_path)
    background_tasks.add_task(tmp_path.unlink)

    return tmp_path


async def _handle_list_sessions(sessions_base_dir: Path) -> JSONResponse:
    if not sessions_base_dir.exists():
        return JSONResponse({"sessions": [], "data_dir": str(sessions_base_dir)})

    sessions = []
    for session_dir in sessions_base_dir.iterdir():
        if not session_dir.is_dir():
            continue

        session_id = session_dir.name
        trace_file = session_dir / f"trace_{session_id}.jsonl"

        if not trace_file.exists():
            continue

        try:
            session = Session(session_dir)
            sessions.append(session.to_dict())
        except Exception as e:
            print(f"Error reading session {session_dir}: {e}")
            continue

    sessions.sort(key=lambda s: s.get("start_time") or 0, reverse=True)
    return JSONResponse({"sessions": sessions, "data_dir": str(sessions_base_dir)})


def _get_session_spans(data_dir: Path, session_id: str) -> list[dict]:
    session_dir = get_session_dir(data_dir, session_id)
    trace_file = session_dir / f"trace_{session_id}.jsonl"
    if not trace_file.exists():
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
    try:
        return _read_jsonl_file(trace_file)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading trace: {str(e)}")


async def _handle_get_session_trace(data_dir: Path, session_id: str) -> JSONResponse:
    spans = _get_session_spans(data_dir, session_id)
    last_span_time = None
    for span in spans:
        if "end_time_unix_nano" in span:
            last_span_time = span["end_time_unix_nano"]
    return JSONResponse({"spans": spans, "last_span_time": last_span_time})


def _get_session_logs_raw(data_dir: Path, session_id: str) -> list[dict]:
    session_dir = get_session_dir(data_dir, session_id)
    log_file = session_dir / f"logs_{session_id}.jsonl"
    if not log_file.exists():
        return []
    logs = []
    with open(log_file, 'r') as f:
        for line in f:
            if line.strip():
                try:
                    logs.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return logs


async def _handle_get_session_raw(data_dir: Path, session_id: str) -> JSONResponse:
    spans = _get_session_spans(data_dir, session_id)
    logs = _get_session_logs_raw(data_dir, session_id)
    return JSONResponse(
        content={"Traces": spans, "Logs": logs},
        media_type="application/json",
        headers={"Content-Type": "application/json; charset=utf-8"}
    )


async def _handle_get_jsonl_records(
    file_path: Path,
    response_key: str
) -> JSONResponse:
    if not file_path.exists():
        return JSONResponse({response_key: []})

    try:
        records = _read_jsonl_file(file_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading {response_key}: {str(e)}")

    return JSONResponse({response_key: records})


async def _handle_get_session_logs(data_dir: Path, session_id: str, limit: int) -> JSONResponse:
    session_dir = get_session_dir(data_dir, session_id)
    trace_file = session_dir / f"trace_{session_id}.jsonl"

    if not trace_file.exists():
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    log_file = session_dir / f"logs_{session_id}.jsonl"
    trace_start_time = _get_trace_start_time(trace_file)

    if not log_file.exists():
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

    return JSONResponse({
        "logs": logs[:limit],
        "total_count": total_count,
        "limit": limit,
        "trace_start_time": trace_start_time
    })


async def _handle_get_session_conversation(data_dir: Path, session_id: str) -> JSONResponse:
    spans = _get_session_spans(data_dir, session_id)
    conversation = Conversation(spans)
    return JSONResponse({"messages": conversation.to_dict_list()})


async def _handle_get_session_audio(
    data_dir: Path,
    session_id: str,
    background_tasks: BackgroundTasks,
    as_download: bool = False
) -> FileResponse:
    tmp_path = _get_combined_audio_file(data_dir, session_id, background_tasks)
    disposition = "attachment" if as_download else "inline"

    return FileResponse(
        str(tmp_path),
        media_type="audio/wav",
        headers={"Content-Disposition": f"{disposition}; filename=session_{session_id}.wav"}
    )


async def _handle_get_session_audio_status(data_dir: Path, session_id: str) -> JSONResponse:
    audio_dir = get_session_audio_dir(data_dir, session_id)

    if not audio_dir.exists():
        return JSONResponse({"chunk_count": 0, "last_modified": None})

    chunks = find_chunks(get_sessions_base_dir(data_dir), session_id)

    last_modified = None
    if chunks:
        last_modified = max(Path(c).stat().st_mtime for c in chunks)

    return JSONResponse({"chunk_count": len(chunks), "last_modified": last_modified})


def register_ui_routes(app: FastAPI, data_dir: Path = None):
    if data_dir is None:
        data_dir = get_default_data_dir()

    sessions_base_dir = get_sessions_base_dir(data_dir)

    app.mount("/css", StaticFiles(directory=str(UI_DIR / "css")), name="css")
    app.mount("/js", StaticFiles(directory=str(UI_DIR / "js")), name="js")
    app.mount("/lib", StaticFiles(directory=str(UI_DIR / "lib")), name="lib")
    app.mount("/images", StaticFiles(directory=str(UI_DIR / "images")), name="images")

    @app.get("/favicon.ico")
    async def favicon():
        return FileResponse(str(UI_DIR / "images" / "favicon.ico"))

    @app.get("/")
    async def index():
        return FileResponse(str(UI_DIR / "sessions_list.html"))

    @app.get("/sessions/{session_id}")
    async def session_detail_page(session_id: str):
        telemetry.send_event("session_view")
        return FileResponse(str(UI_DIR / "session_detail.html"))

    @app.get("/api/sessions")
    async def list_sessions() -> JSONResponse:
        return await _handle_list_sessions(sessions_base_dir)

    @app.get("/api/sessions/{session_id}/trace")
    async def get_session_trace(session_id: str) -> JSONResponse:
        return await _handle_get_session_trace(data_dir, session_id)

    @app.get("/api/sessions/{session_id}/raw")
    async def get_session_raw(session_id: str) -> JSONResponse:
        return await _handle_get_session_raw(data_dir, session_id)

    @app.get("/api/sessions/{session_id}/logs")
    async def get_session_logs(session_id: str, limit: int = 1000) -> JSONResponse:
        return await _handle_get_session_logs(data_dir, session_id, limit)

    @app.get("/api/sessions/{session_id}/conversation")
    async def get_session_conversation(session_id: str) -> JSONResponse:
        return await _handle_get_session_conversation(data_dir, session_id)

    @app.get("/api/sessions/{session_id}/exceptions")
    async def get_session_exceptions(session_id: str) -> JSONResponse:
        exceptions_file = get_session_exceptions_dir(data_dir, session_id) / f"exceptions_{session_id}.jsonl"
        return await _handle_get_jsonl_records(exceptions_file, "exceptions")

    @app.get("/api/sessions/{session_id}/audio")
    async def get_session_audio(session_id: str, background_tasks: BackgroundTasks):
        return await _handle_get_session_audio(data_dir, session_id, background_tasks)

    @app.get("/api/sessions/{session_id}/audio/download")
    async def download_session_audio(session_id: str, background_tasks: BackgroundTasks):
        return await _handle_get_session_audio(data_dir, session_id, background_tasks, as_download=True)

    @app.get("/api/sessions/{session_id}/audio/status")
    async def get_session_audio_status(session_id: str) -> JSONResponse:
        return await _handle_get_session_audio_status(data_dir, session_id)

    @app.get("/api/sessions/{session_id}/metrics")
    async def get_session_metrics(session_id: str) -> JSONResponse:
        spans = _get_session_spans(data_dir, session_id)
        metrics = Metrics(spans)
        return JSONResponse(metrics.to_dict())
