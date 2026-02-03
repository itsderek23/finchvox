import json
import tempfile
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
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
    get_default_data_dir,
)
from finchvox import telemetry


UI_DIR = Path(__file__).parent / "ui"
if not UI_DIR.exists():
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    UI_DIR = PROJECT_ROOT / "ui"


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


def _get_session(data_dir: Path, session_id: str) -> Session:
    session_dir = get_session_dir(data_dir, session_id)
    if not session_dir.exists():
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
    trace_file = session_dir / f"trace_{session_id}.jsonl"
    if not trace_file.exists():
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
    return Session(session_dir)


def _get_session_spans(data_dir: Path, session_id: str) -> list[dict]:
    session = _get_session(data_dir, session_id)
    try:
        return session.get_spans()
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
    session = _get_session(data_dir, session_id)
    return session.get_logs()


async def _handle_get_session_raw(data_dir: Path, session_id: str) -> JSONResponse:
    spans = _get_session_spans(data_dir, session_id)
    logs = _get_session_logs_raw(data_dir, session_id)
    return JSONResponse(
        content={"Traces": spans, "Logs": logs},
        media_type="application/json",
        headers={"Content-Type": "application/json; charset=utf-8"}
    )


async def _handle_get_session_logs(data_dir: Path, session_id: str, limit: int) -> JSONResponse:
    session = _get_session(data_dir, session_id)

    try:
        logs = session.get_logs()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading logs: {str(e)}")

    logs.sort(key=lambda l: int(l.get("time_unix_nano", 0)))
    total_count = len(logs)

    return JSONResponse({
        "logs": logs[:limit],
        "total_count": total_count,
        "limit": limit,
        "trace_start_time": session.start_time_nano
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




async def _handle_upload_session(
    sessions_base_dir: Path,
    file: UploadFile
) -> JSONResponse:
    zip_bytes = await file.read()

    session, error_msg = Session.from_zip(zip_bytes, sessions_base_dir)
    if error_msg:
        raise HTTPException(status_code=400, detail=error_msg)

    return JSONResponse({"success": True, "session_id": session.session_id})


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
        session = _get_session(data_dir, session_id)
        return JSONResponse({"exceptions": session.get_exceptions()})

    @app.get("/api/sessions/{session_id}/audio")
    async def get_session_audio(session_id: str, background_tasks: BackgroundTasks):
        return await _handle_get_session_audio(data_dir, session_id, background_tasks)

    @app.get("/api/sessions/{session_id}/audio/status")
    async def get_session_audio_status(session_id: str) -> JSONResponse:
        return await _handle_get_session_audio_status(data_dir, session_id)

    @app.get("/api/sessions/{session_id}/metrics")
    async def get_session_metrics(session_id: str) -> JSONResponse:
        spans = _get_session_spans(data_dir, session_id)
        metrics = Metrics(spans)
        return JSONResponse(metrics.to_dict())

    @app.get("/api/sessions/{session_id}/download")
    async def download_session(session_id: str):
        session = _get_session(data_dir, session_id)
        zip_buffer = session.to_zip()
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={
                "Content-Disposition": f"attachment; filename=finchvox_session_{session_id}.zip"
            }
        )

    @app.post("/api/sessions/upload")
    async def upload_session(file: UploadFile = File(...)):
        return await _handle_upload_session(sessions_base_dir, file)

    @app.get("/api/sessions/{session_id}/environment")
    async def get_session_environment(session_id: str):
        session_dir = get_session_dir(data_dir, session_id)
        env_file = session_dir / f"environment_{session_id}.json"

        if not env_file.exists():
            raise HTTPException(
                status_code=404, detail="Environment data not found"
            )

        return json.loads(env_file.read_text())
