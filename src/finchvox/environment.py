import asyncio
import platform
import sys
import threading
from typing import Optional

import aiohttp
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor

_captured_environment: Optional[dict] = None
_environment_sent_traces: set[str] = set()
_lock = threading.Lock()


def capture_environment() -> dict:
    global _captured_environment

    env = {
        "os": {"system": "unknown", "release": "unknown", "machine": "unknown"},
        "python": {"version": "unknown", "implementation": "unknown"},
        "packages": {},
        "captured_at": None,
    }

    try:
        env["os"] = {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        }
    except Exception:
        pass

    try:
        env["python"] = {
            "version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "implementation": platform.python_implementation(),
        }
    except Exception:
        pass

    try:
        from importlib.metadata import distributions

        env["packages"] = {
            d.metadata["Name"]: d.version
            for d in distributions()
            if d.metadata["Name"]
        }
    except Exception:
        pass

    try:
        from datetime import datetime, timezone

        env["captured_at"] = datetime.now(timezone.utc).isoformat()
    except Exception:
        pass

    _captured_environment = env
    return env


def get_captured_environment() -> Optional[dict]:
    return _captured_environment


class EnvironmentSpanProcessor(SpanProcessor):
    def __init__(self, endpoint: str):
        self._endpoint = endpoint

    def on_start(self, span, parent_context):
        pass

    def on_end(self, span: ReadableSpan):
        trace_id = format(span.get_span_context().trace_id, "032x")
        if trace_id == "0" * 32:
            return

        with _lock:
            if trace_id in _environment_sent_traces:
                return
            _environment_sent_traces.add(trace_id)

        env = get_captured_environment()
        if not env:
            return

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self._send_environment(trace_id, env))
            else:
                asyncio.run(self._send_environment(trace_id, env))
        except Exception:
            pass

    async def _send_environment(self, trace_id: str, env: dict):
        try:
            url = f"{self._endpoint}/collector/environment/{trace_id}"
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=env, timeout=aiohttp.ClientTimeout(total=5)
                ):
                    pass
        except Exception:
            pass

    def shutdown(self):
        pass

    def force_flush(self, timeout_millis=30000):
        return True
