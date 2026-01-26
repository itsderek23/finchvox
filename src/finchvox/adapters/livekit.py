import json
from typing import Optional

from finchvox.adapters.base import BaseTraceAdapter, SpanTypeConfig


SPAN_CONFIGS = {
    "agent_session": SpanTypeConfig(
        display_name="AGENT_SESSION",
        category="root",
        css_class="bar-conversation",
    ),
    "user_turn": SpanTypeConfig(
        display_name="USER_TURN",
        category="stt",
        css_class="bar-stt",
    ),
    "agent_turn": SpanTypeConfig(
        display_name="AGENT_TURN",
        category="turn",
        css_class="bar-turn",
    ),
    "user_speaking": SpanTypeConfig(
        display_name="USER_SPEAKING",
        category="stt",
        css_class="bar-stt",
    ),
    "llm_node": SpanTypeConfig(
        display_name="LLM_NODE",
        category="llm",
        css_class="bar-llm",
    ),
    "llm_request": SpanTypeConfig(
        display_name="LLM_REQUEST",
        category="llm",
        css_class="bar-llm",
    ),
    "llm_request_run": SpanTypeConfig(
        display_name="LLM_REQUEST_RUN",
        category="llm",
        css_class="bar-llm",
    ),
    "tts_node": SpanTypeConfig(
        display_name="TTS_NODE",
        category="tts",
        css_class="bar-tts",
    ),
    "tts_request": SpanTypeConfig(
        display_name="TTS_REQUEST",
        category="tts",
        css_class="bar-tts",
    ),
    "tts_request_run": SpanTypeConfig(
        display_name="TTS_REQUEST_RUN",
        category="tts",
        css_class="bar-tts",
    ),
    "agent_speaking": SpanTypeConfig(
        display_name="AGENT_SPEAKING",
        category="tts",
        css_class="bar-tts",
    ),
}

DEFAULT_CONFIG = SpanTypeConfig(
    display_name="OTHER",
    category="other",
    css_class="bar-default",
)


class LiveKitAdapter(BaseTraceAdapter):
    def get_platform(self) -> str:
        return "livekit"

    def get_span_config(self, span_name: str) -> SpanTypeConfig:
        config = SPAN_CONFIGS.get(span_name)
        if config:
            return config
        return SpanTypeConfig(
            display_name=span_name.upper().replace("-", "_"),
            category="other",
            css_class="bar-default",
        )

    def get_transcript(self, span: dict) -> Optional[str]:
        transcript = self._get_attribute(span, "lk.transcript")
        if transcript:
            return transcript
        user_text = self._get_attribute(span, "lk.user_text")
        if user_text:
            return user_text
        return self._get_attribute(span, "transcript")

    def get_output_text(self, span: dict) -> Optional[str]:
        response_text = self._get_attribute(span, "lk.response.text")
        if response_text:
            return response_text
        chat_ctx = self._get_attribute(span, "lk.chat_ctx")
        if chat_ctx:
            return chat_ctx
        return self._get_attribute(span, "output")

    def get_ttfb(self, span: dict) -> Optional[float]:
        span_name = span.get("name", "")

        if span_name == "llm_request":
            metrics_json = self._get_attribute(span, "lk.llm_metrics")
            if metrics_json:
                try:
                    metrics = json.loads(metrics_json)
                    ttft = metrics.get("ttft")
                    if ttft is not None:
                        return ttft
                except (json.JSONDecodeError, TypeError):
                    pass

        if span_name == "tts_request":
            metrics_json = self._get_attribute(span, "lk.tts_metrics")
            if metrics_json:
                try:
                    metrics = json.loads(metrics_json)
                    ttfb = metrics.get("ttfb")
                    if ttfb is not None:
                        return ttfb
                except (json.JSONDecodeError, TypeError):
                    pass

        if span_name in ("user_turn", "user_speaking"):
            delay = self._get_attribute(span, "lk.transcription_delay")
            if delay is not None:
                return delay

        ttfb = self._get_attribute(span, "lk.ttfb")
        if ttfb is not None:
            return ttfb
        return self._get_attribute(span, "metrics.ttfb")

    def get_turn_span_names(self) -> list[str]:
        return ["user_turn", "agent_turn"]
