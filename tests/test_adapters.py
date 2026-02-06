import pytest

from finchvox.adapters import (
    detect_platform,
    get_adapter,
    PipecatAdapter,
    LiveKitAdapter,
)


class TestDetectPlatform:
    def test_detects_pipecat_by_default(self):
        spans = [{"name": "conversation", "attributes": []}]
        assert detect_platform(spans) == "pipecat"

    def test_detects_livekit_by_scope_name(self):
        spans = [
            {
                "name": "agent_session",
                "instrumentation_scope": {"name": "livekit-agents"},
            }
        ]
        assert detect_platform(spans) == "livekit"

    def test_detects_livekit_by_attribute_prefix(self):
        spans = [
            {
                "name": "some_span",
                "attributes": [
                    {"key": "lk.transcript", "value": {"string_value": "hello"}}
                ],
            }
        ]
        assert detect_platform(spans) == "livekit"

    def test_returns_pipecat_for_empty_spans(self):
        assert detect_platform([]) == "pipecat"


class TestGetAdapter:
    def test_returns_pipecat_adapter_for_pipecat_spans(self):
        spans = [{"name": "conversation"}]
        adapter = get_adapter(spans)
        assert isinstance(adapter, PipecatAdapter)

    def test_returns_livekit_adapter_for_livekit_spans(self):
        spans = [
            {
                "name": "agent_session",
                "instrumentation_scope": {"name": "livekit-agents"},
            }
        ]
        adapter = get_adapter(spans)
        assert isinstance(adapter, LiveKitAdapter)


class TestPipecatAdapter:
    @pytest.fixture
    def adapter(self):
        return PipecatAdapter()

    def test_get_platform(self, adapter):
        assert adapter.get_platform() == "pipecat"

    def test_get_span_config_conversation(self, adapter):
        config = adapter.get_span_config("conversation")
        assert config.display_name == "CONVERSATION"
        assert config.category == "root"
        assert config.css_class == "bar-conversation"

    def test_get_span_config_turn(self, adapter):
        config = adapter.get_span_config("turn")
        assert config.display_name == "TURN"
        assert config.category == "turn"
        assert config.css_class == "bar-turn"

    def test_get_span_config_stt(self, adapter):
        config = adapter.get_span_config("stt")
        assert config.category == "stt"

    def test_get_span_config_llm(self, adapter):
        config = adapter.get_span_config("llm")
        assert config.category == "llm"

    def test_get_span_config_tts(self, adapter):
        config = adapter.get_span_config("tts")
        assert config.category == "tts"

    def test_get_span_config_unknown(self, adapter):
        config = adapter.get_span_config("unknown_span")
        assert config.display_name == "UNKNOWN_SPAN"
        assert config.category == "other"
        assert config.css_class == "bar-default"

    def test_get_transcript(self, adapter):
        span = {
            "attributes": [
                {"key": "transcript", "value": {"string_value": "Hello world"}}
            ]
        }
        assert adapter.get_transcript(span) == "Hello world"

    def test_get_output_text(self, adapter):
        span = {
            "attributes": [{"key": "text", "value": {"string_value": "Response text"}}]
        }
        assert adapter.get_output_text(span) == "Response text"

    def test_get_output_text_fallback_to_output(self, adapter):
        span = {
            "attributes": [{"key": "output", "value": {"string_value": "Output text"}}]
        }
        assert adapter.get_output_text(span) == "Output text"

    def test_get_ttfb(self, adapter):
        span = {
            "attributes": [{"key": "metrics.ttfb", "value": {"double_value": 0.234}}]
        }
        assert adapter.get_ttfb(span) == 0.234

    def test_get_turn_span_names(self, adapter):
        assert adapter.get_turn_span_names() == ["turn"]

    def test_normalize_span(self, adapter):
        span = {
            "name": "stt",
            "span_id_hex": "abc123",
            "attributes": [
                {"key": "transcript", "value": {"string_value": "Hello"}},
                {"key": "metrics.ttfb", "value": {"double_value": 0.5}},
            ],
        }
        normalized = adapter.normalize_span(span)

        assert "_normalized" in normalized
        assert normalized["_normalized"]["platform"] == "pipecat"
        assert normalized["_normalized"]["display_name"] == "STT"
        assert normalized["_normalized"]["category"] == "stt"
        assert normalized["_normalized"]["css_class"] == "bar-stt"
        assert normalized["_normalized"]["transcript"] == "Hello"
        assert normalized["_normalized"]["ttfb_seconds"] == 0.5


class TestLiveKitAdapter:
    @pytest.fixture
    def adapter(self):
        return LiveKitAdapter()

    def test_get_platform(self, adapter):
        assert adapter.get_platform() == "livekit"

    def test_get_span_config_agent_session(self, adapter):
        config = adapter.get_span_config("agent_session")
        assert config.display_name == "AGENT_SESSION"
        assert config.category == "root"
        assert config.css_class == "bar-conversation"

    def test_get_span_config_user_turn(self, adapter):
        config = adapter.get_span_config("user_turn")
        assert config.display_name == "USER_TURN"
        assert config.category == "stt"

    def test_get_span_config_agent_turn(self, adapter):
        config = adapter.get_span_config("agent_turn")
        assert config.display_name == "AGENT_TURN"
        assert config.category == "turn"

    def test_get_span_config_user_speaking(self, adapter):
        config = adapter.get_span_config("user_speaking")
        assert config.category == "stt"

    def test_get_span_config_llm_node(self, adapter):
        config = adapter.get_span_config("llm_node")
        assert config.category == "llm"

    def test_get_span_config_tts_node(self, adapter):
        config = adapter.get_span_config("tts_node")
        assert config.category == "tts"

    def test_get_span_config_agent_speaking(self, adapter):
        config = adapter.get_span_config("agent_speaking")
        assert config.category == "tts"

    def test_get_transcript_lk_transcript(self, adapter):
        span = {
            "attributes": [{"key": "lk.transcript", "value": {"string_value": "Hello"}}]
        }
        assert adapter.get_transcript(span) == "Hello"

    def test_get_transcript_lk_user_text(self, adapter):
        span = {
            "attributes": [
                {"key": "lk.user_text", "value": {"string_value": "User said"}}
            ]
        }
        assert adapter.get_transcript(span) == "User said"

    def test_get_output_text_lk_response(self, adapter):
        span = {
            "attributes": [
                {"key": "lk.response.text", "value": {"string_value": "Response"}}
            ]
        }
        assert adapter.get_output_text(span) == "Response"

    def test_get_ttfb_lk(self, adapter):
        span = {"attributes": [{"key": "lk.ttfb", "value": {"double_value": 0.123}}]}
        assert adapter.get_ttfb(span) == 0.123

    def test_get_turn_span_names(self, adapter):
        assert adapter.get_turn_span_names() == ["user_turn", "agent_turn"]

    def test_normalize_span(self, adapter):
        span = {
            "name": "llm_node",
            "span_id_hex": "xyz789",
            "attributes": [
                {"key": "lk.response.text", "value": {"string_value": "AI response"}},
                {"key": "lk.ttfb", "value": {"double_value": 0.3}},
            ],
        }
        normalized = adapter.normalize_span(span)

        assert "_normalized" in normalized
        assert normalized["_normalized"]["platform"] == "livekit"
        assert normalized["_normalized"]["display_name"] == "LLM_NODE"
        assert normalized["_normalized"]["category"] == "llm"
        assert normalized["_normalized"]["css_class"] == "bar-llm"
        assert normalized["_normalized"]["output_text"] == "AI response"
        assert normalized["_normalized"]["ttfb_seconds"] == 0.3
