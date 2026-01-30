from dataclasses import dataclass, asdict


@dataclass
class Message:
    role: str
    content: str
    timestamp: int
    was_interrupted: bool
    span_ids: list[str]

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MessageAccumulator:
    role: str | None = None
    texts: list[str] = None
    span_ids: list[str] = None
    timestamp: int = 0
    first_span: dict | None = None

    def __post_init__(self):
        if self.texts is None:
            self.texts = []
        if self.span_ids is None:
            self.span_ids = []

    def has_content(self) -> bool:
        return bool(self.texts and self.role)

    def reset(self, role: str, text: str, span: dict):
        self.role = role
        self.texts = [text]
        self.span_ids = [span.get("span_id_hex")]
        self.timestamp = span.get("start_time_unix_nano", 0)
        self.first_span = span

    def append(self, text: str, span: dict):
        self.texts.append(text)
        self.span_ids.append(span.get("span_id_hex"))


class Conversation:
    def __init__(self, spans: list[dict]):
        self.spans = spans
        self._messages: list[Message] | None = None

    def _get_attribute(self, span: dict, key: str) -> str | bool | None:
        attrs = span.get("attributes", [])
        for attr in attrs:
            if attr.get("key") == key:
                value = attr.get("value", {})
                return (
                    value.get("string_value")
                    or value.get("bool_value")
                    or value.get("int_value")
                    or value.get("double_value")
                )
        return None

    def _get_parent_turn(self, span: dict) -> dict | None:
        parent_id = span.get("parent_span_id_hex")
        if not parent_id:
            return None
        for s in self.spans:
            if s.get("span_id_hex") == parent_id and s.get("name") == "turn":
                return s
        return None

    def _get_span_text(self, span: dict) -> str:
        name = span.get("name")
        if name == "stt":
            return self._get_attribute(span, "transcript") or ""
        elif name == "tts":
            return self._get_attribute(span, "text") or ""
        return ""

    def _get_span_role(self, span: dict) -> str:
        return "user" if span.get("name") == "stt" else "assistant"

    def _get_interruption_status(self, span: dict | None) -> bool:
        if not span:
            return False
        turn = self._get_parent_turn(span)
        if not turn:
            return False
        return bool(self._get_attribute(turn, "turn.was_interrupted"))

    def _create_message_from_accumulator(
        self, acc: MessageAccumulator, was_interrupted: bool
    ) -> Message:
        return Message(
            role=acc.role,
            content=" ".join(acc.texts),
            timestamp=acc.timestamp,
            was_interrupted=was_interrupted and acc.role == "assistant",
            span_ids=acc.span_ids,
        )

    def _flush_accumulator(
        self, acc: MessageAccumulator, messages: list[Message], check_interruption: bool
    ):
        if not acc.has_content():
            return
        was_interrupted = self._get_interruption_status(acc.first_span) if check_interruption else False
        messages.append(self._create_message_from_accumulator(acc, was_interrupted))

    def _build_messages_from_spans(
        self, spans: list[dict], check_interruption: bool = True
    ) -> list[Message]:
        spans_sorted = sorted(spans, key=lambda s: s.get("start_time_unix_nano", 0))
        messages: list[Message] = []
        acc = MessageAccumulator()

        for span in spans_sorted:
            text = self._get_span_text(span)
            if not text:
                continue

            role = self._get_span_role(span)
            if role == acc.role:
                acc.append(text, span)
                continue

            self._flush_accumulator(acc, messages, check_interruption)
            acc.reset(role, text, span)

        self._flush_accumulator(acc, messages, check_interruption)
        return messages

    def get_messages(self) -> list[Message]:
        if self._messages is not None:
            return self._messages

        stt_tts_spans = [s for s in self.spans if s.get("name") in ("stt", "tts")]
        self._messages = self._build_messages_from_spans(stt_tts_spans)
        return self._messages

    def to_dict_list(self) -> list[dict]:
        return [m.to_dict() for m in self.get_messages()]
