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

    def _get_turn_spans(self) -> dict[str, list[dict]]:
        turn_spans: dict[str, list[dict]] = {}
        for span in self.spans:
            name = span.get("name")
            if name not in ("stt", "tts"):
                continue
            turn = self._get_parent_turn(span)
            if not turn:
                continue
            turn_id = turn.get("span_id_hex")
            if turn_id not in turn_spans:
                turn_spans[turn_id] = []
            turn_spans[turn_id].append(span)
        return turn_spans

    def _get_span_text(self, span: dict) -> str:
        name = span.get("name")
        if name == "stt":
            return self._get_attribute(span, "transcript") or ""
        elif name == "tts":
            return self._get_attribute(span, "text") or ""
        return ""

    def _build_messages_for_turn(
        self, turn: dict, spans: list[dict]
    ) -> list[Message]:
        spans_sorted = sorted(spans, key=lambda s: s.get("start_time_unix_nano", 0))
        messages: list[Message] = []
        current_role: str | None = None
        current_texts: list[str] = []
        current_span_ids: list[str] = []
        first_timestamp: int | None = None

        was_interrupted = self._get_attribute(turn, "turn.was_interrupted")
        was_interrupted = bool(was_interrupted) if was_interrupted is not None else False

        for span in spans_sorted:
            name = span.get("name")
            role = "user" if name == "stt" else "assistant"
            text = self._get_span_text(span)
            if not text:
                continue

            if role != current_role:
                if current_role is not None and current_texts:
                    messages.append(
                        Message(
                            role=current_role,
                            content=" ".join(current_texts),
                            timestamp=first_timestamp,
                            was_interrupted=was_interrupted if current_role == "assistant" else False,
                            span_ids=current_span_ids,
                        )
                    )
                current_role = role
                current_texts = [text]
                current_span_ids = [span.get("span_id_hex")]
                first_timestamp = span.get("start_time_unix_nano")
            else:
                current_texts.append(text)
                current_span_ids.append(span.get("span_id_hex"))

        if current_role is not None and current_texts:
            messages.append(
                Message(
                    role=current_role,
                    content=" ".join(current_texts),
                    timestamp=first_timestamp,
                    was_interrupted=was_interrupted if current_role == "assistant" else False,
                    span_ids=current_span_ids,
                )
            )

        return messages

    def get_messages(self) -> list[Message]:
        if self._messages is not None:
            return self._messages

        turn_spans = self._get_turn_spans()
        turns = [s for s in self.spans if s.get("name") == "turn"]
        turns_sorted = sorted(turns, key=lambda t: t.get("start_time_unix_nano", 0))

        all_messages: list[Message] = []
        for turn in turns_sorted:
            turn_id = turn.get("span_id_hex")
            spans = turn_spans.get(turn_id, [])
            if spans:
                messages = self._build_messages_for_turn(turn, spans)
                all_messages.extend(messages)

        self._messages = all_messages
        return self._messages

    def to_dict_list(self) -> list[dict]:
        return [m.to_dict() for m in self.get_messages()]
