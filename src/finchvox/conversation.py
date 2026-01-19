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

    def _get_span_role(self, span: dict) -> str:
        return "user" if span.get("name") == "stt" else "assistant"

    def _create_message(
        self,
        role: str,
        texts: list[str],
        span_ids: list[str],
        timestamp: int,
        was_interrupted: bool,
    ) -> Message:
        return Message(
            role=role,
            content=" ".join(texts),
            timestamp=timestamp,
            was_interrupted=was_interrupted and role == "assistant",
            span_ids=span_ids,
        )

    def _build_messages_for_turn(
        self, turn: dict, spans: list[dict]
    ) -> list[Message]:
        spans_sorted = sorted(spans, key=lambda s: s.get("start_time_unix_nano", 0))
        was_interrupted = bool(self._get_attribute(turn, "turn.was_interrupted"))

        messages: list[Message] = []
        acc_role: str | None = None
        acc_texts: list[str] = []
        acc_span_ids: list[str] = []
        acc_timestamp: int = 0

        for span in spans_sorted:
            text = self._get_span_text(span)
            if not text:
                continue

            role = self._get_span_role(span)
            if role == acc_role:
                acc_texts.append(text)
                acc_span_ids.append(span.get("span_id_hex"))
                continue

            if acc_texts:
                messages.append(
                    self._create_message(acc_role, acc_texts, acc_span_ids, acc_timestamp, was_interrupted)
                )

            acc_role = role
            acc_texts = [text]
            acc_span_ids = [span.get("span_id_hex")]
            acc_timestamp = span.get("start_time_unix_nano", 0)

        if acc_texts:
            messages.append(
                self._create_message(acc_role, acc_texts, acc_span_ids, acc_timestamp, was_interrupted)
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
