from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class SpanTypeConfig:
    display_name: str
    category: str
    css_class: str
    sort_order: int = 999
    hidden: bool = False


class BaseTraceAdapter(ABC):
    @abstractmethod
    def get_platform(self) -> str:
        pass

    @abstractmethod
    def get_span_config(self, span_name: str) -> SpanTypeConfig:
        pass

    @abstractmethod
    def get_transcript(self, span: dict) -> Optional[str]:
        pass

    @abstractmethod
    def get_output_text(self, span: dict) -> Optional[str]:
        pass

    @abstractmethod
    def get_ttfb(self, span: dict) -> Optional[float]:
        pass

    @abstractmethod
    def get_turn_span_names(self) -> list[str]:
        pass

    def _get_attribute(
        self, span: dict, key: str
    ) -> Optional[str | bool | int | float]:
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

    def normalize_span(self, span: dict) -> dict:
        span_name = span.get("name", "")
        config = self.get_span_config(span_name)

        span["_normalized"] = {
            "platform": self.get_platform(),
            "display_name": config.display_name,
            "category": config.category,
            "css_class": config.css_class,
            "sort_order": config.sort_order,
            "hidden": config.hidden,
            "transcript": self.get_transcript(span),
            "output_text": self.get_output_text(span),
            "ttfb_seconds": self.get_ttfb(span),
        }
        return span

    def normalize_spans(self, spans: list[dict]) -> list[dict]:
        return [self.normalize_span(span) for span in spans]
