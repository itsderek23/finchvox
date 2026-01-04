"""Trace metadata and utilities."""

import json
from pathlib import Path
from typing import Optional


class Trace:
    """
    Represents a trace and provides calculated metadata.

    Loads span data from a trace JSONL file and calculates:
    - Start time (earliest span start)
    - End time (latest span end)
    - Duration in milliseconds
    - Span count
    """

    def __init__(self, trace_file: Path):
        """
        Initialize trace from a trace file path.

        Args:
            trace_file: Path to trace_{trace_id}.jsonl file
        """
        self.trace_file = trace_file
        self.trace_id = trace_file.stem.replace("trace_", "")
        self._span_count: Optional[int] = None
        self._min_start_nano: Optional[int] = None
        self._max_end_nano: Optional[int] = None
        self._service_name: Optional[str] = None
        self._load_metadata()

    def _load_metadata(self):
        """Load span metadata from trace file."""
        span_count = 0
        min_start = None
        max_end = None
        service_name = None

        try:
            with open(self.trace_file, 'r') as f:
                for line in f:
                    if line.strip():
                        span = json.loads(line)
                        span_count += 1

                        if "start_time_unix_nano" in span:
                            start_nano = int(span["start_time_unix_nano"])
                            if min_start is None or start_nano < min_start:
                                min_start = start_nano

                        if "end_time_unix_nano" in span:
                            end_nano = int(span["end_time_unix_nano"])
                            if max_end is None or end_nano > max_end:
                                max_end = end_nano

                        # Extract service name from first span with resource attributes
                        if service_name is None and "resource" in span:
                            resource = span["resource"]
                            if "attributes" in resource:
                                for attr in resource["attributes"]:
                                    if attr.get("key") == "service.name":
                                        value = attr.get("value", {})
                                        service_name = value.get("string_value")
                                        break
        except Exception as e:
            print(f"Error loading trace {self.trace_file}: {e}")

        self._span_count = span_count
        self._min_start_nano = min_start
        self._max_end_nano = max_end
        self._service_name = service_name

    @property
    def span_count(self) -> int:
        """Get total span count."""
        return self._span_count or 0

    @property
    def start_time(self) -> Optional[float]:
        """Get trace start time in seconds (Unix timestamp)."""
        if self._min_start_nano:
            return self._min_start_nano / 1_000_000_000
        return None

    @property
    def end_time(self) -> Optional[float]:
        """Get trace end time in seconds (Unix timestamp)."""
        if self._max_end_nano:
            return self._max_end_nano / 1_000_000_000
        return None

    @property
    def duration_ms(self) -> Optional[float]:
        """Get trace duration in milliseconds."""
        if self._min_start_nano and self._max_end_nano:
            return (self._max_end_nano - self._min_start_nano) / 1_000_000
        return None

    @property
    def service_name(self) -> Optional[str]:
        """Get service name from first span with resource attributes."""
        return self._service_name

    def to_dict(self) -> dict:
        """Convert to dictionary for API response."""
        return {
            "trace_id": self.trace_id,
            "service_name": self.service_name,
            "span_count": self.span_count,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
        }
