import statistics
from dataclasses import asdict, dataclass


@dataclass
class TTFBDataPoint:
    timestamp_ms: float
    relative_time_ms: float
    ttfb_ms: float
    span_id: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TTFBStats:
    avg_ms: float
    min_ms: float
    max_ms: float
    p50_ms: float
    p95_ms: float
    count: int

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TTFBSeries:
    service: str
    data_points: list[TTFBDataPoint]
    stats: TTFBStats

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "data_points": [dp.to_dict() for dp in self.data_points],
            "stats": self.stats.to_dict()
        }


class Metrics:
    SERVICES = ("stt", "llm", "tts")

    def __init__(self, spans: list[dict]):
        self.spans = spans
        self._session_start_ms: float | None = None
        self._ttfb_series: dict[str, TTFBSeries] | None = None

    def _find_min_start_nano(self) -> int | None:
        min_start = None
        for span in self.spans:
            start = span.get("start_time_unix_nano")
            if not start:
                continue
            start_int = int(start)
            if min_start is None or start_int < min_start:
                min_start = start_int
        return min_start

    @property
    def session_start_ms(self) -> float:
        if self._session_start_ms is None:
            min_start = self._find_min_start_nano()
            self._session_start_ms = (min_start or 0) / 1_000_000
        return self._session_start_ms

    def _get_attribute(self, span: dict, key: str) -> float | None:
        attrs = span.get("attributes", [])
        for attr in attrs:
            if attr.get("key") == key:
                value = attr.get("value", {})
                return value.get("double_value") or value.get("int_value")
        return None

    def _compute_stats(self, values: list[float]) -> TTFBStats:
        if not values:
            return TTFBStats(avg_ms=0, min_ms=0, max_ms=0, p50_ms=0, p95_ms=0, count=0)

        sorted_vals = sorted(values)
        n = len(sorted_vals)
        p50_idx = int(n * 0.5)
        p95_idx = min(int(n * 0.95), n - 1)

        return TTFBStats(
            avg_ms=statistics.mean(values),
            min_ms=min(values),
            max_ms=max(values),
            p50_ms=sorted_vals[p50_idx],
            p95_ms=sorted_vals[p95_idx],
            count=n
        )

    def _extract_ttfb_data_point(self, span: dict) -> TTFBDataPoint | None:
        ttfb_seconds = self._get_attribute(span, "metrics.ttfb")
        if ttfb_seconds is None:
            return None

        start_nano = int(span.get("start_time_unix_nano", 0))
        timestamp_ms = start_nano / 1_000_000

        return TTFBDataPoint(
            timestamp_ms=timestamp_ms,
            relative_time_ms=timestamp_ms - self.session_start_ms,
            ttfb_ms=ttfb_seconds * 1000,
            span_id=span.get("span_id_hex", "")
        )

    def _collect_data_points(self) -> dict[str, list[TTFBDataPoint]]:
        series: dict[str, list[TTFBDataPoint]] = {s: [] for s in self.SERVICES}
        for span in self.spans:
            name = span.get("name")
            if name not in self.SERVICES:
                continue
            data_point = self._extract_ttfb_data_point(span)
            if data_point:
                series[name].append(data_point)
        return series

    def _build_series(self, service: str, data_points: list[TTFBDataPoint]) -> TTFBSeries:
        data_points.sort(key=lambda dp: dp.timestamp_ms)
        stats = self._compute_stats([dp.ttfb_ms for dp in data_points])
        return TTFBSeries(service=service, data_points=data_points, stats=stats)

    def get_ttfb_series(self) -> dict[str, TTFBSeries]:
        if self._ttfb_series is not None:
            return self._ttfb_series

        series = self._collect_data_points()
        result = {
            service: self._build_series(service, data_points)
            for service, data_points in series.items()
            if data_points
        }

        self._ttfb_series = result
        return result

    def to_dict(self) -> dict:
        series = self.get_ttfb_series()
        return {
            "series": {s: series[s].to_dict() for s in series},
            "services": list(series.keys())
        }
