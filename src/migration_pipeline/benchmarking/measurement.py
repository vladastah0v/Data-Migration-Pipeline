import math
from array import array
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Event, Thread
from time import perf_counter

import psutil


@dataclass(slots=True)
class ProcessMeasurement:
    duration_seconds: float = 0
    cpu_seconds: float = 0
    initial_rss_bytes: int = 0
    peak_rss_bytes: int = 0
    _process: psutil.Process = field(default_factory=psutil.Process, repr=False)

    @property
    def cpu_percent(self) -> float:
        return rate(self.cpu_seconds * 100, self.duration_seconds)

    @contextmanager
    def measure(self) -> Iterator[None]:
        stopped = Event()
        self.initial_rss_bytes = self._process.memory_info().rss
        self.peak_rss_bytes = self.initial_rss_bytes

        def sample() -> None:
            while not stopped.wait(0.05):
                self.peak_rss_bytes = max(self.peak_rss_bytes, self._process.memory_info().rss)

        sampler = Thread(target=sample, daemon=True)
        sampler.start()
        cpu_start = self._cpu_seconds()
        start = perf_counter()
        try:
            yield
        finally:
            self.duration_seconds = perf_counter() - start
            self.cpu_seconds = self._cpu_seconds() - cpu_start
            stopped.set()
            sampler.join()
            self.peak_rss_bytes = max(self.peak_rss_bytes, self._process.memory_info().rss)

    def _cpu_seconds(self) -> float:
        cpu = self._process.cpu_times()
        return cpu.user + cpu.system


def rate(numerator: float, duration: float) -> float:
    return numerator / duration if duration > 0 else 0.0


def batch_percentiles(values: Sequence[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * 0.50) - 1], ordered[math.ceil(len(ordered) * 0.95) - 1]


class BatchMeasurements:
    def __init__(self) -> None:
        self.durations = array("d")
        self.totals: dict[str, float | None] = dict.fromkeys(
            (
                "extraction_duration_seconds",
                "transform_duration_seconds",
                "load_duration_seconds",
                "transaction_duration_seconds",
            ),
            0.0,
        )

    def add(self, fields: dict[str, object]) -> None:
        self.durations.append(fields["batch_duration_seconds"])
        for key, total in self.totals.items():
            value = fields.get(key)
            self.totals[key] = total + value if total is not None and value is not None else None

    def total(self, field: str) -> float | None:
        return self.totals[field] if self.durations else None
