import time
from contextlib import contextmanager
from typing import Dict


class Timer:
    """Accumulates named timing segments for a single request."""

    def __init__(self):
        self._marks: Dict[str, float] = {}
        self._start = time.perf_counter()

    @contextmanager
    def segment(self, name: str):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self._marks[name] = (time.perf_counter() - t0) * 1000  # ms

    def elapsed_ms(self) -> float:
        return (time.perf_counter() - self._start) * 1000

    def get(self, name: str) -> float:
        return round(self._marks.get(name, 0.0), 1)

    def total_ms(self) -> float:
        return round(self.elapsed_ms(), 1)
