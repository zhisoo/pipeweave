"""Sliding/tumbling window aggregation support for pipelines."""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Optional


@dataclass
class WindowConfig:
    size: int
    step: int = 0  # 0 means tumbling (step == size)
    aggregator: Optional[Callable[[list[Any]], Any]] = None

    def __post_init__(self) -> None:
        if self.size <= 0:
            raise ValueError("size must be positive")
        if self.step < 0:
            raise ValueError("step must be non-negative")
        if self.aggregator is not None and not callable(self.aggregator):
            raise TypeError("aggregator must be callable")
        if self.step == 0:
            self.step = self.size


class WindowBuffer:
    """Collects items and emits windows according to WindowConfig."""

    def __init__(self, config: WindowConfig) -> None:
        self._config = config
        self._buf: Deque[Any] = deque()
        self._since_last_emit: int = 0

    @property
    def pending(self) -> int:
        return len(self._buf)

    def add(self, item: Any) -> list[list[Any]]:
        """Add an item; return zero or more windows ready for emission."""
        self._buf.append(item)
        self._since_last_emit += 1
        windows: list[list[Any]] = []
        while (
            len(self._buf) >= self._config.size
            and self._since_last_emit >= self._config.step
        ):
            window = list(self._buf)[: self._config.size]
            windows.append(window)
            # advance by step
            for _ in range(self._config.step):
                if self._buf:
                    self._buf.popleft()
            self._since_last_emit = 0
        return windows

    def flush(self) -> list[list[Any]]:
        """Return remaining items as a partial window (if any)."""
        if self._buf:
            return [list(self._buf)]
        return []

    def aggregate(self, window: list[Any]) -> Any:
        if self._config.aggregator:
            return self._config.aggregator(window)
        return window
