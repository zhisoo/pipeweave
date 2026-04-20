"""Replay support: record pipeline inputs and re-run them later."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class ReplayConfig:
    max_entries: int = 100
    enabled: bool = True

    def __post_init__(self) -> None:
        if self.max_entries <= 0:
            raise ValueError("max_entries must be a positive integer")


@dataclass
class ReplayEntry:
    input: Any
    run_id: str
    timestamp: float = field(default_factory=time.time)

    def __repr__(self) -> str:  # pragma: no cover
        return f"ReplayEntry(run_id={self.run_id!r}, timestamp={self.timestamp:.3f})"


class ReplayBuffer:
    """Stores pipeline inputs so they can be replayed."""

    def __init__(self, config: Optional[ReplayConfig] = None) -> None:
        self._config = config or ReplayConfig()
        self._entries: List[ReplayEntry] = []

    @property
    def entries(self) -> List[ReplayEntry]:
        return list(self._entries)

    def record(self, run_id: str, input: Any) -> None:
        if not self._config.enabled:
            return
        entry = ReplayEntry(input=input, run_id=run_id)
        self._entries.append(entry)
        if len(self._entries) > self._config.max_entries:
            self._entries.pop(0)

    def get(self, run_id: str) -> Optional[ReplayEntry]:
        for entry in reversed(self._entries):
            if entry.run_id == run_id:
                return entry
        return None

    def clear(self) -> None:
        self._entries.clear()

    def __len__(self) -> int:
        return len(self._entries)
