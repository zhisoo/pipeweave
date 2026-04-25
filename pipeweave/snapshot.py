"""Pipeline snapshot: capture and restore pipeline state at a point in time."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pipeweave.errors import PipeweaveError


class SnapshotError(PipeweaveError):
    """Raised when a snapshot operation fails."""


@dataclass
class SnapshotConfig:
    max_snapshots: int = 10
    include_metadata: bool = True

    def __post_init__(self) -> None:
        if self.max_snapshots <= 0:
            raise ValueError("max_snapshots must be a positive integer")


@dataclass
class PipelineSnapshot:
    """Immutable record of pipeline state at a given moment."""

    run_id: str
    stage_names: List[str]
    results: Dict[str, Any]
    metadata: Dict[str, Any]
    captured_at: float = field(default_factory=time.monotonic)

    def __repr__(self) -> str:  # pragma: no cover
        age_ms = (time.monotonic() - self.captured_at) * 1000
        return (
            f"PipelineSnapshot(run_id={self.run_id!r}, "
            f"stages={self.stage_names}, age_ms={age_ms:.1f})"
        )


class SnapshotStore:
    """Stores a bounded history of pipeline snapshots."""

    def __init__(self, config: Optional[SnapshotConfig] = None) -> None:
        self._config = config or SnapshotConfig()
        self._snapshots: List[PipelineSnapshot] = []

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def capture(self, snapshot: PipelineSnapshot) -> None:
        """Add *snapshot* to the store, evicting the oldest entry if full."""
        self._snapshots.append(snapshot)
        if len(self._snapshots) > self._config.max_snapshots:
            self._snapshots.pop(0)

    def clear(self, run_id: Optional[str] = None) -> None:
        """Remove snapshots, optionally filtered to *run_id*."""
        if run_id is None:
            self._snapshots.clear()
        else:
            self._snapshots = [s for s in self._snapshots if s.run_id != run_id]

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def latest(self, run_id: Optional[str] = None) -> Optional[PipelineSnapshot]:
        """Return the most recent snapshot, optionally filtered to *run_id*."""
        candidates = (
            [s for s in self._snapshots if s.run_id == run_id]
            if run_id is not None
            else self._snapshots
        )
        return candidates[-1] if candidates else None

    def all(self, run_id: Optional[str] = None) -> List[PipelineSnapshot]:
        """Return all snapshots, optionally filtered to *run_id*."""
        if run_id is None:
            return list(self._snapshots)
        return [s for s in self._snapshots if s.run_id == run_id]

    def __len__(self) -> int:
        return len(self._snapshots)
