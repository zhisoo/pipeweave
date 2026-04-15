"""Pipeline execution metrics collection and reporting."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class StageMetrics:
    """Timing and outcome metrics for a single stage execution."""

    stage_name: str
    started_at: float = field(default_factory=time.monotonic)
    ended_at: Optional[float] = None
    attempts: int = 0
    succeeded: bool = False
    error: Optional[str] = None

    def finish(self, *, succeeded: bool, error: Optional[str] = None) -> None:
        self.ended_at = time.monotonic()
        self.succeeded = succeeded
        self.error = error

    @property
    def duration_ms(self) -> Optional[float]:
        if self.ended_at is None:
            return None
        return (self.ended_at - self.started_at) * 1_000

    def __repr__(self) -> str:  # pragma: no cover
        status = "ok" if self.succeeded else f"err({self.error})"
        dur = f"{self.duration_ms:.2f}ms" if self.duration_ms is not None else "?"
        return f"<StageMetrics stage={self.stage_name!r} {status} dur={dur} attempts={self.attempts}>"


@dataclass
class PipelineMetrics:
    """Aggregate metrics for a complete pipeline run."""

    pipeline_name: str
    started_at: float = field(default_factory=time.monotonic)
    ended_at: Optional[float] = None
    stages: List[StageMetrics] = field(default_factory=list)

    def record_stage(self, sm: StageMetrics) -> None:
        self.stages.append(sm)

    def finish(self) -> None:
        self.ended_at = time.monotonic()

    @property
    def total_duration_ms(self) -> Optional[float]:
        if self.ended_at is None:
            return None
        return (self.ended_at - self.started_at) * 1_000

    @property
    def succeeded_stages(self) -> List[StageMetrics]:
        return [s for s in self.stages if s.succeeded]

    @property
    def failed_stages(self) -> List[StageMetrics]:
        return [s for s in self.stages if not s.succeeded]

    def summary(self) -> Dict[str, object]:
        return {
            "pipeline": self.pipeline_name,
            "total_duration_ms": self.total_duration_ms,
            "stages_total": len(self.stages),
            "stages_succeeded": len(self.succeeded_stages),
            "stages_failed": len(self.failed_stages),
            "stage_details": [
                {
                    "name": s.stage_name,
                    "succeeded": s.succeeded,
                    "duration_ms": s.duration_ms,
                    "attempts": s.attempts,
                    "error": s.error,
                }
                for s in self.stages
            ],
        }
