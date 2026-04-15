"""Pipeline execution context for passing metadata and state between stages."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class StageResult:
    """Holds the result of a single stage execution."""

    stage_name: str
    success: bool
    output: Any = None
    error: Optional[Exception] = None
    attempts: int = 1
    duration_ms: float = 0.0

    def __repr__(self) -> str:
        status = "ok" if self.success else "failed"
        return f"StageResult({self.stage_name!r}, {status}, attempts={self.attempts})"


@dataclass
class PipelineContext:
    """Carries state, metadata, and results across pipeline stages."""

    metadata: Dict[str, Any] = field(default_factory=dict)
    stage_results: List[StageResult] = field(default_factory=list)
    _start_time: float = field(default_factory=time.monotonic, init=False, repr=False)

    # -- result tracking -------------------------------------------------------

    def record(self, result: StageResult) -> None:
        """Append a stage result to the execution history."""
        self.stage_results.append(result)

    @property
    def last_result(self) -> Optional[StageResult]:
        """Return the most recently recorded stage result, or None."""
        return self.stage_results[-1] if self.stage_results else None

    @property
    def failed_stages(self) -> List[StageResult]:
        """Return all stage results that represent failures."""
        return [r for r in self.stage_results if not r.success]

    @property
    def succeeded(self) -> bool:
        """True only if every recorded stage succeeded."""
        return bool(self.stage_results) and all(
            r.success for r in self.stage_results
        )

    # -- timing ----------------------------------------------------------------

    @property
    def elapsed_ms(self) -> float:
        """Wall-clock time in milliseconds since the context was created."""
        return (time.monotonic() - self._start_time) * 1_000

    # -- metadata helpers ------------------------------------------------------

    def set(self, key: str, value: Any) -> None:
        """Store an arbitrary value in the metadata dict."""
        self.metadata[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from the metadata dict."""
        return self.metadata.get(key, default)
