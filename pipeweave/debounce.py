"""Debounce middleware: skip a stage if it was called too recently."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from pipeweave.errors import PipeweaveError


@dataclass
class DebounceConfig:
    """Configuration for debounce behaviour.

    Attributes:
        interval_ms: Minimum milliseconds that must elapse between two
                     successive executions of the *same* stage.  Must be > 0.
        stages: Optional list of stage names to debounce.  When *None* every
                stage in the pipeline is debounced.
    """
    interval_ms: float
    stages: Optional[list[str]] = field(default=None)

    def __post_init__(self) -> None:
        if self.interval_ms <= 0:
            raise PipeweaveError("interval_ms must be greater than 0")


class Debouncer:
    """Tracks last-call timestamps and decides whether a stage should run."""

    def __init__(self, config: DebounceConfig) -> None:
        self._config = config
        self._last_called: Dict[str, float] = {}

    def should_run(self, stage_name: str) -> bool:
        """Return True if enough time has elapsed since the last call."""
        if self._config.stages is not None and stage_name not in self._config.stages:
            return True
        now = time.monotonic()
        last = self._last_called.get(stage_name)
        if last is None or (now - last) * 1000 >= self._config.interval_ms:
            self._last_called[stage_name] = now
            return True
        return False

    def reset(self, stage_name: Optional[str] = None) -> None:
        """Clear recorded timestamps (all stages or a specific one)."""
        if stage_name is None:
            self._last_called.clear()
        else:
            self._last_called.pop(stage_name, None)


def make_debounce_middleware(debouncer: Debouncer):
    """Return middleware that skips a stage when it fires too quickly."""
    from pipeweave.context import StageResult

    async def debounce_middleware(ctx, call_next):
        stage_name = ctx.current_stage or ""
        if not debouncer.should_run(stage_name):
            # Return the previous result unchanged (skip execution)
            prev = ctx.last_result
            value = prev.value if prev is not None else None
            return StageResult(stage=stage_name, value=value, skipped=True)
        return await call_next(ctx)

    return debounce_middleware
