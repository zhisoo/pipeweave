"""Dead-letter queue for capturing and inspecting failed stage results."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, List, Optional

from pipeweave.context import PipelineContext, StageResult


@dataclass
class DeadLetterEntry:
    stage_name: str
    input_data: Any
    error: Exception
    attempt: int = 1

    def __repr__(self) -> str:
        return (
            f"DeadLetterEntry(stage={self.stage_name!r}, "
            f"error={self.error!r}, attempt={self.attempt})"
        )


class DeadLetterQueue:
    """Collects failed stage results for later inspection or reprocessing."""

    def __init__(self, max_size: int = 100) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be a positive integer")
        self._max_size = max_size
        self._entries: List[DeadLetterEntry] = []
        self._lock = asyncio.Lock()

    @property
    def entries(self) -> List[DeadLetterEntry]:
        return list(self._entries)

    @property
    def size(self) -> int:
        return len(self._entries)

    async def push(self, entry: DeadLetterEntry) -> None:
        async with self._lock:
            if len(self._entries) >= self._max_size:
                self._entries.pop(0)
            self._entries.append(entry)

    async def drain(self) -> List[DeadLetterEntry]:
        async with self._lock:
            items = list(self._entries)
            self._entries.clear()
            return items

    def make_middleware(self) -> Callable:
        """Return middleware that captures failures into this queue."""
        dlq = self

        async def _deadletter_middleware(
            ctx: PipelineContext,
            result: StageResult,
            call_next: Callable[..., Coroutine],
        ) -> StageResult:
            outcome: StageResult = await call_next(ctx, result)
            if not outcome.success and outcome.error is not None:
                entry = DeadLetterEntry(
                    stage_name=outcome.stage_name,
                    input_data=result.value,
                    error=outcome.error,
                )
                await dlq.push(entry)
            return outcome

        return _deadletter_middleware
