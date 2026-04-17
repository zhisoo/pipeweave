"""Priority queue middleware for ordering pipeline stage execution by priority."""
from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable
from pipeweave.context import PipelineContext, StageResult
from pipeweave.errors import PipeweaveError


class PriorityQueueError(PipeweaveError):
    pass


@dataclass
class PriorityQueueConfig:
    max_size: int = 0  # 0 = unlimited

    def __post_init__(self) -> None:
        if self.max_size < 0:
            raise ValueError("max_size must be >= 0")


@dataclass(order=True)
class _PrioritizedItem:
    priority: int
    item: Any = field(compare=False)


class StagePriorityQueue:
    """Async priority queue that wraps stage handlers, executing highest priority first."""

    def __init__(self, config: PriorityQueueConfig | None = None) -> None:
        self._config = config or PriorityQueueConfig()
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(
            maxsize=self._config.max_size
        )

    async def put(self, priority: int, handler: Callable[..., Awaitable[StageResult]]) -> None:
        """Enqueue a handler with given priority (lower number = higher priority)."""
        await self._queue.put(_PrioritizedItem(priority=priority, item=handler))

    async def run_all(self, ctx: PipelineContext, data: Any) -> list[StageResult]:
        """Drain the queue, executing handlers in priority order."""
        results: list[StageResult] = []
        while not self._queue.empty():
            entry: _PrioritizedItem = await self._queue.get()
            result = await entry.item(ctx, data)
            results.append(result)
            self._queue.task_done()
        return results

    @property
    def size(self) -> int:
        return self._queue.qsize()


def make_priority_middleware(priority: int = 0):
    """Middleware factory that tags a stage with a priority value stored in context metadata."""
    async def middleware(ctx: PipelineContext, result: StageResult, next_call):
        ctx.set("stage_priority", priority)
        return await next_call(ctx, result)
    return middleware
