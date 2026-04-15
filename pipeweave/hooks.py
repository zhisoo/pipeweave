"""Lifecycle hooks for pipeline execution."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Awaitable, Any, Optional

from pipeweave.context import PipelineContext, StageResult

SyncOrAsync = Callable[..., Any]


@dataclass
class HookSet:
    """Collection of optional lifecycle callbacks for a pipeline."""

    on_pipeline_start: Optional[SyncOrAsync] = None
    on_pipeline_end: Optional[SyncOrAsync] = None
    on_stage_start: Optional[SyncOrAsync] = None
    on_stage_end: Optional[SyncOrAsync] = None
    on_stage_error: Optional[SyncOrAsync] = None


async def _invoke(hook: Optional[SyncOrAsync], *args: Any) -> None:
    """Call *hook* with *args*, supporting both sync and async callables."""
    if hook is None:
        return
    import asyncio
    result = hook(*args)
    if asyncio.isfuture(result) or asyncio.iscoroutine(result):
        await result


class HookRunner:
    """Dispatches lifecycle events to a :class:`HookSet`."""

    def __init__(self, hooks: Optional[HookSet] = None) -> None:
        self._hooks: HookSet = hooks or HookSet()

    async def pipeline_start(self, ctx: PipelineContext) -> None:
        await _invoke(self._hooks.on_pipeline_start, ctx)

    async def pipeline_end(self, ctx: PipelineContext) -> None:
        await _invoke(self._hooks.on_pipeline_end, ctx)

    async def stage_start(self, stage_name: str, data: Any, ctx: PipelineContext) -> None:
        await _invoke(self._hooks.on_stage_start, stage_name, data, ctx)

    async def stage_end(self, stage_name: str, result: StageResult, ctx: PipelineContext) -> None:
        await _invoke(self._hooks.on_stage_end, stage_name, result, ctx)

    async def stage_error(self, stage_name: str, exc: Exception, ctx: PipelineContext) -> None:
        await _invoke(self._hooks.on_stage_error, stage_name, exc, ctx)
