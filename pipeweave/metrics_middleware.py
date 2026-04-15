"""Middleware that attaches StageMetrics to each stage execution."""
from __future__ import annotations

from typing import Awaitable, Callable

from pipeweave.context import PipelineContext, StageResult
from pipeweave.metrics import PipelineMetrics, StageMetrics

_METRICS_KEY = "__pipeweave_metrics__"


def get_pipeline_metrics(ctx: PipelineContext) -> PipelineMetrics | None:
    """Retrieve the PipelineMetrics object stored in *ctx*, or None."""
    return ctx.get(_METRICS_KEY)  # type: ignore[return-value]


def attach_pipeline_metrics(
    ctx: PipelineContext,
    pipeline_name: str,
) -> PipelineMetrics:
    """Create and store a fresh PipelineMetrics on *ctx*."""
    pm = PipelineMetrics(pipeline_name=pipeline_name)
    ctx.set(_METRICS_KEY, pm)
    return pm


def metrics_middleware(
    handler: Callable[[object, PipelineContext], Awaitable[StageResult]],
    stage_name: str,
) -> Callable[[object, PipelineContext], Awaitable[StageResult]]:
    """Middleware factory that records per-stage timing and outcome."""

    async def _inner(data: object, ctx: PipelineContext) -> StageResult:
        sm = StageMetrics(stage_name=stage_name)
        sm.attempts += 1

        result: StageResult = await handler(data, ctx)

        sm.finish(
            succeeded=result.ok,
            error=str(result.error) if result.error is not None else None,
        )

        pm: PipelineMetrics | None = get_pipeline_metrics(ctx)
        if pm is not None:
            pm.record_stage(sm)

        return result

    return _inner
