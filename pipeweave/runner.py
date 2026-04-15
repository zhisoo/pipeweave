"""High-level pipeline runner that integrates middleware and hooks."""
from __future__ import annotations

import asyncio
from typing import Any, Optional

from pipeweave.pipeline import Pipeline
from pipeweave.context import PipelineContext, StageResult
from pipeweave.middleware import MiddlewareChain
from pipeweave.hooks import HookSet, HookRunner
from pipeweave.errors import PipelineAbortedError, StageError


class PipelineRunner:
    """Executes a :class:`Pipeline` with optional middleware and lifecycle hooks."""

    def __init__(
        self,
        pipeline: Pipeline,
        *,
        middleware: Optional[MiddlewareChain] = None,
        hooks: Optional[HookSet] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        self._pipeline = pipeline
        self._chain = middleware or MiddlewareChain()
        self._hooks = HookRunner(hooks)
        self._metadata = metadata or {}

    async def run(self, initial_data: Any = None) -> PipelineContext:
        """Run the pipeline and return the populated :class:`PipelineContext`."""
        ctx = PipelineContext(metadata=self._metadata)
        await self._hooks.pipeline_start(ctx)

        data = initial_data
        try:
            for stage_name, stage_fn, config in self._pipeline.stages:
                await self._hooks.stage_start(stage_name, data, ctx)
                handler = self._chain.wrap(self._make_handler(stage_fn, config))
                result = await handler(stage_name, data, ctx)
                ctx.record(stage_name, result)
                await self._hooks.stage_end(stage_name, result, ctx)

                if not result.success:
                    await self._hooks.stage_error(stage_name, result.error, ctx)
                    if config.abort_on_error:
                        raise PipelineAbortedError(stage_name, result.error)
                else:
                    data = result.value
        finally:
            await self._hooks.pipeline_end(ctx)

        return ctx

    @staticmethod
    def _make_handler(stage_fn, config):
        """Return a bare async handler that executes *stage_fn* with retry logic."""
        from pipeweave.retry import RetryPolicy
        import time

        async def _handler(stage_name: str, data: Any, ctx: PipelineContext) -> StageResult:
            policy: RetryPolicy = config.retry_policy or RetryPolicy(max_attempts=1)
            last_exc: Optional[Exception] = None
            for attempt in range(policy.max_attempts):
                try:
                    value = await stage_fn(data)
                    return StageResult(stage_name=stage_name, value=value, success=True)
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    from pipeweave.retry import is_retryable, compute_delay
                    if attempt + 1 < policy.max_attempts and is_retryable(exc, policy):
                        delay = compute_delay(attempt, policy)
                        await asyncio.sleep(delay)
                    else:
                        break
            return StageResult(stage_name=stage_name, value=None, success=False, error=last_exc)

        return _handler
