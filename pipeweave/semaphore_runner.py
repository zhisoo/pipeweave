"""PipelineRunner variant that gates every stage through a SemaphorePool."""

from __future__ import annotations

from typing import Any, Optional

from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner
from pipeweave.semaphore import SemaphoreConfig, SemaphorePool
from pipeweave.context import PipelineContext


class SemaphorePipelineRunner(PipelineRunner):
    """Wraps each stage handler so it runs inside the named semaphore.

    The semaphore name defaults to the stage name, allowing per-stage
    concurrency limits defined in *SemaphoreConfig.limits*.
    """

    def __init__(
        self,
        pipeline: Pipeline,
        config: Optional[SemaphoreConfig] = None,
    ) -> None:
        self._pool = SemaphorePool(config or SemaphoreConfig())
        # Wrap stages before passing to the parent runner.
        super().__init__(self._wrap_pipeline(pipeline))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _wrap_pipeline(self, pipeline: Pipeline) -> Pipeline:
        """Return a new pipeline whose stages are semaphore-gated."""
        from pipeweave.pipeline import Pipeline, StageConfig
        from pipeweave.stage import Stage

        wrapped = Pipeline()
        for stage in pipeline._stages:  # type: ignore[attr-defined]
            name = stage.name
            original_fn = stage.fn
            pool = self._pool

            async def _gated(data: Any, _name=name, _fn=original_fn) -> Any:
                async with pool.acquire(_name):
                    return await _fn(data)

            wrapped.pipe(
                Stage(
                    name=name,
                    fn=_gated,
                    config=stage.config,
                )
            )
        return wrapped

    @property
    def semaphore_pool(self) -> SemaphorePool:
        """Expose the underlying pool for inspection."""
        return self._pool
