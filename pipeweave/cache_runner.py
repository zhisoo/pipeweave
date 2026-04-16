"""PipelineRunner variant with transparent stage-level caching."""
from __future__ import annotations

from typing import Any, Optional

from pipeweave.cache import CacheConfig, StageCache, cache_middleware
from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner
from pipeweave.context import PipelineContext


class CachedPipelineRunner(PipelineRunner):
    """Runs a pipeline with an attached StageCache.

    The cache is injected as the first middleware so every stage
    can short-circuit if a fresh cached result exists.
    """

    def __init__(
        self,
        pipeline: Pipeline,
        config: Optional[CacheConfig] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(pipeline, **kwargs)
        self._cache = StageCache(config)
        self._middleware.add(cache_middleware(self._cache))

    @property
    def cache(self) -> StageCache:
        return self._cache

    async def run(self, data: Any, ctx: Optional[PipelineContext] = None) -> Any:
        if ctx is None:
            ctx = PipelineContext()
        return await super().run(data, ctx)
