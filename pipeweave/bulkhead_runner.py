"""PipelineRunner variant with bulkhead protection."""
from __future__ import annotations
from typing import Optional

from pipeweave.runner import PipelineRunner
from pipeweave.bulkhead import Bulkhead, BulkheadConfig, make_bulkhead_middleware
from pipeweave.pipeline import Pipeline


class BulkheadPipelineRunner(PipelineRunner):
    """Runs a pipeline with a bulkhead applied to every stage."""

    def __init__(
        self,
        pipeline: Pipeline,
        config: Optional[BulkheadConfig] = None,
    ) -> None:
        self._bulkhead = Bulkhead(config or BulkheadConfig())
        mw = make_bulkhead_middleware(self._bulkhead)
        # Prepend bulkhead middleware to any existing middleware
        pipeline = Pipeline(
            stages=pipeline.stages,
            middleware=[mw] + list(pipeline.middleware),
            retry_policy=pipeline.retry_policy,
        )
        super().__init__(pipeline)

    @property
    def bulkhead(self) -> Bulkhead:
        return self._bulkhead
