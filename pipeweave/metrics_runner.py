"""A PipelineRunner subclass that wires metrics collection automatically."""
from __future__ import annotations

from pipeweave.context import PipelineContext
from pipeweave.metrics import PipelineMetrics
from pipeweave.metrics_middleware import attach_pipeline_metrics, metrics_middleware
from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner


class MetricsPipelineRunner(PipelineRunner):
    """PipelineRunner that automatically collects execution metrics.

    After ``run()`` completes the ``PipelineMetrics`` object is available
    via ``last_metrics`` and also stored inside the ``PipelineContext``.
    """

    def __init__(self, pipeline: Pipeline) -> None:
        super().__init__(pipeline)
        self._last_metrics: PipelineMetrics | None = None

    @property
    def last_metrics(self) -> PipelineMetrics | None:
        return self._last_metrics

    async def run(self, data: object, ctx: PipelineContext | None = None) -> object:
        if ctx is None:
            ctx = PipelineContext()

        pm = attach_pipeline_metrics(ctx, self._pipeline.name)

        # Inject metrics_middleware for every stage.
        for stage in self._pipeline.stages:
            self._middleware.add(
                lambda h, s=stage.name: metrics_middleware(h, s)
            )

        result = await super().run(data, ctx)

        pm.finish()
        self._last_metrics = pm
        return result
