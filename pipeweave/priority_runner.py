"""PipelineRunner variant that executes stages via a StagePriorityQueue."""
from __future__ import annotations
from typing import Any
from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner
from pipeweave.context import PipelineContext
from pipeweave.priority_queue import StagePriorityQueue, PriorityQueueConfig


class PriorityPipelineRunner(PipelineRunner):
    """Runs pipeline stages ordered by an explicit priority mapping.

    Parameters
    ----------
    pipeline:
        The pipeline to run.
    priorities:
        Mapping of stage name -> integer priority (lower = runs first).
        Stages not listed default to priority 0.
    config:
        Optional PriorityQueueConfig.
    """

    def __init__(
        self,
        pipeline: Pipeline,
        priorities: dict[str, int] | None = None,
        config: PriorityQueueConfig | None = None,
    ) -> None:
        super().__init__(pipeline)
        self._priorities = priorities or {}
        self._config = config or PriorityQueueConfig()

    async def run(self, data: Any, ctx: PipelineContext | None = None) -> Any:
        if ctx is None:
            ctx = PipelineContext()

        pq = StagePriorityQueue(self._config)

        for stage in self._pipeline.stages:
            priority = self._priorities.get(stage.name, 0)
            handler = self._make_handler(stage)
            await pq.put(priority, lambda c, d, h=handler: h(c, d))

        results = await pq.run_all(ctx, data)
        return results[-1].value if results else data

    @property
    def priorities(self) -> dict[str, int]:
        return dict(self._priorities)
