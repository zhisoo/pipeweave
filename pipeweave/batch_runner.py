"""Pipeline runner with batch processing support."""
from __future__ import annotations
from typing import Any, AsyncIterator, List

from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner
from pipeweave.batch import BatchConfig, iter_batches


class BatchPipelineRunner:
    """Runs a pipeline once per batch collected from an async iterator.

    Each batch (a list) is passed as the *input* to the pipeline.
    """

    def __init__(self, pipeline: Pipeline, config: BatchConfig | None = None) -> None:
        self._pipeline = pipeline
        self._config = config or BatchConfig()
        self._runner = PipelineRunner(pipeline)

    @property
    def batch_config(self) -> BatchConfig:
        return self._config

    async def run_stream(self, source: AsyncIterator[Any]) -> List[Any]:
        """Collect batches from *source* and run the pipeline on each.

        Args:
            source: An async iterator yielding individual items to be batched.

        Returns:
            A list of pipeline results, one entry per batch processed.
        """
        results: List[Any] = []
        async for batch in iter_batches(source, self._config):
            result = await self._runner.run(batch)
            results.append(result)
        return results

    async def run_stream_flat(self, source: AsyncIterator[Any]) -> List[Any]:
        """Like :meth:`run_stream`, but flattens pipeline results into a single list.

        Useful when each pipeline run returns a list of items and the caller
        wants a single flat list rather than a list-of-lists.

        Args:
            source: An async iterator yielding individual items to be batched.

        Returns:
            A flat list containing all items from every batch result.
        """
        results: List[Any] = []
        async for batch in iter_batches(source, self._config):
            result = await self._runner.run(batch)
            if isinstance(result, list):
                results.extend(result)
            else:
                results.append(result)
        return results
