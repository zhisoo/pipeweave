"""PipelineRunner variant that enforces a pipeline-level timeout."""
from __future__ import annotations

import asyncio
from typing import Any

from pipeweave.errors import PipelineAbortedError
from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner
from pipeweave.timeout import TimeoutConfig


class TimeoutPipelineRunner(PipelineRunner):
    """A :class:`PipelineRunner` that aborts the whole pipeline if it exceeds
    the configured :attr:`TimeoutConfig.pipeline_timeout`.

    Parameters
    ----------
    pipeline:
        The pipeline to execute.
    config:
        Timeout settings.  If *pipeline_timeout* is ``None`` the runner
        behaves identically to the base :class:`PipelineRunner`.
    """

    def __init__(self, pipeline: Pipeline, config: TimeoutConfig | None = None) -> None:
        super().__init__(pipeline)
        self._config: TimeoutConfig = config or TimeoutConfig()

    async def run(self, initial_input: Any = None) -> Any:  # type: ignore[override]
        """Execute the pipeline, raising :class:`PipelineAbortedError` on timeout."""
        timeout = self._config.pipeline_timeout
        if timeout is None:
            return await super().run(initial_input)

        try:
            return await asyncio.wait_for(super().run(initial_input), timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise PipelineAbortedError(
                f"Pipeline aborted: exceeded pipeline_timeout of {timeout}s"
            ) from exc
