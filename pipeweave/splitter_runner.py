"""Convenience runner that wires a SplitterConfig into a Pipeline."""
from __future__ import annotations

from .pipeline import Pipeline
from .runner import PipelineRunner
from .splitter import SplitterConfig, make_splitter_middleware


class SplitterPipelineRunner:
    """PipelineRunner with an attached splitter middleware."""

    def __init__(self, pipeline: Pipeline, config: SplitterConfig) -> None:
        mw = make_splitter_middleware(config)
        self._runner = PipelineRunner(pipeline, middleware=[mw])
        self._config = config

    @property
    def splitter_config(self) -> SplitterConfig:
        return self._config

    async def run(self, data):
        return await self._runner.run(data)
