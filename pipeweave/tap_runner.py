"""Convenience runner that wires TapConfig into a Pipeline automatically."""
from __future__ import annotations

from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner
from pipeweave.middleware import MiddlewareChain
from pipeweave.tap import TapConfig, make_tap_middleware


class TapPipelineRunner(PipelineRunner):
    """PipelineRunner with a tap middleware pre-attached."""

    def __init__(self, pipeline: Pipeline, tap_config: TapConfig) -> None:
        super().__init__(pipeline)
        self._tap_config = tap_config
        chain: MiddlewareChain = self._middleware  # type: ignore[attr-defined]
        chain.add(make_tap_middleware(tap_config))

    @property
    def tap_config(self) -> TapConfig:
        return self._tap_config
