"""Convenience runner that wires an Aggregator into a Pipeline automatically."""
from __future__ import annotations

from typing import Any

from .aggregator import Aggregator, AggregatorConfig, make_aggregator_middleware
from .runner import PipelineRunner
from .pipeline import Pipeline


class AggregatorPipelineRunner(PipelineRunner):
    """PipelineRunner with built-in aggregation of stage results."""

    def __init__(self, pipeline: Pipeline, config: AggregatorConfig) -> None:
        self._agg_config = config
        self._aggregator = Aggregator(config)
        mw = make_aggregator_middleware(config, self._aggregator)
        super().__init__(pipeline, extra_middleware=[mw])

    @property
    def aggregator(self) -> Aggregator:
        return self._aggregator

    @property
    def aggregated_value(self) -> Any:
        return self._aggregator.value

    def reset_aggregator(self) -> None:
        self._aggregator.reset()
