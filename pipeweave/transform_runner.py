"""PipelineRunner variant with built-in transform middleware."""
from __future__ import annotations

from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner
from pipeweave.transform import TransformConfig, make_transform_middleware


class TransformPipelineRunner(PipelineRunner):
    """PipelineRunner that applies field-level transforms to every stage result."""

    def __init__(self, pipeline: Pipeline, config: TransformConfig) -> None:
        super().__init__(pipeline)
        self._transform_config = config
        self._middleware.add(make_transform_middleware(config))

    @property
    def transform_config(self) -> TransformConfig:
        return self._transform_config
