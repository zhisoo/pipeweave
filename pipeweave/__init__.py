"""pipeweave — lightweight async data transformation pipelines."""

from pipeweave.context import PipelineContext, StageResult
from pipeweave.errors import PipelineAbortedError, PipeweaveError, StageError
from pipeweave.pipeline import Pipeline, StageConfig, pipe
from pipeweave.retry import RetryPolicy, compute_delay, is_retryable

__all__ = [
    # pipeline
    "Pipeline",
    "StageConfig",
    "pipe",
    # context
    "PipelineContext",
    "StageResult",
    # retry
    "RetryPolicy",
    "compute_delay",
    "is_retryable",
    # errors
    "PipeweaveError",
    "StageError",
    "PipelineAbortedError",
]

__version__ = "0.1.0"
