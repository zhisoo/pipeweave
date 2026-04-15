"""Custom exception hierarchy for pipeweave."""

from typing import Any, Optional


class PipeweaveError(Exception):
    """Base class for all pipeweave exceptions."""


class StageError(PipeweaveError):
    """Raised when a pipeline stage fails after all retry attempts."""

    def __init__(
        self,
        stage_name: str,
        cause: Exception,
        input_data: Optional[Any] = None,
    ) -> None:
        self.stage_name = stage_name
        self.cause = cause
        self.input_data = input_data
        super().__init__(
            f"Stage '{stage_name}' failed: {cause!r}"
        )


class PipelineAbortedError(PipeweaveError):
    """Raised when a pipeline is aborted due to an unrecoverable stage error."""

    def __init__(self, stage_error: StageError) -> None:
        self.stage_error = stage_error
        super().__init__(
            f"Pipeline aborted at stage '{stage_error.stage_name}': {stage_error.cause!r}"
        )


class ConfigurationError(PipeweaveError):
    """Raised when a pipeline or stage is misconfigured."""
