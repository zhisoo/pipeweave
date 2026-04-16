"""Fallback middleware: substitute a default value when a stage fails."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from pipeweave.context import PipelineContext, StageResult
from pipeweave.errors import StageError


@dataclass
class FallbackConfig:
    """Configuration for stage-level fallback behaviour."""

    # Static default value to return on failure.
    default_value: Any = None

    # Optional async callable ``(error, ctx) -> Any`` for dynamic fallbacks.
    handler: Optional[Callable] = None

    # If True the fallback result is marked as successful in the context.
    mark_success: bool = True

    def __post_init__(self) -> None:
        if self.handler is not None and not callable(self.handler):
            raise TypeError("handler must be callable")


def make_fallback_middleware(config: FallbackConfig):
    """Return a middleware that catches stage errors and applies *config*."""

    async def fallback_middleware(stage_name: str, call_next, ctx: PipelineContext):
        result: StageResult = await call_next(stage_name, ctx)

        if result.success:
            return result

        # Determine fallback value.
        if config.handler is not None:
            try:
                value = await config.handler(result.error, ctx)
            except Exception as exc:  # noqa: BLE001
                # Handler itself failed – return original error result.
                return result
        else:
            value = config.default_value

        return StageResult(
            stage_name=stage_name,
            success=config.mark_success,
            value=value,
            error=None if config.mark_success else result.error,
        )

    return fallback_middleware
