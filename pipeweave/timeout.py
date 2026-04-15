"""Per-stage and pipeline-level timeout support."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional

from pipeweave.errors import StageError


@dataclass
class TimeoutConfig:
    """Timeout settings for a stage or pipeline."""

    stage_timeout: Optional[float] = None   # seconds; None means no limit
    pipeline_timeout: Optional[float] = None

    def __post_init__(self) -> None:
        if self.stage_timeout is not None and self.stage_timeout <= 0:
            raise ValueError("stage_timeout must be a positive number")
        if self.pipeline_timeout is not None and self.pipeline_timeout <= 0:
            raise ValueError("pipeline_timeout must be a positive number")


async def run_with_timeout(
    coro: Coroutine[Any, Any, Any],
    timeout: Optional[float],
    stage_name: str = "<unknown>",
) -> Any:
    """Await *coro*, raising :class:`StageError` if *timeout* seconds elapse.

    Parameters
    ----------
    coro:
        The coroutine to execute.
    timeout:
        Maximum allowed wall-clock seconds.  ``None`` disables the guard.
    stage_name:
        Used to build the error message on failure.
    """
    if timeout is None:
        return await coro

    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError as exc:
        raise StageError(
            stage_name=stage_name,
            message=f"Stage '{stage_name}' timed out after {timeout}s",
            original=exc,
        ) from exc


def timeout_middleware(
    stage_timeout: Optional[float] = None,
) -> Callable[..., Any]:
    """Return a middleware that enforces a per-stage *stage_timeout*.

    Usage::

        chain.add(timeout_middleware(stage_timeout=2.0))
    """
    async def _middleware(stage_name: str, call_next: Callable[..., Any], ctx: Any) -> Any:
        coro = call_next(ctx)
        return await run_with_timeout(coro, stage_timeout, stage_name=stage_name)

    _middleware.__name__ = "timeout_middleware"
    return _middleware
