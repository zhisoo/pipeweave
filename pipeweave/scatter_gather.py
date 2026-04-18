"""Scatter-gather: fan out input to multiple async handlers and collect results."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, List, Optional

from pipeweave.errors import PipeweaveError


class ScatterGatherError(PipeweaveError):
    """Raised when scatter-gather encounters a fatal error."""


@dataclass
class ScatterGatherConfig:
    handlers: List[Callable[..., Coroutine[Any, Any, Any]]]
    timeout: Optional[float] = None
    return_exceptions: bool = False

    def __post_init__(self) -> None:
        if not self.handlers:
            raise ValueError("handlers must not be empty")
        for h in self.handlers:
            if not callable(h):
                raise TypeError(f"handler {h!r} is not callable")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be positive")


async def scatter_gather(
    config: ScatterGatherConfig,
    input_value: Any,
) -> List[Any]:
    """Run all handlers concurrently with *input_value* and return their results.

    If *return_exceptions* is False (default) the first exception propagates.
    If *return_exceptions* is True, exceptions are included in the result list.
    """
    coros = [h(input_value) for h in config.handlers]

    async def _run_all() -> List[Any]:
        return await asyncio.gather(*coros, return_exceptions=config.return_exceptions)

    if config.timeout is not None:
        try:
            results = await asyncio.wait_for(_run_all(), timeout=config.timeout)
        except asyncio.TimeoutError as exc:
            raise ScatterGatherError(
                f"scatter_gather timed out after {config.timeout}s"
            ) from exc
    else:
        results = await _run_all()

    if not config.return_exceptions:
        for r in results:
            if isinstance(r, BaseException):
                raise ScatterGatherError("scatter_gather handler raised") from r

    return list(results)
