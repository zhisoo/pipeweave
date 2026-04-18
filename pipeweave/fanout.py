"""Fan-out middleware: broadcast a stage's output to multiple downstream handlers."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Iterable

from pipeweave.context import PipelineContext, StageResult
from pipeweave.errors import PipeweaveError


class FanoutError(PipeweaveError):
    """Raised when fan-out configuration is invalid."""


@dataclass
class FanoutConfig:
    """Configuration for fan-out behaviour."""

    handlers: list[Callable] = field(default_factory=list)
    fail_fast: bool = True  # abort remaining handlers on first error

    def __post_init__(self) -> None:
        if not self.handlers:
            raise FanoutError("FanoutConfig requires at least one handler.")
        for h in self.handlers:
            if not callable(h):
                raise FanoutError(f"All handlers must be callable, got {h!r}.")


async def _invoke(handler: Callable, value: object) -> object:
    result = handler(value)
    if asyncio.iscoroutine(result):
        return await result
    return result


def make_fanout_middleware(config: FanoutConfig):
    """Return middleware that fans out successful results to every handler."""

    async def middleware(
        stage_name: str,
        call_next: Callable,
        data: object,
        ctx: PipelineContext,
    ) -> StageResult:
        result: StageResult = await call_next(data, ctx)

        if not result.success:
            return result

        errors: list[Exception] = []
        for handler in config.handlers:
            try:
                await _invoke(handler, result.value)
            except Exception as exc:  # noqa: BLE001
                if config.fail_fast:
                    raise
                errors.append(exc)

        return result

    return middleware
