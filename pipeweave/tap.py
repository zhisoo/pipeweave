"""Side-effect tap middleware — observe stage results without altering them."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Awaitable, Union

from pipeweave.context import PipelineContext, StageResult

Handler = Callable[[str, StageResult, PipelineContext], Union[None, Awaitable[None]]]


@dataclass
class TapConfig:
    """Configuration for the tap middleware."""
    handlers: list[Handler] = field(default_factory=list)
    only_stages: list[str] = field(default_factory=list)   # empty = all stages
    only_failures: bool = False
    only_successes: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.handlers, list):
            raise TypeError("handlers must be a list")
        for h in self.handlers:
            if not callable(h):
                raise TypeError(f"each handler must be callable, got {h!r}")
        if self.only_failures and self.only_successes:
            raise ValueError("only_failures and only_successes cannot both be True")


async def _invoke(handler: Handler, stage: str, result: StageResult, ctx: PipelineContext) -> None:
    ret = handler(stage, result, ctx)
    if asyncio.iscoroutine(ret):
        await ret


def make_tap_middleware(config: TapConfig):
    """Return a middleware that calls tap handlers as a side-effect."""

    async def _middleware(stage_name: str, result: StageResult, ctx: PipelineContext, next_):
        # Apply stage filter
        if config.only_stages and stage_name not in config.only_stages:
            return await next_(stage_name, result, ctx)

        # Apply success/failure filter
        if config.only_failures and result.success:
            return await next_(stage_name, result, ctx)
        if config.only_successes and not result.success:
            return await next_(stage_name, result, ctx)

        for handler in config.handlers:
            try:
                await _invoke(handler, stage_name, result, ctx)
            except Exception:
                pass  # taps must never break the pipeline

        return await next_(stage_name, result, ctx)

    return _middleware
