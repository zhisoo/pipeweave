"""Conditional pipeline splitter – routes results to different handlers based on a predicate."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, List, Optional

from .errors import PipeweaveError
from .context import PipelineContext, StageResult


class SplitterError(PipeweaveError):
    """Raised for splitter configuration or runtime errors."""


@dataclass
class SplitterRoute:
    predicate: Callable[[StageResult], bool]
    handler: Callable[[Any, PipelineContext], Awaitable[Any]]
    name: str = ""

    def __post_init__(self) -> None:
        if not callable(self.predicate):
            raise SplitterError("predicate must be callable")
        if not callable(self.handler):
            raise SplitterError("handler must be callable")


@dataclass
class SplitterConfig:
    routes: List[SplitterRoute] = field(default_factory=list)
    default_handler: Optional[Callable[[Any, PipelineContext], Awaitable[Any]]] = None
    stop_on_first_match: bool = True

    def __post_init__(self) -> None:
        if not self.routes:
            raise SplitterError("at least one route is required")
        if self.default_handler is not None and not callable(self.default_handler):
            raise SplitterError("default_handler must be callable")


def make_splitter_middleware(config: SplitterConfig):
    """Return middleware that routes the current result through matching handlers."""

    async def middleware(stage_name: str, data: Any, ctx: PipelineContext, next_):
        result: StageResult = await next_(stage_name, data, ctx)

        if not result.success:
            return result

        matched = False
        value = result.value

        for route in config.routes:
            if route.predicate(result):
                matched = True
                value = await route.handler(value, ctx)
                if config.stop_on_first_match:
                    break

        if not matched and config.default_handler is not None:
            value = await config.default_handler(value, ctx)

        return StageResult(stage=result.stage, success=True, value=value, error=None)

    return middleware
