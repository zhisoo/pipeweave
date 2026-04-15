"""Middleware support for pipeline stages."""
from __future__ import annotations

import time
import logging
from typing import Callable, Awaitable, Any
from functools import wraps

from pipeweave.context import PipelineContext, StageResult

Logger = logging.getLogger(__name__)

MiddlewareFn = Callable[[str, Any, PipelineContext], Awaitable[StageResult]]


class MiddlewareChain:
    """Composes a list of middleware functions around a stage handler."""

    def __init__(self) -> None:
        self._middlewares: list[Callable] = []

    def add(self, middleware: Callable) -> "MiddlewareChain":
        """Register a middleware. Returns self for chaining."""
        self._middlewares.append(middleware)
        return self

    def wrap(self, handler: MiddlewareFn) -> MiddlewareFn:
        """Wrap *handler* with all registered middlewares (outermost first)."""
        wrapped = handler
        for mw in reversed(self._middlewares):
            wrapped = mw(wrapped)
        return wrapped


def logging_middleware(next_handler: MiddlewareFn) -> MiddlewareFn:
    """Log entry/exit and duration for every stage invocation."""

    @wraps(next_handler)
    async def _handler(stage_name: str, data: Any, ctx: PipelineContext) -> StageResult:
        Logger.debug("[%s] starting", stage_name)
        t0 = time.perf_counter()
        result = await next_handler(stage_name, data, ctx)
        elapsed = time.perf_counter() - t0
        status = "ok" if result.success else "error"
        Logger.debug("[%s] finished status=%s duration=%.4fs", stage_name, status, elapsed)
        return result

    return _handler


def timing_middleware(next_handler: MiddlewareFn) -> MiddlewareFn:
    """Record wall-clock duration of each stage in the context metadata."""

    @wraps(next_handler)
    async def _handler(stage_name: str, data: Any, ctx: PipelineContext) -> StageResult:
        t0 = time.perf_counter()
        result = await next_handler(stage_name, data, ctx)
        elapsed = time.perf_counter() - t0
        timings: dict = ctx.get("timings", {})
        timings[stage_name] = round(elapsed, 6)
        ctx.set("timings", timings)
        return result

    return _handler


def validation_middleware(validator: Callable[[Any], None]) -> Callable:
    """Return a middleware that validates input *data* before passing it on."""

    def _middleware(next_handler: MiddlewareFn) -> MiddlewareFn:
        @wraps(next_handler)
        async def _handler(stage_name: str, data: Any, ctx: PipelineContext) -> StageResult:
            validator(data)
            return await next_handler(stage_name, data, ctx)

        return _handler

    return _middleware
