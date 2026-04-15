"""Token-bucket rate limiter middleware for pipeweave pipelines."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, Awaitable

from pipeweave.context import PipelineContext, StageResult


@dataclass
class RateLimiterConfig:
    """Configuration for the token-bucket rate limiter."""

    rate: float  # tokens replenished per second
    capacity: float  # maximum token bucket size

    def __post_init__(self) -> None:
        if self.rate <= 0:
            raise ValueError("rate must be positive")
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")


class RateLimiter:
    """Async token-bucket rate limiter."""

    def __init__(self, config: RateLimiterConfig) -> None:
        self._config = config
        self._tokens: float = config.capacity
        self._last_refill: float = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            self._config.capacity,
            self._tokens + elapsed * self._config.rate,
        )
        self._last_refill = now

    async def acquire(self, tokens: float = 1.0) -> None:
        """Block until *tokens* are available, then consume them."""
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                wait_for = (tokens - self._tokens) / self._config.rate
            await asyncio.sleep(wait_for)


MiddlewareCallable = Callable[
    [PipelineContext, StageResult], Awaitable[StageResult]
]


def rate_limiter_middleware(
    limiter: RateLimiter,
    tokens_per_call: float = 1.0,
) -> Callable[[MiddlewareCallable], MiddlewareCallable]:
    """Return a middleware factory that gates each stage call through *limiter*."""

    def factory(next_handler: MiddlewareCallable) -> MiddlewareCallable:
        async def handler(
            ctx: PipelineContext, result: StageResult
        ) -> StageResult:
            await limiter.acquire(tokens_per_call)
            return await next_handler(ctx, result)

        return handler

    return factory
