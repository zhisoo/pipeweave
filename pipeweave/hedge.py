"""Hedged requests: launch a duplicate request after a delay if the first hasn't finished."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Awaitable, Any, Optional

from .errors import StageError


@dataclass
class HedgeConfig:
    """Configuration for hedged request middleware."""
    delay: float  # seconds before launching the hedge
    max_hedges: int = 1
    hedge_on: tuple[type[BaseException], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.delay <= 0:
            raise ValueError("delay must be positive")
        if self.max_hedges < 1:
            raise ValueError("max_hedges must be >= 1")


def make_hedge_middleware(config: HedgeConfig) -> Callable:
    """Return middleware that hedges slow stage calls."""

    def middleware(handler: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        async def wrapper(data: Any, ctx: Any) -> Any:
            loop = asyncio.get_event_loop()
            tasks: list[asyncio.Task] = []

            async def attempt() -> Any:
                return await handler(data, ctx)

            first = loop.create_task(attempt())
            tasks.append(first)

            hedges_launched = 0

            async def launch_hedges() -> None:
                nonlocal hedges_launched
                while hedges_launched < config.max_hedges:
                    await asyncio.sleep(config.delay)
                    if all(t.done() for t in tasks):
                        break
                    t = loop.create_task(attempt())
                    tasks.append(t)
                    hedges_launched += 1

            hedge_task = loop.create_task(launch_hedges())

            try:
                done, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                hedge_task.cancel()
                for t in pending:
                    t.cancel()

                result_task = next(iter(done))
                exc = result_task.exception()
                if exc is not None:
                    raise exc
                return result_task.result()
            finally:
                hedge_task.cancel()

        return wrapper

    return middleware
