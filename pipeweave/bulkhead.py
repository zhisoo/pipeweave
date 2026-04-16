"""Bulkhead pattern: limit concurrent executions per stage group."""
from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from typing import Optional

from pipeweave.errors import PipeweaveError


class BulkheadFullError(PipeweaveError):
    """Raised when the bulkhead has no available slots."""


@dataclass
class BulkheadConfig:
    max_concurrent: int = 10
    max_queue: int = 0  # 0 = no queuing, reject immediately

    def __post_init__(self) -> None:
        if self.max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        if self.max_queue < 0:
            raise ValueError("max_queue must be >= 0")


class Bulkhead:
    """Async bulkhead that limits concurrent stage executions."""

    def __init__(self, config: BulkheadConfig) -> None:
        self._config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrent)
        self._queued: int = 0

    @property
    def config(self) -> BulkheadConfig:
        return self._config

    @property
    def queued(self) -> int:
        return self._queued

    async def acquire(self) -> None:
        if self._semaphore.locked():
            if self._config.max_queue == 0 or self._queued >= self._config.max_queue:
                raise BulkheadFullError(
                    f"Bulkhead full: {self._config.max_concurrent} concurrent, "
                    f"{self._config.max_queue} queue slots"
                )
            self._queued += 1
            try:
                await self._semaphore.acquire()
            finally:
                self._queued -= 1
        else:
            await self._semaphore.acquire()

    def release(self) -> None:
        self._semaphore.release()


def make_bulkhead_middleware(bulkhead: Bulkhead):
    """Return middleware that enforces the given bulkhead."""
    from pipeweave.middleware import MiddlewareChain  # local to avoid circular

    async def bulkhead_middleware(ctx, result, call_next):
        await bulkhead.acquire()
        try:
            return await call_next(ctx, result)
        finally:
            bulkhead.release()

    return bulkhead_middleware
