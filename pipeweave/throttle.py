"""Rate limiting and concurrency throttling for pipeline stages."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ThrottleConfig:
    """Configuration for rate limiting a pipeline stage."""

    max_calls: int = 0          # 0 = unlimited
    period: float = 1.0         # seconds; used when max_calls > 0
    max_concurrency: int = 0    # 0 = unlimited

    def __post_init__(self) -> None:
        if self.max_calls < 0:
            raise ValueError("max_calls must be >= 0")
        if self.period <= 0:
            raise ValueError("period must be > 0")
        if self.max_concurrency < 0:
            raise ValueError("max_concurrency must be >= 0")


class Throttle:
    """Async throttle that enforces rate limits and concurrency caps."""

    def __init__(self, config: ThrottleConfig) -> None:
        self._config = config
        self._semaphore: Optional[asyncio.Semaphore] = (
            asyncio.Semaphore(config.max_concurrency)
            if config.max_concurrency > 0
            else None
        )
        self._call_times: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Block until the throttle allows the next call."""
        if self._config.max_calls > 0:
            await self._rate_limit()
        if self._semaphore is not None:
            await self._semaphore.acquire()

    def release(self) -> None:
        """Release the concurrency slot (no-op if concurrency is unlimited)."""
        if self._semaphore is not None:
            self._semaphore.release()

    async def _rate_limit(self) -> None:
        async with self._lock:
            now = time.monotonic()
            window_start = now - self._config.period
            self._call_times = [
                t for t in self._call_times if t > window_start
            ]
            if len(self._call_times) >= self._config.max_calls:
                oldest = self._call_times[0]
                sleep_for = self._config.period - (now - oldest)
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
            self._call_times.append(time.monotonic())

    async def __aenter__(self) -> "Throttle":
        await self.acquire()
        return self

    async def __aexit__(self, *_: object) -> None:
        self.release()
