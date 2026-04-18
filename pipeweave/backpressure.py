"""Backpressure middleware: pause upstream when a stage queue is full."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass
class BackpressureConfig:
    """Configuration for backpressure control."""

    high_watermark: int = 100  # pause when pending >= this
    low_watermark: int = 50   # resume when pending drops to this
    timeout: float | None = None  # seconds to wait before giving up

    def __post_init__(self) -> None:
        if self.high_watermark <= 0:
            raise ValueError("high_watermark must be positive")
        if self.low_watermark <= 0:
            raise ValueError("low_watermark must be positive")
        if self.low_watermark >= self.high_watermark:
            raise ValueError("low_watermark must be less than high_watermark")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be positive if set")


class BackpressureController:
    """Tracks pending item count and gates producers via an asyncio.Event."""

    def __init__(self, config: BackpressureConfig) -> None:
        self._cfg = config
        self._pending: int = 0
        self._open = asyncio.Event()
        self._open.set()  # starts open (not paused)

    @property
    def pending(self) -> int:
        return self._pending

    def increment(self) -> None:
        self._pending += 1
        if self._pending >= self._cfg.high_watermark:
            self._open.clear()  # pause producers

    def decrement(self) -> None:
        if self._pending > 0:
            self._pending -= 1
        if self._pending <= self._cfg.low_watermark:
            self._open.set()  # resume producers

    async def wait_for_capacity(self) -> bool:
        """Block until below high watermark. Returns False on timeout."""
        try:
            await asyncio.wait_for(
                self._open.wait(),
                timeout=self._cfg.timeout,
            )
            return True
        except asyncio.TimeoutError:
            return False


def make_backpressure_middleware(controller: BackpressureController):
    """Middleware that decrements the controller counter after each stage."""
    async def _middleware(ctx, result, call_next):
        controller.increment()
        try:
            return await call_next(ctx, result)
        finally:
            controller.decrement()
    return _middleware
