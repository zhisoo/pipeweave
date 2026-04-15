"""Tests for pipeweave.throttle."""

from __future__ import annotations

import asyncio
import time

import pytest

from pipeweave.throttle import Throttle, ThrottleConfig


# ---------------------------------------------------------------------------
# ThrottleConfig validation
# ---------------------------------------------------------------------------

def test_default_config_is_unlimited():
    cfg = ThrottleConfig()
    assert cfg.max_calls == 0
    assert cfg.max_concurrency == 0
    assert cfg.period == 1.0


def test_negative_max_calls_raises():
    with pytest.raises(ValueError, match="max_calls"):
        ThrottleConfig(max_calls=-1)


def test_zero_period_raises():
    with pytest.raises(ValueError, match="period"):
        ThrottleConfig(period=0)


def test_negative_concurrency_raises():
    with pytest.raises(ValueError, match="max_concurrency"):
        ThrottleConfig(max_concurrency=-1)


# ---------------------------------------------------------------------------
# Concurrency cap
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrency_limits_parallel_tasks():
    cfg = ThrottleConfig(max_concurrency=2)
    throttle = Throttle(cfg)
    active: list[int] = []
    peak: list[int] = []

    async def task():
        async with throttle:
            active.append(1)
            peak.append(len(active))
            await asyncio.sleep(0.02)
            active.pop()

    await asyncio.gather(*[task() for _ in range(5)])
    assert max(peak) <= 2


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rate_limit_enforces_max_calls_per_period():
    cfg = ThrottleConfig(max_calls=3, period=0.3)
    throttle = Throttle(cfg)
    timestamps: list[float] = []

    async def task():
        await throttle.acquire()
        timestamps.append(time.monotonic())
        throttle.release()

    await asyncio.gather(*[task() for _ in range(6)])

    # The 4th call must start at least ~0.3 s after the 1st
    assert timestamps[3] - timestamps[0] >= 0.25


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_context_manager_releases_on_exit():
    cfg = ThrottleConfig(max_concurrency=1)
    throttle = Throttle(cfg)

    async with throttle:
        pass  # should not deadlock

    # Semaphore value should be back to 1
    assert throttle._semaphore is not None
    assert throttle._semaphore._value == 1  # type: ignore[attr-defined]
