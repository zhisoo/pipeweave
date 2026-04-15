"""Tests for pipeweave.rate_limiter."""

from __future__ import annotations

import asyncio
import time

import pytest

from pipeweave.context import PipelineContext, StageResult
from pipeweave.rate_limiter import (
    RateLimiterConfig,
    RateLimiter,
    rate_limiter_middleware,
)


# ---------------------------------------------------------------------------
# RateLimiterConfig validation
# ---------------------------------------------------------------------------

def test_valid_config_accepted():
    cfg = RateLimiterConfig(rate=10.0, capacity=5.0)
    assert cfg.rate == 10.0
    assert cfg.capacity == 5.0


def test_zero_rate_raises():
    with pytest.raises(ValueError, match="rate must be positive"):
        RateLimiterConfig(rate=0, capacity=5)


def test_negative_rate_raises():
    with pytest.raises(ValueError, match="rate must be positive"):
        RateLimiterConfig(rate=-1, capacity=5)


def test_zero_capacity_raises():
    with pytest.raises(ValueError, match="capacity must be positive"):
        RateLimiterConfig(rate=5, capacity=0)


def test_negative_capacity_raises():
    with pytest.raises(ValueError, match="capacity must be positive"):
        RateLimiterConfig(rate=5, capacity=-3)


# ---------------------------------------------------------------------------
# RateLimiter token-bucket behaviour
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_acquire_within_capacity_is_immediate():
    limiter = RateLimiter(RateLimiterConfig(rate=100.0, capacity=10.0))
    start = time.monotonic()
    for _ in range(5):
        await limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 0.1, "5 calls within capacity should complete quickly"


@pytest.mark.asyncio
async def test_acquire_beyond_capacity_waits():
    # 1 token/s, capacity 1 — second call must wait ~1 s
    limiter = RateLimiter(RateLimiterConfig(rate=1.0, capacity=1.0))
    await limiter.acquire()  # drains the bucket
    start = time.monotonic()
    await limiter.acquire()  # must wait for refill
    elapsed = time.monotonic() - start
    assert elapsed >= 0.9, "should have waited approximately 1 second"


# ---------------------------------------------------------------------------
# rate_limiter_middleware integration
# ---------------------------------------------------------------------------

def _make_ctx() -> PipelineContext:
    return PipelineContext()


def _ok_result() -> StageResult:
    return StageResult(stage_name="s", success=True, output=42)


@pytest.mark.asyncio
async def test_middleware_passes_result_through():
    limiter = RateLimiter(RateLimiterConfig(rate=100.0, capacity=10.0))
    factory = rate_limiter_middleware(limiter)

    async def terminal(ctx: PipelineContext, r: StageResult) -> StageResult:
        return r

    handler = factory(terminal)
    result = await handler(_make_ctx(), _ok_result())
    assert result.success is True
    assert result.output == 42


@pytest.mark.asyncio
async def test_middleware_calls_next_handler():
    limiter = RateLimiter(RateLimiterConfig(rate=100.0, capacity=10.0))
    factory = rate_limiter_middleware(limiter)
    calls: list[int] = []

    async def terminal(ctx: PipelineContext, r: StageResult) -> StageResult:
        calls.append(1)
        return r

    handler = factory(terminal)
    await handler(_make_ctx(), _ok_result())
    await handler(_make_ctx(), _ok_result())
    assert len(calls) == 2
