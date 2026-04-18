"""Tests for pipeweave.hedge."""
from __future__ import annotations

import asyncio
import pytest

from pipeweave.hedge import HedgeConfig, make_hedge_middleware


# --- config validation ---

def test_valid_config_accepted():
    cfg = HedgeConfig(delay=0.1)
    assert cfg.delay == 0.1
    assert cfg.max_hedges == 1


def test_zero_delay_raises():
    with pytest.raises(ValueError, match="delay must be positive"):
        HedgeConfig(delay=0)


def test_negative_delay_raises():
    with pytest.raises(ValueError, match="delay must be positive"):
        HedgeConfig(delay=-1.0)


def test_zero_max_hedges_raises():
    with pytest.raises(ValueError, match="max_hedges must be >= 1"):
        HedgeConfig(delay=0.1, max_hedges=0)


def test_multiple_hedges_accepted():
    cfg = HedgeConfig(delay=0.05, max_hedges=3)
    assert cfg.max_hedges == 3


# --- behaviour ---

@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


async def _fast_handler(data, ctx):
    return data * 2


async def _slow_then_fast(data, ctx, *, calls):
    calls.append(1)
    if len(calls) == 1:
        await asyncio.sleep(10)  # first call is slow
    return data + 1


def test_fast_handler_returns_immediately(event_loop):
    cfg = HedgeConfig(delay=0.05)
    mw = make_hedge_middleware(cfg)
    wrapped = mw(_fast_handler)
    result = event_loop.run_until_complete(wrapped(3, object()))
    assert result == 6


def test_hedge_fires_when_first_is_slow(event_loop):
    calls = []

    async def slow_first(data, ctx):
        calls.append("call")
        if len(calls) == 1:
            await asyncio.sleep(5)
        return data + 10

    cfg = HedgeConfig(delay=0.05)
    mw = make_hedge_middleware(cfg)
    wrapped = mw(slow_first)
    result = event_loop.run_until_complete(wrapped(1, object()))
    assert result == 11
    assert len(calls) >= 2


def test_exception_from_all_attempts_propagates(event_loop):
    async def always_fail(data, ctx):
        raise ValueError("boom")

    cfg = HedgeConfig(delay=0.01)
    mw = make_hedge_middleware(cfg)
    wrapped = mw(always_fail)
    with pytest.raises(ValueError, match="boom"):
        event_loop.run_until_complete(wrapped(1, object()))
