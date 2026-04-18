"""Tests for pipeweave.scatter_gather."""
from __future__ import annotations

import asyncio
import pytest

from pipeweave.scatter_gather import (
    ScatterGatherConfig,
    ScatterGatherError,
    scatter_gather,
)


def test_empty_handlers_raises():
    with pytest.raises(ValueError, match="handlers must not be empty"):
        ScatterGatherConfig(handlers=[])


def test_non_callable_handler_raises():
    with pytest.raises(TypeError):
        ScatterGatherConfig(handlers=["not_a_callable"])


def test_zero_timeout_raises():
    async def h(x): return x
    with pytest.raises(ValueError, match="timeout must be positive"):
        ScatterGatherConfig(handlers=[h], timeout=0)


def test_negative_timeout_raises():
    async def h(x): return x
    with pytest.raises(ValueError, match="timeout must be positive"):
        ScatterGatherConfig(handlers=[h], timeout=-1.0)


def test_valid_config_accepted():
    async def h(x): return x
    cfg = ScatterGatherConfig(handlers=[h, h], timeout=5.0, return_exceptions=True)
    assert len(cfg.handlers) == 2
    assert cfg.timeout == 5.0


@pytest.mark.asyncio
async def test_all_results_returned():
    async def double(x): return x * 2
    async def triple(x): return x * 3

    cfg = ScatterGatherConfig(handlers=[double, triple])
    results = await scatter_gather(cfg, 4)
    assert sorted(results) == [8, 12]


@pytest.mark.asyncio
async def test_exception_propagates_by_default():
    async def good(x): return x
    async def bad(x): raise ValueError("boom")

    cfg = ScatterGatherConfig(handlers=[good, bad])
    with pytest.raises((ScatterGatherError, ValueError)):
        await scatter_gather(cfg, 1)


@pytest.mark.asyncio
async def test_return_exceptions_includes_errors():
    async def good(x): return x + 1
    async def bad(x): raise RuntimeError("oops")

    cfg = ScatterGatherConfig(handlers=[good, bad], return_exceptions=True)
    results = await scatter_gather(cfg, 10)
    assert len(results) == 2
    values = [r for r in results if not isinstance(r, BaseException)]
    errors = [r for r in results if isinstance(r, BaseException)]
    assert values == [11]
    assert len(errors) == 1


@pytest.mark.asyncio
async def test_timeout_raises_scatter_gather_error():
    async def slow(x):
        await asyncio.sleep(5)
        return x

    cfg = ScatterGatherConfig(handlers=[slow], timeout=0.05)
    with pytest.raises(ScatterGatherError, match="timed out"):
        await scatter_gather(cfg, 42)
