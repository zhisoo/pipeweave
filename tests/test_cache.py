"""Tests for pipeweave.cache."""
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock

from pipeweave.cache import CacheConfig, StageCache, cache_middleware
from pipeweave.context import PipelineContext, StageResult


# ---------------------------------------------------------------------------
# CacheConfig validation
# ---------------------------------------------------------------------------

def test_default_config_accepted():
    cfg = CacheConfig()
    assert cfg.ttl is None
    assert cfg.max_size == 256


def test_zero_ttl_raises():
    with pytest.raises(ValueError):
        CacheConfig(ttl=0)


def test_negative_ttl_raises():
    with pytest.raises(ValueError):
        CacheConfig(ttl=-1)


def test_zero_max_size_raises():
    with pytest.raises(ValueError):
        CacheConfig(max_size=0)


# ---------------------------------------------------------------------------
# StageCache get/set
# ---------------------------------------------------------------------------

@pytest.fixture
def cache():
    return StageCache(CacheConfig(ttl=60))


def _ok(value):
    return StageResult(stage="s", ok=True, value=value)


def test_miss_returns_none(cache):
    result = asyncio.get_event_loop().run_until_complete(cache.get("s", {"x": 1}))
    assert result is None


def test_set_then_get_returns_result(cache):
    loop = asyncio.get_event_loop()
    r = _ok(42)
    loop.run_until_complete(cache.set("s", {"x": 1}, r))
    got = loop.run_until_complete(cache.get("s", {"x": 1}))
    assert got is r


def test_expired_entry_returns_none():
    cache = StageCache(CacheConfig(ttl=0.01))
    loop = asyncio.get_event_loop()
    r = _ok("hi")
    loop.run_until_complete(cache.set("s", "input", r))
    import time; time.sleep(0.05)
    got = loop.run_until_complete(cache.get("s", "input"))
    assert got is None


def test_max_size_evicts_oldest():
    cache = StageCache(CacheConfig(max_size=2))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(cache.set("s", "a", _ok(1)))
    loop.run_until_complete(cache.set("s", "b", _ok(2)))
    loop.run_until_complete(cache.set("s", "c", _ok(3)))
    assert cache.size() == 2


def test_invalidate_removes_stage_entries():
    cache = StageCache()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(cache.set("stage1", "x", _ok(1)))
    loop.run_until_complete(cache.set("stage1", "y", _ok(2)))
    loop.run_until_complete(cache.set("stage2", "x", _ok(3)))
    removed = cache.invalidate("stage1")
    assert removed == 2
    assert cache.size() == 1


# ---------------------------------------------------------------------------
# cache_middleware
# ---------------------------------------------------------------------------

def test_middleware_returns_cached_result_without_calling_next():
    cache = StageCache()
    loop = asyncio.get_event_loop()
    r = _ok("cached")
    loop.run_until_complete(cache.set("mystage", "data", r))

    ctx = PipelineContext(metadata={"current_stage": "mystage"})
    call_next = AsyncMock()
    mw = cache_middleware(cache)
    got = loop.run_until_complete(mw(ctx, "data", call_next))
    assert got is r
    call_next.assert_not_awaited()


def test_middleware_stores_ok_result():
    cache = StageCache()
    loop = asyncio.get_event_loop()
    r = _ok("fresh")
    ctx = PipelineContext(metadata={"current_stage": "s"})

    async def call_next(c, d):
        return r

    mw = cache_middleware(cache)
    loop.run_until_complete(mw(ctx, "inp", call_next))
    assert cache.size() == 1


def test_middleware_does_not_cache_error_result():
    cache = StageCache()
    loop = asyncio.get_event_loop()
    err = StageResult(stage="s", ok=False, error=Exception("boom"))
    ctx = PipelineContext(metadata={"current_stage": "s"})

    async def call_next(c, d):
        return err

    mw = cache_middleware(cache)
    loop.run_until_complete(mw(ctx, "inp", call_next))
    assert cache.size() == 0
