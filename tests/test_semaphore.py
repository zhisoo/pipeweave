"""Tests for pipeweave.semaphore (SemaphoreConfig + SemaphorePool)."""

import asyncio
import pytest

from pipeweave.semaphore import SemaphoreConfig, SemaphorePool


# ---------------------------------------------------------------------------
# SemaphoreConfig validation
# ---------------------------------------------------------------------------

def test_default_config_accepted():
    cfg = SemaphoreConfig()
    assert cfg.default_limit == 1
    assert cfg.limits == {}


def test_custom_limits_accepted():
    cfg = SemaphoreConfig(default_limit=3, limits={"heavy": 2, "light": 5})
    assert cfg.limits["heavy"] == 2


def test_zero_default_limit_raises():
    with pytest.raises(ValueError, match="default_limit"):
        SemaphoreConfig(default_limit=0)


def test_negative_default_limit_raises():
    with pytest.raises(ValueError, match="default_limit"):
        SemaphoreConfig(default_limit=-1)


def test_zero_named_limit_raises():
    with pytest.raises(ValueError, match="'stage_a'"):
        SemaphoreConfig(limits={"stage_a": 0})


def test_negative_named_limit_raises():
    with pytest.raises(ValueError, match="'stage_b'"):
        SemaphoreConfig(limits={"stage_b": -3})


# ---------------------------------------------------------------------------
# SemaphorePool behaviour
# ---------------------------------------------------------------------------

def test_pool_creates_semaphore_on_first_access():
    pool = SemaphorePool(SemaphoreConfig(default_limit=2))
    assert pool.names() == []
    pool.available("x")
    assert "x" in pool.names()


def test_available_uses_named_limit():
    pool = SemaphorePool(SemaphoreConfig(default_limit=1, limits={"fast": 4}))
    assert pool.available("fast") == 4


def test_available_falls_back_to_default():
    pool = SemaphorePool(SemaphoreConfig(default_limit=3))
    assert pool.available("unknown") == 3


@pytest.mark.asyncio
async def test_acquire_decrements_available():
    pool = SemaphorePool(SemaphoreConfig(default_limit=2))
    async with pool.acquire("s"):
        assert pool.available("s") == 1
    assert pool.available("s") == 2


@pytest.mark.asyncio
async def test_acquire_blocks_when_limit_reached():
    pool = SemaphorePool(SemaphoreConfig(default_limit=1))
    results = []

    async def task(label: str):
        async with pool.acquire("shared"):
            results.append(f"start:{label}")
            await asyncio.sleep(0.01)
            results.append(f"end:{label}")

    await asyncio.gather(task("A"), task("B"))
    # With limit=1 the tasks must not interleave
    assert results.index("end:A") < results.index("start:B") or \
           results.index("end:B") < results.index("start:A")
