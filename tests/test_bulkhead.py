"""Tests for pipeweave.bulkhead."""
from __future__ import annotations
import asyncio
import pytest

from pipeweave.bulkhead import Bulkhead, BulkheadConfig, BulkheadFullError, make_bulkhead_middleware
from pipeweave.context import PipelineContext, StageResult


def test_valid_config_accepted():
    cfg = BulkheadConfig(max_concurrent=5, max_queue=10)
    assert cfg.max_concurrent == 5
    assert cfg.max_queue == 10


def test_zero_max_concurrent_raises():
    with pytest.raises(ValueError, match="max_concurrent"):
        BulkheadConfig(max_concurrent=0)


def test_negative_max_queue_raises():
    with pytest.raises(ValueError, match="max_queue"):
        BulkheadConfig(max_queue=-1)


@pytest.mark.asyncio
async def test_acquire_and_release():
    bh = Bulkhead(BulkheadConfig(max_concurrent=2))
    await bh.acquire()
    await bh.acquire()
    bh.release()
    bh.release()


@pytest.mark.asyncio
async def test_bulkhead_rejects_when_full_no_queue():
    bh = Bulkhead(BulkheadConfig(max_concurrent=1, max_queue=0))
    await bh.acquire()
    with pytest.raises(BulkheadFullError):
        await bh.acquire()
    bh.release()


@pytest.mark.asyncio
async def test_bulkhead_queues_when_queue_allowed():
    bh = Bulkhead(BulkheadConfig(max_concurrent=1, max_queue=5))
    await bh.acquire()  # fills the slot

    results = []

    async def waiter():
        await bh.acquire()
        results.append("acquired")
        bh.release()

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0)  # let waiter queue
    assert bh.queued == 1
    bh.release()  # free slot
    await task
    assert results == ["acquired"]


@pytest.mark.asyncio
async def test_bulkhead_queue_full_raises():
    bh = Bulkhead(BulkheadConfig(max_concurrent=1, max_queue=0))
    await bh.acquire()
    with pytest.raises(BulkheadFullError):
        await bh.acquire()
    bh.release()


@pytest.mark.asyncio
async def test_middleware_releases_on_exception():
    bh = Bulkhead(BulkheadConfig(max_concurrent=2))
    mw = make_bulkhead_middleware(bh)
    ctx = PipelineContext()
    result = StageResult(stage="s", value=1, success=True)

    async def boom(c, r):
        raise RuntimeError("fail")

    with pytest.raises(RuntimeError):
        await mw(ctx, result, boom)

    # slot should be released; we can acquire again
    await bh.acquire()
    bh.release()
