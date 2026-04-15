"""Tests for the core Pipeline and StageConfig classes."""

import asyncio
import pytest

from pipeweave.pipeline import Pipeline, StageConfig


async def double(x: int) -> int:
    return x * 2


async def add_ten(x: int) -> int:
    return x + 10


async def always_fails(x):
    raise ValueError("intentional failure")


async def flaky_factory(fail_times: int):
    """Return a coroutine function that fails `fail_times` then succeeds."""
    calls = {"n": 0}

    async def flaky(x):
        calls["n"] += 1
        if calls["n"] <= fail_times:
            raise RuntimeError("transient error")
        return x + 1

    return flaky


@pytest.mark.asyncio
async def test_single_stage():
    pipeline = Pipeline()
    pipeline.pipe(StageConfig(name="double"), double)
    result = await pipeline.run(5)
    assert result == 10


@pytest.mark.asyncio
async def test_chained_stages():
    pipeline = Pipeline()
    pipeline.pipe(StageConfig(name="double"), double).pipe(
        StageConfig(name="add_ten"), add_ten
    )
    result = await pipeline.run(5)
    assert result == 20


@pytest.mark.asyncio
async def test_retry_success_after_transient_failure():
    flaky = await flaky_factory(fail_times=2)
    pipeline = Pipeline()
    pipeline.pipe(StageConfig(name="flaky", retries=3, retry_delay=0.0), flaky)
    result = await pipeline.run(0)
    assert result == 1


@pytest.mark.asyncio
async def test_retry_exhausted_raises():
    pipeline = Pipeline()
    pipeline.pipe(
        StageConfig(name="fails", retries=2, retry_delay=0.0), always_fails
    )
    with pytest.raises(RuntimeError, match="failed after 2 attempts"):
        await pipeline.run(42)


@pytest.mark.asyncio
async def test_stream_processing():
    pipeline = Pipeline()
    pipeline.pipe(StageConfig(name="double"), double)

    async def source():
        for i in range(1, 4):
            yield i

    results = [item async for item in pipeline.stream(source())]
    assert results == [2, 4, 6]


@pytest.mark.asyncio
async def test_timeout_raises():
    async def slow(x):
        await asyncio.sleep(5)
        return x

    pipeline = Pipeline()
    pipeline.pipe(StageConfig(name="slow", retries=1, timeout=0.05), slow)
    with pytest.raises(RuntimeError, match="failed after 1 attempts"):
        await pipeline.run(1)
