"""Integration tests for CachedPipelineRunner."""
from __future__ import annotations

import asyncio
import pytest

from pipeweave.pipeline import Pipeline, StageConfig, pipe
from pipeweave.cache import CacheConfig
from pipeweave.cache_runner import CachedPipelineRunner


@pytest.fixture
def event_loop():
    """Provide a single event loop for all tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def build_pipeline(call_counter: list) -> Pipeline:
    async def double(x: int) -> int:
        call_counter.append(x)
        return x * 2

    p = Pipeline(name="test")
    p.add_stage(StageConfig(name="double", fn=double))
    return p


def test_second_call_uses_cache(event_loop):
    counter: list = []
    p = build_pipeline(counter)
    runner = CachedPipelineRunner(p, config=CacheConfig(ttl=60))
    r1 = event_loop.run_until_complete(runner.run(5))
    r2 = event_loop.run_until_complete(runner.run(5))
    assert r1 == r2 == 10
    assert len(counter) == 1  # fn called only once


def test_different_inputs_both_cached(event_loop):
    counter: list = []
    p = build_pipeline(counter)
    runner = CachedPipelineRunner(p)
    event_loop.run_until_complete(runner.run(3))
    event_loop.run_until_complete(runner.run(4))
    event_loop.run_until_complete(runner.run(3))
    event_loop.run_until_complete(runner.run(4))
    assert len(counter) == 2


def test_cache_property_exposed():
    p = build_pipeline([])
    runner = CachedPipelineRunner(p)
    assert runner.cache is not None


def test_invalidate_forces_recompute(event_loop):
    counter: list = []
    p = build_pipeline(counter)
    runner = CachedPipelineRunner(p, config=CacheConfig(ttl=60))
    event_loop.run_until_complete(runner.run(7))
    runner.cache.invalidate("double")
    event_loop.run_until_complete(runner.run(7))
    assert len(counter) == 2
