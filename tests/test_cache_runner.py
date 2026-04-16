"""Integration tests for CachedPipelineRunner."""
from __future__ import annotations

import asyncio
import pytest

from pipeweave.pipeline import Pipeline, StageConfig, pipe
from pipeweave.cache import CacheConfig
from pipeweave.cache_runner import CachedPipelineRunner


def build_pipeline(call_counter: list) -> Pipeline:
    async def double(x: int) -> int:
        call_counter.append(x)
        return x * 2

    p = Pipeline(name="test")
    p.add_stage(StageConfig(name="double", fn=double))
    return p


def test_second_call_uses_cache():
    counter: list = []
    p = build_pipeline(counter)
    runner = CachedPipelineRunner(p, config=CacheConfig(ttl=60))
    loop = asyncio.get_event_loop()
    r1 = loop.run_until_complete(runner.run(5))
    r2 = loop.run_until_complete(runner.run(5))
    assert r1 == r2 == 10
    assert len(counter) == 1  # fn called only once


def test_different_inputs_both_cached():
    counter: list = []
    p = build_pipeline(counter)
    runner = CachedPipelineRunner(p)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(runner.run(3))
    loop.run_until_complete(runner.run(4))
    loop.run_until_complete(runner.run(3))
    loop.run_until_complete(runner.run(4))
    assert len(counter) == 2


def test_cache_property_exposed():
    p = build_pipeline([])
    runner = CachedPipelineRunner(p)
    assert runner.cache is not None


def test_invalidate_forces_recompute():
    counter: list = []
    p = build_pipeline(counter)
    runner = CachedPipelineRunner(p, config=CacheConfig(ttl=60))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(runner.run(7))
    runner.cache.invalidate("double")
    loop.run_until_complete(runner.run(7))
    assert len(counter) == 2
