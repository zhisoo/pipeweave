"""Integration tests for SemaphorePipelineRunner."""

import asyncio
import pytest

from pipeweave.pipeline import Pipeline
from pipeweave.stage import Stage
from pipeweave.semaphore import SemaphoreConfig
from pipeweave.semaphore_runner import SemaphorePipelineRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_pipeline(*names: str) -> Pipeline:
    """Build a trivial pipeline with identity stages."""
    p = Pipeline()
    for name in names:
        async def _fn(data, _n=name):
            return data
        p.pipe(Stage(name=name, fn=_fn))
    return p


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_runner_exposes_pool():
    runner = SemaphorePipelineRunner(build_pipeline("a"))
    assert runner.semaphore_pool is not None


@pytest.mark.asyncio
async def test_basic_run_returns_result():
    runner = SemaphorePipelineRunner(build_pipeline("step"))
    ctx = await runner.run(42)
    assert ctx.last_result().value == 42


@pytest.mark.asyncio
async def test_named_limit_respected():
    """Only 1 concurrent execution of 'slow' stage at a time."""
    cfg = SemaphoreConfig(default_limit=1, limits={"slow": 1})
    active = []
    max_concurrent = [0]

    async def slow_fn(data):
        active.append(1)
        max_concurrent[0] = max(max_concurrent[0], len(active))
        await asyncio.sleep(0.02)
        active.pop()
        return data

    p = Pipeline()
    p.pipe(Stage(name="slow", fn=slow_fn))

    runner = SemaphorePipelineRunner(p, cfg)
    await asyncio.gather(runner.run(1), runner.run(2))
    assert max_concurrent[0] == 1


@pytest.mark.asyncio
async def test_default_limit_higher_allows_concurrency():
    """With limit=2 two tasks should overlap."""
    cfg = SemaphoreConfig(default_limit=2)
    active = []
    max_concurrent = [0]

    async def fn(data):
        active.append(1)
        max_concurrent[0] = max(max_concurrent[0], len(active))
        await asyncio.sleep(0.02)
        active.pop()
        return data

    p = Pipeline()
    p.pipe(Stage(name="par", fn=fn))

    runner = SemaphorePipelineRunner(p, cfg)
    await asyncio.gather(runner.run(1), runner.run(2))
    assert max_concurrent[0] == 2
