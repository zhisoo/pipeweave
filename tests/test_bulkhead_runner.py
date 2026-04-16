"""Tests for BulkheadPipelineRunner."""
from __future__ import annotations
import asyncio
import pytest

from pipeweave.pipeline import Pipeline, StageConfig
from pipeweave.stage import Stage
from pipeweave.bulkhead import BulkheadConfig, BulkheadFullError
from pipeweave.bulkhead_runner import BulkheadPipelineRunner


def build_pipeline():
    async def double(x):
        return x * 2

    stage = Stage(StageConfig(name="double", fn=double))
    return Pipeline(stages=[stage])


@pytest.mark.asyncio
async def test_runner_returns_correct_result():
    runner = BulkheadPipelineRunner(build_pipeline())
    result = await runner.run(3)
    assert result.value == 6


@pytest.mark.asyncio
async def test_bulkhead_property_exposed():
    cfg = BulkheadConfig(max_concurrent=3)
    runner = BulkheadPipelineRunner(build_pipeline(), config=cfg)
    assert runner.bulkhead.config.max_concurrent == 3


@pytest.mark.asyncio
async def test_concurrent_runs_within_limit():
    cfg = BulkheadConfig(max_concurrent=5)
    runner = BulkheadPipelineRunner(build_pipeline(), config=cfg)
    results = await asyncio.gather(*[runner.run(i) for i in range(5)])
    values = {r.value for r in results}
    assert values == {0, 2, 4, 6, 8}


@pytest.mark.asyncio
async def test_exceeding_concurrency_raises_when_no_queue():
    async def slow(x):
        await asyncio.sleep(0.1)
        return x

    from pipeweave.stage import Stage
    stage = Stage(StageConfig(name="slow", fn=slow))
    pipeline = Pipeline(stages=[stage])
    cfg = BulkheadConfig(max_concurrent=1, max_queue=0)
    runner = BulkheadPipelineRunner(pipeline, config=cfg)

    t1 = asyncio.create_task(runner.run(1))
    await asyncio.sleep(0)  # let t1 acquire
    with pytest.raises(BulkheadFullError):
        await runner.run(2)
    await t1
