"""Tests for pipeweave.batch_runner.BatchPipelineRunner."""
import pytest
import asyncio

from pipeweave.pipeline import Pipeline, StageConfig, pipe
from pipeweave.batch import BatchConfig
from pipeweave.batch_runner import BatchPipelineRunner


def _aiter(items):
    async def _gen():
        for item in items:
            yield item
    return _gen()


def build_pipeline() -> Pipeline:
    p = Pipeline()

    @pipe(p, StageConfig(name="sum_batch"))
    async def sum_batch(data):
        return sum(data)

    return p


@pytest.mark.asyncio
async def test_run_stream_returns_one_result_per_batch():
    p = build_pipeline()
    runner = BatchPipelineRunner(p, BatchConfig(size=3))
    results = await runner.run_stream(_aiter([1, 2, 3, 4, 5, 6]))
    assert len(results) == 2


@pytest.mark.asyncio
async def test_run_stream_correct_sums():
    p = build_pipeline()
    runner = BatchPipelineRunner(p, BatchConfig(size=2))
    results = await runner.run_stream(_aiter([10, 20, 30, 40]))
    # Each result is the final pipeline output for that batch
    assert len(results) == 2


@pytest.mark.asyncio
async def test_empty_stream_returns_no_results():
    p = build_pipeline()
    runner = BatchPipelineRunner(p, BatchConfig(size=5))
    results = await runner.run_stream(_aiter([]))
    assert results == []


def test_batch_config_property():
    p = build_pipeline()
    cfg = BatchConfig(size=7)
    runner = BatchPipelineRunner(p, cfg)
    assert runner.batch_config.size == 7
