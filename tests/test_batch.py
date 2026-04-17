"""Tests for pipeweave.batch."""
import pytest
import asyncio
from typing import AsyncIterator, List

from pipeweave.batch import BatchConfig, BatchRunner, iter_batches


def _aiter(items):
    async def _gen():
        for item in items:
            yield item
    return _gen()


# --- BatchConfig ---

def test_valid_config_accepted():
    cfg = BatchConfig(size=5, timeout=1.0)
    assert cfg.size == 5
    assert cfg.timeout == 1.0


def test_default_size_is_ten():
    assert BatchConfig().size == 10


def test_zero_size_raises():
    with pytest.raises(ValueError, match="size"):
        BatchConfig(size=0)


def test_negative_size_raises():
    with pytest.raises(ValueError, match="size"):
        BatchConfig(size=-1)


def test_zero_timeout_raises():
    with pytest.raises(ValueError, match="timeout"):
        BatchConfig(timeout=0)


def test_negative_timeout_raises():
    with pytest.raises(ValueError, match="timeout"):
        BatchConfig(timeout=-0.5)


# --- iter_batches ---

@pytest.mark.asyncio
async def test_iter_batches_exact_multiple():
    batches = [b async for b in iter_batches(_aiter(range(6)), BatchConfig(size=2))]
    assert batches == [[0, 1], [2, 3], [4, 5]]


@pytest.mark.asyncio
async def test_iter_batches_partial_tail():
    batches = [b async for b in iter_batches(_aiter(range(5)), BatchConfig(size=3))]
    assert batches == [[0, 1, 2], [3, 4]]


@pytest.mark.asyncio
async def test_iter_batches_empty_source():
    batches = [b async for b in iter_batches(_aiter([]), BatchConfig(size=3))]
    assert batches == []


# --- BatchRunner ---

@pytest.mark.asyncio
async def test_batch_runner_collects_results():
    async def double(batch: List[int]):
        return [x * 2 for x in batch]

    runner = BatchRunner(double, BatchConfig(size=3))
    results = await runner.run(_aiter(range(6)))
    assert results == [[0, 2, 4], [6, 8, 10]]


@pytest.mark.asyncio
async def test_batch_runner_single_batch():
    async def identity(batch):
        return batch

    runner = BatchRunner(identity, BatchConfig(size=100))
    results = await runner.run(_aiter([1, 2, 3]))
    assert results == [[1, 2, 3]]
