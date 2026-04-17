"""Tests for priority_queue and priority_runner modules."""
import asyncio
import pytest
from pipeweave.priority_queue import (
    PriorityQueueConfig,
    StagePriorityQueue,
    make_priority_middleware,
)
from pipeweave.context import PipelineContext, StageResult
from pipeweave.priority_runner import PriorityPipelineRunner
from pipeweave.pipeline import Pipeline
from pipeweave.stage import Stage


# --- PriorityQueueConfig ---

def test_valid_config_accepted():
    cfg = PriorityQueueConfig(max_size=10)
    assert cfg.max_size == 10

def test_zero_max_size_accepted():
    cfg = PriorityQueueConfig(max_size=0)
    assert cfg.max_size == 0

def test_negative_max_size_raises():
    with pytest.raises(ValueError):
        PriorityQueueConfig(max_size=-1)


# --- StagePriorityQueue ---

def _make_ctx():
    return PipelineContext()

def _ok_result(value):
    return StageResult(stage_name="s", value=value, success=True)


async def _handler(value):
    async def h(ctx, data):
        return _ok_result(value)
    return h


@pytest.mark.asyncio
async def test_run_all_empty_returns_empty():
    pq = StagePriorityQueue()
    results = await pq.run_all(_make_ctx(), None)
    assert results == []


@pytest.mark.asyncio
async def test_run_all_executes_in_priority_order():
    order = []

    async def make_handler(tag):
        async def h(ctx, data):
            order.append(tag)
            return _ok_result(tag)
        return h

    pq = StagePriorityQueue()
    await pq.put(2, await make_handler("low"))
    await pq.put(0, await make_handler("high"))
    await pq.put(1, await make_handler("mid"))

    await pq.run_all(_make_ctx(), None)
    assert order == ["high", "mid", "low"]


@pytest.mark.asyncio
async def test_size_reflects_queue_length():
    pq = StagePriorityQueue()
    assert pq.size == 0
    await pq.put(0, lambda ctx, d: _ok_result(1))
    assert pq.size == 1


# --- make_priority_middleware ---

@pytest.mark.asyncio
async def test_priority_middleware_sets_metadata():
    ctx = _make_ctx()
    result = _ok_result(42)

    async def next_call(c, r):
        return r

    mw = make_priority_middleware(priority=5)
    await mw(ctx, result, next_call)
    assert ctx.get("stage_priority") == 5


# --- PriorityPipelineRunner ---

def build_pipeline():
    async def double(data):
        return data * 2

    async def add_one(data):
        return data + 1

    p = Pipeline()
    p.add_stage(Stage(name="double", fn=double))
    p.add_stage(Stage(name="add_one", fn=add_one))
    return p


@pytest.mark.asyncio
async def test_priority_runner_returns_last_result():
    p = build_pipeline()
    runner = PriorityPipelineRunner(p, priorities={"double": 0, "add_one": 1})
    result = await runner.run(3)
    # double runs first (priority 0): 3*2=6, then add_one: 6+1=7
    assert result == 7


@pytest.mark.asyncio
async def test_priority_runner_reversed_priorities():
    p = build_pipeline()
    runner = PriorityPipelineRunner(p, priorities={"double": 1, "add_one": 0})
    result = await runner.run(3)
    # add_one runs first: 3+1=4, then double: 4*2=8
    assert result == 8


def test_priorities_property():
    p = build_pipeline()
    runner = PriorityPipelineRunner(p, priorities={"double": 1})
    assert runner.priorities == {"double": 1}
