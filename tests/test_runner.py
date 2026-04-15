"""Integration tests for PipelineRunner with middleware and hooks."""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock

from pipeweave.pipeline import Pipeline, StageConfig, pipe
from pipeweave.middleware import MiddlewareChain, timing_middleware
from pipeweave.hooks import HookSet
from pipeweave.runner import PipelineRunner
from pipeweave.errors import PipelineAbortedError


async def double(x):
    return x * 2


async def add_ten(x):
    return x + 10


async def always_fail(x):
    raise ValueError("boom")


def build_pipeline(*stages):
    p = Pipeline()
    for name, fn, cfg in stages:
        p.add_stage(name, fn, cfg)
    return p


@pytest.mark.asyncio
async def test_runner_basic_pipeline():
    p = build_pipeline(
        ("double", double, StageConfig()),
        ("add_ten", add_ten, StageConfig()),
    )
    runner = PipelineRunner(p)
    ctx = await runner.run(5)
    assert ctx.last_result().value == 20


@pytest.mark.asyncio
async def test_runner_records_all_stages():
    p = build_pipeline(
        ("double", double, StageConfig()),
        ("add_ten", add_ten, StageConfig()),
    )
    ctx = await PipelineRunner(p).run(3)
    assert ctx.get_result("double").value == 6
    assert ctx.get_result("add_ten").value == 16


@pytest.mark.asyncio
async def test_runner_abort_on_error_raises():
    p = build_pipeline(
        ("fail", always_fail, StageConfig(abort_on_error=True)),
    )
    with pytest.raises(PipelineAbortedError):
        await PipelineRunner(p).run(1)


@pytest.mark.asyncio
async def test_runner_continues_when_abort_false():
    p = build_pipeline(
        ("fail", always_fail, StageConfig(abort_on_error=False)),
        ("double", double, StageConfig()),
    )
    ctx = await PipelineRunner(p).run(4)
    assert not ctx.get_result("fail").success
    # double receives None because previous stage failed
    assert ctx.get_result("double") is not None


@pytest.mark.asyncio
async def test_runner_timing_middleware_populates_context():
    p = build_pipeline(("double", double, StageConfig()))
    chain = MiddlewareChain().add(timing_middleware)
    ctx = await PipelineRunner(p, middleware=chain).run(7)
    assert "double" in ctx.get("timings", {})


@pytest.mark.asyncio
async def test_runner_hooks_called():
    calls = []
    hooks = HookSet(
        on_pipeline_start=lambda ctx: calls.append("pipeline_start"),
        on_pipeline_end=lambda ctx: calls.append("pipeline_end"),
        on_stage_start=lambda name, data, ctx: calls.append(f"stage_start:{name}"),
        on_stage_end=lambda name, result, ctx: calls.append(f"stage_end:{name}"),
    )
    p = build_pipeline(("double", double, StageConfig()))
    await PipelineRunner(p, hooks=hooks).run(2)
    assert calls == ["pipeline_start", "stage_start:double", "stage_end:double", "pipeline_end"]
