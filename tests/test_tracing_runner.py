"""Integration tests for TracingPipelineRunner."""
from __future__ import annotations

import asyncio

import pytest

from pipeweave.pipeline import Pipeline, StageConfig
from pipeweave.stage import Stage
from pipeweave.tracing import SpanConfig
from pipeweave.tracing_runner import TracingPipelineRunner


@pytest.fixture()
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def build_pipeline() -> Pipeline:
    async def double(x):
        return x * 2

    async def add_one(x):
        return x + 1

    p = Pipeline()
    p.pipe(Stage("double", double, StageConfig()))
    p.pipe(Stage("add_one", add_one, StageConfig()))
    return p


@pytest.mark.asyncio
async def test_last_trace_is_none_before_run():
    runner = TracingPipelineRunner(build_pipeline())
    assert runner.last_trace is None


@pytest.mark.asyncio
async def test_last_trace_populated_after_run():
    runner = TracingPipelineRunner(build_pipeline())
    await runner.run(3)
    assert runner.last_trace is not None


@pytest.mark.asyncio
async def test_span_count_matches_stages():
    p = build_pipeline()
    runner = TracingPipelineRunner(p)
    await runner.run(5)
    assert len(runner.last_trace.spans) == 2


@pytest.mark.asyncio
async def test_span_stage_names():
    runner = TracingPipelineRunner(build_pipeline())
    await runner.run(1)
    names = [s.stage_name for s in runner.last_trace.spans]
    assert "double" in names
    assert "add_one" in names


@pytest.mark.asyncio
async def test_all_spans_successful():
    runner = TracingPipelineRunner(build_pipeline())
    await runner.run(10)
    assert all(s.success for s in runner.last_trace.spans)


@pytest.mark.asyncio
async def test_custom_service_name_in_spans():
    cfg = SpanConfig(service_name="my-svc")
    runner = TracingPipelineRunner(build_pipeline(), config=cfg)
    await runner.run(2)
    assert all(s.service_name == "my-svc" for s in runner.last_trace.spans)


@pytest.mark.asyncio
async def test_each_run_gets_fresh_trace():
    runner = TracingPipelineRunner(build_pipeline())
    await runner.run(1)
    trace1 = runner.last_trace
    await runner.run(2)
    trace2 = runner.last_trace
    assert trace1 is not trace2
    assert trace1.trace_id != trace2.trace_id


@pytest.mark.asyncio
async def test_on_span_finish_called_for_each_stage():
    finished = []
    cfg = SpanConfig(on_span_finish=finished.append)
    runner = TracingPipelineRunner(build_pipeline(), config=cfg)
    await runner.run(4)
    assert len(finished) == 2
