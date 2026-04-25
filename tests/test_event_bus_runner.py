"""Integration tests for EventBusPipelineRunner."""
from __future__ import annotations

import pytest

from pipeweave.event_bus import EventBus
from pipeweave.event_bus_runner import EventBusPipelineRunner
from pipeweave.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_pipeline() -> Pipeline:
    p = Pipeline()

    async def double(x):
        return x * 2

    async def add_one(x):
        return x + 1

    p.pipe(double, name="double")
    p.pipe(add_one, name="add_one")
    return p


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_returns_correct_result():
    runner = EventBusPipelineRunner(build_pipeline())
    result = await runner.run(3)
    # double(3) = 6, add_one(6) = 7
    assert result == 7


@pytest.mark.asyncio
async def test_bus_property_exposed():
    runner = EventBusPipelineRunner(build_pipeline())
    assert isinstance(runner.bus, EventBus)


@pytest.mark.asyncio
async def test_external_bus_accepted():
    bus = EventBus()
    runner = EventBusPipelineRunner(build_pipeline(), bus=bus)
    assert runner.bus is bus


@pytest.mark.asyncio
async def test_stage_before_event_fired():
    runner = EventBusPipelineRunner(build_pipeline())
    events = []

    async def _before(**kw):
        events.append(("before", kw["stage"]))

    runner.bus.subscribe("stage.before", _before)
    await runner.run(1)
    assert ("before", "double") in events
    assert ("before", "add_one") in events


@pytest.mark.asyncio
async def test_stage_after_event_fired():
    runner = EventBusPipelineRunner(build_pipeline())
    events = []

    async def _after(**kw):
        events.append(("after", kw["stage"], kw["result"]))

    runner.bus.subscribe("stage.after", _after)
    await runner.run(2)
    assert ("after", "double", 4) in events
    assert ("after", "add_one", 5) in events


@pytest.mark.asyncio
async def test_pipeline_done_event_fired():
    runner = EventBusPipelineRunner(build_pipeline())
    done_results = []

    async def _done(**kw):
        done_results.append(kw["result"])

    runner.bus.subscribe("pipeline.done", _done)
    await runner.run(4)
    assert done_results == [9]  # double(4)=8, add_one(8)=9


@pytest.mark.asyncio
async def test_stage_error_event_fired_on_failure():
    p = Pipeline()

    async def explode(x):
        raise ValueError("kaboom")

    p.pipe(explode, name="explode")
    runner = EventBusPipelineRunner(p)
    errors = []

    async def _err(**kw):
        errors.append((kw["stage"], type(kw["error"]).__name__))

    runner.bus.subscribe("stage.error", _err)

    with pytest.raises(Exception):
        await runner.run(1)

    assert errors == [("explode", "ValueError")]
