"""Integration tests for AggregatorPipelineRunner."""
import asyncio
import operator
import pytest

from pipeweave.pipeline import Pipeline, StageConfig
from pipeweave.stage import Stage
from pipeweave.aggregator import AggregatorConfig
from pipeweave.aggregator_runner import AggregatorPipelineRunner


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def build_pipeline():
    async def double(x):
        return x * 2

    async def inc(x):
        return x + 1

    p = Pipeline()
    p.pipe(Stage("double", double, StageConfig()))
    p.pipe(Stage("inc", inc, StageConfig()))
    return p


def test_aggregated_value_after_run(event_loop):
    p = build_pipeline()
    cfg = AggregatorConfig(reducer=operator.add, initial=0)
    runner = AggregatorPipelineRunner(p, cfg)
    event_loop.run_until_complete(runner.run(3))
    # double(3)=6, inc(6)=7 -> 0+6+7=13
    assert runner.aggregated_value == 13


def test_reset_clears_between_runs(event_loop):
    p = build_pipeline()
    cfg = AggregatorConfig(reducer=operator.add, initial=0)
    runner = AggregatorPipelineRunner(p, cfg)
    event_loop.run_until_complete(runner.run(1))
    runner.reset_aggregator()
    assert runner.aggregated_value == 0


def test_aggregator_property_exposed(event_loop):
    p = build_pipeline()
    cfg = AggregatorConfig(reducer=operator.add, initial=0)
    runner = AggregatorPipelineRunner(p, cfg)
    from pipeweave.aggregator import Aggregator
    assert isinstance(runner.aggregator, Aggregator)


def test_stages_filter_limits_aggregation(event_loop):
    p = build_pipeline()
    cfg = AggregatorConfig(reducer=operator.add, initial=0, stages=["double"])
    runner = AggregatorPipelineRunner(p, cfg)
    event_loop.run_until_complete(runner.run(3))
    # only double(3)=6 is fed
    assert runner.aggregated_value == 6
