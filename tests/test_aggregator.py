"""Tests for pipeweave.aggregator."""
import asyncio
import operator
import pytest

from pipeweave.aggregator import (
    Aggregator,
    AggregatorConfig,
    AggregatorError,
    make_aggregator_middleware,
)
from pipeweave.context import PipelineContext, StageResult


def _make_ctx():
    return PipelineContext()


def _ok(value):
    return StageResult(stage="s", success=True, value=value)


def _err():
    return StageResult(stage="s", success=False, value=None, error=RuntimeError("boom"))


# --- config validation ---

def test_valid_config_accepted():
    cfg = AggregatorConfig(reducer=operator.add, initial=0)
    assert cfg.initial == 0


def test_non_callable_reducer_raises():
    with pytest.raises(AggregatorError):
        AggregatorConfig(reducer="not_callable")  # type: ignore


# --- Aggregator.feed ---

def test_feed_reduces_values():
    cfg = AggregatorConfig(reducer=operator.add, initial=0)
    agg = Aggregator(cfg)
    asyncio.get_event_loop().run_until_complete(agg.feed("s1", 3))
    asyncio.get_event_loop().run_until_complete(agg.feed("s1", 7))
    assert agg.value == 10


def test_feed_skips_unlisted_stage():
    cfg = AggregatorConfig(reducer=operator.add, initial=0, stages=["allowed"])
    agg = Aggregator(cfg)
    asyncio.get_event_loop().run_until_complete(agg.feed("other", 99))
    assert agg.value == 0


def test_emit_partial_records_history():
    cfg = AggregatorConfig(reducer=operator.add, initial=0, emit_partial=True)
    agg = Aggregator(cfg)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(agg.feed("s", 1))
    loop.run_until_complete(agg.feed("s", 2))
    assert agg.partials == [1, 3]


def test_reset_clears_state():
    cfg = AggregatorConfig(reducer=operator.add, initial=0)
    agg = Aggregator(cfg)
    asyncio.get_event_loop().run_until_complete(agg.feed("s", 5))
    agg.reset()
    assert agg.value == 0


# --- middleware ---

def test_middleware_feeds_on_success():
    cfg = AggregatorConfig(reducer=operator.add, initial=0)
    agg = Aggregator(cfg)
    mw = make_aggregator_middleware(cfg, agg)

    async def _next(name, ctx, result):
        return result

    result = _ok(4)
    asyncio.get_event_loop().run_until_complete(mw("s", _make_ctx(), result, _next))
    assert agg.value == 4


def test_middleware_skips_on_failure():
    cfg = AggregatorConfig(reducer=operator.add, initial=0)
    agg = Aggregator(cfg)
    mw = make_aggregator_middleware(cfg, agg)

    async def _next(name, ctx, result):
        return result

    asyncio.get_event_loop().run_until_complete(mw("s", _make_ctx(), _err(), _next))
    assert agg.value == 0


def test_middleware_exposes_aggregator():
    cfg = AggregatorConfig(reducer=operator.add, initial=0)
    mw = make_aggregator_middleware(cfg)
    assert isinstance(mw._aggregator, Aggregator)
