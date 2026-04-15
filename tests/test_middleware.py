"""Tests for pipeweave.middleware."""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from pipeweave.middleware import MiddlewareChain, logging_middleware, timing_middleware, validation_middleware
from pipeweave.context import PipelineContext, StageResult


def _make_ctx():
    return PipelineContext()


def _ok_result(name="stage"):
    return StageResult(stage_name=name, value=42, success=True)


async def _base_handler(stage_name, data, ctx):
    return _ok_result(stage_name)


@pytest.mark.asyncio
async def test_empty_chain_passes_through():
    chain = MiddlewareChain()
    wrapped = chain.wrap(_base_handler)
    result = await wrapped("s", None, _make_ctx())
    assert result.success and result.value == 42


@pytest.mark.asyncio
async def test_logging_middleware_does_not_alter_result(caplog):
    import logging
    with caplog.at_level(logging.DEBUG, logger="pipeweave.middleware"):
        wrapped = logging_middleware(_base_handler)
        result = await wrapped("my_stage", {}, _make_ctx())
    assert result.success
    assert "my_stage" in caplog.text


@pytest.mark.asyncio
async def test_timing_middleware_records_timings():
    ctx = _make_ctx()
    wrapped = timing_middleware(_base_handler)
    await wrapped("step1", None, ctx)
    timings = ctx.get("timings", {})
    assert "step1" in timings
    assert timings["step1"] >= 0


@pytest.mark.asyncio
async def test_validation_middleware_passes_valid_data():
    def validator(data):
        assert isinstance(data, int)

    chain = MiddlewareChain()
    chain.add(validation_middleware(validator))
    wrapped = chain.wrap(_base_handler)
    result = await wrapped("s", 5, _make_ctx())
    assert result.success


@pytest.mark.asyncio
async def test_validation_middleware_raises_on_invalid_data():
    def validator(data):
        if data is None:
            raise ValueError("data must not be None")

    chain = MiddlewareChain()
    chain.add(validation_middleware(validator))
    wrapped = chain.wrap(_base_handler)
    with pytest.raises(ValueError, match="data must not be None"):
        await wrapped("s", None, _make_ctx())


@pytest.mark.asyncio
async def test_middleware_execution_order():
    order = []

    def make_mw(tag):
        def _mw(nxt):
            async def _h(name, data, ctx):
                order.append(f"{tag}:before")
                r = await nxt(name, data, ctx)
                order.append(f"{tag}:after")
                return r
            return _h
        return _mw

    chain = MiddlewareChain()
    chain.add(make_mw("A")).add(make_mw("B"))
    await chain.wrap(_base_handler)("s", None, _make_ctx())
    assert order == ["A:before", "B:before", "B:after", "A:after"]
