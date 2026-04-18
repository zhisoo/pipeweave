"""Tests for pipeweave.fanout."""
from __future__ import annotations

import asyncio
import pytest

from pipeweave.context import PipelineContext, StageResult
from pipeweave.errors import PipeweaveError
from pipeweave.fanout import FanoutConfig, FanoutError, make_fanout_middleware


# ---------------------------------------------------------------------------
# FanoutConfig validation
# ---------------------------------------------------------------------------

def test_empty_handlers_raises():
    with pytest.raises(FanoutError):
        FanoutConfig(handlers=[])


def test_non_callable_handler_raises():
    with pytest.raises(FanoutError):
        FanoutConfig(handlers=["not_callable"])


def test_valid_config_accepted():
    cfg = FanoutConfig(handlers=[lambda x: x])
    assert len(cfg.handlers) == 1


def test_fail_fast_default_is_true():
    cfg = FanoutConfig(handlers=[lambda x: x])
    assert cfg.fail_fast is True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ctx() -> PipelineContext:
    from pipeweave.context import PipelineContext
    return PipelineContext()


def _ok_result(value: object = 42) -> StageResult:
    return StageResult(stage="s", success=True, value=value)


def _err_result() -> StageResult:
    return StageResult(stage="s", success=False, value=None, error=RuntimeError("boom"))


async def _next_ok(data, ctx):
    return _ok_result(data)


async def _next_err(data, ctx):
    return _err_result()


# ---------------------------------------------------------------------------
# Middleware behaviour
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_handlers_called_on_success():
    received = []
    cfg = FanoutConfig(handlers=[received.append])
    mw = make_fanout_middleware(cfg)
    await mw("s", _next_ok, 42, _make_ctx())
    assert received == [42]


@pytest.mark.asyncio
async def test_handlers_not_called_on_failure():
    received = []
    cfg = FanoutConfig(handlers=[received.append])
    mw = make_fanout_middleware(cfg)
    result = await mw("s", _next_err, 42, _make_ctx())
    assert not result.success
    assert received == []


@pytest.mark.asyncio
async def test_multiple_handlers_all_called():
    log: list[str] = []
    cfg = FanoutConfig(handlers=[lambda v: log.append(f"a:{v}"), lambda v: log.append(f"b:{v}")])
    mw = make_fanout_middleware(cfg)
    await mw("s", _next_ok, 7, _make_ctx())
    assert "a:7" in log and "b:7" in log


@pytest.mark.asyncio
async def test_fail_fast_raises_on_handler_error():
    def bad(v):
        raise ValueError("handler error")

    cfg = FanoutConfig(handlers=[bad], fail_fast=True)
    mw = make_fanout_middleware(cfg)
    with pytest.raises(ValueError, match="handler error"):
        await mw("s", _, _make_ctx())


@pytest.mark.asyncio
async def test_fail_fast_false_continues_after_error():
    log: list[str] = []

    def bad(v):
        raise ValueError("oops")

    cfg = FanoutConfig(handlers=[bad, lambda v: log.append("ok")], fail_fast=False)
    mw = make_fanout_middleware(cfg)
    await mw("s", _next_ok, 1, _make_ctx())
    assert log == ["ok"]


@pytest.mark.asyncio
async def test_async_handler_awaited():
    received = []

    async def async_handler(v):
        received.append(v)

    cfg = FanoutConfig(handlers=[async_handler])
    mw = make_fanout_middleware(cfg)
    await mw("s", _next_ok, 99, _make_ctx())
    assert received == [99]
