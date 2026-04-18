"""Tests for pipeweave.tap."""
from __future__ import annotations

import asyncio
import pytest

from pipeweave.tap import TapConfig, make_tap_middleware
from pipeweave.context import PipelineContext, StageResult


def _ok_result(value=42):
    return StageResult(stage="s", success=True, output=value)


def _err_result(value=None):
    return StageResult(stage="s", success=False, output=value, error=RuntimeError("boom"))


def _make_ctx():
    return PipelineContext(run_id="run-1")


async def _noop_next(stage, result, ctx):
    return result


# --- TapConfig validation ---

def test_valid_config_accepted():
    cfg = TapConfig(handlers=[lambda s, r, c: None])
    assert len(cfg.handlers) == 1


def test_non_callable_handler_raises():
    with pytest.raises(TypeError):
        TapConfig(handlers=["not_callable"])


def test_both_filters_raises():
    with pytest.raises(ValueError):
        TapConfig(handlers=[], only_failures=True, only_successes=True)


def test_empty_handlers_accepted():
    cfg = TapConfig()
    assert cfg.handlers == []


# --- middleware behaviour ---

@pytest.mark.asyncio
async def test_handler_called_on_success():
    called = []
    cfg = TapConfig(handlers=[lambda s, r, c: called.append((s, r.success))])
    mw = make_tap_middleware(cfg)
    result = _ok_result()
    await mw("stage1", result, _make_ctx(), _noop_next)
    assert called == [("stage1", True)]


@pytest.mark.asyncio
async def test_handler_not_called_when_stage_filtered():
    called = []
    cfg = TapConfig(handlers=[lambda s, r, c: called.append(s)], only_stages=["other"])
    mw = make_tap_middleware(cfg)
    await mw("stage1", _ok_result(), _make_ctx(), _noop_next)
    assert called == []


@pytest.mark.asyncio
async def test_only_failures_skips_success():
    called = []
    cfg = TapConfig(handlers=[lambda s, r, c: called.append(s)], only_failures=True)
    mw = make_tap_middleware(cfg)
    await mw("stage1", _ok_result(), _make_ctx(), _noop_next)
    assert called == []


@pytest.mark.asyncio
async def test_only_failures_fires_on_error():
    called = []
    cfg = TapConfig(handlers=[lambda s, r, c: called.append(s)], only_failures=True)
    mw = make_tap_middleware(cfg)
    await mw("stage1", _err_result(), _make_ctx(), _noop_next)
    assert called == ["stage1"]


@pytest.mark.asyncio
async def test_async_handler_awaited():
    called = []
    async def ah(s, r, c):
        called.append(s)
    cfg = TapConfig(handlers=[ah])
    mw = make_tap_middleware(cfg)
    await mw("stage1", _ok_result(), _make_ctx(), _noop_next)
    assert called == ["stage1"]


@pytest.mark.asyncio
async def test_handler_exception_does_not_break_pipeline():
    def bad(s, r, c):
        raise RuntimeError("tap error")
    cfg = TapConfig(handlers=[bad])
    mw = make_tap_middleware(cfg)
    result = _ok_result()
    out = await mw("stage1", result, _make_ctx(), _noop_next)
    assert out is result
