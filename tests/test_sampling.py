"""Tests for pipeweave.sampling."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, call

from pipeweave.errors import PipeweaveError
from pipeweave.sampling import SamplingConfig, make_sampling_middleware
from pipeweave.context import StageResult, PipelineContext


def _ok_result(value=42, stage_name="s"):
    return StageResult(stage_name=stage_name, success=True, value=value, error=None)


def _make_ctx():
    return PipelineContext()


# --- SamplingConfig validation ---

def test_valid_rate_accepted():
    cfg = SamplingConfig(rate=0.5)
    assert cfg.rate == 0.5


def test_rate_zero_accepted():
    cfg = SamplingConfig(rate=0.0)
    assert cfg.rate == 0.0


def test_rate_one_accepted():
    cfg = SamplingConfig(rate=1.0)
    assert cfg.rate == 1.0


def test_rate_above_one_raises():
    with pytest.raises(PipeweaveError):
        SamplingConfig(rate=1.1)


def test_rate_negative_raises():
    with pytest.raises(PipeweaveError):
        SamplingConfig(rate=-0.1)


# --- make_sampling_middleware ---

@pytest.mark.asyncio
async def test_rate_one_never_drops():
    cfg = SamplingConfig(rate=1.0, seed=0)
    mw = make_sampling_middleware(cfg)
    result = _ok_result()
    call_next = AsyncMock(return_value=result)
    ctx = _make_ctx()
    for _ in range(20):
        r = await mw(ctx, call_next)
        assert r.value == 42


@pytest.mark.asyncio
async def test_rate_zero_always_drops():
    cfg = SamplingConfig(rate=0.0, seed=0)
    mw = make_sampling_middleware(cfg)
    result = _ok_result(value=99)
    call_next = AsyncMock(return_value=result)
    ctx = _make_ctx()
    for _ in range(10):
        r = await mw(ctx, call_next)
        assert r.value is None


@pytest.mark.asyncio
async def test_failed_result_not_dropped():
    cfg = SamplingConfig(rate=0.0, seed=0)
    mw = make_sampling_middleware(cfg)
    failed = StageResult(stage_name="s", success=False, value=None, error=ValueError("oops"))
    call_next = AsyncMock(return_value=failed)
    ctx = _make_ctx()
    r = await mw(ctx, call_next)
    assert r.success is False
    assert isinstance(r.error, ValueError)


@pytest.mark.asyncio
async def test_on_dropped_callback_called():
    dropped_calls = []

    def on_drop(ctx, result):
        dropped_calls.append(result.value)

    cfg = SamplingConfig(rate=0.0, seed=42, on_dropped=on_drop)
    mw = make_sampling_middleware(cfg)
    result = _ok_result(value=7)
    call_next = AsyncMock(return_value=result)
    ctx = _make_ctx()
    await mw(ctx, call_next)
    assert dropped_calls == [7]


@pytest.mark.asyncio
async def test_deterministic_with_seed():
    cfg1 = SamplingConfig(rate=0.5, seed=123)
    cfg2 = SamplingConfig(rate=0.5, seed=123)
    mw1 = make_sampling_middleware(cfg1)
    mw2 = make_sampling_middleware(cfg2)
    results = []
    for mw in (mw1, mw2):
        run = []
        for _ in range(10):
            r = await mw(_make_ctx(), AsyncMock(return_value=_ok_result(value=1)))
            run.append(r.value)
        results.append(run)
    assert results[0] == results[1]
