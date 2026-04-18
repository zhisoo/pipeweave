"""Tests for pipeweave.debounce."""
from __future__ import annotations

import asyncio
import time

import pytest

from pipeweave.debounce import DebounceConfig, Debouncer, make_debounce_middleware
from pipeweave.errors import PipeweaveError
from pipeweave.context import PipelineContext, StageResult


# ---------------------------------------------------------------------------
# DebounceConfig validation
# ---------------------------------------------------------------------------

def test_valid_config_accepted():
    cfg = DebounceConfig(interval_ms=200)
    assert cfg.interval_ms == 200


def test_zero_interval_raises():
    with pytest.raises(PipeweaveError):
        DebounceConfig(interval_ms=0)


def test_negative_interval_raises():
    with pytest.raises(PipeweaveError):
        DebounceConfig(interval_ms=-50)


def test_stages_filter_stored():
    cfg = DebounceConfig(interval_ms=100, stages=["step1", "step2"])
    assert cfg.stages == ["step1", "step2"]


# ---------------------------------------------------------------------------
# Debouncer.should_run
# ---------------------------------------------------------------------------

def test_first_call_always_runs():
    d = Debouncer(DebounceConfig(interval_ms=500))
    assert d.should_run("stage_a") is True


def test_second_immediate_call_blocked():
    d = Debouncer(DebounceConfig(interval_ms=500))
    d.should_run("stage_a")
    assert d.should_run("stage_a") is False


def test_call_allowed_after_interval(monkeypatch):
    d = Debouncer(DebounceConfig(interval_ms=100))
    d.should_run("stage_a")
    # Simulate time passing
    original = time.monotonic
    monkeypatch.setattr(time, "monotonic", lambda: original() + 0.2)
    assert d.should_run("stage_a") is True


def test_unlisted_stage_always_runs():
    d = Debouncer(DebounceConfig(interval_ms=500, stages=["only_this"]))
    d.should_run("other")
    assert d.should_run("other") is True


def test_reset_clears_specific_stage():
    d = Debouncer(DebounceConfig(interval_ms=500))
    d.should_run("stage_a")
    d.reset("stage_a")
    assert d.should_run("stage_a") is True


def test_reset_all_clears_all_stages():
    d = Debouncer(DebounceConfig(interval_ms=500))
    d.should_run("stage_a")
    d.should_run("stage_b")
    d.reset()
    assert d.should_run("stage_a") is True
    assert d.should_run("stage_b") is True


# ---------------------------------------------------------------------------
# make_debounce_middleware
# ---------------------------------------------------------------------------

def _make_ctx(stage_name: str = "s"):
    ctx = PipelineContext()
    ctx.current_stage = stage_name
    return ctx


@pytest.mark.asyncio
async def test_middleware_passes_through_on_first_call():
    debouncer = Debouncer(DebounceConfig(interval_ms=500))
    mw = make_debounce_middleware(debouncer)
    ctx = _make_ctx("s")
    expected = StageResult(stage="s", value=42)

    async def call_next(_ctx):
        return expected

    result = await mw(ctx, call_next)
    assert result is expected


@pytest.mark.asyncio
async def test_middleware_skips_on_rapid_second_call():
    debouncer = Debouncer(DebounceConfig(interval_ms=500))
    mw = make_debounce_middleware(debouncer)
    ctx = _make_ctx("s")

    async def call_next(_ctx):
        return StageResult(stage="s", value=99)

    await mw(ctx, call_next)  # first call — runs
    result = await mw(ctx, call_next)  # second call — debounced
    assert result.skipped is True
