"""Tests for pipeweave.checkpoint."""
from __future__ import annotations

import pytest
import asyncio

from pipeweave.checkpoint import CheckpointStore, make_checkpoint_middleware
from pipeweave.context import PipelineContext, StageResult


# ---------------------------------------------------------------------------
# CheckpointStore
# ---------------------------------------------------------------------------

def test_save_and_load():
    s = CheckpointStore()
    s.save("r1", "stage_a", 42)
    assert s.load("r1", "stage_a") == 42


def test_has_returns_false_for_missing():
    s = CheckpointStore()
    assert not s.has("r1", "stage_a")


def test_has_returns_true_after_save():
    s = CheckpointStore()
    s.save("r1", "stage_a", "val")
    assert s.has("r1", "stage_a")


def test_clear_all():
    s = CheckpointStore()
    s.save("r1", "a", 1)
    s.save("r2", "b", 2)
    s.clear()
    assert len(s) == 0


def test_clear_by_run_id():
    s = CheckpointStore()
    s.save("r1", "a", 1)
    s.save("r2", "b", 2)
    s.clear("r1")
    assert not s.has("r1", "a")
    assert s.has("r2", "b")


def test_load_missing_returns_none():
    s = CheckpointStore()
    assert s.load("x", "y") is None


# ---------------------------------------------------------------------------
# make_checkpoint_middleware
# ---------------------------------------------------------------------------

def _make_ctx(**meta):
    return PipelineContext(metadata=meta)


def _ok_handler(output):
    async def handler(ctx, stage_name):
        return StageResult(stage=stage_name, output=output, success=True)
    return handler


def _err_handler():
    async def handler(ctx, stage_name):
        return StageResult(stage=stage_name, output=None, success=False, error=RuntimeError("boom"))
    return handler


@pytest.mark.asyncio
async def test_result_saved_on_success():
    store = CheckpointStore()
    mw = make_checkpoint_middleware(store)
    ctx = _make_ctx(run_id="run1")
    resultstep1", _ok_handler(99))
    assert result.success
    assert store.has("run1", "step1")
    assert store.load("run1", "step1") == 99


@pytest.mark.asyncio
async def test_not_saved_on_failure():
    store = CheckpointStore()
    mw = make_checkpoint_middleware(store)
    ctx = _make_ctx(run_id="run1")
    await mw(ctx, "step1", _err_handler())
    assert not store.has("run1", "step1")


@pytest.mark.asyncio
async def test_checkpoint_skips_handler():
    store = CheckpointStore()
    store.save("run1", "step1", "cached_value")
    called = []

    async def handler(ctx, stage_name):
        called.append(True)
        return StageResult(stage=stage_name, output="fresh", success=True)

    mw = make_checkpoint_middleware(store)
    ctx = _make_ctx(run_id="run1")
    result = await mw(ctx, "step1", handler)
    assert result.output == "cached_value"
    assert not called


@pytest.mark.asyncio
async def test_stage_filter_skips_non_listed():
    store = CheckpointStore()
    mw = make_checkpoint_middleware(store, stages=["step2"])
    ctx = _make_ctx(run_id="run1")
    await mw(ctx, "step1", _ok_handler(7))
    assert not store.has("run1", "step1")


@pytest.mark.asyncio
async def test_default_run_id_used_when_missing():
    store = CheckpointStore()
    mw = make_checkpoint_middleware(store)
    ctx = _make_ctx()  # no run_id
    await mw(ctx, "step1", _ok_handler(5))
    assert store.has("default", "step1")
