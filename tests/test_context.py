"""Tests for pipeweave.context — PipelineContext and StageResult."""

import time

import pytest

from pipeweave.context import PipelineContext, StageResult


# ---------------------------------------------------------------------------
# StageResult
# ---------------------------------------------------------------------------


def test_stage_result_repr_success():
    r = StageResult(stage_name="parse", success=True, output=42, attempts=1)
    assert "ok" in repr(r)
    assert "parse" in repr(r)


def test_stage_result_repr_failure():
    r = StageResult(stage_name="validate", success=False, error=ValueError("bad"))
    assert "failed" in repr(r)


# ---------------------------------------------------------------------------
# PipelineContext — metadata
# ---------------------------------------------------------------------------


def test_set_and_get_metadata():
    ctx = PipelineContext()
    ctx.set("user_id", 99)
    assert ctx.get("user_id") == 99


def test_get_missing_key_returns_default():
    ctx = PipelineContext()
    assert ctx.get("missing") is None
    assert ctx.get("missing", "fallback") == "fallback"


def test_metadata_init_kwarg():
    ctx = PipelineContext(metadata={"env": "test"})
    assert ctx.get("env") == "test"


# ---------------------------------------------------------------------------
# PipelineContext — stage results
# ---------------------------------------------------------------------------


def test_last_result_none_when_empty():
    ctx = PipelineContext()
    assert ctx.last_result is None


def test_record_and_last_result():
    ctx = PipelineContext()
    r = StageResult(stage_name="step1", success=True, output="hello")
    ctx.record(r)
    assert ctx.last_result is r


def test_failed_stages_filters_correctly():
    ctx = PipelineContext()
    ctx.record(StageResult("a", success=True))
    ctx.record(StageResult("b", success=False, error=RuntimeError("oops")))
    ctx.record(StageResult("c", success=True))
    assert len(ctx.failed_stages) == 1
    assert ctx.failed_stages[0].stage_name == "b"


def test_succeeded_all_pass():
    ctx = PipelineContext()
    ctx.record(StageResult("x", success=True))
    ctx.record(StageResult("y", success=True))
    assert ctx.succeeded is True


def test_succeeded_false_when_any_fail():
    ctx = PipelineContext()
    ctx.record(StageResult("x", success=True))
    ctx.record(StageResult("y", success=False))
    assert ctx.succeeded is False


def test_succeeded_false_when_empty():
    ctx = PipelineContext()
    assert ctx.succeeded is False


# ---------------------------------------------------------------------------
# PipelineContext — timing
# ---------------------------------------------------------------------------


def test_elapsed_ms_grows_over_time():
    ctx = PipelineContext()
    t0 = ctx.elapsed_ms
    time.sleep(0.05)
    t1 = ctx.elapsed_ms
    assert t1 > t0
    assert t1 >= 40  # at least ~40 ms elapsed
