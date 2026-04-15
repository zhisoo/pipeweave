"""Tests for metrics_middleware and MetricsPipelineRunner integration."""
from __future__ import annotations

import pytest

from pipeweave.context import PipelineContext, StageResult
from pipeweave.metrics import PipelineMetrics
from pipeweave.metrics_middleware import (
    attach_pipeline_metrics,
    get_pipeline_metrics,
    metrics_middleware,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ctx() -> PipelineContext:
    return PipelineContext()


def _ok_result(value: object = "data") -> StageResult:
    return StageResult(stage_name="s", value=value, ok=True)


def _err_result(msg: str = "oops") -> StageResult:
    return StageResult(stage_name="s", value=None, ok=False, error=Exception(msg))


# ---------------------------------------------------------------------------
# attach / get helpers
# ---------------------------------------------------------------------------

class TestAttachGetMetrics:
    def test_attach_returns_pipeline_metrics(self):
        ctx = _make_ctx()
        pm = attach_pipeline_metrics(ctx, "p")
        assert isinstance(pm, PipelineMetrics)
        assert pm.pipeline_name == "p"

    def test_get_returns_same_object(self):
        ctx = _make_ctx()
        pm = attach_pipeline_metrics(ctx, "p")
        assert get_pipeline_metrics(ctx) is pm

    def test_get_returns_none_when_not_attached(self):
        ctx = _make_ctx()
        assert get_pipeline_metrics(ctx) is None


# ---------------------------------------------------------------------------
# metrics_middleware
# ---------------------------------------------------------------------------

class TestMetricsMiddleware:
    @pytest.mark.asyncio
    async def test_records_success_stage(self):
        ctx = _make_ctx()
        pm = attach_pipeline_metrics(ctx, "pipe")

        async def handler(data, c):
            return _ok_result(data)

        wrapped = metrics_middleware(handler, "stage_a")
        result = await wrapped("input", ctx)

        assert result.ok
        assert len(pm.stages) == 1
        sm = pm.stages[0]
        assert sm.stage_name == "stage_a"
        assert sm.succeeded
        assert sm.error is None
        assert sm.duration_ms is not None

    @pytest.mark.asyncio
    async def test_records_failed_stage(self):
        ctx = _make_ctx()
        pm = attach_pipeline_metrics(ctx, "pipe")

        async def handler(data, c):
            return _err_result("bad")

        wrapped = metrics_middleware(handler, "stage_b")
        await wrapped("input", ctx)

        sm = pm.stages[0]
        assert not sm.succeeded
        assert "bad" in sm.error

    @pytest.mark.asyncio
    async def test_no_pipeline_metrics_does_not_raise(self):
        ctx = _make_ctx()  # no pm attached

        async def handler(data, c):
            return _ok_result(data)

        wrapped = metrics_middleware(handler, "stage_c")
        result = await wrapped("x", ctx)
        assert result.ok
