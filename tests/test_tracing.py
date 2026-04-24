"""Tests for pipeweave.tracing."""
from __future__ import annotations

import pytest

from pipeweave.context import PipelineContext, StageResult
from pipeweave.tracing import (
    Span,
    SpanConfig,
    TraceContext,
    make_tracing_middleware,
)


# ---------------------------------------------------------------------------
# SpanConfig
# ---------------------------------------------------------------------------

def test_default_service_name():
    cfg = SpanConfig()
    assert cfg.service_name == "pipeweave"


def test_empty_service_name_raises():
    with pytest.raises(ValueError, match="service_name"):
        SpanConfig(service_name="")


def test_blank_service_name_raises():
    with pytest.raises(ValueError, match="service_name"):
        SpanConfig(service_name="   ")


def test_custom_service_name_accepted():
    cfg = SpanConfig(service_name="my-service")
    assert cfg.service_name == "my-service"


# ---------------------------------------------------------------------------
# Span
# ---------------------------------------------------------------------------

def test_span_duration_none_before_finish():
    span = Span(
        trace_id="t1", span_id="s1", parent_span_id=None,
        stage_name="stage", service_name="svc",
    )
    assert span.duration_ms is None


def test_span_duration_after_finish():
    span = Span(
        trace_id="t1", span_id="s1", parent_span_id=None,
        stage_name="stage", service_name="svc",
    )
    span.finish(success=True)
    assert span.duration_ms is not None
    assert span.duration_ms >= 0


def test_span_success_flag():
    span = Span(
        trace_id="t1", span_id="s1", parent_span_id=None,
        stage_name="stage", service_name="svc",
    )
    span.finish(success=False)
    assert span.success is False


# ---------------------------------------------------------------------------
# TraceContext
# ---------------------------------------------------------------------------

def test_trace_context_generates_trace_id():
    tc = TraceContext()
    assert len(tc.trace_id) == 32  # uuid4 hex


def test_trace_context_accepts_custom_trace_id():
    tc = TraceContext(trace_id="custom-id")
    assert tc.trace_id == "custom-id"


def test_new_span_appended():
    tc = TraceContext()
    span = tc.new_span("s1", "svc")
    assert span in tc.spans
    assert len(tc.spans) == 1


def test_spans_returns_copy():
    tc = TraceContext()
    tc.new_span("s1", "svc")
    spans = tc.spans
    spans.clear()
    assert len(tc.spans) == 1


# ---------------------------------------------------------------------------
# make_tracing_middleware
# ---------------------------------------------------------------------------

def _make_ctx(name: str = "stage") -> PipelineContext:
    from pipeweave.context import PipelineContext
    ctx = PipelineContext(run_id="run-1", stage_name=name)
    return ctx


def _ok_result(val=42) -> StageResult:
    return StageResult(stage_name="stage", value=val, success=True)


def _err_result() -> StageResult:
    return StageResult(stage_name="stage", value=None, success=False,
                       error=RuntimeError("boom"))


@pytest.mark.asyncio
async def test_middleware_records_span_on_success():
    cfg = SpanConfig()
    tc = TraceContext()
    mw = make_tracing_middleware(cfg, tc)

    async def call_next(ctx, result):
        return _ok_result()

    await mw(_make_ctx(), _ok_result(), call_next)
    assert len(tc.spans) == 1
    assert tc.spans[0].success is True


@pytest.mark.asyncio
async def test_middleware_records_span_on_failure():
    cfg = SpanConfig()
    tc = TraceContext()
    mw = make_tracing_middleware(cfg, tc)

    async def call_next(ctx, result):
        return _err_result()

    await mw(_make_ctx(), _ok_result(), call_next)
    assert tc.spans[0].success is False


@pytest.mark.asyncio
async def test_on_span_finish_callback_called():
    finished = []
    cfg = SpanConfig(on_span_finish=finished.append)
    tc = TraceContext()
    mw = make_tracing_middleware(cfg, tc)

    async def call_next(ctx, result):
        return _ok_result()

    await mw(_make_ctx(), _ok_result(), call_next)
    assert len(finished) == 1
    assert isinstance(finished[0], Span)


@pytest.mark.asyncio
async def test_include_input_tag():
    cfg = SpanConfig(include_input=True)
    tc = TraceContext()
    mw = make_tracing_middleware(cfg, tc)

    async def call_next(ctx, result):
        return _ok_result()

    await mw(_make_ctx(), _ok_result(val="hello"), call_next)
    assert "input" in tc.spans[0].tags


@pytest.mark.asyncio
async def test_include_output_tag_on_success():
    cfg = SpanConfig(include_output=True)
    tc = TraceContext()
    mw = make_tracing_middleware(cfg, tc)

    async def call_next(ctx, result):
        return _ok_result(val="world")

    await mw(_make_ctx(), _ok_result(), call_next)
    assert "output" in tc.spans[0].tags


@pytest.mark.asyncio
async def test_output_tag_absent_on_failure():
    cfg = SpanConfig(include_output=True)
    tc = TraceContext()
    mw = make_tracing_middleware(cfg, tc)

    async def call_next(ctx, result):
        return _err_result()

    await mw(_make_ctx(), _ok_result(), call_next)
    assert "output" not in tc.spans[0].tags
