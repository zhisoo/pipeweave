"""Distributed tracing support for pipeline stages."""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipeweave.context import PipelineContext, StageResult


@dataclass
class SpanConfig:
    """Configuration for tracing spans."""

    service_name: str = "pipeweave"
    include_input: bool = False
    include_output: bool = False
    on_span_finish: Optional[Callable[["Span"], None]] = None

    def __post_init__(self) -> None:
        if not self.service_name or not self.service_name.strip():
            raise ValueError("service_name must be a non-empty string")


@dataclass
class Span:
    """Represents a single tracing span for a pipeline stage."""

    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    stage_name: str
    service_name: str
    started_at: float = field(default_factory=time.monotonic)
    finished_at: Optional[float] = None
    success: Optional[bool] = None
    tags: Dict[str, object] = field(default_factory=dict)

    def finish(self, *, success: bool) -> None:
        self.finished_at = time.monotonic()
        self.success = success

    @property
    def duration_ms(self) -> Optional[float]:
        if self.finished_at is None:
            return None
        return (self.finished_at - self.started_at) * 1_000

    def __repr__(self) -> str:  # pragma: no cover
        status = "ok" if self.success else "err"
        dur = f"{self.duration_ms:.2f}ms" if self.duration_ms is not None else "ongoing"
        return f"<Span {self.stage_name} [{status}] {dur}>"


class TraceContext:
    """Holds all spans for a single pipeline run."""

    def __init__(self, trace_id: Optional[str] = None) -> None:
        self.trace_id: str = trace_id or uuid.uuid4().hex
        self._spans: List[Span] = []

    def new_span(self, stage_name: str, service_name: str,
                 parent_span_id: Optional[str] = None) -> Span:
        span = Span(
            trace_id=self.trace_id,
            span_id=uuid.uuid4().hex,
            parent_span_id=parent_span_id,
            stage_name=stage_name,
            service_name=service_name,
        )
        self._spans.append(span)
        return span

    @property
    def spans(self) -> List[Span]:
        return list(self._spans)


def make_tracing_middleware(config: SpanConfig, trace_ctx: TraceContext):
    """Return middleware that records a span for each stage execution."""

    async def tracing_middleware(
        ctx: PipelineContext,
        result: StageResult,
        call_next: Callable,
    ) -> StageResult:
        span = trace_ctx.new_span(
            stage_name=ctx.stage_name,
            service_name=config.service_name,
        )
        if config.include_input:
            span.tags["input"] = repr(result.value)

        outcome: StageResult = await call_next(ctx, result)

        span.finish(success=outcome.success)
        if config.include_output and outcome.success:
            span.tags["output"] = repr(outcome.value)
        if config.on_span_finish:
            config.on_span_finish(span)
        return outcome

    return tracing_middleware
