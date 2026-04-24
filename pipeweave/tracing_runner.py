"""Pipeline runner with built-in distributed tracing."""
from __future__ import annotations

from typing import Optional

from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner
from pipeweave.tracing import SpanConfig, TraceContext, make_tracing_middleware


class TracingPipelineRunner(PipelineRunner):
    """A :class:`PipelineRunner` that attaches a tracing middleware to every run.

    A fresh :class:`TraceContext` is created for each :meth:`run` call so that
    spans from different runs are never mixed together.
    """

    def __init__(self, pipeline: Pipeline, config: Optional[SpanConfig] = None) -> None:
        super().__init__(pipeline)
        self._config = config or SpanConfig()
        self._last_trace: Optional[TraceContext] = None

    @property
    def last_trace(self) -> Optional[TraceContext]:
        """The :class:`TraceContext` produced by the most recent :meth:`run` call."""
        return self._last_trace

    async def run(self, input_value):
        trace_ctx = TraceContext()
        self._last_trace = trace_ctx
        mw = make_tracing_middleware(self._config, trace_ctx)
        # Temporarily prepend tracing middleware for this run.
        original_chain = self._pipeline._middleware  # type: ignore[attr-defined]
        from pipeweave.middleware import MiddlewareChain
        patched_chain = MiddlewareChain()
        patched_chain.add(mw)
        for existing in original_chain._middlewares:  # type: ignore[attr-defined]
            patched_chain.add(existing)
        old_mw = self._pipeline._middleware  # type: ignore[attr-defined]
        self._pipeline._middleware = patched_chain  # type: ignore[attr-defined]
        try:
            return await super().run(input_value)
        finally:
            self._pipeline._middleware = old_mw  # type: ignore[attr-defined]
