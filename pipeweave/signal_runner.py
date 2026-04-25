"""PipelineRunner variant that supports cooperative cancellation via CancellationSignal."""
from __future__ import annotations

from typing import Any, Optional

from pipeweave.context import PipelineContext
from pipeweave.errors import PipelineAbortedError
from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner
from pipeweave.signal import CancellationSignal, SignalConfig, SignalReason


class SignalPipelineRunner:
    """Wraps a PipelineRunner and aborts execution when a CancellationSignal fires."""

    def __init__(
        self,
        pipeline: Pipeline,
        signal: Optional[CancellationSignal] = None,
        signal_config: Optional[SignalConfig] = None,
    ) -> None:
        self._runner = PipelineRunner(pipeline)
        self._signal = signal or CancellationSignal(signal_config)

    @property
    def signal(self) -> CancellationSignal:
        return self._signal

    async def run(self, data: Any, context: Optional[PipelineContext] = None) -> Any:
        if self._signal.is_cancelled:
            raise PipelineAbortedError(
                f"Pipeline cancelled before start: {self._signal.reason}"
            )

        ctx = context or PipelineContext()

        # Inject a pre-stage hook that checks the signal before each stage
        original_pipeline = self._runner._pipeline  # type: ignore[attr-defined]

        async def _guarded_run() -> Any:
            result = data
            for stage in original_pipeline._stages:  # type: ignore[attr-defined]
                if self._signal.is_cancelled:
                    raise PipelineAbortedError(
                        f"Pipeline cancelled at stage '{stage.name}': "
                        f"{self._signal.reason}"
                    )
                result = await self._runner._make_handler(stage)(result, ctx)  # type: ignore[attr-defined]
            return result

        return await _guarded_run()

    def cancel(self, reason: SignalReason = SignalReason.CANCELLED) -> None:
        self._signal.cancel(reason)
