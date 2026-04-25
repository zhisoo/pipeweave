"""PipelineRunner that emits lifecycle events via EventBus."""
from __future__ import annotations

from typing import Optional

from pipeweave.context import PipelineContext
from pipeweave.event_bus import EventBus, EventBusConfig
from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner


class EventBusPipelineRunner:
    """Wraps :class:`PipelineRunner` and fires stage lifecycle events.

    Events emitted
    --------------
    ``stage.before``  – before each stage handler is invoked.
    ``stage.after``   – after a stage completes successfully.
    ``stage.error``   – when a stage raises an unhandled exception.
    ``pipeline.done`` – after the full pipeline finishes.
    """

    def __init__(
        self,
        pipeline: Pipeline,
        bus: Optional[EventBus] = None,
        bus_config: Optional[EventBusConfig] = None,
    ) -> None:
        self._pipeline = pipeline
        self._bus = bus if bus is not None else EventBus(bus_config or EventBusConfig())
        self._runner = PipelineRunner(self._wrap(pipeline))

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _wrap(self, pipeline: Pipeline) -> Pipeline:
        """Return a new Pipeline whose stages emit bus events."""
        from pipeweave.pipeline import Pipeline, StageConfig
        from pipeweave.stage import Stage

        wrapped = Pipeline()
        for stage in pipeline._stages:  # type: ignore[attr-defined]
            original_fn = stage.fn
            bus = self._bus
            name = stage.name

            async def _handler(data, _fn=original_fn, _name=name, _bus=bus):
                await _bus.publish("stage.before", stage=_name, data=data)
                try:
                    result = await _fn(data)
                    await _bus.publish("stage.after", stage=_name, result=result)
                    return result
                except Exception as exc:
                    await _bus.publish("stage.error", stage=_name, error=exc)
                    raise

            wrapped.pipe(_handler, config=stage.config, name=name)
        return wrapped

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def bus(self) -> EventBus:
        """The underlying :class:`EventBus` instance."""
        return self._bus

    async def run(self, data, context: Optional[PipelineContext] = None):
        """Run the pipeline and emit ``pipeline.done`` when finished."""
        result = await self._runner.run(data, context)
        await self._bus.publish("pipeline.done", result=result)
        return result
