"""Aggregator middleware: collect stage results and reduce them into one value."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional

from .errors import PipeweaveError


class AggregatorError(PipeweaveError):
    """Raised when aggregator configuration is invalid."""


@dataclass
class AggregatorConfig:
    reducer: Callable[[Any, Any], Any]
    initial: Any = None
    stages: List[str] = field(default_factory=list)  # empty = all stages
    emit_partial: bool = False

    def __post_init__(self) -> None:
        if not callable(self.reducer):
            raise AggregatorError("reducer must be callable")


class Aggregator:
    """Accumulates stage outputs and reduces them into a single value."""

    def __init__(self, config: AggregatorConfig) -> None:
        self._config = config
        self._lock = asyncio.Lock()
        self._accumulator: Any = config.initial
        self._partials: List[Any] = []

    @property
    def value(self) -> Any:
        return self._accumulator

    @property
    def partials(self) -> List[Any]:
        return list(self._partials)

    async def feed(self, stage_name: str, result: Any) -> None:
        cfg = self._config
        if cfg.stages and stage_name not in cfg.stages:
            return
        async with self._lock:
            self._accumulator = cfg.reducer(self._accumulator, result)
            if cfg.emit_partial:
                self._partials.append(self._accumulator)

    def reset(self) -> None:
        self._accumulator = self._config.initial
        self._partials.clear()


def make_aggregator_middleware(config: AggregatorConfig, aggregator: Optional[Aggregator] = None):
    """Return middleware that feeds successful stage results into *aggregator*."""
    agg = aggregator or Aggregator(config)

    async def middleware(stage_name: str, context, result, next_):
        outcome = await next_(stage_name, context, result)
        if outcome.success:
            await agg.feed(stage_name, outcome.value)
        return outcome

    middleware._aggregator = agg  # type: ignore[attr-defined]
    return middleware
