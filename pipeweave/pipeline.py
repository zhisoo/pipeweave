"""Core pipeline module for composing async data transformation stages."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable, List, Optional


@dataclass
class StageConfig:
    """Configuration for a single stage."""

    name: str
    retries: int = 3
    retry_delay: float = 0.5
    timeout: Optional[float] = None


@dataclass
class Pipeline:
    """Composable async data transformation pipeline."""

    stages: List[tuple[StageConfig, Callable]] = field(default_factory=list)

    def pipe(self, config: StageConfig, fn: Callable) -> "Pipeline":
        """Register a transformation stage and return self for chaining."""
        self.stages.append((config, fn))
        return self

    async def _run_stage(
        self, config: StageConfig, fn: Callable, data: Any
    ) -> Any:
        """Execute a single stage with retry and optional timeout logic."""
        last_exc: Optional[Exception] = None

        for attempt in range(1, config.retries + 1):
            try:
                coro = fn(data) if asyncio.iscoroutinefunction(fn) else asyncio.to_thread(fn, data)
                if config.timeout is not None:
                    result = await asyncio.wait_for(coro, timeout=config.timeout)
                else:
                    result = await coro
                return result
            except asyncio.TimeoutError as exc:
                last_exc = exc
                if attempt < config.retries:
                    await asyncio.sleep(config.retry_delay)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt < config.retries:
                    await asyncio.sleep(config.retry_delay)

        raise RuntimeError(
            f"Stage '{config.name}' failed after {config.retries} attempts"
        ) from last_exc

    async def run(self, data: Any) -> Any:
        """Execute all stages sequentially, passing output as next input."""
        current = data
        for config, fn in self.stages:
            current = await self._run_stage(config, fn, current)
        return current

    async def stream(self, items: AsyncIterator[Any]) -> AsyncIterator[Any]:
        """Process a stream of items through the full pipeline."""
        async for item in items:
            yield await self.run(item)

    def __len__(self) -> int:
        """Return the number of stages in the pipeline."""
        return len(self.stages)

    def __repr__(self) -> str:
        """Return a readable summary of the pipeline stages."""
        names = [config.name for config, _ in self.stages]
        return f"Pipeline(stages={names})"
