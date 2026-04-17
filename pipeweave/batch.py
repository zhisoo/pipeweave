"""Batch processing support for pipeline stages."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, List
import asyncio


@dataclass
class BatchConfig:
    size: int = 10
    timeout: float | None = None  # seconds to wait before flushing partial batch

    def __post_init__(self) -> None:
        if self.size < 1:
            raise ValueError("size must be >= 1")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be > 0")


async def iter_batches(
    source: AsyncIterator[Any],
    config: BatchConfig,
) -> AsyncIterator[List[Any]]:
    """Yield lists of items from *source* according to *config*."""
    batch: List[Any] = []

    async def _next() -> Any:
        return await source.__anext__()

    while True:
        try:
            if config.timeout is not None:
                item = await asyncio.wait_for(_next(), timeout=config.timeout)
            else:
                item = await _next()
            batch.append(item)
            if len(batch) >= config.size:
                yield batch
                batch = []
        except StopAsyncIteration:
            if batch:
                yield batch
            return
        except asyncio.TimeoutError:
            if batch:
                yield batch
                batch = []


class BatchRunner:
    """Run a single async callable over batches from an async iterator."""

    def __init__(self, fn, config: BatchConfig | None = None) -> None:
        self._fn = fn
        self._config = config or BatchConfig()

    async def run(self, source: AsyncIterator[Any]) -> List[Any]:
        results: List[Any] = []
        async for batch in iter_batches(source, self._config):
            result = await self._fn(batch)
            results.append(result)
        return results
