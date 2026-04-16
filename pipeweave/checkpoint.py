"""Checkpoint middleware: persist and restore pipeline stage results."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from pipeweave.context import PipelineContext, StageResult


@dataclass
class CheckpointStore:
    """In-memory store for stage checkpoints keyed by (run_id, stage_name)."""

    _data: Dict[tuple, Any] = field(default_factory=dict, init=False, repr=False)

    def save(self, run_id: str, stage: str, value: Any) -> None:
        self._data[(run_id, stage)] = value

    def load(self, run_id: str, stage: str) -> Optional[Any]:
        return self._data.get((run_id, stage))

    def has(self, run_id: str, stage: str) -> bool:
        return (run_id, stage) in self._data

    def clear(self, run_id: Optional[str] = None) -> None:
        if run_id is None:
            self._data.clear()
        else:
            keys = [k for k in self._data if k[0] == run_id]
            for k in keys:
                del self._data[k]

    def __len__(self) -> int:
        return len(self._data)


def make_checkpoint_middleware(
    store: CheckpointStore,
    run_id_key: str = "run_id",
    stages: Optional[list] = None,
) -> Callable:
    """Return middleware that saves/restores stage outputs via *store*.

    If a checkpoint exists for (run_id, stage_name) the handler is skipped and
    the stored value is returned directly, enabling pipeline resumption.
    """

    async def checkpoint_middleware(ctx: PipelineContext, stage_name: str, handler):
        run_id: str = ctx.get(run_id_key, "default")

        if stages is not None and stage_name not in stages:
            return await handler(ctx, stage_name)

        if store.has(run_id, stage_name):
            cached = store.load(run_id, stage_name)
            return StageResult(stage=stage_name, output=cached, success=True)

        result: StageResult = await handler(ctx, stage_name)

        if result.success:
            store.save(run_id, stage_name, result.output)

        return result

    return checkpoint_middleware
