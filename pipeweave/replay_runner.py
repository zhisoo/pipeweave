"""PipelineRunner subclass with replay support."""
from __future__ import annotations

import uuid
from typing import Any, Optional

from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner
from pipeweave.replay import ReplayBuffer, ReplayConfig


class ReplayPipelineRunner(PipelineRunner):
    """Runs a pipeline and records each input for later replay."""

    def __init__(self, pipeline: Pipeline, config: Optional[ReplayConfig] = None) -> None:
        super().__init__(pipeline)
        self._buffer = ReplayBuffer(config or ReplayConfig())

    @property
    def replay_buffer(self) -> ReplayBuffer:
        return self._buffer

    async def run(self, input: Any, run_id: Optional[str] = None) -> Any:  # type: ignore[override]
        rid = run_id or str(uuid.uuid4())
        self._buffer.record(rid, input)
        return await super().run(input)

    async def replay(self, run_id: str) -> Any:
        """Re-run the pipeline using the recorded input for *run_id*."""
        entry = self._buffer.get(run_id)
        if entry is None:
            raise KeyError(f"No recorded input for run_id={run_id!r}")
        return await super().run(entry.input)
