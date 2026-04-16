"""Convenience runner that wires FallbackConfig into every stage."""
from __future__ import annotations

from typing import Any, Dict, Optional

from pipeweave.fallback import FallbackConfig, make_fallback_middleware
from pipeweave.middleware import MiddlewareChain
from pipeweave.pipeline import Pipeline
from pipeweave.runner import PipelineRunner


class FallbackPipelineRunner(PipelineRunner):
    """PipelineRunner that applies a FallbackConfig to all stages."""

    def __init__(
        self,
        pipeline: Pipeline,
        fallback_config: Optional[FallbackConfig] = None,
        middleware: Optional[MiddlewareChain] = None,
    ) -> None:
        config = fallback_config or FallbackConfig()
        fb_mw = make_fallback_middleware(config)

        chain = middleware or MiddlewareChain()
        chain.add(fb_mw)

        super().__init__(pipeline, middleware=chain)
        self._fallback_config = fallback_config

    @property
    def fallback_config(self) -> Optional[FallbackConfig]:
        return self._fallback_config
