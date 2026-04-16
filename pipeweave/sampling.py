"""Sampling middleware for pipeweave pipelines."""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable, Optional

from pipeweave.errors import PipeweaveError


@dataclass
class SamplingConfig:
    """Configuration for pipeline stage sampling."""

    rate: float = 1.0  # 0.0 to 1.0
    seed: Optional[int] = None
    on_dropped: Optional[Callable] = None

    def __post_init__(self) -> None:
        if not (0.0 <= self.rate <= 1.0):
            raise PipeweaveError(
                f"rate must be between 0.0 and 1.0, got {self.rate}"
            )


def make_sampling_middleware(config: SamplingConfig):
    """Return middleware that randomly drops stage results based on sample rate."""
    rng = random.Random(config.seed)

    async def sampling_middleware(ctx, call_next):
        result = await call_next(ctx)

        if result.success and rng.random() > config.rate:
            # Drop the result — pass through a sentinel None value
            if config.on_dropped is not None:
                if hasattr(config.on_dropped, "__call__"):
                    try:
                        import asyncio
                        if asyncio.iscoroutinefunction(config.on_dropped):
                            await config.on_dropped(ctx, result)
                        else:
                            config.on_dropped(ctx, result)
                    except Exception:
                        pass
            return result.__class__(
                stage_name=result.stage_name,
                success=True,
                value=None,
                error=None,
                sampled=False,
            )

        return result

    return sampling_middleware
