"""Circuit breaker middleware for pipeweave pipelines."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional

from pipeweave.context import PipelineContext, StageResult
from pipeweave.errors import StageError


class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 1
    exceptions: tuple = (Exception,)

    def __post_init__(self) -> None:
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if self.recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be positive")
        if self.half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be >= 1")


class CircuitBreaker:
    """Per-stage circuit breaker that trips after repeated failures."""

    def __init__(self, config: Optional[CircuitBreakerConfig] = None) -> None:
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._half_open_calls = 0
        self._opened_at: Optional[float] = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    async def _try_recover(self) -> None:
        if (
            self._state == CircuitState.OPEN
            and self._opened_at is not None
            and (time.monotonic() - self._opened_at) >= self.config.recovery_timeout
        ):
            self._state = CircuitState.HALF_OPEN
            self._half_open_calls = 0

    async def before_call(self, stage_name: str) -> None:
        async with self._lock:
            await self._try_recover()
            if self._state == CircuitState.OPEN:
                raise StageError(
                    stage=stage_name,
                    message=f"Circuit breaker OPEN for stage '{stage_name}'",
                )
            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.config.half_open_max_calls:
                    raise StageError(
                        stage=stage_name,
                        message=f"Circuit breaker HALF_OPEN limit reached for '{stage_name}'",
                    )
                self._half_open_calls += 1

    async def on_success(self) -> None:
        async with self._lock:
            self._failure_count = 0
            self._state = CircuitState.CLOSED
            self._opened_at = None

    async def on_failure(self) -> None:
        async with self._lock:
            self._failure_count += 1
            if (
                self._state in (CircuitState.CLOSED, CircuitState.HALF_OPEN)
                and self._failure_count >= self.config.failure_threshold
            ):
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()


def circuit_breaker_middleware(
    breaker: CircuitBreaker,
) -> Callable:
    """Middleware factory that wraps a stage with the given CircuitBreaker."""

    async def middleware(
        ctx: PipelineContext,
        stage_name: str,
        call_next: Callable,
    ) -> StageResult:
        await breaker.before_call(stage_name)
        result: StageResult = await call_next(ctx, stage_name)
        if result.success:
            await breaker.on_success()
        else:
            await breaker.on_failure()
        return result

    return middleware
