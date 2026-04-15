"""Tests for pipeweave.circuit_breaker."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from pipeweave.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    circuit_breaker_middleware,
)
from pipeweave.context import PipelineContext, StageResult
from pipeweave.errors import StageError


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def test_default_config_values():
    cfg = CircuitBreakerConfig()
    assert cfg.failure_threshold == 5
    assert cfg.recovery_timeout == 30.0
    assert cfg.half_open_max_calls == 1


def test_invalid_failure_threshold_raises():
    with pytest.raises(ValueError, match="failure_threshold"):
        CircuitBreakerConfig(failure_threshold=0)


def test_invalid_recovery_timeout_raises():
    with pytest.raises(ValueError, match="recovery_timeout"):
        CircuitBreakerConfig(recovery_timeout=0)


def test_invalid_half_open_max_calls_raises():
    with pytest.raises(ValueError, match="half_open_max_calls"):
        CircuitBreakerConfig(half_open_max_calls=0)


# ---------------------------------------------------------------------------
# CircuitBreaker state transitions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_initial_state_is_closed():
    cb = CircuitBreaker()
    assert cb.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_opens_after_threshold_failures():
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))
    for _ in range(3):
        await cb.on_failure()
    assert cb.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_rejects_calls_when_open():
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1))
    await cb.on_failure()
    assert cb.state == CircuitState.OPEN
    with pytest.raises(StageError, match="OPEN"):
        await cb.before_call("my_stage")


@pytest.mark.asyncio
async def test_success_resets_to_closed():
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=2))
    await cb.on_failure()
    await cb.on_success()
    assert cb.state == CircuitState.CLOSED
    assert cb._failure_count == 0


@pytest.mark.asyncio
async def test_transitions_to_half_open_after_timeout(monkeypatch):
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1, recovery_timeout=1.0))
    await cb.on_failure()
    assert cb.state == CircuitState.OPEN

    # Simulate time passing beyond recovery_timeout
    monkeypatch.setattr(
        "pipeweave.circuit_breaker.time.monotonic",
        lambda: cb._opened_at + 2.0,  # type: ignore[operator]
    )
    await cb.before_call("stage")
    assert cb.state == CircuitState.HALF_OPEN


@pytest.mark.asyncio
async def test_half_open_success_closes_circuit(monkeypatch):
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1, recovery_timeout=1.0))
    await cb.on_failure()
    monkeypatch.setattr(
        "pipeweave.circuit_breaker.time.monotonic",
        lambda: cb._opened_at + 2.0,  # type: ignore[operator]
    )
    await cb.before_call("stage")
    await cb.on_success()
    assert cb.state == CircuitState.CLOSED


# ---------------------------------------------------------------------------
# Middleware integration
# ---------------------------------------------------------------------------

def _make_ctx() -> PipelineContext:
    return PipelineContext()


@pytest.mark.asyncio
async def test_middleware_passes_through_on_success():
    cb = CircuitBreaker()
    mw = circuit_breaker_middleware(cb)
    ok_result = StageResult(stage="s", success=True, output=42)
    call_next = AsyncMock(return_value=ok_result)

    result = await mw(_make_ctx(), "s", call_next)
    assert result.success is True
    assert cb.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_middleware_increments_failures_on_error():
    cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))
    mw = circuit_breaker_middleware(cb)
    err_result = StageResult(stage="s", success=False, error=Exception("boom"))
    call_next = AsyncMock(return_value=err_result)

    for _ in range(3):
        await mw(_make_ctx(), "s", call_next)
    assert cb.state == CircuitState.OPEN
