"""Tests for pipeweave.stage.Stage."""

from __future__ import annotations

import asyncio

import pytest

from pipeweave.errors import StageError
from pipeweave.retry import RetryPolicy
from pipeweave.stage import Stage
from pipeweave.throttle import ThrottleConfig


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stage_returns_result():
    async def double(x: int) -> int:
        return x * 2

    stage = Stage("double", double)
    assert await stage.run(21) == 42


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stage_retries_on_failure():
    calls: list[int] = []

    async def flaky(x: int) -> int:
        calls.append(1)
        if len(calls) < 3:
            raise ValueError("not yet")
        return x

    policy = RetryPolicy(max_attempts=3, base_delay=0.0)
    stage = Stage("flaky", flaky, retry_policy=policy)
    result = await stage.run(7)
    assert result == 7
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_stage_raises_stage_error_after_exhausting_retries():
    async def always_fail() -> None:
        raise RuntimeError("boom")

    policy = RetryPolicy(max_attempts=2, base_delay=0.0)
    stage = Stage("bad", always_fail, retry_policy=policy)

    with pytest.raises(StageError) as exc_info:
        await stage.run()

    assert exc_info.value.stage_name == "bad"


# ---------------------------------------------------------------------------
# Throttle integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stage_with_concurrency_throttle():
    active: list[int] = []
    peak: list[int] = []

    async def work(_: int) -> int:
        active.append(1)
        peak.append(len(active))
        await asyncio.sleep(0.01)
        active.pop()
        return 1

    cfg = ThrottleConfig(max_concurrency=2)
    stage = Stage("work", work, throttle_config=cfg)
    await asyncio.gather(*[stage.run(i) for i in range(6)])
    assert max(peak) <= 2
