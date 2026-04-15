"""Tests for pipeweave.timeout."""
from __future__ import annotations

import asyncio
import pytest

from pipeweave.timeout import TimeoutConfig, run_with_timeout, timeout_middleware
from pipeweave.errors import StageError


# ---------------------------------------------------------------------------
# TimeoutConfig
# ---------------------------------------------------------------------------

class TestTimeoutConfig:
    def test_defaults_are_none(self):
        cfg = TimeoutConfig()
        assert cfg.stage_timeout is None
        assert cfg.pipeline_timeout is None

    def test_valid_values_accepted(self):
        cfg = TimeoutConfig(stage_timeout=1.5, pipeline_timeout=10.0)
        assert cfg.stage_timeout == 1.5
        assert cfg.pipeline_timeout == 10.0

    def test_zero_stage_timeout_raises(self):
        with pytest.raises(ValueError, match="stage_timeout"):
            TimeoutConfig(stage_timeout=0)

    def test_negative_stage_timeout_raises(self):
        with pytest.raises(ValueError, match="stage_timeout"):
            TimeoutConfig(stage_timeout=-1.0)

    def test_zero_pipeline_timeout_raises(self):
        with pytest.raises(ValueError, match="pipeline_timeout"):
            TimeoutConfig(pipeline_timeout=0)


# ---------------------------------------------------------------------------
# run_with_timeout
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_with_timeout_no_limit():
    async def fast():
        return 42

    result = await run_with_timeout(fast(), timeout=None)
    assert result == 42


@pytest.mark.asyncio
async def test_run_with_timeout_completes_in_time():
    async def fast():
        return "ok"

    result = await run_with_timeout(fast(), timeout=5.0, stage_name="s1")
    assert result == "ok"


@pytest.mark.asyncio
async def test_run_with_timeout_raises_stage_error_on_expiry():
    async def slow():
        await asyncio.sleep(10)
        return "never"

    with pytest.raises(StageError) as exc_info:
        await run_with_timeout(slow(), timeout=0.05, stage_name="slow_stage")

    assert "slow_stage" in str(exc_info.value)
    assert "timed out" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# timeout_middleware
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_timeout_middleware_passes_through_fast_stage():
    mw = timeout_middleware(stage_timeout=5.0)

    async def call_next(ctx):
        return ctx["value"] * 2

    result = await mw("double", call_next, {"value": 21})
    assert result == 42


@pytest.mark.asyncio
async def test_timeout_middleware_raises_on_slow_stage():
    mw = timeout_middleware(stage_timeout=0.05)

    async def call_next(ctx):
        await asyncio.sleep(10)
        return "never"

    with pytest.raises(StageError):
        await mw("slow", call_next, {})


@pytest.mark.asyncio
async def test_timeout_middleware_no_limit_allows_any_duration():
    mw = timeout_middleware(stage_timeout=None)

    async def call_next(ctx):
        # A tiny sleep is fine; no timeout guard active
        await asyncio.sleep(0.01)
        return "done"

    result = await mw("any", call_next, {})
    assert result == "done"
