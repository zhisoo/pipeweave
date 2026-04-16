"""Tests for pipeweave.fallback and FallbackPipelineRunner."""
from __future__ import annotations

import pytest

from pipeweave.context import PipelineContext, StageResult
from pipeweave.fallback import FallbackConfig, make_fallback_middleware
from pipeweave.fallback_runner import FallbackPipelineRunner
from pipeweave.pipeline import Pipeline, StageConfig
from pipeweave.stage import Stage


# ---------------------------------------------------------------------------
# FallbackConfig validation
# ---------------------------------------------------------------------------

def test_default_config_accepted():
    cfg = FallbackConfig()
    assert cfg.default_value is None
    assert cfg.handler is None
    assert cfg.mark_success is True


def test_non_callable_handler_raises():
    with pytest.raises(TypeError):
        FallbackConfig(handler="not_callable")


# ---------------------------------------------------------------------------
# make_fallback_middleware
# ---------------------------------------------------------------------------

def _make_ctx():
    return PipelineContext()


def _ok_result(name="s"):
    return StageResult(stage_name=name, success=True, value=42)


def _err_result(name="s", exc=None):
    return StageResult(stage_name=name, success=False, error=exc or ValueError("boom"))


@pytest.mark.asyncio
async def test_success_passes_through():
    mw = make_fallback_middleware(FallbackConfig(default_value=-1))
    ctx = _make_ctx()

    async def call_next(name, c):
        return _ok_result(name)

    result = await mw("s", call_next, ctx)
    assert result.success
    assert result.value == 42


@pytest.mark.asyncio
async def test_static_fallback_on_error():
    mw = make_fallback_middleware(FallbackConfig(default_value=99))
    ctx = _make_ctx()

    async def call_next(name, c):
        return _err_result(name)

    result = await mw("s", call_next, ctx)
    assert result.success
    assert result.value == 99


@pytest.mark.asyncio
async def test_dynamic_handler_called_with_error():
    received = {}

    async def handler(error, ctx):
        received["error"] = error
        return "dynamic"

    mw = make_fallback_middleware(FallbackConfig(handler=handler))
    ctx = _make_ctx()
    err = RuntimeError("oops")

    async def call_next(name, c):
        return _err_result(name, exc=err)

    result = await mw("s", call_next, ctx)
    assert result.value == "dynamic"
    assert received["error"] is err


@pytest.mark.asyncio
async def test_mark_success_false_preserves_error():
    mw = make_fallback_middleware(FallbackConfig(default_value=0, mark_success=False))
    ctx = _make_ctx()

    async def call_next(name, c):
        return _err_result(name)

    result = await mw("s", call_next, ctx)
    assert not result.success
    assert result.value == 0
    assert result.error is not None


# ---------------------------------------------------------------------------
# FallbackPipelineRunner integration
# ---------------------------------------------------------------------------

def build_pipeline():
    async def fail_stage(value, ctx):
        raise ValueError("intentional")

    p = Pipeline()
    p.add_stage(Stage("fail", fail_stage, StageConfig()))
    return p


@pytest.mark.asyncio
async def test_runner_uses_fallback():
    runner = FallbackPipelineRunner(
        build_pipeline(), fallback_config=FallbackConfig(default_value="safe")
    )
    ctx = await runner.run("input")
    result = ctx.last_result()
    assert result is not None
    assert result.value == "safe"
