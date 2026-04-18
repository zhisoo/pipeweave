"""Tests for pipeweave.splitter."""
from __future__ import annotations

import pytest

from pipeweave.context import PipelineContext, StageResult
from pipeweave.splitter import (
    SplitterConfig,
    SplitterError,
    SplitterRoute,
    make_splitter_middleware,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _ok_result(value=42):
    return StageResult(stage="s", success=True, value=value, error=None)


def _err_result():
    return StageResult(stage="s", success=False, value=None, error=RuntimeError("boom"))


def _make_ctx():
    return PipelineContext(run_id="test")


async def _identity(value, ctx):
    return value


async def _double(value, ctx):
    return value * 2


def _always_true(result):
    return True


def _always_false(result):
    return False


# ---------------------------------------------------------------------------
# SplitterRoute validation
# ---------------------------------------------------------------------------

def test_non_callable_predicate_raises():
    with pytest.raises(SplitterError, match="predicate"):
        SplitterRoute(predicate="bad", handler=_identity)


def test_non_callable_handler_raises():
    with pytest.raises(SplitterError, match="handler"):
        SplitterRoute(predicate=_always_true, handler="bad")


# ---------------------------------------------------------------------------
# SplitterConfig validation
# ---------------------------------------------------------------------------

def test_empty_routes_raises():
    with pytest.raises(SplitterError, match="at least one route"):
        SplitterConfig(routes=[])


def test_non_callable_default_handler_raises():
    route = SplitterRoute(predicate=_always_true, handler=_identity)
    with pytest.raises(SplitterError, match="default_handler"):
        SplitterConfig(routes=[route], default_handler="bad")


def test_valid_config_accepted():
    route = SplitterRoute(predicate=_always_true, handler=_identity, name="r1")
    cfg = SplitterConfig(routes=[route])
    assert len(cfg.routes) == 1
    assert cfg.stop_on_first_match is True


# ---------------------------------------------------------------------------
# middleware behaviour
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_matching_route_transforms_value():
    route = SplitterRoute(predicate=_always_true, handler=_double)
    cfg = SplitterConfig(routes=[route])
    mw = make_splitter_middleware(cfg)

    async def next_(name, data, ctx):
        return _ok_result(10)

    result = await mw("s", 10, _make_ctx(), next_)
    assert result.success
    assert result.value == 20


@pytest.mark.asyncio
async def test_no_match_uses_default_handler():
    route = SplitterRoute(predicate=_always_false, handler=_double)
    cfg = SplitterConfig(routes=[route], default_handler=_double)
    mw = make_splitter_middleware(cfg)

    async def next_(name, data, ctx):
        return _ok_result(5)

    result = await mw("s", 5, _make_ctx(), next_)
    assert result.value == 10


@pytest.mark.asyncio
async def test_error_result_passes_through_unchanged():
    route = SplitterRoute(predicate=_always_true, handler=_double)
    cfg = SplitterConfig(routes=[route])
    mw = make_splitter_middleware(cfg)

    async def next_(name, data, ctx):
        return _err_result()

    result = await mw("s", None, _make_ctx(), next_)
    assert not result.success


@pytest.mark.asyncio
async def test_stop_on_first_match_only_runs_one_route():
    calls = []

    async def handler_a(value, ctx):
        calls.append("a")
        return value

    async def handler_b(value, ctx):
        calls.append("b")
        return value

    routes = [
        SplitterRoute(predicate=_always_true, handler=handler_a),
        SplitterRoute(predicate=_always_true, handler=handler_b),
    ]
    cfg = SplitterConfig(routes=routes, stop_on_first_match=True)
    mw = make_splitter_middleware(cfg)

    async def next_(name, data, ctx):
        return _ok_result(1)

    await mw("s", 1, _make_ctx(), next_)
    assert calls == ["a"]
