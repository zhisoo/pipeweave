"""Tests for the dead-letter queue."""
from __future__ import annotations

import asyncio
import pytest

from pipeweave.deadletter import DeadLetterEntry, DeadLetterQueue
from pipeweave.context import PipelineContext, StageResult


# ---------------------------------------------------------------------------
# DeadLetterEntry
# ---------------------------------------------------------------------------

def test_entry_repr():
    err = ValueError("boom")
    e = DeadLetterEntry(stage_name="s1", input_data=42, error=err)
    r = repr(e)
    assert "s1" in r
    assert "boom" in r
    assert "attempt=1" in r


# ---------------------------------------------------------------------------
# DeadLetterQueue construction
# ---------------------------------------------------------------------------

def test_default_max_size_accepted():
    dlq = DeadLetterQueue()
    assert dlq.size == 0


def test_zero_max_size_raises():
    with pytest.raises(ValueError):
        DeadLetterQueue(max_size=0)


def test_negative_max_size_raises():
    with pytest.raises(ValueError):
        DeadLetterQueue(max_size=-1)


# ---------------------------------------------------------------------------
# push / drain
# ---------------------------------------------------------------------------

def _entry(name="stage", data=None, error=None):
    return DeadLetterEntry(
        stage_name=name,
        input_data=data,
        error=error or RuntimeError("fail"),
    )


def test_push_and_size():
    dlq = DeadLetterQueue(max_size=10)
    asyncio.get_event_loop().run_until_complete(dlq.push(_entry()))
    assert dlq.size == 1


def test_drain_clears_queue():
    dlq = DeadLetterQueue(max_size=10)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dlq.push(_entry("a")))
    loop.run_until_complete(dlq.push(_entry("b")))
    items = loop.run_until_complete(dlq.drain())
    assert len(items) == 2
    assert dlq.size == 0


def test_overflow_evicts_oldest():
    dlq = DeadLetterQueue(max_size=2)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dlq.push(_entry("first")))
    loop.run_until_complete(dlq.push(_entry("second")))
    loop.run_until_complete(dlq.push(_entry("third")))
    assert dlq.size == 2
    assert dlq.entries[0].stage_name == "second"


# ---------------------------------------------------------------------------
# middleware
# ---------------------------------------------------------------------------

def _make_ctx():
    return PipelineContext()


def _ok_result(name="s"):
    return StageResult(stage_name=name, value=1, success=True)


def _err_result(name="s", err=None):
    return StageResult(stage_name=name, value=None, success=False, error=err or ValueError("x"))


def test_middleware_captures_failure():
    dlq = DeadLetterQueue()
    mw = dlq.make_middleware()
    ctx = _make_ctx()
    incoming = _ok_result("s1")
    failing = _err_result("s1")

    async def call_next(c, r):
        return failing

    asyncio.get_event_loop().run_until_complete(mw(ctx, incoming, call_next))
    assert dlq.size == 1
    assert dlq.entries[0].stage_name == "s1"


def test_middleware_ignores_success():
    dlq = DeadLetterQueue()
    mw = dlq.make_middleware()
    ctx = _make_ctx()
    incoming = _ok_result("s1")
    success = _ok_result("s1")

    async def call_next(c, r):
        return success

    asyncio.get_event_loop().run_until_complete(mw(ctx, incoming, call_next))
    assert dlq.size == 0
