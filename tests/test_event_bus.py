"""Tests for pipeweave.event_bus."""
from __future__ import annotations

import pytest

from pipeweave.event_bus import EventBus, EventBusConfig


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def test_default_config_accepted():
    cfg = EventBusConfig()
    assert cfg.max_listeners_per_event == 64
    assert cfg.propagate_listener_errors is False


def test_zero_max_listeners_raises():
    with pytest.raises(ValueError):
        EventBusConfig(max_listeners_per_event=0)


def test_negative_max_listeners_raises():
    with pytest.raises(ValueError):
        EventBusConfig(max_listeners_per_event=-1)


# ---------------------------------------------------------------------------
# Subscribe / unsubscribe
# ---------------------------------------------------------------------------

def test_subscribe_non_callable_raises():
    bus = EventBus()
    with pytest.raises(TypeError):
        bus.subscribe("evt", "not-callable")  # type: ignore


def test_listener_count_zero_for_unknown_event():
    bus = EventBus()
    assert bus.listener_count("unknown") == 0


def test_listener_count_increments_on_subscribe():
    bus = EventBus()

    async def _ln(**kw): pass

    bus.subscribe("evt", _ln)
    assert bus.listener_count("evt") == 1


def test_unsubscribe_removes_listener():
    bus = EventBus()

    async def _ln(**kw): pass

    bus.subscribe("evt", _ln)
    bus.unsubscribe("evt", _ln)
    assert bus.listener_count("evt") == 0


def test_unsubscribe_missing_listener_is_silent():
    bus = EventBus()

    async def _ln(**kw): pass

    bus.unsubscribe("evt", _ln)  # should not raise


def test_listener_cap_raises():
    bus = EventBus(EventBusConfig(max_listeners_per_event=2))

    async def _ln(**kw): pass

    bus.subscribe("evt", _ln)
    bus.subscribe("evt", _ln)
    with pytest.raises(RuntimeError, match="cap"):
        bus.subscribe("evt", _ln)


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_publish_calls_all_listeners():
    bus = EventBus()
    calls = []

    async def _a(**kw): calls.append("a")
    async def _b(**kw): calls.append("b")

    bus.subscribe("evt", _a)
    bus.subscribe("evt", _b)
    await bus.publish("evt")
    assert sorted(calls) == ["a", "b"]


@pytest.mark.asyncio
async def test_publish_passes_kwargs():
    bus = EventBus()
    received = {}

    async def _ln(**kw): received.update(kw)

    bus.subscribe("evt", _ln)
    await bus.publish("evt", stage="s1", data=42)
    assert received == {"stage": "s1", "data": 42}


@pytest.mark.asyncio
async def test_publish_no_listeners_is_silent():
    bus = EventBus()
    await bus.publish("no_one_listening")  # should not raise


@pytest.mark.asyncio
async def test_listener_error_suppressed_by_default():
    bus = EventBus()

    async def _bad(**kw): raise RuntimeError("boom")

    bus.subscribe("evt", _bad)
    await bus.publish("evt")  # should not propagate


@pytest.mark.asyncio
async def test_listener_error_propagated_when_configured():
    bus = EventBus(EventBusConfig(propagate_listener_errors=True))

    async def _bad(**kw): raise RuntimeError("boom")

    bus.subscribe("evt", _bad)
    with pytest.raises(RuntimeError, match="boom"):
        await bus.publish("evt")


# ---------------------------------------------------------------------------
# Clear
# ---------------------------------------------------------------------------

def test_clear_specific_event():
    bus = EventBus()

    async def _ln(**kw): pass

    bus.subscribe("a", _ln)
    bus.subscribe("b", _ln)
    bus.clear("a")
    assert bus.listener_count("a") == 0
    assert bus.listener_count("b") == 1


def test_clear_all_events():
    bus = EventBus()

    async def _ln(**kw): pass

    bus.subscribe("a", _ln)
    bus.subscribe("b", _ln)
    bus.clear()
    assert bus.listener_count("a") == 0
    assert bus.listener_count("b") == 0
