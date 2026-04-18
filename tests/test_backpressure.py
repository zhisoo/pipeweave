"""Tests for pipeweave.backpressure."""
from __future__ import annotations

import asyncio
import pytest

from pipeweave.backpressure import BackpressureConfig, BackpressureController, make_backpressure_middleware


def test_valid_config_accepted():
    cfg = BackpressureConfig(high_watermark=10, low_watermark=5)
    assert cfg.high_watermark == 10
    assert cfg.low_watermark == 5


def test_zero_high_watermark_raises():
    with pytest.raises(ValueError, match="high_watermark"):
        BackpressureConfig(high_watermark=0, low_watermark=5)


def test_zero_low_watermark_raises():
    with pytest.raises(ValueError, match="low_watermark must be positive"):
        BackpressureConfig(high_watermark=10, low_watermark=0)


def test_low_ge_high_raises():
    with pytest.raises(ValueError, match="low_watermark must be less than"):
        BackpressureConfig(high_watermark=5, low_watermark=5)


def test_negative_timeout_raises():
    with pytest.raises(ValueError, match="timeout"):
        BackpressureConfig(high_watermark=10, low_watermark=5, timeout=-1.0)


def test_increment_and_decrement():
    ctrl = BackpressureController(BackpressureConfig(high_watermark=3, low_watermark=1))
    assert ctrl.pending == 0
    ctrl.increment()
    assert ctrl.pending == 1
    ctrl.decrement()
    assert ctrl.pending == 0


def test_pauses_at_high_watermark():
    ctrl = BackpressureController(BackpressureConfig(high_watermark=2, low_watermark=1))
    ctrl.increment()
    ctrl.increment()  # hits high watermark
    assert not ctrl._open.is_set()


def test_resumes_at_low_watermark():
    ctrl = BackpressureController(BackpressureConfig(high_watermark=2, low_watermark=1))
    ctrl.increment()
    ctrl.increment()  # paused
    ctrl.decrement()  # back to low watermark -> resume
    assert ctrl._open.is_set()


@pytest.mark.asyncio
async def test_wait_for_capacity_returns_true_when_open():
    ctrl = BackpressureController(BackpressureConfig(high_watermark=5, low_watermark=2))
    result = await ctrl.wait_for_capacity()
    assert result is True


@pytest.mark.asyncio
async def test_wait_for_capacity_times_out():
    ctrl = BackpressureController(BackpressureConfig(high_watermark=2, low_watermark=1, timeout=0.05))
    ctrl.increment()
    ctrl.increment()  # paused
    result = await ctrl.wait_for_capacity()
    assert result is False


@pytest.mark.asyncio
async def test_middleware_decrements_after_call():
    ctrl = BackpressureController(BackpressureConfig(high_watermark=10, low_watermark=5))
    mw = make_backpressure_middleware(ctrl)

    async def call_next(ctx, result):
        assert ctrl.pending == 1
        return result

    await mw(object(), object(), call_next)
    assert ctrl.pending == 0
