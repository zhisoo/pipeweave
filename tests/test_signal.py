"""Tests for pipeweave.signal."""
import pytest

from pipeweave.signal import CancellationSignal, SignalConfig, SignalReason


def test_default_config_accepted():
    cfg = SignalConfig()
    assert cfg.propagate is True
    assert cfg.reason == SignalReason.CANCELLED


def test_custom_config_accepted():
    cfg = SignalConfig(propagate=False, reason=SignalReason.TIMEOUT)
    assert cfg.propagate is False
    assert cfg.reason == SignalReason.TIMEOUT


def test_invalid_reason_raises():
    with pytest.raises(ValueError, match="reason must be a SignalReason"):
        SignalConfig(reason="bad")  # type: ignore[arg-type]


def test_initial_state_not_cancelled():
    sig = CancellationSignal()
    assert not sig.is_cancelled
    assert sig.reason is None


def test_cancel_sets_cancelled():
    sig = CancellationSignal()
    sig.cancel()
    assert sig.is_cancelled
    assert sig.reason == SignalReason.CANCELLED


def test_cancel_with_reason():
    sig = CancellationSignal()
    sig.cancel(SignalReason.TIMEOUT)
    assert sig.reason == SignalReason.TIMEOUT


def test_cancel_idempotent():
    sig = CancellationSignal()
    sig.cancel(SignalReason.CANCELLED)
    sig.cancel(SignalReason.TIMEOUT)  # second call should be ignored
    assert sig.reason == SignalReason.CANCELLED


def test_on_cancel_callback_called():
    sig = CancellationSignal()
    received = []
    sig.on_cancel(lambda r: received.append(r))
    sig.cancel(SignalReason.EXTERNAL)
    assert received == [SignalReason.EXTERNAL]


def test_on_cancel_immediate_if_already_cancelled():
    sig = CancellationSignal()
    sig.cancel(SignalReason.TIMEOUT)
    received = []
    sig.on_cancel(lambda r: received.append(r))
    assert received == [SignalReason.TIMEOUT]


def test_on_cancel_non_callable_raises():
    sig = CancellationSignal()
    with pytest.raises(ValueError, match="callable"):
        sig.on_cancel("not-callable")  # type: ignore[arg-type]


def test_reset_clears_state():
    sig = CancellationSignal()
    sig.cancel()
    sig.reset()
    assert not sig.is_cancelled
    assert sig.reason is None


@pytest.mark.asyncio
async def test_wait_resolves_on_cancel():
    import asyncio

    sig = CancellationSignal()

    async def _cancel_soon():
        await asyncio.sleep(0.01)
        sig.cancel(SignalReason.EXTERNAL)

    asyncio.create_task(_cancel_soon())
    reason = await sig.wait()
    assert reason == SignalReason.EXTERNAL
