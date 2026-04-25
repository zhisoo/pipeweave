"""Tests for pipeweave.signal_runner."""
import asyncio

import pytest

from pipeweave.errors import PipelineAbortedError
from pipeweave.pipeline import Pipeline, StageConfig
from pipeweave.signal import CancellationSignal, SignalReason
from pipeweave.signal_runner import SignalPipelineRunner


def build_pipeline() -> Pipeline:
    async def double(x: int) -> int:
        return x * 2

    async def add_one(x: int) -> int:
        return x + 1

    p = Pipeline("test")
    p.pipe(double, StageConfig(name="double"))
    p.pipe(add_one, StageConfig(name="add_one"))
    return p


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_normal_run_succeeds():
    runner = SignalPipelineRunner(build_pipeline())
    result = await runner.run(3)
    assert result == 7  # (3*2)+1


@pytest.mark.asyncio
async def test_pre_cancelled_raises_before_start():
    sig = CancellationSignal()
    sig.cancel(SignalReason.EXTERNAL)
    runner = SignalPipelineRunner(build_pipeline(), signal=sig)
    with pytest.raises(PipelineAbortedError, match="before start"):
        await runner.run(1)


@pytest.mark.asyncio
async def test_cancel_mid_run_raises():
    sig = CancellationSignal()
    call_count = 0

    async def slow_stage(x: int) -> int:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            sig.cancel(SignalReason.TIMEOUT)
        return x

    p = Pipeline("mid_cancel")
    p.pipe(slow_stage, StageConfig(name="s1"))
    p.pipe(slow_stage, StageConfig(name="s2"))

    runner = SignalPipelineRunner(p, signal=sig)
    with pytest.raises(PipelineAbortedError, match="cancelled at stage"):
        await runner.run(5)

    assert call_count == 1


@pytest.mark.asyncio
async def test_signal_property_exposed():
    sig = CancellationSignal()
    runner = SignalPipelineRunner(build_pipeline(), signal=sig)
    assert runner.signal is sig


@pytest.mark.asyncio
async def test_cancel_method_delegates_to_signal():
    runner = SignalPipelineRunner(build_pipeline())
    runner.cancel(SignalReason.EXTERNAL)
    assert runner.signal.is_cancelled
    assert runner.signal.reason == SignalReason.EXTERNAL
