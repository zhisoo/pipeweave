"""Tests for pipeweave.replay and pipeweave.replay_runner."""
import pytest

from pipeweave.replay import ReplayBuffer, ReplayConfig, ReplayEntry
from pipeweave.replay_runner import ReplayPipelineRunner
from pipeweave.pipeline import Pipeline
from pipeweave.stage import Stage


# --- ReplayConfig ---

def test_valid_config_accepted():
    cfg = ReplayConfig(max_entries=50)
    assert cfg.max_entries == 50

def test_zero_max_entries_raises():
    with pytest.raises(ValueError):
        ReplayConfig(max_entries=0)

def test_negative_max_entries_raises():
    with pytest.raises(ValueError):
        ReplayConfig(max_entries=-1)


# --- ReplayBuffer ---

def test_record_and_get():
    buf = ReplayBuffer()
    buf.record("run-1", {"x": 1})
    entry = buf.get("run-1")
    assert entry is not None
    assert entry.input == {"x": 1}
    assert entry.run_id == "run-1"

def test_get_missing_returns_none():
    buf = ReplayBuffer()
    assert buf.get("nope") is None

def test_len():
    buf = ReplayBuffer()
    buf.record("a", 1)
    buf.record("b", 2)
    assert len(buf) == 2

def test_entries_returns_copy():
    buf = ReplayBuffer()
    buf.record("a", 1)
    entries = buf.entries
    entries.clear()
    assert len(buf) == 1

def test_max_entries_evicts_oldest():
    buf = ReplayBuffer(ReplayConfig(max_entries=3))
    for i in range(4):
        buf.record(f"run-{i}", i)
    assert len(buf) == 3
    assert buf.get("run-0") is None
    assert buf.get("run-3") is not None

def test_clear():
    buf = ReplayBuffer()
    buf.record("a", 1)
    buf.clear()
    assert len(buf) == 0

def test_disabled_does_not_record():
    buf = ReplayBuffer(ReplayConfig(enabled=False))
    buf.record("a", 1)
    assert len(buf) == 0


# --- ReplayPipelineRunner ---

def _build_pipeline():
    async def double(x):
        return x * 2
    stage = Stage(name="double", fn=double)
    p = Pipeline()
    p.pipe(stage)
    return p


@pytest.mark.asyncio
async def test_run_records_input():
    runner = ReplayPipelineRunner(_build_pipeline())
    await runner.run(5, run_id="r1")
    entry = runner.replay_buffer.get("r1")
    assert entry is not None
    assert entry.input == 5


@pytest.mark.asyncio
async def test_replay_reruns_pipeline():
    runner = ReplayPipelineRunner(_build_pipeline())
    result1 = await runner.run(7, run_id="r2")
    result2 = await runner.replay("r2")
    assert result1 == result2


@pytest.mark.asyncio
async def test_replay_missing_run_id_raises():
    runner = ReplayPipelineRunner(_build_pipeline())
    with pytest.raises(KeyError):
        await runner.replay("nonexistent")
