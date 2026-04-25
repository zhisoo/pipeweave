"""Tests for pipeweave.snapshot."""
from __future__ import annotations

import time

import pytest

from pipeweave.snapshot import (
    PipelineSnapshot,
    SnapshotConfig,
    SnapshotStore,
)


# ---------------------------------------------------------------------------
# SnapshotConfig
# ---------------------------------------------------------------------------


def test_default_config_accepted():
    cfg = SnapshotConfig()
    assert cfg.max_snapshots == 10
    assert cfg.include_metadata is True


def test_custom_config_accepted():
    cfg = SnapshotConfig(max_snapshots=5, include_metadata=False)
    assert cfg.max_snapshots == 5
    assert cfg.include_metadata is False


def test_zero_max_snapshots_raises():
    with pytest.raises(ValueError, match="max_snapshots"):
        SnapshotConfig(max_snapshots=0)


def test_negative_max_snapshots_raises():
    with pytest.raises(ValueError, match="max_snapshots"):
        SnapshotConfig(max_snapshots=-3)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _snap(run_id: str = "run-1", stages=None, results=None, metadata=None):
    return PipelineSnapshot(
        run_id=run_id,
        stage_names=stages or ["a", "b"],
        results=results or {"a": 1, "b": 2},
        metadata=metadata or {"env": "test"},
    )


# ---------------------------------------------------------------------------
# SnapshotStore – basic operations
# ---------------------------------------------------------------------------


def test_empty_store_has_length_zero():
    store = SnapshotStore()
    assert len(store) == 0


def test_capture_increments_length():
    store = SnapshotStore()
    store.capture(_snap())
    assert len(store) == 1


def test_latest_returns_none_when_empty():
    store = SnapshotStore()
    assert store.latest() is None


def test_latest_returns_most_recent():
    store = SnapshotStore()
    store.capture(_snap(run_id="run-1"))
    store.capture(_snap(run_id="run-2"))
    snap = store.latest()
    assert snap is not None
    assert snap.run_id == "run-2"


def test_latest_filtered_by_run_id():
    store = SnapshotStore()
    store.capture(_snap(run_id="run-1"))
    store.capture(_snap(run_id="run-2"))
    snap = store.latest(run_id="run-1")
    assert snap is not None
    assert snap.run_id == "run-1"


def test_latest_filtered_run_id_not_found_returns_none():
    store = SnapshotStore()
    store.capture(_snap(run_id="run-1"))
    assert store.latest(run_id="run-99") is None


def test_all_returns_all_snapshots():
    store = SnapshotStore()
    store.capture(_snap(run_id="run-1"))
    store.capture(_snap(run_id="run-2"))
    assert len(store.all()) == 2


def test_all_filtered_by_run_id():
    store = SnapshotStore()
    for _ in range(3):
        store.capture(_snap(run_id="run-A"))
    store.capture(_snap(run_id="run-B"))
    assert len(store.all(run_id="run-A")) == 3
    assert len(store.all(run_id="run-B")) == 1


# ---------------------------------------------------------------------------
# Eviction
# ---------------------------------------------------------------------------


def test_oldest_evicted_when_full():
    cfg = SnapshotConfig(max_snapshots=3)
    store = SnapshotStore(config=cfg)
    for i in range(4):
        store.capture(_snap(run_id=f"run-{i}"))
    assert len(store) == 3
    ids = [s.run_id for s in store.all()]
    assert "run-0" not in ids
    assert "run-3" in ids


# ---------------------------------------------------------------------------
# Clear
# ---------------------------------------------------------------------------


def test_clear_all_removes_everything():
    store = SnapshotStore()
    store.capture(_snap(run_id="run-1"))
    store.capture(_snap(run_id="run-2"))
    store.clear()
    assert len(store) == 0


def test_clear_by_run_id_removes_only_matching():
    store = SnapshotStore()
    store.capture(_snap(run_id="run-1"))
    store.capture(_snap(run_id="run-2"))
    store.clear(run_id="run-1")
    assert len(store) == 1
    assert store.latest().run_id == "run-2"


# ---------------------------------------------------------------------------
# PipelineSnapshot captured_at
# ---------------------------------------------------------------------------


def test_captured_at_is_recent():
    before = time.monotonic()
    snap = _snap()
    after = time.monotonic()
    assert before <= snap.captured_at <= after
