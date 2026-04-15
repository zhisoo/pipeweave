"""Unit tests for pipeweave.metrics (StageMetrics, PipelineMetrics)."""
from __future__ import annotations

import time

import pytest

from pipeweave.metrics import PipelineMetrics, StageMetrics


# ---------------------------------------------------------------------------
# StageMetrics
# ---------------------------------------------------------------------------

class TestStageMetrics:
    def test_initial_state(self):
        sm = StageMetrics(stage_name="parse")
        assert sm.stage_name == "parse"
        assert sm.ended_at is None
        assert sm.duration_ms is None
        assert sm.attempts == 0
        assert not sm.succeeded
        assert sm.error is None

    def test_finish_success(self):
        sm = StageMetrics(stage_name="parse")
        time.sleep(0.01)
        sm.finish(succeeded=True)
        assert sm.succeeded
        assert sm.error is None
        assert sm.duration_ms is not None
        assert sm.duration_ms > 0

    def test_finish_failure(self):
        sm = StageMetrics(stage_name="parse")
        sm.finish(succeeded=False, error="boom")
        assert not sm.succeeded
        assert sm.error == "boom"

    def test_duration_ms_grows_with_time(self):
        sm = StageMetrics(stage_name="slow")
        time.sleep(0.02)
        sm.finish(succeeded=True)
        assert sm.duration_ms >= 15  # generous lower bound


# ---------------------------------------------------------------------------
# PipelineMetrics
# ---------------------------------------------------------------------------

class TestPipelineMetrics:
    def _make_pm(self, name: str = "my_pipe") -> PipelineMetrics:
        return PipelineMetrics(pipeline_name=name)

    def test_initial_state(self):
        pm = self._make_pm()
        assert pm.pipeline_name == "my_pipe"
        assert pm.stages == []
        assert pm.ended_at is None
        assert pm.total_duration_ms is None

    def test_record_stage(self):
        pm = self._make_pm()
        sm = StageMetrics(stage_name="a")
        sm.finish(succeeded=True)
        pm.record_stage(sm)
        assert len(pm.stages) == 1

    def test_succeeded_and_failed_partitions(self):
        pm = self._make_pm()
        s1 = StageMetrics(stage_name="a")
        s1.finish(succeeded=True)
        s2 = StageMetrics(stage_name="b")
        s2.finish(succeeded=False, error="err")
        pm.record_stage(s1)
        pm.record_stage(s2)
        assert len(pm.succeeded_stages) == 1
        assert len(pm.failed_stages) == 1

    def test_finish_sets_duration(self):
        pm = self._make_pm()
        time.sleep(0.01)
        pm.finish()
        assert pm.total_duration_ms is not None
        assert pm.total_duration_ms > 0

    def test_summary_keys(self):
        pm = self._make_pm("pipe")
        sm = StageMetrics(stage_name="x")
        sm.finish(succeeded=True)
        pm.record_stage(sm)
        pm.finish()
        s = pm.summary()
        assert s["pipeline"] == "pipe"
        assert s["stages_total"] == 1
        assert s["stages_succeeded"] == 1
        assert s["stages_failed"] == 0
        assert len(s["stage_details"]) == 1
        detail = s["stage_details"][0]
        assert detail["name"] == "x"
        assert detail["succeeded"] is True
