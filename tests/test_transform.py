"""Tests for pipeweave.transform."""
import pytest
from pipeweave.transform import TransformConfig, make_transform_middleware
from pipeweave.context import StageResult, PipelineContext


def _ok_result(value, name="stage"):
    return StageResult(stage_name=name, value=value, success=True, error=None)


def _make_ctx():
    return PipelineContext(run_id="test-run")


async def _identity(ctx, result):
    return result


async def _run(config, value):
    mw = make_transform_middleware(config)
    ctx = _make_ctx()
    result = _ok_result(value)
    final = None

    async def capture(c, r):
        nonlocal final
        final = r

    await mw(ctx, result, capture)
    return final


def test_valid_config_accepted():
    cfg = TransformConfig(mappings={"x": int}, rename={"x": "y"}, drop_keys=["z"])
    assert cfg is not None


def test_non_callable_mapping_raises():
    with pytest.raises(ValueError, match="mapping for 'x' must be callable"):
        TransformConfig(mappings={"x": 42})


def test_non_callable_filter_raises():
    with pytest.raises(ValueError, match="filter at index 0 must be callable"):
        TransformConfig(filters=["not_callable"])


def test_empty_rename_target_raises():
    with pytest.raises(ValueError, match="rename target for 'a'"):
        TransformConfig(rename={"a": ""})


@pytest.mark.asyncio
async def test_mapping_applied():
    cfg = TransformConfig(mappings={"n": lambda v: v * 2})
    result = await _run(cfg, {"n": 5})
    assert result.value == {"n": 10}


@pytest.mark.asyncio
async def test_rename_applied():
    cfg = TransformConfig(rename={"old": "new"})
    result = await _run(cfg, {"old": 1})
    assert "new" in result.value
    assert "old" not in result.value


@pytest.mark.asyncio
async def test_drop_keys_applied():
    cfg = TransformConfig(drop_keys=["secret"])
    result = await _run(cfg, {"a": 1, "secret": "hidden"})
    assert "secret" not in result.value
    assert result.value["a"] == 1


@pytest.mark.asyncio
async def test_filter_passes_matching_value():
    cfg = TransformConfig(filters=[lambda v: isinstance(v, dict) and v.get("ok")])
    result = await _run(cfg, {"ok": True})
    assert result.value == {"ok": True}


@pytest.mark.asyncio
async def test_filter_sets_none_on_mismatch():
    cfg = TransformConfig(filters=[lambda v: False])
    result = await _run(cfg, {"x": 99})
    assert result.value is None


@pytest.mark.asyncio
async def test_non_dict_value_skips_dict_ops():
    cfg = TransformConfig(mappings={"x": str}, drop_keys=["x"])
    result = await _run(cfg, "plain string")
    assert result.value == "plain string"
