"""Tests for pipeweave.window."""
import pytest
from pipeweave.window import WindowConfig, WindowBuffer


def test_valid_config_accepted():
    cfg = WindowConfig(size=4)
    assert cfg.size == 4
    assert cfg.step == 4  # tumbling default


def test_explicit_step_stored():
    cfg = WindowConfig(size=4, step=2)
    assert cfg.step == 2


def test_zero_size_raises():
    with pytest.raises(ValueError, match="size must be positive"):
        WindowConfig(size=0)


def test_negative_size_raises():
    with pytest.raises(ValueError, match="size must be positive"):
        WindowConfig(size=-1)


def test_negative_step_raises():
    with pytest.raises(ValueError, match="step must be non-negative"):
        WindowConfig(size=3, step=-1)


def test_non_callable_aggregator_raises():
    with pytest.raises(TypeError, match="aggregator must be callable"):
        WindowConfig(size=3, aggregator="sum")  # type: ignore


def test_tumbling_window_emits_on_full():
    buf = WindowBuffer(WindowConfig(size=3))
    assert buf.add(1) == []
    assert buf.add(2) == []
    windows = buf.add(3)
    assert windows == [[1, 2, 3]]
    assert buf.pending == 0


def test_tumbling_window_second_window():
    buf = WindowBuffer(WindowConfig(size=2))
    buf.add("a")
    buf.add("b")  # first window
    buf.add("c")
    windows = buf.add("d")
    assert windows == [["c", "d"]]


def test_sliding_window_overlaps():
    buf = WindowBuffer(WindowConfig(size=3, step=1))
    buf.add(1)
    buf.add(2)
    windows = buf.add(3)
    assert [1, 2, 3] in windows
    windows2 = buf.add(4)
    assert [2, 3, 4] in windows2


def test_flush_returns_partial():
    buf = WindowBuffer(WindowConfig(size=5))
    buf.add("x")
    buf.add("y")
    partial = buf.flush()
    assert partial == [["x", "y"]]


def test_flush_empty_buffer():
    buf = WindowBuffer(WindowConfig(size=3))
    assert buf.flush() == []


def test_aggregate_with_custom_fn():
    cfg = WindowConfig(size=3, aggregator=sum)
    buf = WindowBuffer(cfg)
    buf.add(1)
    buf.add(2)
    windows = buf.add(3)
    assert buf.aggregate(windows[0]) == 6


def test_aggregate_default_returns_list():
    buf = WindowBuffer(WindowConfig(size=2))
    buf.add(10)
    windows = buf.add(20)
    assert buf.aggregate(windows[0]) == [10, 20]
