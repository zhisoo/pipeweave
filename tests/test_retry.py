"""Tests for pipeweave.retry and pipeweave.errors."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from pipeweave.errors import PipelineAbortedError, StageError
from pipeweave.retry import RetryPolicy, with_retry


# ---------------------------------------------------------------------------
# RetryPolicy unit tests
# ---------------------------------------------------------------------------

class TestRetryPolicy:
    def test_default_values(self):
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.base_delay == 0.5
        assert policy.jitter is True

    def test_compute_delay_grows_exponentially(self):
        policy = RetryPolicy(base_delay=1.0, exponential_base=2.0, jitter=False)
        assert policy.compute_delay(1) == pytest.approx(1.0)
        assert policy.compute_delay(2) == pytest.approx(2.0)
        assert policy.compute_delay(3) == pytest.approx(4.0)

    def test_compute_delay_capped_at_max(self):
        policy = RetryPolicy(base_delay=10.0, max_delay=15.0, jitter=False)
        assert policy.compute_delay(10) <= 15.0

    def test_is_retryable_default(self):
        policy = RetryPolicy()
        assert policy.is_retryable(ValueError("boom")) is True

    def test_is_retryable_filtered(self):
        policy = RetryPolicy(retryable_exceptions=(TimeoutError,))
        assert policy.is_retryable(TimeoutError()) is True
        assert policy.is_retryable(ValueError()) is False


# ---------------------------------------------------------------------------
# with_retry integration tests
# ---------------------------------------------------------------------------

class TestWithRetry:
    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self):
        fn = AsyncMock(return_value=42)
        policy = RetryPolicy(max_attempts=3)
        result = await with_retry(fn, policy)
        assert result == 42
        fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_retries_and_succeeds(self):
        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "ok"

        policy = RetryPolicy(max_attempts=3, base_delay=0, jitter=False)
        result = await with_retry(flaky, policy)
        assert result == "ok"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_raises_after_max_attempts(self):
        fn = AsyncMock(side_effect=RuntimeError("always fails"))
        policy = RetryPolicy(max_attempts=3, base_delay=0, jitter=False)
        with pytest.raises(RuntimeError, match="always fails"):
            await with_retry(fn, policy)
        assert fn.await_count == 3

    @pytest.mark.asyncio
    async def test_non_retryable_raises_immediately(self):
        fn = AsyncMock(side_effect=KeyError("bad key"))
        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0,
            jitter=False,
            retryable_exceptions=(TimeoutError,),
        )
        with pytest.raises(KeyError):
            await with_retry(fn, policy)
        fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_on_retry_callback_invoked(self):
        callback = MagicMock()
        fn = AsyncMock(side_effect=[ValueError("1"), ValueError("2"), "done"])
        fn.side_effect = [ValueError("1"), ValueError("2"), "done"]

        async def _fn():
            return await fn()

        policy = RetryPolicy(max_attempts=3, base_delay=0, jitter=False, on_retry=callback)
        await with_retry(_fn, policy)
        assert callback.call_count == 2


# ---------------------------------------------------------------------------
# Error hierarchy tests
# ---------------------------------------------------------------------------

class TestErrors:
    def test_stage_error_message(self):
        cause = ValueError("bad value")
        err = StageError("transform", cause, input_data={"x": 1})
        assert "transform" in str(err)
        assert err.stage_name == "transform"
        assert err.cause is cause
        assert err.input_data == {"x": 1}

    def test_pipeline_aborted_wraps_stage_error(self):
        stage_err = StageError("load", IOError("disk full"))
        aborted = PipelineAbortedError(stage_err)
        assert "load" in str(aborted)
        assert aborted.stage_error is stage_err
