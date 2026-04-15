"""Stage wrapper that integrates throttling with retry and error handling."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional

from pipeweave.errors import StageError
from pipeweave.retry import RetryPolicy, compute_delay, is_retryable
from pipeweave.throttle import Throttle, ThrottleConfig


AsyncFn = Callable[..., Awaitable[Any]]


class Stage:
    """A single async processing stage with optional throttle and retry."""

    def __init__(
        self,
        name: str,
        fn: AsyncFn,
        *,
        retry_policy: Optional[RetryPolicy] = None,
        throttle_config: Optional[ThrottleConfig] = None,
    ) -> None:
        self.name = name
        self._fn = fn
        self._retry = retry_policy or RetryPolicy()
        self._throttle = Throttle(throttle_config) if throttle_config else None

    async def run(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the stage function, applying throttle and retry logic."""
        attempt = 0
        last_exc: Optional[Exception] = None

        while attempt <= self._retry.max_attempts:
            if self._throttle is not None:
                async with self._throttle:
                    result = await self._call_fn(*args, **kwargs)
            else:
                result = await self._call_fn(*args, **kwargs)

            if isinstance(result, Exception):
                last_exc = result
                if not is_retryable(result, self._retry) or attempt >= self._retry.max_attempts:
                    raise StageError(self.name, result, attempt)
                delay = compute_delay(attempt, self._retry)
                await asyncio.sleep(delay)
                attempt += 1
                continue

            return result

        raise StageError(self.name, last_exc or RuntimeError("unknown"), attempt)

    async def _call_fn(self, *args: Any, **kwargs: Any) -> Any:
        try:
            return await self._fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            return exc
