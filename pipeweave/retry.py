"""Retry logic and backoff strategies for pipeline stages."""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable, Optional, Tuple, Type

logger = logging.getLogger(__name__)


@dataclass
class RetryPolicy:
    """Configuration for retry behavior on stage failure."""

    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 30.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: Tuple[Type[Exception], ...] = field(
        default_factory=lambda: (Exception,)
    )
    on_retry: Optional[Callable[[int, Exception], None]] = None

    def compute_delay(self, attempt: int) -> float:
        """Compute backoff delay for a given attempt number (1-indexed)."""
        delay = min(
            self.base_delay * (self.exponential_base ** (attempt - 1)),
            self.max_delay,
        )
        if self.jitter:
            import random
            delay *= 0.5 + random.random() * 0.5
        return delay

    def is_retryable(self, exc: Exception) -> bool:
        """Return True if the exception should trigger a retry."""
        return isinstance(exc, self.retryable_exceptions)


async def with_retry(coro_fn: Callable, policy: RetryPolicy, *args, **kwargs):
    """Execute an async callable with the given retry policy.

    Args:
        coro_fn: Async callable to execute.
        policy: RetryPolicy controlling retry behaviour.
        *args, **kwargs: Forwarded to coro_fn.

    Returns:
        Result of coro_fn on success.

    Raises:
        The last exception raised after all attempts are exhausted.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(1, policy.max_attempts + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            if not policy.is_retryable(exc):
                logger.debug("Non-retryable exception on attempt %d: %s", attempt, exc)
                raise

            last_exc = exc
            if attempt == policy.max_attempts:
                break

            delay = policy.compute_delay(attempt)
            logger.warning(
                "Attempt %d/%d failed (%s). Retrying in %.2fs…",
                attempt,
                policy.max_attempts,
                exc,
                delay,
            )

            if policy.on_retry:
                policy.on_retry(attempt, exc)

            await asyncio.sleep(delay)

    raise last_exc  # type: ignore[misc]
