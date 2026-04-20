"""Named async semaphore pool for limiting concurrent stage executions."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class SemaphoreConfig:
    """Configuration for the named semaphore pool."""

    default_limit: int = 1
    limits: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.default_limit < 1:
            raise ValueError("default_limit must be >= 1")
        for name, limit in self.limits.items():
            if limit < 1:
                raise ValueError(
                    f"limit for '{name}' must be >= 1, got {limit}"
                )


class SemaphorePool:
    """A pool of named asyncio semaphores.

    Each unique name gets its own semaphore so that different stage
    groups can be throttled independently.
    """

    def __init__(self, config: Optional[SemaphoreConfig] = None) -> None:
        self._config = config or SemaphoreConfig()
        self._semaphores: Dict[str, asyncio.Semaphore] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_or_create(self, name: str) -> asyncio.Semaphore:
        if name not in self._semaphores:
            limit = self._config.limits.get(name, self._config.default_limit)
            self._semaphores[name] = asyncio.Semaphore(limit)
        return self._semaphores[name]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self, name: str) -> "_SemaphoreContext":
        """Return an async context manager that acquires the named semaphore."""
        return _SemaphoreContext(self._get_or_create(name))

    def available(self, name: str) -> int:
        """Return the number of available slots for *name*."""
        sem = self._get_or_create(name)
        return sem._value  # type: ignore[attr-defined]

    def names(self) -> list:
        """Return the names of all semaphores that have been created."""
        return list(self._semaphores.keys())


class _SemaphoreContext:
    """Thin async context manager wrapper around an asyncio.Semaphore."""

    def __init__(self, semaphore: asyncio.Semaphore) -> None:
        self._sem = semaphore

    async def __aenter__(self) -> "_SemaphoreContext":
        await self._sem.acquire()
        return self

    async def __aexit__(self, *_exc) -> None:
        self._sem.release()
