"""Simple in-process event bus for pipeline stage lifecycle events."""
from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional


@dataclass
class EventBusConfig:
    """Configuration for the event bus."""

    max_listeners_per_event: int = 64
    propagate_listener_errors: bool = False

    def __post_init__(self) -> None:
        if self.max_listeners_per_event < 1:
            raise ValueError("max_listeners_per_event must be >= 1")


Listener = Callable[..., Awaitable[None]]


class EventBus:
    """Async publish/subscribe event bus.

    Listeners are coroutine functions.  Publishing dispatches all
    registered listeners concurrently via ``asyncio.gather``.
    """

    def __init__(self, config: Optional[EventBusConfig] = None) -> None:
        self._config = config or EventBusConfig()
        self._listeners: Dict[str, List[Listener]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def subscribe(self, event: str, listener: Listener) -> None:
        """Register *listener* for *event*."""
        if not callable(listener):
            raise TypeError("listener must be callable")
        bucket = self._listeners[event]
        cap = self._config.max_listeners_per_event
        if len(bucket) >= cap:
            raise RuntimeError(
                f"Listener cap ({cap}) reached for event '{event}'"
            )
        bucket.append(listener)

    def unsubscribe(self, event: str, listener: Listener) -> None:
        """Remove *listener* from *event*.  Silently ignores missing entries."""
        bucket = self._listeners.get(event, [])
        try:
            bucket.remove(listener)
        except ValueError:
            pass

    def listener_count(self, event: str) -> int:
        """Return the number of listeners registered for *event*."""
        return len(self._listeners.get(event, []))

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    async def publish(self, event: str, **kwargs: Any) -> None:
        """Dispatch *event* to all registered listeners concurrently."""
        listeners = list(self._listeners.get(event, []))
        if not listeners:
            return
        if self._config.propagate_listener_errors:
            await asyncio.gather(*(ln(**kwargs) for ln in listeners))
        else:
            results = await asyncio.gather(
                *(ln(**kwargs) for ln in listeners),
                return_exceptions=True,
            )
            # Errors are silently swallowed unless propagation is enabled.
            _ = results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def clear(self, event: Optional[str] = None) -> None:
        """Remove all listeners for *event*, or every event if *event* is None."""
        if event is None:
            self._listeners.clear()
        else:
            self._listeners.pop(event, None)
