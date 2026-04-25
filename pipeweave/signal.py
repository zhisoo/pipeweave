"""Pipeline signal/cancellation token support."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, List, Optional


class SignalReason(str, Enum):
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    EXTERNAL = "external"


@dataclass
class SignalConfig:
    propagate: bool = True
    reason: SignalReason = SignalReason.CANCELLED

    def __post_init__(self) -> None:
        if not isinstance(self.reason, SignalReason):
            raise ValueError(f"reason must be a SignalReason, got {self.reason!r}")


class CancellationSignal:
    """A cooperative cancellation token that stages can observe."""

    def __init__(self, config: Optional[SignalConfig] = None) -> None:
        self._config = config or SignalConfig()
        self._cancelled = False
        self._reason: Optional[SignalReason] = None
        self._event = asyncio.Event()
        self._callbacks: List[Callable[[SignalReason], None]] = []

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    @property
    def reason(self) -> Optional[SignalReason]:
        return self._reason

    def cancel(self, reason: SignalReason = SignalReason.CANCELLED) -> None:
        if self._cancelled:
            return
        self._cancelled = True
        self._reason = reason
        self._event.set()
        for cb in self._callbacks:
            cb(reason)

    def on_cancel(self, callback: Callable[[SignalReason], None]) -> None:
        if not callable(callback):
            raise ValueError("callback must be callable")
        if self._cancelled and self._reason is not None:
            callback(self._reason)
        else:
            self._callbacks.append(callback)

    async def wait(self) -> SignalReason:
        await self._event.wait()
        return self._reason  # type: ignore[return-value]

    def reset(self) -> None:
        self._cancelled = False
        self._reason = None
        self._event.clear()

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CancellationSignal(cancelled={self._cancelled}, "
            f"reason={self._reason!r})"
        )
