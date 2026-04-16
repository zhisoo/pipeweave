"""Simple result caching middleware for pipeline stages."""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from pipeweave.context import PipelineContext, StageResult


@dataclass
class CacheConfig:
    ttl: Optional[float] = None  # seconds; None = never expire
    max_size: int = 256
    key_fn: Optional[Callable[[Any], str]] = None

    def __post_init__(self) -> None:
        if self.ttl is not None and self.ttl <= 0:
            raise ValueError("ttl must be positive")
        if self.max_size <= 0:
            raise ValueError("max_size must be positive")


@dataclass
class _Entry:
    result: StageResult
    created_at: float = field(default_factory=time.monotonic)


class StageCache:
    """LRU-ish in-memory cache keyed by stage name + serialised input."""

    def __init__(self, config: Optional[CacheConfig] = None) -> None:
        self._cfg = config or CacheConfig()
        self._store: Dict[str, _Entry] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    def _make_key(self, stage_name: str, data: Any) -> str:
        if self._cfg.key_fn:
            raw = self._cfg.key_fn(data)
        else:
            try:
                raw = json.dumps(data, sort_keys=True, default=str)
            except Exception:
                raw = str(data)
        digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
        return f"{stage_name}:{digest}"

    def _is_expired(self, entry: _Entry) -> bool:
        if self._cfg.ttl is None:
            return False
        return (time.monotonic() - entry.created_at) > self._cfg.ttl

    async def get(self, stage_name: str, data: Any) -> Optional[StageResult]:
        key = self._make_key(stage_name, data)
        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if self._is_expired(entry):
                del self._store[key]
                return None
            return entry.result

    async def set(self, stage_name: str, data: Any, result: StageResult) -> None:
        key = self._make_key(stage_name, data)
        async with self._lock:
            if len(self._store) >= self._cfg.max_size:
                oldest = next(iter(self._store))
                del self._store[oldest]
            self._store[key] = _Entry(result=result)

    def invalidate(self, stage_name: str) -> int:
        prefix = f"{stage_name}:"
        keys = [k for k in self._store if k.startswith(prefix)]
        for k in keys:
            del self._store[k]
        return len(keys)

    def size(self) -> int:
        return len(self._store)


def cache_middleware(cache: StageCache) -> Callable:
    """Middleware factory that wraps a stage with cache lookup/store."""
    async def _middleware(ctx: PipelineContext, data: Any, call_next: Callable) -> StageResult:
        stage_name = ctx.get("current_stage", "unknown")
        cached = await cache.get(stage_name, data)
        if cached is not None:
            return cached
        result: StageResult = await call_next(ctx, data)
        if result.ok:
            await cache.set(stage_name, data, result)
        return result
    return _middleware
