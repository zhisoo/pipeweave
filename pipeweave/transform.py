"""Field-level transform middleware for mapping/filtering pipeline data."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pipeweave.errors import StageError


@dataclass
class TransformConfig:
    """Configuration for field-level data transformation."""
    mappings: Dict[str, Callable[[Any], Any]] = field(default_factory=dict)
    filters: List[Callable[[Any], bool]] = field(default_factory=list)
    rename: Dict[str, str] = field(default_factory=dict)
    drop_keys: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        for key, fn in self.mappings.items():
            if not callable(fn):
                raise ValueError(f"mapping for '{key}' must be callable")
        for i, fn in enumerate(self.filters):
            if not callable(fn):
                raise ValueError(f"filter at index {i} must be callable")
        for old, new in self.rename.items():
            if not isinstance(new, str) or not new:
                raise ValueError(f"rename target for '{old}' must be a non-empty string")


def make_transform_middleware(config: TransformConfig):
    """Return middleware that applies TransformConfig to each stage result."""
    async def _middleware(ctx, result, next_):
        value = result.value

        if isinstance(value, dict):
            # apply mappings
            for key, fn in config.mappings.items():
                if key in value:
                    try:
                        value = {**value, key: fn(value[key])}
                    except Exception as exc:
                        raise StageError(
                            stage_name=result.stage_name,
                            message=f"transform mapping '{key}' failed: {exc}",
                            cause=exc,
                        ) from exc

            # rename keys
            for old, new in config.rename.items():
                if old in value:
                    value = {k if k != old else new: v for k, v in value.items()}

            # drop keys
            for key in config.drop_keys:
                value.pop(key, None)

        # apply filters — if any returns False, skip forwarding (return None value)
        for fn in config.filters:
            try:
                if not fn(value):
                    from pipeweave.context import StageResult
                    filtered = StageResult(
                        stage_name=result.stage_name,
                        value=None,
                        success=True,
                        error=None,
                    )
                    return await next_(ctx, filtered)
            except Exception as exc:
                raise StageError(
                    stage_name=result.stage_name,
                    message=f"transform filter failed: {exc}",
                    cause=exc,
                ) from exc

        from pipeweave.context import StageResult
        transformed = StageResult(
            stage_name=result.stage_name,
            value=value,
            success=result.success,
            error=result.error,
        )
        return await next_(ctx, transformed)

    return _middleware
