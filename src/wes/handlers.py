from __future__ import annotations

import importlib
import threading
from dataclasses import dataclass, field
from typing import Any, Callable


class PermanentError(Exception):
    """Raise from a handler to fail the step without retrying."""


@dataclass
class Context:
    """Everything a handler gets. Handlers must be idempotent: delivery is at-least-once."""

    run_id: str
    step_id: str
    attempt: int
    tenant_id: str
    run_input: dict[str, Any]
    deps: dict[str, Any]          # dependency step id -> its output (None if skipped)
    params: dict[str, Any]
    item: Any = None              # fan-out children only
    item_index: int | None = None
    lost_lease: threading.Event = field(default_factory=threading.Event)

    @property
    def idempotency_key(self) -> str:
        """Stable across retries of the same step: use it to dedupe side effects."""
        return f"{self.run_id}/{self.step_id}"


Handler = Callable[[Context], dict[str, Any] | None]


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, name: str) -> Callable[[Handler], Handler]:
        def deco(fn: Handler) -> Handler:
            if name in self._handlers:
                raise ValueError(f"handler '{name}' already registered")
            self._handlers[name] = fn
            return fn
        return deco

    def get(self, name: str) -> Handler:
        return self._handlers[name]

    def types(self) -> list[str]:
        return sorted(self._handlers)

    def __contains__(self, name: str) -> bool:
        return name in self._handlers


def load_registry(module_path: str) -> HandlerRegistry:
    """Import a module that exposes a HandlerRegistry named `registry`."""
    module = importlib.import_module(module_path)
    registry = getattr(module, "registry", None)
    if not isinstance(registry, HandlerRegistry):
        raise RuntimeError(f"{module_path} must define `registry = HandlerRegistry()`")
    return registry
