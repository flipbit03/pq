"""Task handler registry and resolution."""

import importlib
from collections.abc import Callable
from typing import Any


class TaskRegistry:
    """Registry for task handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, Callable[[dict[str, Any]], None]] = {}

    def register(self, name: str, handler: Callable[[dict[str, Any]], None]) -> None:
        """Register a task handler by name."""
        self._handlers[name] = handler

    def get(self, name: str) -> Callable[[dict[str, Any]], None] | None:
        """Get a registered handler by name, or None if not found."""
        return self._handlers.get(name)

    def resolve(self, name: str) -> Callable[[dict[str, Any]], None]:
        """Resolve a handler by name or import path.

        First checks the registry, then falls back to dynamic import
        for paths in "module.path:func_name" format.

        Raises:
            ValueError: If handler cannot be resolved.
        """
        # Check registry first
        handler = self._handlers.get(name)
        if handler is not None:
            return handler

        # Fall back to dynamic import
        if ":" not in name:
            raise ValueError(f"Unknown task: {name}")

        module_path, func_name = name.rsplit(":", 1)
        try:
            module = importlib.import_module(module_path)
            handler = getattr(module, func_name)
            if not callable(handler):
                raise ValueError(f"{name} is not callable")
            return handler
        except (ImportError, AttributeError) as e:
            raise ValueError(f"Cannot import task {name}: {e}") from e

    def clear(self) -> None:
        """Clear all registered handlers."""
        self._handlers.clear()


def get_function_path(func: Callable[..., Any]) -> str:
    """Get the import path for a function as 'module:name'.

    Note: Only works with functions and methods, not arbitrary callables.
    """
    return f"{func.__module__}:{func.__name__}"  # type: ignore[attr-defined]
