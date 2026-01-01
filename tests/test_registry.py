"""Tests for task registry."""

from typing import Any

import pytest

from pq.registry import TaskRegistry, get_function_path


def sample_handler(payload: dict[str, Any]) -> None:
    """Sample task handler for testing."""
    pass


class TestTaskRegistry:
    """Tests for TaskRegistry class."""

    def test_register_and_get(self, registry: TaskRegistry) -> None:
        """Register a handler and retrieve it."""
        registry.register("my_task", sample_handler)
        assert registry.get("my_task") is sample_handler

    def test_get_unregistered_returns_none(self, registry: TaskRegistry) -> None:
        """Getting an unregistered task returns None."""
        assert registry.get("nonexistent") is None

    def test_resolve_registered_name(self, registry: TaskRegistry) -> None:
        """Resolve returns registered handler."""
        registry.register("my_task", sample_handler)
        assert registry.resolve("my_task") is sample_handler

    def test_resolve_function_path(self, registry: TaskRegistry) -> None:
        """Resolve imports function from module:name path."""
        handler = registry.resolve("tests.test_registry:sample_handler")
        assert handler is sample_handler

    def test_resolve_unknown_raises(self, registry: TaskRegistry) -> None:
        """Resolve raises ValueError for unknown task without colon."""
        with pytest.raises(ValueError, match="Unknown task"):
            registry.resolve("nonexistent")

    def test_resolve_invalid_module_raises(self, registry: TaskRegistry) -> None:
        """Resolve raises ValueError for invalid module path."""
        with pytest.raises(ValueError, match="Cannot import"):
            registry.resolve("nonexistent.module:func")

    def test_resolve_invalid_attr_raises(self, registry: TaskRegistry) -> None:
        """Resolve raises ValueError for invalid attribute."""
        with pytest.raises(ValueError, match="Cannot import"):
            registry.resolve("tests.test_registry:nonexistent_func")

    def test_clear(self, registry: TaskRegistry) -> None:
        """Clear removes all registered handlers."""
        registry.register("task1", sample_handler)
        registry.register("task2", sample_handler)
        registry.clear()
        assert registry.get("task1") is None
        assert registry.get("task2") is None


class TestGetFunctionPath:
    """Tests for get_function_path helper."""

    def test_returns_module_and_name(self) -> None:
        """Returns 'module:name' format."""
        path = get_function_path(sample_handler)
        assert path == "tests.test_registry:sample_handler"

    def test_roundtrip_with_resolve(self, registry: TaskRegistry) -> None:
        """Path from get_function_path can be resolved."""
        path = get_function_path(sample_handler)
        resolved = registry.resolve(path)
        assert resolved is sample_handler
