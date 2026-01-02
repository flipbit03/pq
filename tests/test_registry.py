"""Tests for function path resolution."""

import pytest

from pq.registry import get_function_path, resolve_function_path


def sample_handler(value: int) -> None:
    """Sample task handler for testing."""
    pass


class TestGetFunctionPath:
    """Tests for get_function_path function."""

    def test_returns_module_and_name(self) -> None:
        """Returns 'module:name' format."""
        path = get_function_path(sample_handler)
        assert path == "tests.test_registry:sample_handler"

    def test_lambda_raises(self) -> None:
        """Lambda functions raise ValueError."""
        with pytest.raises(ValueError, match="Cannot enqueue lambda"):
            get_function_path(lambda x: x)

    def test_closure_raises(self) -> None:
        """Closures raise ValueError."""
        captured = 42

        def closure_func() -> int:
            return captured

        with pytest.raises(ValueError, match="Cannot enqueue closure"):
            get_function_path(closure_func)


class TestResolveFunctionPath:
    """Tests for resolve_function_path function."""

    def test_resolves_valid_path(self) -> None:
        """Resolves a valid module:name path."""
        handler = resolve_function_path("tests.test_registry:sample_handler")
        assert handler is sample_handler

    def test_invalid_format_raises(self) -> None:
        """Path without colon raises ValueError."""
        with pytest.raises(ValueError, match="Invalid function path"):
            resolve_function_path("no_colon_here")

    def test_invalid_module_raises(self) -> None:
        """Non-existent module raises ValueError."""
        with pytest.raises(ValueError, match="Cannot import module"):
            resolve_function_path("nonexistent.module:func")

    def test_invalid_function_raises(self) -> None:
        """Non-existent function raises ValueError."""
        with pytest.raises(ValueError, match="has no function"):
            resolve_function_path("tests.test_registry:nonexistent_func")

    def test_roundtrip(self) -> None:
        """Path from get_function_path can be resolved back."""
        path = get_function_path(sample_handler)
        resolved = resolve_function_path(path)
        assert resolved is sample_handler
