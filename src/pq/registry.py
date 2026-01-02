"""Function path resolution for task handlers."""

import importlib
from collections.abc import Callable
from types import FunctionType
from typing import Any, cast


def get_function_path(func: Callable[..., Any]) -> str:
    """Get the import path for a function as 'module:name'.

    Args:
        func: A callable function (must be a top-level function, not a lambda).

    Returns:
        Import path in 'module:name' format.

    Raises:
        ValueError: If the function cannot be serialized (lambda, closure, etc).
    """
    # Cast to FunctionType for type checker - we verify it has the needed attributes
    fn = cast(FunctionType, func)

    # Check for lambda
    if fn.__name__ == "<lambda>":
        raise ValueError(
            "Cannot enqueue lambda functions - use a named function instead"
        )

    # Check for closure (has free variables)
    if hasattr(fn, "__code__") and fn.__code__.co_freevars:
        raise ValueError(
            f"Cannot enqueue closure '{fn.__name__}' - closures capture local variables "
            "and cannot be imported by the worker"
        )

    module = getattr(fn, "__module__", None)
    name = getattr(fn, "__name__", None)

    if module is None or name is None:
        raise ValueError(f"Cannot determine import path for {func}")

    return f"{module}:{name}"


def resolve_function_path(path: str) -> Callable[..., Any]:
    """Import and return a function from its 'module:name' path.

    Args:
        path: Import path in 'module:name' format.

    Returns:
        The imported callable function.

    Raises:
        ValueError: If the path is invalid or the function cannot be imported.
    """
    if ":" not in path:
        raise ValueError(
            f"Invalid function path: {path} (expected 'module:name' format)"
        )

    module_path, func_name = path.rsplit(":", 1)

    try:
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)
        if not callable(func):
            raise ValueError(f"{path} is not callable")
        return func
    except ImportError as e:
        raise ValueError(f"Cannot import module '{module_path}': {e}") from e
    except AttributeError as e:
        raise ValueError(f"Module '{module_path}' has no function '{func_name}'") from e
