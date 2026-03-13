"""Tests for global_rate_limit decorator."""

from __future__ import annotations

import os
from unittest.mock import patch

from kvk_connect.utils.rate_limit import global_rate_limit


class TestGlobalRateLimit:
    """Test suite for the global_rate_limit decorator factory."""

    def test_decorator_is_callable(self) -> None:
        """global_rate_limit() returns a decorator that can be applied."""
        decorator = global_rate_limit(calls=10, period=1)
        assert callable(decorator)

    def test_decorated_function_is_callable(self) -> None:
        """Applying the decorator produces a callable."""
        @global_rate_limit(calls=10, period=1)
        def my_func() -> str:
            return "ok"

        assert callable(my_func)

    def test_decorated_function_returns_value(self) -> None:
        """Decorated function passes return value through."""
        @global_rate_limit(calls=100, period=1)
        def my_func(x: int) -> int:
            return x * 2

        assert my_func(5) == 10

    def test_decorated_function_passes_arguments(self) -> None:
        """Decorated function forwards positional and keyword arguments."""
        calls = []

        @global_rate_limit(calls=100, period=1)
        def my_func(a: int, b: str = "default") -> tuple:
            calls.append((a, b))
            return (a, b)

        result = my_func(1, b="hello")
        assert result == (1, "hello")
        assert calls == [(1, "hello")]

    def test_multiple_calls_within_limit_succeed(self) -> None:
        """Multiple calls within the rate limit all complete successfully."""
        results = []

        @global_rate_limit(calls=100, period=1)
        def my_func(n: int) -> int:
            return n

        for i in range(5):
            results.append(my_func(i))

        assert results == list(range(5))

    def test_rate_limit_calls_env_var_is_read(self) -> None:
        """RATE_LIMIT_CALLS env var sets the default for the module-level constant."""
        with patch.dict(os.environ, {"RATE_LIMIT_CALLS": "42"}):
            # Re-import to pick up the patched env var
            import importlib
            import kvk_connect.utils.rate_limit as rl
            importlib.reload(rl)
            assert rl.RATE_LIMIT_CALLS == 42
            # Restore
            importlib.reload(rl)
