from __future__ import annotations

import os

from ratelimit import limits, sleep_and_retry

RATE_LIMIT_CALLS = int(os.getenv("RATE_LIMIT_CALLS", "100"))


def global_rate_limit(calls: int = RATE_LIMIT_CALLS, period: int = 1):
    """Decorator to apply a global rate limit to a function."""

    def deco(func):
        @sleep_and_retry
        @limits(calls=calls, period=period)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return deco
