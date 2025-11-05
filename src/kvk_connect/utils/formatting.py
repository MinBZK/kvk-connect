from __future__ import annotations


def truncate_float(value: float | None, digits: int = 5) -> str:
    """Truncate a float to a given number of digits after the decimal point and return as string."""

    if value is None or value == 0.0:
        return ""
    factor = 10**digits
    truncated = int(value * factor) / factor
    return f"{truncated:.{digits}f}".replace(".", ",")
