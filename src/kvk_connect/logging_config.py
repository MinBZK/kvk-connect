import logging
from datetime import UTC, datetime


class LocalTimezoneFormatter(logging.Formatter):
    """Formatter that converts UTC timestamps to local system timezone."""

    def formatTime(self, record, datefmt=None):  # noqa: N802, overrides base method
        """Convert UTC timestamp to timezone-aware datetime, then to local time."""
        dt_utc = datetime.fromtimestamp(record.created, tz=UTC)
        dt_local = dt_utc.astimezone()

        if datefmt:
            return dt_local.strftime(datefmt)
        else:
            return dt_local.strftime("%Y-%m-%d %H:%M:%S")


def configure(level: int = logging.INFO):
    """Configure logging with local timezone formatter."""
    handler = logging.StreamHandler()
    formatter = LocalTimezoneFormatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)

    logging.basicConfig(level=level, handlers=[handler], force=True)
