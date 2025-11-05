import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def pytest_sessionstart(session: Any) -> None:
    """Ensure tests run with `tests/` as current working directory so relative
    paths like `data/...` resolve the same in CI and locally.
    """
    tests_dir = Path(__file__).resolve().parent
    os.chdir(tests_dir)
    logger.info("Changed working directory to tests directory: %s", tests_dir)
