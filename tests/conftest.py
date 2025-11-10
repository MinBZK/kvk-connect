"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Generator

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from kvk_connect.models.orm.base import Base

logger = logging.getLogger(__name__)


def pytest_sessionstart(session: Any) -> None:
    """Ensure tests run with `tests/` as current working directory so relative
    paths like `data/...` resolve the same in CI and locally.
    """
    tests_dir = Path(__file__).resolve().parent
    os.chdir(tests_dir)
    logger.info("Changed working directory to tests directory: %s", tests_dir)


@pytest.fixture(scope="session")
def test_database_url() -> str:
    """Return in-memory SQLite database URL for testing."""
    return "sqlite:///:memory:"


@pytest.fixture(scope="function")
def db_engine(test_database_url: str):
    """Create test database engine with all tables."""
    engine = create_engine(
        test_database_url,
        connect_args={"check_same_thread": False},
        echo=False,
    )

    # Create all tables
    Base.metadata.create_all(engine)

    yield engine

    # Cleanup
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(db_engine) -> Generator[Session, None, None]:
    """Provide clean database session for each test."""
    SessionLocal = sessionmaker(bind=db_engine)
    session = SessionLocal()

    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture
def mock_kvk_basisprofiel_response() -> dict:
    """Load mock basisprofiel response from test input file."""
    test_file = Path(__file__).parent / "data/test_input_basisprofiel.json"
    with open(test_file, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def mock_kvk_vestigingsprofiel_response() -> dict:

    """Load mock basisprofiel response from test input file."""
    test_file = Path(__file__).parent / "data/test_input_vestigingsprofielen.json"
    with open(test_file, encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture
def mock_kvk_vestigingen_response() -> dict:
    """Mock KVK Vestigingen API response."""
    test_file = Path(__file__).parent / "data/test_input_vestigingen.json"
    with open(test_file, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def mock_kvk_signaal_response() -> dict:
    """Mock KVK Mutation Signal API response."""
    test_file = Path(__file__).parent / "data/test_input_mutatiesignalen_api.json"
    with open(test_file, encoding="utf-8") as f:
        return json.load(f)
