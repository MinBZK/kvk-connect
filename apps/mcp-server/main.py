# ruff: noqa: D103
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from functools import wraps
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version
from typing import Any, Callable

from mcp.server.fastmcp import FastMCP
from sqlalchemy import create_engine
from starlette.requests import Request
from starlette.responses import JSONResponse

from config import config
from kvk_connect import logging_config
from kvk_connect.db.mcp_onbekend_vraag_writer import McpOnbekendVraagWriter
from kvk_connect.db.mirror_reader import KVKMirrorReader
from kvk_connect.db.init import ensure_database_initialized
from kvk_connect.models.orm.base import Base
from kvk_connect.services.mirror_service import KVKMirrorService

logger = logging.getLogger(__name__)

mcp = FastMCP("kvk-connect")
_service: KVKMirrorService | None = None


# ---------------------------------------------------------------------------
# Logging decorator
# ---------------------------------------------------------------------------


def log_tool_call(fn: Callable) -> Callable:
    """Logt tool naam, resultaatstatus en uitvoertijd."""

    @wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        first_arg = next(iter(kwargs.values()), args[0] if args else "")
        logger.info("→ %s(%s)", fn.__name__, str(first_arg)[:40])
        start = time.monotonic()
        try:
            result = await fn(*args, **kwargs)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            logger.info("← %s OK [%dms]", fn.__name__, elapsed_ms)
            return result
        except Exception:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            logger.error("← %s ERROR [%dms]", fn.__name__, elapsed_ms, exc_info=True)
            raise

    return wrapper


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


# ---------------------------------------------------------------------------
# Laag 1: exacte lookups
# ---------------------------------------------------------------------------


@mcp.tool()
@log_tool_call
async def get_bedrijf(kvk_nummer: str, include_non_mailing: bool = False) -> str:
    """Geeft basisprofiel voor KVK-nummer (8 cijfers, bijv. '12345678')."""
    assert _service is not None
    return json.dumps(_service.get_bedrijf(kvk_nummer, include_non_mailing=include_non_mailing))


@mcp.tool()
@log_tool_call
async def get_vestiging(vestigingsnummer: str) -> str:
    """Geeft vestigingsprofiel voor vestigingsnummer (12 cijfers, bijv. '000012345678')."""
    assert _service is not None
    return json.dumps(_service.get_vestiging(vestigingsnummer))


@mcp.tool()
@log_tool_call
async def list_vestigingen(kvk_nummer: str, include_non_mailing: bool = False) -> str:
    """Geeft alle vestigingsnummers en locatiedata voor KVK-nummer (8 cijfers)."""
    assert _service is not None
    return json.dumps(_service.list_vestigingen(kvk_nummer, include_non_mailing=include_non_mailing))


@mcp.tool()
@log_tool_call
async def get_alles(kvk_nummer: str, include_non_mailing: bool = False) -> str:
    """Geeft basisprofiel plus alle vestigingsdetails voor KVK-nummer (8 cijfers) in één aanroep."""
    assert _service is not None
    return json.dumps(_service.get_alles(kvk_nummer, include_non_mailing=include_non_mailing))


@mcp.tool()
@log_tool_call
async def check_doorstarter(kvk_nummer: str) -> str:
    """Zoekt actieve opvolger op hetzelfde adres voor KVK-nummer (8 cijfers)."""
    assert _service is not None
    return json.dumps(_service.check_doorstarter(kvk_nummer))


# ---------------------------------------------------------------------------
# Laag 2: analytisch
# ---------------------------------------------------------------------------


@mcp.tool()
@log_tool_call
async def zoek_op_naam_prefix(naam_prefix: str, limit: int = 25) -> str:
    """Zoekt bedrijven op naam-prefix (bijv. 'Bakkerij'). Maximaal 100 resultaten."""
    assert _service is not None
    return json.dumps(_service.zoek_op_naam_prefix(naam_prefix, limit=limit))


@mcp.tool()
@log_tool_call
async def filter_op_sbi(sbi_prefix: str, gemeente: str = "", limit: int = 100) -> str:
    """Geeft actieve vestigingen voor SBI-sector (bijv. '86' voor zorg), optioneel gefilterd op gemeente."""
    assert _service is not None
    return json.dumps(_service.filter_op_sbi(sbi_prefix, gemeente=gemeente or None, limit=limit))


@mcp.tool()
@log_tool_call
async def check_actiefstatus_batch(kvk_nummers: list[str]) -> str:
    """Controleert actiefstatus voor een lijst KVK-nummers (maximaal 200 per aanroep)."""
    assert _service is not None
    try:
        return json.dumps(_service.check_actiefstatus_batch(kvk_nummers))
    except ValueError as e:
        return json.dumps({"status": "fout", "bericht": str(e), "data_quality": {"coverage_warnings": []}})


# ---------------------------------------------------------------------------
# Laag 3: onbekende vragen + historie
# ---------------------------------------------------------------------------


@mcp.tool()
@log_tool_call
async def report_onbekende_vraag(vraag: str) -> str:
    """Registreer een vraag die niet beantwoord kan worden met de beschikbare tools."""
    assert _service is not None
    return json.dumps(_service.report_onbekende_vraag(vraag))


@mcp.tool()
@log_tool_call
async def get_basisprofiel_historie(kvk_nummer: str) -> str:
    """Geeft de wijzigingsgeschiedenis van een basisprofiel voor KVK-nummer (8 cijfers)."""
    assert _service is not None
    return json.dumps(_service.get_basisprofiel_historie(kvk_nummer))


@mcp.tool()
@log_tool_call
async def get_vestigingsprofiel_historie(vestigingsnummer: str) -> str:
    """Geeft de wijzigingsgeschiedenis van een vestigingsprofiel voor vestigingsnummer (12 cijfers)."""
    assert _service is not None
    return json.dumps(_service.get_vestigingsprofiel_historie(vestigingsnummer))


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    global _service

    parser = argparse.ArgumentParser(description="KVK-Connect MCP server.")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG log level.")
    args = parser.parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging_config.configure(level=log_level)

    try:
        app_version = pkg_version("kvk-connect")
    except PackageNotFoundError:
        app_version = "onbekend"
    logger.info("kvk-connect mcp-server v%s gestart", app_version)

    engine = create_engine(config.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
    ensure_database_initialized(engine, Base)

    reader = KVKMirrorReader(engine)
    writer = McpOnbekendVraagWriter(engine)
    _service = KVKMirrorService(reader, writer)

    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8000"))
    logger.info("MCP server luistert op %s:%d", host, port)
    mcp.run(transport="streamable-http", host=host, port=port)


if __name__ == "__main__":
    main()
