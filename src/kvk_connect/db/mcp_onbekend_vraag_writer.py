from __future__ import annotations

import logging

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from kvk_connect.models.orm.mcp_onbekende_vraag_orm import McpOnbekendVraagORM

logger = logging.getLogger(__name__)


class McpOnbekendVraagWriter:
    """Schrijft onbekende MCP-vragen naar de database voor frequentie-analyse."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def add(self, vraag_tekst: str) -> None:
        """Sla een niet-ondersteunde vraag op in de database."""
        with Session(self.engine) as session:
            session.add(McpOnbekendVraagORM(vraag_tekst=vraag_tekst))
            session.commit()
            logger.debug("Onbekende vraag geregistreerd: %s", vraag_tekst[:80])
