from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from kvk_connect.models.orm.base import Base


class McpOnbekendVraagORM(Base):
    """Registratie van vragen die niet beantwoord konden worden via de MCP server.

    Gebruikt voor analyse van 'most frequently asked questions' die nog niet ondersteund worden.
    """

    __tablename__ = "mcp_onbekende_vragen"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    vraag_tekst: Mapped[str] = mapped_column(Text, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        "timestamp", DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True
    )
