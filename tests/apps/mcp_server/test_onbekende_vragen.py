"""Tests voor registratie van onbekende MCP-vragen."""

from __future__ import annotations

from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from kvk_connect.models.orm.mcp_onbekende_vraag_orm import McpOnbekendVraagORM
from kvk_connect.services.mirror_service import KVKMirrorService


class TestReportOnbekeneVraag:
    def test_retourneert_niet_ondersteund(self, service: KVKMirrorService) -> None:
        result = service.report_onbekende_vraag("Wie is de eigenaar van dit bedrijf?")
        assert result["status"] == "niet_ondersteund"
        assert "bericht" in result
        assert "data_quality" in result

    def test_schrijft_naar_database(self, db_engine: Engine, service: KVKMirrorService) -> None:
        service.report_onbekende_vraag("Wat is het KvK-nummer van ACME?")
        with Session(db_engine) as session:
            rows = session.execute(select(McpOnbekendVraagORM)).scalars().all()
        teksten = [r.vraag_tekst for r in rows]
        assert "Wat is het KvK-nummer van ACME?" in teksten

    def test_meerdere_aanroepen_accumuleren(self, db_engine: Engine, service: KVKMirrorService) -> None:
        service.report_onbekende_vraag("Vraag 1")
        service.report_onbekende_vraag("Vraag 2")
        service.report_onbekende_vraag("Vraag 1")  # duplicaat
        with Session(db_engine) as session:
            rows = session.execute(select(McpOnbekendVraagORM)).scalars().all()
        assert len(rows) == 3

    def test_frequentie_analyse_mogelijk(self, db_engine: Engine, service: KVKMirrorService) -> None:
        service.report_onbekende_vraag("Populaire vraag")
        service.report_onbekende_vraag("Populaire vraag")
        service.report_onbekende_vraag("Zeldzame vraag")
        with Session(db_engine) as session:
            rows = session.execute(select(McpOnbekendVraagORM)).scalars().all()
        from collections import Counter
        freq = Counter(r.vraag_tekst for r in rows)
        assert freq["Populaire vraag"] == 2
        assert freq["Zeldzame vraag"] == 1
