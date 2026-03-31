"""Gedeelde fixtures voor MCP-server tests.

Gebruikt SQLite in-memory als DB-simulatie: echte SQLAlchemy-sessies, geen mocks.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from kvk_connect.db.mcp_onbekend_vraag_writer import McpOnbekendVraagWriter
from kvk_connect.db.mirror_reader import KVKMirrorReader
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.mcp_onbekende_vraag_orm import McpOnbekendVraagORM  # noqa: F401 — registers with Base
from kvk_connect.models.orm.vestigingen_orm import VestigingenORM
from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM
from kvk_connect.services.mirror_service import KVKMirrorService


@pytest.fixture
def reader(db_engine: Engine) -> KVKMirrorReader:
    return KVKMirrorReader(db_engine)


@pytest.fixture
def writer(db_engine: Engine) -> McpOnbekendVraagWriter:
    return McpOnbekendVraagWriter(db_engine)


@pytest.fixture
def service(db_engine: Engine) -> KVKMirrorService:
    return KVKMirrorService(KVKMirrorReader(db_engine), McpOnbekendVraagWriter(db_engine))


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def make_basisprofiel(
    engine: Engine,
    kvk_nummer: str = "12345678",
    naam: str = "Test BV",
    hoofdactiviteit: str = "62010",
    rechtsvorm: str = "BeslVenn",
    registratie_datum_aanvang: datetime | None = None,
    registratie_datum_einde: datetime | None = None,
    niet_leverbaar_code: str | None = None,
    ind_non_mailing: str | None = None,
) -> BasisProfielORM:
    orm = BasisProfielORM(
        kvk_nummer=kvk_nummer,
        naam=naam,
        hoofdactiviteit=hoofdactiviteit,
        rechtsvorm=rechtsvorm,
        registratie_datum_aanvang=registratie_datum_aanvang,
        registratie_datum_einde=registratie_datum_einde,
        niet_leverbaar_code=niet_leverbaar_code,
        ind_non_mailing=ind_non_mailing,
        last_updated=datetime.now(UTC),
    )
    with Session(engine) as session:
        session.add(orm)
        session.commit()
    return orm


def make_vestiging(
    engine: Engine,
    kvk_nummer: str = "12345678",
    vestigingsnummer: str = "000012345678",
) -> VestigingenORM:
    orm = VestigingenORM(
        kvk_nummer=kvk_nummer,
        vestigingsnummer=vestigingsnummer,
        last_updated=datetime.now(UTC),
    )
    with Session(engine) as session:
        session.add(orm)
        session.commit()
    return orm


def make_vestigingsprofiel(
    engine: Engine,
    vestigingsnummer: str = "000012345678",
    kvk_nummer: str = "12345678",
    ind_hoofdvestiging: str = "Ja",
    hoofdactiviteit: str = "62010",
    cor_adres_plaats: str = "Amsterdam",
    cor_adres_volledig: str = "Teststraat 1, 1234AB Amsterdam",
    registratie_datum_einde_vestiging: datetime | None = None,
    niet_leverbaar_code: str | None = None,
    ind_non_mailing: str | None = None,
) -> VestigingsProfielORM:
    orm = VestigingsProfielORM(
        vestigingsnummer=vestigingsnummer,
        kvk_nummer=kvk_nummer,
        ind_hoofdvestiging=ind_hoofdvestiging,
        hoofdactiviteit=hoofdactiviteit,
        cor_adres_plaats=cor_adres_plaats,
        cor_adres_volledig=cor_adres_volledig,
        registratie_datum_einde_vestiging=registratie_datum_einde_vestiging,
        niet_leverbaar_code=niet_leverbaar_code,
        ind_non_mailing=ind_non_mailing,
        last_updated=datetime.now(UTC),
    )
    with Session(engine) as session:
        session.add(orm)
        session.commit()
    return orm
