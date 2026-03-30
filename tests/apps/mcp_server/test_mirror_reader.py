"""Tests voor KVKMirrorReader — adapter-laag die ORM naar domain-objecten converteert."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine

from kvk_connect.db.mirror_reader import KVKMirrorReader
from kvk_connect.models.orm.basisprofiel_historie_orm import BasisProfielHistorieORM
from kvk_connect.models.orm.vestigingen_orm import VestigingenORM
from kvk_connect.models.orm.vestigingsprofiel_historie_orm import VestigingsProfielHistorieORM
from kvk_connect.models.orm.mcp_onbekende_vraag_orm import McpOnbekendVraagORM  # noqa: F401
from sqlalchemy.orm import Session

from tests.apps.mcp_server.conftest import make_basisprofiel, make_vestiging, make_vestigingsprofiel


class TestGetBasisprofiel:
    def test_gevonden(self, db_engine: Engine, reader: KVKMirrorReader) -> None:
        make_basisprofiel(db_engine, kvk_nummer="12345678", naam="Test BV")
        result = reader.get_basisprofiel("12345678")
        assert result is not None
        assert result.kvk_nummer == "12345678"
        assert result.naam == "Test BV"

    def test_niet_gevonden(self, reader: KVKMirrorReader) -> None:
        assert reader.get_basisprofiel("99999999") is None

    def test_datum_als_iso_string(self, db_engine: Engine, reader: KVKMirrorReader) -> None:
        make_basisprofiel(
            db_engine,
            kvk_nummer="11111111",
            registratie_datum_aanvang=datetime(2020, 1, 15, tzinfo=UTC),
        )
        result = reader.get_basisprofiel("11111111")
        assert result is not None
        assert result.registratie_datum_aanvang == "2020-01-15"

    def test_niet_leverbaar_code_doorgegeven(self, db_engine: Engine, reader: KVKMirrorReader) -> None:
        make_basisprofiel(db_engine, kvk_nummer="22222222", niet_leverbaar_code="IPD0005")
        result = reader.get_basisprofiel("22222222")
        assert result is not None
        assert result.niet_leverbaar_code == "IPD0005"


class TestGetVestigingsnummers:
    def test_normaal(self, db_engine: Engine, reader: KVKMirrorReader) -> None:
        make_basisprofiel(db_engine, kvk_nummer="12345678")
        make_vestiging(db_engine, kvk_nummer="12345678", vestigingsnummer="000012345678")
        result = reader.get_vestigingsnummers("12345678")
        assert result == ["000012345678"]

    def test_sentinel_gefilterd(self, db_engine: Engine, reader: KVKMirrorReader) -> None:
        make_basisprofiel(db_engine, kvk_nummer="12345678")
        with Session(db_engine) as session:
            session.add(
                VestigingenORM(
                    kvk_nummer="12345678",
                    vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
                    last_updated=datetime.now(UTC),
                )
            )
            session.commit()
        result = reader.get_vestigingsnummers("12345678")
        assert VestigingenORM.SENTINEL_VESTIGINGSNUMMER not in result
        assert result == []

    def test_leeg_als_geen_vestigingen(self, reader: KVKMirrorReader) -> None:
        assert reader.get_vestigingsnummers("99999999") == []


class TestGetVestigingsprofiel:
    def test_gevonden(self, db_engine: Engine, reader: KVKMirrorReader) -> None:
        make_basisprofiel(db_engine, kvk_nummer="12345678")
        make_vestigingsprofiel(db_engine, vestigingsnummer="000012345678", kvk_nummer="12345678")
        result = reader.get_vestigingsprofiel("000012345678")
        assert result is not None
        assert result.vestigingsnummer == "000012345678"

    def test_niet_gevonden(self, reader: KVKMirrorReader) -> None:
        assert reader.get_vestigingsprofiel("999999999999") is None

    def test_gps_als_string(self, db_engine: Engine, reader: KVKMirrorReader) -> None:
        from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM
        with Session(db_engine) as session:
            session.add(VestigingsProfielORM(
                vestigingsnummer="000099999999",
                kvk_nummer="99999998",
                cor_adres_gps_latitude=52.370216,
                cor_adres_gps_longitude=4.895168,
                last_updated=datetime.now(UTC),
            ))
            session.commit()
        result = reader.get_vestigingsprofiel("000099999999")
        assert result is not None
        assert result.cor_adres_gps_latitude == "52.370216"
        assert result.cor_adres_gps_longitude == "4.895168"


class TestGetBasisprofieldHistorie:
    def test_volgorde_desc(self, db_engine: Engine, reader: KVKMirrorReader) -> None:
        with Session(db_engine) as session:
            session.add_all([
                BasisProfielHistorieORM(
                    kvk_nummer="12345678",
                    gewijzigd_op=datetime(2024, 1, 1, tzinfo=UTC),
                    naam="Oud",
                ),
                BasisProfielHistorieORM(
                    kvk_nummer="12345678",
                    gewijzigd_op=datetime(2024, 6, 1, tzinfo=UTC),
                    naam="Nieuw",
                ),
            ])
            session.commit()
        result = reader.get_basisprofiel_historie("12345678")
        assert len(result) == 2
        assert result[0].gewijzigd_op > result[1].gewijzigd_op

    def test_leeg_als_geen_historie(self, reader: KVKMirrorReader) -> None:
        assert reader.get_basisprofiel_historie("99999999") == []


class TestZoekOpNaamPrefix:
    def test_prefix_match(self, db_engine: Engine, reader: KVKMirrorReader) -> None:
        make_basisprofiel(db_engine, kvk_nummer="11111111", naam="Bakkerij de Mol")
        make_basisprofiel(db_engine, kvk_nummer="22222222", naam="Bakkerij van Dam")
        make_basisprofiel(db_engine, kvk_nummer="33333333", naam="Slagerij Hendriks")
        result = reader.zoek_op_naam_prefix("Bakkerij", limit=10)
        namen = [r.naam for r in result]
        assert "Bakkerij de Mol" in namen
        assert "Bakkerij van Dam" in namen
        assert "Slagerij Hendriks" not in namen

    def test_limit_gerespecteerd(self, db_engine: Engine, reader: KVKMirrorReader) -> None:
        for i in range(5):
            make_basisprofiel(db_engine, kvk_nummer=f"1000000{i}", naam=f"Firma {i}")
        result = reader.zoek_op_naam_prefix("Firma", limit=3)
        assert len(result) <= 3


class TestCheckActiefstatusBatch:
    def test_batch_gevonden(self, db_engine: Engine, reader: KVKMirrorReader) -> None:
        make_basisprofiel(db_engine, kvk_nummer="11111111")
        make_basisprofiel(db_engine, kvk_nummer="22222222")
        result = reader.check_actiefstatus_batch(["11111111", "22222222"])
        kvk_nummers = [r.kvk_nummer for r in result]
        assert "11111111" in kvk_nummers
        assert "22222222" in kvk_nummers

    def test_onbekende_nummers_geven_leeg(self, reader: KVKMirrorReader) -> None:
        result = reader.check_actiefstatus_batch(["99999999"])
        assert result == []


class TestGetKvkNummersOpVestigingsnummers:
    def test_vindt_andere_kvk(self, db_engine: Engine, reader: KVKMirrorReader) -> None:
        make_basisprofiel(db_engine, kvk_nummer="11111111")
        make_basisprofiel(db_engine, kvk_nummer="22222222")
        make_vestiging(db_engine, kvk_nummer="11111111", vestigingsnummer="000011111111")
        make_vestiging(db_engine, kvk_nummer="22222222", vestigingsnummer="000011111111")
        result = reader.get_kvk_nummers_op_vestigingsnummers(["000011111111"], exclude_kvk_nummer="11111111")
        kvk_nummers = [k for k, _ in result]
        assert "22222222" in kvk_nummers
        assert "11111111" not in kvk_nummers

    def test_lege_lijst_geeft_leeg(self, reader: KVKMirrorReader) -> None:
        assert reader.get_kvk_nummers_op_vestigingsnummers([], "11111111") == []
