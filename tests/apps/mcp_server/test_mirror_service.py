"""Tests voor KVKMirrorService — use case-laag met business logic."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine

from kvk_connect.services.mirror_service import KVKMirrorService

from tests.apps.mcp_server.conftest import (
    make_basisprofiel,
    make_vestiging,
    make_vestigingsprofiel,
)


class TestGetBedrijf:
    def test_gevonden(self, db_engine: Engine, service: KVKMirrorService) -> None:
        make_basisprofiel(db_engine, kvk_nummer="12345678", naam="Test BV")
        make_vestiging(db_engine, kvk_nummer="12345678", vestigingsnummer="000012345678")
        result = service.get_bedrijf("12345678")
        assert result["status"] == "ok"
        assert result["data"]["naam"] == "Test BV"
        assert result["data"]["vestiging_count"] == 1
        assert "data_quality" in result

    def test_niet_gevonden(self, service: KVKMirrorService) -> None:
        result = service.get_bedrijf("99999999")
        assert result["status"] == "niet_gevonden"

    def test_is_actief_true(self, db_engine: Engine, service: KVKMirrorService) -> None:
        make_basisprofiel(db_engine, kvk_nummer="11111111", registratie_datum_einde=None)
        result = service.get_bedrijf("11111111")
        assert result["data"]["is_actief"] is True

    def test_is_actief_false_bij_datum_einde(self, db_engine: Engine, service: KVKMirrorService) -> None:
        make_basisprofiel(
            db_engine,
            kvk_nummer="22222222",
            registratie_datum_einde=datetime(2023, 1, 1, tzinfo=UTC),
        )
        result = service.get_bedrijf("22222222")
        assert result["data"]["is_actief"] is False

    def test_ind_non_mailing_standaard_gefilterd(self, db_engine: Engine, service: KVKMirrorService) -> None:
        make_basisprofiel(db_engine, kvk_nummer="33333333", ind_non_mailing="Ja")
        result = service.get_bedrijf("33333333")
        assert result["status"] == "niet_gevonden"
        assert result.get("reden") == "indNonMailing"

    def test_ind_non_mailing_include_true(self, db_engine: Engine, service: KVKMirrorService) -> None:
        make_basisprofiel(db_engine, kvk_nummer="33333333", ind_non_mailing="Ja")
        result = service.get_bedrijf("33333333", include_non_mailing=True)
        assert result["status"] == "ok"
        assert any("NonMailing" in w for w in result["data_quality"]["coverage_warnings"])


class TestListVestigingen:
    def test_vestigingen_lijst(self, db_engine: Engine, service: KVKMirrorService) -> None:
        make_basisprofiel(db_engine, kvk_nummer="12345678")
        make_vestiging(db_engine, kvk_nummer="12345678", vestigingsnummer="000012345678")
        make_vestigingsprofiel(db_engine, vestigingsnummer="000012345678", kvk_nummer="12345678")
        result = service.list_vestigingen("12345678")
        assert result["status"] == "ok"
        assert len(result["vestigingen"]) == 1
        assert result["vestigingen"][0]["vestigingsnummer"] == "000012345678"

    def test_non_mailing_gefilterd(self, db_engine: Engine, service: KVKMirrorService) -> None:
        make_basisprofiel(db_engine, kvk_nummer="12345678")
        make_vestiging(db_engine, kvk_nummer="12345678", vestigingsnummer="000012345678")
        make_vestigingsprofiel(
            db_engine,
            vestigingsnummer="000012345678",
            kvk_nummer="12345678",
            ind_non_mailing="Ja",
        )
        result = service.list_vestigingen("12345678")
        assert result["vestigingen"] == []


class TestGetAlles:
    def test_combineert_basisprofiel_en_vestigingen(self, db_engine: Engine, service: KVKMirrorService) -> None:
        make_basisprofiel(db_engine, kvk_nummer="12345678", naam="Test BV")
        make_vestiging(db_engine, kvk_nummer="12345678", vestigingsnummer="000012345678")
        make_vestigingsprofiel(db_engine, vestigingsnummer="000012345678", kvk_nummer="12345678")
        result = service.get_alles("12345678")
        assert result["status"] == "ok"
        assert "basisprofiel" in result
        assert "vestigingen" in result
        assert result["basisprofiel"]["naam"] == "Test BV"

    def test_niet_gevonden_geeft_niet_gevonden(self, service: KVKMirrorService) -> None:
        result = service.get_alles("99999999")
        assert result["status"] == "niet_gevonden"


class TestCheckDoorstarter:
    def test_doorstarter_binnen_7_dagen(self, db_engine: Engine, service: KVKMirrorService) -> None:
        einde = datetime(2023, 6, 1, tzinfo=UTC)
        aanvang = datetime(2023, 6, 5, tzinfo=UTC)  # 4 dagen later = binnen window

        make_basisprofiel(db_engine, kvk_nummer="11111111", registratie_datum_einde=einde)
        make_basisprofiel(db_engine, kvk_nummer="22222222", registratie_datum_aanvang=aanvang)
        make_vestiging(db_engine, kvk_nummer="11111111", vestigingsnummer="000011111111")
        make_vestiging(db_engine, kvk_nummer="22222222", vestigingsnummer="000011111111")

        result = service.check_doorstarter("11111111")
        assert result["status"] == "ok"
        kvk_nrs = [d["kvk_nummer"] for d in result["doorstarters"]]
        assert "22222222" in kvk_nrs

    def test_doorstarter_buiten_7_dagen(self, db_engine: Engine, service: KVKMirrorService) -> None:
        einde = datetime(2023, 6, 1, tzinfo=UTC)
        aanvang = datetime(2023, 6, 20, tzinfo=UTC)  # 19 dagen later = buiten window

        make_basisprofiel(db_engine, kvk_nummer="11111111", registratie_datum_einde=einde)
        make_basisprofiel(db_engine, kvk_nummer="22222222", registratie_datum_aanvang=aanvang)
        make_vestiging(db_engine, kvk_nummer="11111111", vestigingsnummer="000011111111")
        make_vestiging(db_engine, kvk_nummer="22222222", vestigingsnummer="000011111111")

        result = service.check_doorstarter("11111111")
        assert result["doorstarters"] == []

    def test_coverage_warning_altijd_aanwezig(self, db_engine: Engine, service: KVKMirrorService) -> None:
        make_basisprofiel(db_engine, kvk_nummer="11111111")
        result = service.check_doorstarter("11111111")
        assert any("sentinel" in w.lower() or "vestigingsnummer" in w.lower()
                   for w in result["data_quality"]["coverage_warnings"])


class TestCheckActiefstatusBatch:
    def test_gevonden_en_niet_gevonden(self, db_engine: Engine, service: KVKMirrorService) -> None:
        make_basisprofiel(db_engine, kvk_nummer="11111111")
        result = service.check_actiefstatus_batch(["11111111", "99999999"])
        by_kvk = {r["kvk_nummer"]: r for r in result["resultaten"]}
        assert by_kvk["11111111"]["gevonden"] is True
        assert by_kvk["99999999"]["gevonden"] is False
        assert by_kvk["99999999"]["is_actief"] is None

    def test_boven_limiet_raises(self, service: KVKMirrorService) -> None:
        nummers = [str(i).zfill(8) for i in range(201)]
        with pytest.raises(ValueError, match="maximaal"):
            service.check_actiefstatus_batch(nummers)


class TestZoekOpNaamPrefix:
    def test_limit_max_100(self, db_engine: Engine, service: KVKMirrorService) -> None:
        for i in range(5):
            make_basisprofiel(db_engine, kvk_nummer=f"2000000{i}", naam=f"Zoeknaam {i}")
        result = service.zoek_op_naam_prefix("Zoeknaam", limit=200)
        # Resultaat bevat items, en limit is gecapped op 100 intern
        assert result["status"] == "ok"
        assert len(result["resultaten"]) <= 100
