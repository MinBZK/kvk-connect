"""Tests voor de MCP tool-handlers en main()-wiring in apps/mcp-server/main.py."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Engine

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "apps", "mcp-server"))

from tests.apps.mcp_server.conftest import make_basisprofiel, make_vestiging, make_vestigingsprofiel


def _inject_service(db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    """Stelt main._service in op een echte KVKMirrorService met SQLite in-memory."""
    import main
    from kvk_connect.db.mcp_onbekend_vraag_writer import McpOnbekendVraagWriter
    from kvk_connect.db.mirror_reader import KVKMirrorReader
    from kvk_connect.services.mirror_service import KVKMirrorService

    monkeypatch.setattr(
        main,
        "_service",
        KVKMirrorService(KVKMirrorReader(db_engine), McpOnbekendVraagWriter(db_engine)),
    )


# ---------------------------------------------------------------------------
# main() wiring
# ---------------------------------------------------------------------------


class TestMain:
    def test_main_roept_mcp_run_aan(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() initialiseert alles en roept mcp.run(transport=...) aan."""
        import main

        monkeypatch.setattr(sys, "argv", ["main.py"])
        monkeypatch.setattr(main, "create_engine", lambda *a, **kw: db_engine)
        monkeypatch.setattr(main, "ensure_database_initialized", lambda *a, **kw: None)

        run_calls: list[dict] = []
        monkeypatch.setattr(main.mcp, "run", lambda **kw: run_calls.append(kw))

        main.main()

        assert run_calls == [{"transport": "streamable-http"}]

    def test_main_zet_service_instantie(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        """Na main() is _service een KVKMirrorService instantie."""
        import main
        from kvk_connect.services.mirror_service import KVKMirrorService

        monkeypatch.setattr(sys, "argv", ["main.py"])
        monkeypatch.setattr(main, "create_engine", lambda *a, **kw: db_engine)
        monkeypatch.setattr(main, "ensure_database_initialized", lambda *a, **kw: None)
        monkeypatch.setattr(main.mcp, "run", lambda **kw: None)

        main.main()

        assert isinstance(main._service, KVKMirrorService)

    def test_default_host_en_port(self) -> None:
        """Standaard host is 0.0.0.0 en port 8000 — veilig in Docker."""
        import main

        assert main._host == os.getenv("MCP_HOST", "0.0.0.0")
        assert main._port == int(os.getenv("MCP_PORT", "8000"))

    def test_transport_configureerbaar_via_env(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        """MCP_TRANSPORT env var bepaalt transport modus."""
        import main

        monkeypatch.setenv("MCP_TRANSPORT", "stdio")
        monkeypatch.setattr(sys, "argv", ["main.py"])
        monkeypatch.setattr(main, "create_engine", lambda *a, **kw: db_engine)
        monkeypatch.setattr(main, "ensure_database_initialized", lambda *a, **kw: None)

        run_calls: list[dict] = []
        monkeypatch.setattr(main.mcp, "run", lambda **kw: run_calls.append(kw))
        main.main()

        assert run_calls[0]["transport"] == "stdio"

    def test_ongeldig_transport_geeft_exit(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        """Ongeldig MCP_TRANSPORT resulteert in SystemExit."""
        monkeypatch.setenv("MCP_TRANSPORT", "invalid")
        monkeypatch.setattr(sys, "argv", ["main.py"])

        import main

        monkeypatch.setattr(main, "create_engine", lambda *a, **kw: db_engine)
        monkeypatch.setattr(main, "ensure_database_initialized", lambda *a, **kw: None)

        with pytest.raises(SystemExit):
            main.main()

    def test_main_debug_flag_zet_log_level(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        """--debug flag resulteert in DEBUG log level."""
        import logging

        import main

        monkeypatch.setattr(sys, "argv", ["main.py", "--debug"])
        monkeypatch.setattr(main, "create_engine", lambda *a, **kw: db_engine)
        monkeypatch.setattr(main, "ensure_database_initialized", lambda *a, **kw: None)
        monkeypatch.setattr(main.mcp, "run", lambda **kw: None)

        configured_levels: list[int] = []
        original_configure = main.logging_config.configure
        monkeypatch.setattr(main.logging_config, "configure", lambda level: configured_levels.append(level))

        main.main()

        assert configured_levels == [logging.DEBUG]


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


class TestHealth:
    def test_health_retourneert_ok(self) -> None:
        from main import health

        response = asyncio.run(health(MagicMock()))
        assert response.status_code == 200
        assert json.loads(response.body) == {"status": "ok"}


# ---------------------------------------------------------------------------
# Tool handlers — alle 11 tools
# ---------------------------------------------------------------------------


class TestToolHandlers:
    def test_get_bedrijf_gevonden(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import get_bedrijf

        make_basisprofiel(db_engine, kvk_nummer="12345678", naam="Test BV")
        _inject_service(db_engine, monkeypatch)

        result = json.loads(asyncio.run(get_bedrijf("12345678")))
        assert result["status"] == "ok"
        assert result["data"]["naam"] == "Test BV"

    def test_get_bedrijf_niet_gevonden(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import get_bedrijf

        _inject_service(db_engine, monkeypatch)
        result = json.loads(asyncio.run(get_bedrijf("99999999")))
        assert result["status"] == "niet_gevonden"

    def test_get_bedrijf_include_non_mailing(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import get_bedrijf

        make_basisprofiel(db_engine, kvk_nummer="12345678", ind_non_mailing="Ja")
        _inject_service(db_engine, monkeypatch)

        assert json.loads(asyncio.run(get_bedrijf("12345678")))["status"] == "niet_gevonden"
        assert json.loads(asyncio.run(get_bedrijf("12345678", include_non_mailing=True)))["status"] == "ok"

    def test_get_vestiging_gevonden(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import get_vestiging

        make_basisprofiel(db_engine, kvk_nummer="12345678")
        make_vestigingsprofiel(db_engine, vestigingsnummer="000012345678", kvk_nummer="12345678")
        _inject_service(db_engine, monkeypatch)

        result = json.loads(asyncio.run(get_vestiging("000012345678")))
        assert result["status"] == "ok"
        assert result["data"]["vestigingsnummer"] == "000012345678"

    def test_get_vestiging_niet_gevonden(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import get_vestiging

        _inject_service(db_engine, monkeypatch)
        result = json.loads(asyncio.run(get_vestiging("999999999999")))
        assert result["status"] == "niet_gevonden"

    def test_list_vestigingen(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import list_vestigingen

        make_basisprofiel(db_engine, kvk_nummer="12345678")
        make_vestiging(db_engine, kvk_nummer="12345678", vestigingsnummer="000012345678")
        make_vestigingsprofiel(db_engine, vestigingsnummer="000012345678", kvk_nummer="12345678")
        _inject_service(db_engine, monkeypatch)

        result = json.loads(asyncio.run(list_vestigingen("12345678")))
        assert result["status"] == "ok"
        assert len(result["vestigingen"]) == 1
        assert result["vestigingen"][0]["vestigingsnummer"] == "000012345678"

    def test_get_alles(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import get_alles

        make_basisprofiel(db_engine, kvk_nummer="12345678", naam="Test BV")
        make_vestiging(db_engine, kvk_nummer="12345678", vestigingsnummer="000012345678")
        make_vestigingsprofiel(db_engine, vestigingsnummer="000012345678", kvk_nummer="12345678")
        _inject_service(db_engine, monkeypatch)

        result = json.loads(asyncio.run(get_alles("12345678")))
        assert result["status"] == "ok"
        assert result["basisprofiel"]["naam"] == "Test BV"
        assert len(result["vestigingen"]) == 1

    def test_get_alles_niet_gevonden(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import get_alles

        _inject_service(db_engine, monkeypatch)
        result = json.loads(asyncio.run(get_alles("99999999")))
        assert result["status"] == "niet_gevonden"

    def test_check_doorstarter_zonder_opvolger(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import check_doorstarter

        make_basisprofiel(db_engine, kvk_nummer="12345678")
        _inject_service(db_engine, monkeypatch)

        result = json.loads(asyncio.run(check_doorstarter("12345678")))
        assert result["status"] == "ok"
        assert result["doorstarters"] == []

    def test_zoek_op_naam_prefix(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import zoek_op_naam_prefix

        make_basisprofiel(db_engine, kvk_nummer="12345678", naam="Bakkerij de Mol")
        make_basisprofiel(db_engine, kvk_nummer="87654321", naam="Slagerij Hendriks")
        _inject_service(db_engine, monkeypatch)

        result = json.loads(asyncio.run(zoek_op_naam_prefix("Bakkerij")))
        assert result["status"] == "ok"
        namen = [r["naam"] for r in result["resultaten"]]
        assert "Bakkerij de Mol" in namen
        assert "Slagerij Hendriks" not in namen

    def test_filter_op_sbi(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import filter_op_sbi

        make_basisprofiel(db_engine, kvk_nummer="12345678")
        make_vestiging(db_engine, kvk_nummer="12345678", vestigingsnummer="000012345678")
        make_vestigingsprofiel(
            db_engine, vestigingsnummer="000012345678", kvk_nummer="12345678", hoofdactiviteit="62010"
        )
        _inject_service(db_engine, monkeypatch)

        result = json.loads(asyncio.run(filter_op_sbi("620")))
        assert result["status"] == "ok"
        assert len(result["resultaten"]) == 1

    def test_filter_op_sbi_gemeente_filter(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import filter_op_sbi

        make_basisprofiel(db_engine, kvk_nummer="12345678")
        make_vestiging(db_engine, kvk_nummer="12345678", vestigingsnummer="000012345678")
        make_vestigingsprofiel(
            db_engine,
            vestigingsnummer="000012345678",
            kvk_nummer="12345678",
            hoofdactiviteit="62010",
            cor_adres_plaats="Amsterdam",
        )
        _inject_service(db_engine, monkeypatch)

        assert len(json.loads(asyncio.run(filter_op_sbi("620", gemeente="Amsterdam")))["resultaten"]) == 1
        assert json.loads(asyncio.run(filter_op_sbi("620", gemeente="Rotterdam")))["resultaten"] == []

    def test_check_actiefstatus_batch(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import check_actiefstatus_batch

        make_basisprofiel(db_engine, kvk_nummer="12345678")
        _inject_service(db_engine, monkeypatch)

        result = json.loads(asyncio.run(check_actiefstatus_batch(["12345678", "99999999"])))
        by_kvk = {r["kvk_nummer"]: r for r in result["resultaten"]}
        assert by_kvk["12345678"]["gevonden"] is True
        assert by_kvk["99999999"]["gevonden"] is False

    def test_report_onbekende_vraag(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import report_onbekende_vraag

        _inject_service(db_engine, monkeypatch)
        result = json.loads(asyncio.run(report_onbekende_vraag("Wie is de eigenaar?")))
        assert result["status"] == "niet_ondersteund"
        assert "bericht" in result

    def test_get_basisprofiel_historie_leeg(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import get_basisprofiel_historie

        _inject_service(db_engine, monkeypatch)
        result = json.loads(asyncio.run(get_basisprofiel_historie("99999999")))
        assert result["status"] == "ok"
        assert result["historie"] == []

    def test_get_vestigingsprofiel_historie_leeg(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        from main import get_vestigingsprofiel_historie

        _inject_service(db_engine, monkeypatch)
        result = json.loads(asyncio.run(get_vestigingsprofiel_historie("999999999999")))
        assert result["status"] == "ok"
        assert result["historie"] == []


# ---------------------------------------------------------------------------
# JSON-structuur: verplichte velden en foutpaden
# ---------------------------------------------------------------------------


class TestToolJsonStructuur:
    def test_alle_tools_retourneren_geldige_json(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        """Elke tool retourneert een parseerbare JSON string — nooit een exception."""
        import main as m

        _inject_service(db_engine, monkeypatch)

        tools_en_args: list[tuple] = [
            (m.get_bedrijf, ("99999999",)),
            (m.get_vestiging, ("999999999999",)),
            (m.list_vestigingen, ("99999999",)),
            (m.get_alles, ("99999999",)),
            (m.check_doorstarter, ("99999999",)),
            (m.zoek_op_naam_prefix, ("Onbekend",)),
            (m.filter_op_sbi, ("99",)),
            (m.check_actiefstatus_batch, (["99999999"],)),
            (m.report_onbekende_vraag, ("test vraag",)),
            (m.get_basisprofiel_historie, ("99999999",)),
            (m.get_vestigingsprofiel_historie, ("999999999999",)),
        ]
        for fn, args in tools_en_args:
            raw = asyncio.run(fn(*args))
            assert isinstance(raw, str), f"{fn.__name__} retourneert geen string"
            parsed = json.loads(raw)
            assert isinstance(parsed, dict), f"{fn.__name__} retourneert geen JSON object"

    def test_data_quality_altijd_aanwezig(self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        """Elk tool-response bevat een data_quality object."""
        import main as m

        _inject_service(db_engine, monkeypatch)

        tools_en_args: list[tuple] = [
            (m.get_bedrijf, ("99999999",)),
            (m.get_vestiging, ("999999999999",)),
            (m.list_vestigingen, ("99999999",)),
            (m.get_alles, ("99999999",)),
            (m.check_doorstarter, ("99999999",)),
            (m.zoek_op_naam_prefix, ("x",)),
            (m.filter_op_sbi, ("99",)),
            (m.check_actiefstatus_batch, (["99999999"],)),
            (m.report_onbekende_vraag, ("vraag",)),
            (m.get_basisprofiel_historie, ("99999999",)),
            (m.get_vestigingsprofiel_historie, ("999999999999",)),
        ]
        for fn, args in tools_en_args:
            result = json.loads(asyncio.run(fn(*args)))
            assert "data_quality" in result, f"{fn.__name__} mist data_quality sleutel"

    def test_batch_boven_limiet_retourneert_fout_json(
        self, db_engine: Engine, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """check_actiefstatus_batch met >200 nummers retourneert JSON-fout, geen exception."""
        from main import check_actiefstatus_batch

        _inject_service(db_engine, monkeypatch)
        nummers = [str(i).zfill(8) for i in range(201)]
        result = json.loads(asyncio.run(check_actiefstatus_batch(nummers)))
        assert result["status"] == "fout"
        assert "maximaal" in result["bericht"].lower()

    def test_service_niet_geinitialiseerd_geeft_assertion_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Wanneer _service None is gooit elke tool AssertionError (server niet gestart)."""
        import main

        monkeypatch.setattr(main, "_service", None)
        with pytest.raises(AssertionError):
            asyncio.run(main.get_bedrijf("12345678"))
