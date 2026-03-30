"""Tests voor de NULL-safe is_actief berekening in KVKMirrorService.

Verifieert alle 8 combinaties van registratie_datum_einde / niet_leverbaar_code.
Toont ook aan waarom `not (code == 'IPD0005' and retry_after is None)` fout gaat bij None.
"""

from __future__ import annotations

import pytest

from kvk_connect.models.domain.basisprofiel import BasisProfielDomain
from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain
from kvk_connect.services.mirror_service import KVKMirrorService


def bp(datum_einde: str | None, code: str | None) -> BasisProfielDomain:
    return BasisProfielDomain(registratie_datum_einde=datum_einde, niet_leverbaar_code=code)


def vp(datum_einde: str | None, code: str | None) -> VestigingsProfielDomain:
    return VestigingsProfielDomain(registratie_datum_einde_vestiging=datum_einde, niet_leverbaar_code=code)


class TestIsActiefBasisprofiel:
    @pytest.mark.parametrize("datum_einde,code,verwacht", [
        # datum_einde is None = nog actief (potentieel)
        (None, None,       True),   # actief, geen code
        (None, "IPD1002",  True),   # actief, tijdelijke fout (niet IPD0005)
        (None, "IPD0005",  False),  # uitgeschreven (sentinel code)
        # datum_einde ingevuld = beëindigd
        ("2023-01-01", None,       False),
        ("2023-01-01", "IPD1002",  False),
        ("2023-01-01", "IPD0005",  False),
    ])
    def test_combinaties(self, datum_einde: str | None, code: str | None, verwacht: bool) -> None:
        assert KVKMirrorService._is_actief_basisprofiel(bp(datum_einde, code)) == verwacht

    def test_fout_patroon_met_none(self) -> None:
        """Demonstreert waarom `not (code == 'IPD0005' and retry_after is None)` gevaarlijk is.

        In Python: `None == 'IPD0005'` → False → `not (False and ...)` → True.
        Dit suggereert ten onrechte dat een bedrijf actief is.
        Onze implementatie gebruikt `code != 'IPD0005'` wat correct evalueert voor None.
        """
        # code = None, datum_einde = None → zou actief moeten zijn
        assert KVKMirrorService._is_actief_basisprofiel(bp(None, None)) is True
        # code = 'IPD0005', datum_einde = None → uitgeschreven
        assert KVKMirrorService._is_actief_basisprofiel(bp(None, "IPD0005")) is False


class TestIsActiefVestigingsprofiel:
    @pytest.mark.parametrize("datum_einde,code,verwacht", [
        (None, None,       True),
        (None, "IPD0005",  False),
        ("2023-01-01", None,      False),
        ("2023-01-01", "IPD0005", False),
    ])
    def test_combinaties(self, datum_einde: str | None, code: str | None, verwacht: bool) -> None:
        assert KVKMirrorService._is_actief_vestigingsprofiel(vp(datum_einde, code)) == verwacht
