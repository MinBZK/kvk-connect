# ruff: noqa: D103
import os

DEFAULT_BASE_URL = os.getenv("KVK_BASE_URL", "https://api.kvk.nl/api/v1")


def basisprofiel(kvk_nummer: str) -> str:
    return f"{DEFAULT_BASE_URL}/basisprofielen/{kvk_nummer}"


def vestigingen(kvk_nummer: str) -> str:
    return f"{DEFAULT_BASE_URL}/basisprofielen/{kvk_nummer}/vestigingen"


def vestigingsprofiel(vestigingsnummer: str) -> str:
    return f"{DEFAULT_BASE_URL}/vestigingsprofielen/{vestigingsnummer}"


def mutatieservice(abonnement_id: str) -> str:
    return f"{DEFAULT_BASE_URL}/abonnementen/{abonnement_id}"


def mutatieservice_signaal(abonnement_id: str, signaal_id: str) -> str:
    return f"{DEFAULT_BASE_URL}/abonnementen/{abonnement_id}/signalen/{signaal_id}"
