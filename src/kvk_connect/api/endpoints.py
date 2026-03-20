import os

DEFAULT_BASE_URL = os.getenv("KVK_BASE_URL", "https://api.kvk.nl/api/v1")


def basisprofiel(kvk_nummer: str) -> str:
    """Geef de URL voor het basisprofiel van een KVK nummer."""
    return f"{DEFAULT_BASE_URL}/basisprofielen/{kvk_nummer}"


def vestigingen(kvk_nummer: str) -> str:
    """Geef de URL voor de vestigingen van een KVK nummer."""
    return f"{DEFAULT_BASE_URL}/basisprofielen/{kvk_nummer}/vestigingen"


def vestigingsprofiel(vestigingsnummer: str) -> str:
    """Geef de URL voor het vestigingsprofiel van een vestigingsnummer."""
    return f"{DEFAULT_BASE_URL}/vestigingsprofielen/{vestigingsnummer}"


def mutatieservice(abonnement_id: str) -> str:
    """Geef de URL voor de mutatieservice van een abonnement."""
    return f"{DEFAULT_BASE_URL}/abonnementen/{abonnement_id}"


def mutatieservice_signaal(abonnement_id: str, signaal_id: str) -> str:
    """Geef de URL voor een specifiek mutatiesignaal."""
    return f"{DEFAULT_BASE_URL}/abonnementen/{abonnement_id}/signalen/{signaal_id}"
