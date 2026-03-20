from __future__ import annotations

import datetime
import logging

import requests
from requests import Response

from ..exceptions import KVKPermanentError, KVKTemporaryError
from ..models.api.basisprofiel_api import BasisProfielAPI
from ..models.api.mutatiesignalen_api import MutatiesAPI
from ..models.api.vestigingen_api import VestigingenAPI
from ..models.api.vestigingsprofiel_api import VestigingsProfielAPI
from ..utils.rate_limit import global_rate_limit
from . import endpoints
from .session import create_session_with_retries
from kvk_connect.services.kvk_api_protocol import KVKApiClientProtocol

_TEMPORARY_CODES = {"IPD1002", "IPD1003"}

logger = logging.getLogger(__name__)


class KVKApiClient(KVKApiClientProtocol):
    def __init__(self, api_key: str, base_url: str = endpoints.DEFAULT_BASE_URL):
        self.session = create_session_with_retries()  # requests.Session()
        self.session.headers.update({"apikey": api_key})
        self.base_url = base_url
        self.timeout = 600

    def close(self) -> None:
        """Sluit de onderliggende HTTP sessie."""
        self.session.close()

    @staticmethod
    def _get_error_payload(resp: Response) -> str:
        """Extract error payload from response as string for logging."""
        try:
            error_data = resp.json()
            return str(error_data)
        except (ValueError, requests.exceptions.JSONDecodeError):
            return resp.text if resp.text else "No error details available"

    @staticmethod
    def _extract_kvk_fout_codes(resp: Response) -> list[tuple[str, str]]:
        """Extraheer lijst van (code, omschrijving) tuples uit KVK fout-response."""
        try:
            fouten = resp.json().get("fout", [])
            return [(f.get("code", ""), f.get("omschrijving", "")) for f in fouten if f.get("code")]
        except (ValueError, requests.exceptions.JSONDecodeError):
            return []

    @staticmethod
    def _raise_for_kvk_fout(identifier: str, resp: Response) -> None:
        """Raise KVKPermanentError of KVKTemporaryError op basis van foutcodes in de response."""
        fouten = KVKApiClient._extract_kvk_fout_codes(resp)
        for code, omschrijving in fouten:
            if code in _TEMPORARY_CODES:
                raise KVKTemporaryError(identifier, code, omschrijving)
            raise KVKPermanentError(identifier, code, omschrijving)

    @global_rate_limit()
    def get_mutatie_signaal_raw(self, abonnement_id: str, signaal_id: str) -> dict | None:
        """Get raw mutatie signaal data from KVK API.

        Args:
            abonnement_id (str): Abonnement ID.
            signaal_id (str): Signaal ID.

            Return: Originele JSON of None bij fout.
        """
        url = endpoints.mutatieservice_signaal(abonnement_id, signaal_id)
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RetryError:
            logger.warning("KVK API retry exhausted for signaal %s (500s)", signaal_id)
            return None
        except requests.HTTPError as e:
            logger.warning("KVK API error for nummer %s: %s", abonnement_id, e)
            logger.warning("Mogelijke error: %s", self._get_error_payload(resp))
            return None

    @global_rate_limit()
    def get_mutaties_raw(
        self, abonnement_id: str, from_time: datetime.datetime, to_time: datetime.datetime, page: int, size: int
    ) -> dict | None:
        """Get raw mutaties data from KVK API.

        Args:
            abonnement_id (str): Abonnement ID.
            from_time (datetime): Start datetime for mutaties.
            to_time (datetime): End datetime for mutaties.
            page (int): Page number.
            size (int): Number of items per page.

            Return: Originele JSON of None bij fout.
        """
        url = endpoints.mutatieservice(abonnement_id)
        try:
            params = {
                "vanaf": from_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "tot": to_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "pagina": str(page),
                "aantal": str(size),
            }
            resp = self.session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RetryError:
            logger.warning("KVK API retry exhausted for mutaties %s (500s)", abonnement_id)
            return None
        except requests.HTTPError as e:
            logger.warning("KVK API error for nummer %s: %s", abonnement_id, e)
            logger.warning("Mogelijke error: %s", self._get_error_payload(resp))
            return None

    def get_mutaties(
        self, abonnement_id: str, from_time: datetime.datetime, to_time: datetime.datetime, page: int, size: int
    ) -> MutatiesAPI | None:
        """Get mutaties from KVK API in domein model."""
        data = self.get_mutaties_raw(abonnement_id, from_time, to_time, page, size)
        return None if data is None else MutatiesAPI.from_dict(data)

    @global_rate_limit()
    def get_basisprofiel_raw(self, kvk_nummer: str, geo_data: bool = True) -> dict | None:
        """Get raw basisinformatie data from KVK API.

        Args:
            kvk_nummer (str): KVK nummer.
            geo_data (bool): Include geo data or not.

            Return: Originele JSON of None bij fout.
        """
        url = endpoints.basisprofiel(kvk_nummer)
        try:
            resp = self.session.get(url, params={"geoData": geo_data}, timeout=self.timeout)
            resp.raise_for_status()
            logger.debug("KVK Basisinformatie Raw response for kvk nummer %s: %s", kvk_nummer, resp.json())
            return resp.json()
        except requests.exceptions.RetryError:
            logger.warning("KVK API retry exhausted for basisprofiel %s (500s)", kvk_nummer)
            raise KVKTemporaryError(kvk_nummer, "HTTP500", "Gateway 500 retry exhausted")
        except requests.HTTPError as e:
            logger.warning("KVK API error for nummer %s: %s", kvk_nummer, e)
            logger.warning("Mogelijke error: %s", self._get_error_payload(resp))
            self._raise_for_kvk_fout(kvk_nummer, resp)
            return None

    def get_basisprofiel(self, kvk_nummer: str, geo_data: bool = True) -> BasisProfielAPI | None:
        """Get basisinformatie from KVK API in domein model."""
        data = self.get_basisprofiel_raw(kvk_nummer, geo_data)
        return None if data is None else BasisProfielAPI.from_dict(data)

    @global_rate_limit()
    def get_vestigingen_raw(self, kvk_nummer: str) -> dict | None:
        """Get raw vestigingen data from KVK API.

        Args:
            kvk_nummer (str): KVK nummer.
            geo_data (bool): Include geo data or not.

            Return: Originele JSON of None bij fout.
        """
        url = endpoints.vestigingen(kvk_nummer)
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            logger.debug(
                "KVK Vestigingen Raw response for kvk nummer %s: %s, with url: %s", kvk_nummer, resp.json(), url
            )
            return resp.json()
        except requests.exceptions.RetryError:
            logger.warning("KVK API retry exhausted for vestigingen %s (500s)", kvk_nummer)
            raise KVKTemporaryError(kvk_nummer, "HTTP500", "Gateway 500 retry exhausted")
        except requests.HTTPError as e:
            logger.warning("KVK API error for nummer %s: %s", kvk_nummer, e)
            logger.warning("Mogelijke error: %s", self._get_error_payload(resp))
            self._raise_for_kvk_fout(kvk_nummer, resp)
            return None

    def get_vestigingen(self, kvk_nummer: str) -> VestigingenAPI | None:
        """Get vestigingen from KVK API in domein model."""
        data = self.get_vestigingen_raw(kvk_nummer)
        return None if data is None else VestigingenAPI.from_dict(data)

    @global_rate_limit()
    def get_vestigingsprofiel_raw(self, vestigingsnummer: str, geo_data: bool = True) -> dict | None:
        """Get raw vestigingsprofiel data from KVK API.

        Args:
            vestigingsnummer (str): Vestigingsnummer.
            geo_data (bool): Include geo data or not.

            Return: Originele JSON of None bij fout.
        """
        url = endpoints.vestigingsprofiel(vestigingsnummer)
        try:
            resp = self.session.get(url, params={"geoData": geo_data}, timeout=self.timeout)
            resp.raise_for_status()
            logger.debug(
                "KVK VestigingenProfiel Raw response for vestigingen nummer %s: %s, with url: %s",
                vestigingsnummer,
                resp.json(),
                url,
            )
            return resp.json()
        except requests.exceptions.RetryError:
            logger.warning("KVK API retry exhausted for vestigingsprofiel %s (500s)", vestigingsnummer)
            raise KVKTemporaryError(vestigingsnummer, "HTTP500", "Gateway 500 retry exhausted")
        except requests.HTTPError as e:
            logger.warning("KVK API error for nummer %s: %s", vestigingsnummer, e)
            logger.warning("Mogelijke error: %s", self._get_error_payload(resp))
            self._raise_for_kvk_fout(vestigingsnummer, resp)
            return None

    def get_vestigingsprofiel(self, vestigingsnummer: str, geo_data: bool = True) -> VestigingsProfielAPI | None:
        """Get vestigingsprofiel from KVK API in domein model."""
        data = self.get_vestigingsprofiel_raw(vestigingsnummer, geo_data)
        return None if data is None else VestigingsProfielAPI.from_dict(data)
