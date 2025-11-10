import logging

from kvk_connect.api.client import KVKApiClient
from kvk_connect.mappers.kvk_record_mapper import map_kvkbasisprofiel_api_to_kvkrecord
from kvk_connect.mappers.map_vestigingen_api_to_vestigingsnummers import map_vestigingen_api_to_vestigingsnummers
from kvk_connect.mappers.map_vestigingsprofiel_api_to_vestigingsprofiel_domain import (
    map_vestigingsprofiel_api_to_vestigingsprofiel_domain,
)
from kvk_connect.models.domain import KvKVestigingsNummersDomain
from kvk_connect.models.domain.basisprofiel import BasisProfielDomain
from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain
from kvk_connect.utils.tools import clean_and_pad

logger = logging.getLogger(__name__)


class KVKRecordService:
    """Service for fetching and mapping KVK records to domain models."""

    def __init__(self, client: KVKApiClient) -> None:
        self.client = client

    def get_basisprofiel(self, kvk_nummer: str) -> BasisProfielDomain | None:
        """Fetch and map basisprofiel for a given KVK number.

        Returns:
            BasisProfielDomain if found, None otherwise.
        """
        kvk_nummer = clean_and_pad(kvk_nummer)
        bp_api = self.client.get_basisprofiel(kvk_nummer)

        if bp_api is None:
            logger.info("No basisprofiel found for KVK number %s", kvk_nummer)
            return None

        return map_kvkbasisprofiel_api_to_kvkrecord(bp_api)

    def get_vestigingen(self, kvk_nummer: str) -> KvKVestigingsNummersDomain | None:
        """Fetch and map vestigingen for a given KVK number.

        Returns:
            KvKVestigingsNummersDomain if found, None otherwise.
        """
        kvk_nummer = clean_and_pad(kvk_nummer)
        vn_api = self.client.get_vestigingen(kvk_nummer)

        if vn_api is None:
            logger.info("No vestigingen found for KVK number %s", kvk_nummer)
            return None

        return map_vestigingen_api_to_vestigingsnummers(vn_api)

    def get_vestigingsprofiel(self, vestigings_nummer: str) -> VestigingsProfielDomain | None:
        """Fetch and map vestigingsprofiel for a given vestigingsnummer.

        Returns:
            VestigingsProfielDomain if found, None otherwise.
        """
        vestigings_nummer = clean_and_pad(vestigings_nummer)
        vp_api = self.client.get_vestigingsprofiel(vestigings_nummer, geo_data=True)

        if vp_api is None:
            logger.info("No vestigingsprofiel found for vestigingsnummer %s", vestigings_nummer)
            return None

        return map_vestigingsprofiel_api_to_vestigingsprofiel_domain(vp_api)
