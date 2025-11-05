import json
import unittest

from kvk_connect.mappers.kvk_record_mapper import map_kvkbasisprofiel_api_to_kvkrecord
from kvk_connect.mappers.map_vestigingen_api_to_vestigingsnummers import map_vestigingen_api_to_vestigingsnummers
from kvk_connect.mappers.map_vestigingsprofiel_api_to_vestigingsprofiel_domain import (
    map_vestigingsprofiel_api_to_vestigingsprofiel_domain,
)
from kvk_connect.models.api.basisprofiel_api import BasisProfielAPI
from kvk_connect.models.api.vestigingen_api import VestigingenAPI
from kvk_connect.models.api.vestigingsprofiel_api import VestigingsProfielAPI
from kvk_connect.models.domain import BasisProfielDomain, KvKVestigingsNummersDomain
from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain


class TestKvkParser(unittest.TestCase):
    def test_parse_basisprofiel(self):
        with open("data/test_input_basisprofiel.json", encoding="utf-8") as f:
            bapi = BasisProfielAPI.from_dict(json.load(f))

        with open("data/expected_output_basisprofiel.json", encoding="utf-8") as f:
            expected_domeinmodel = BasisProfielDomain.from_dict(json.load(f))

        mapped = map_kvkbasisprofiel_api_to_kvkrecord(bapi)
        assert mapped == expected_domeinmodel

    def test_parse_vestigingsnummers(self):
        with open("data/test_input_vestigingen.json", encoding="utf-8") as f:
            vapi = VestigingenAPI.from_dict(json.load(f))
        with open("data/expected_output_vestigingsnummers.json", encoding="utf-8") as f:
            expected_domainmodel = KvKVestigingsNummersDomain.from_dict(json.load(f))

        mapped = map_vestigingen_api_to_vestigingsnummers(vapi)
        assert mapped == expected_domainmodel

    def test_parse_vestigingsprofielen(self):
        with open("data/test_input_vestigingsprofielen.json", encoding="utf-8") as f:
            vpapi = VestigingsProfielAPI.from_dict(json.load(f))
        with open("data/expected_output_vestigingsprofielen.json", encoding="utf-8") as f:
            expected_domainmodel = VestigingsProfielDomain.from_dict(json.load(f))

        mapped = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(vpapi)
        assert mapped == expected_domainmodel


if __name__ == "__main__":
    unittest.main()
