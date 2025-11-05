import json
import unittest

from kvk_connect.mappers.map_mutatie_abonnement_api_to_mutatieabonnement import (
    map_mutatie_abonnement_api_to_mutatieabonnement,
)
from kvk_connect.models.api.mutatie_abonnementen_api import MutatieAbonnementenAPI
from kvk_connect.models.domain.mutatie_abonnement import MutatieAbonnementDomain


class TestMutatieService(unittest.TestCase):
    def test_MutatieAbbo(self):
        with open("data/test_input_mutatie_abonnement.json", encoding="utf-8") as f:
            mutatie_abonnement = MutatieAbonnementenAPI.from_dict(json.load(f))
            print(mutatie_abonnement)

        with open("data/expected_output_mutatie_abonnement.json", encoding="utf-8") as f:
            expected_domain = MutatieAbonnementDomain.from_dict(json.load(f))

        mapped = map_mutatie_abonnement_api_to_mutatieabonnement(mutatie_abonnement)
        assert mapped == expected_domain


if __name__ == "__main__":
    unittest.main()
