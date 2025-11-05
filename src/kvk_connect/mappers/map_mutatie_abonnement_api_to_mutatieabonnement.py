from kvk_connect.models.api.mutatie_abonnementen_api import MutatieAbonnementenAPI
from kvk_connect.models.domain.mutatie_abonnement import MutatieAbonnementDomain


def map_mutatie_abonnement_api_to_mutatieabonnement(api_model: MutatieAbonnementenAPI) -> MutatieAbonnementDomain:
    """Maps MutatieAbonnementenAPI to MutatieAbonnement.

    Extracts abonnement IDs from the API model.
    """
    abonnement_ids = [abonnement.id for abonnement in api_model.abonnementen]
    return MutatieAbonnementDomain(abonnement_ids=abonnement_ids)
