import os

from dotenv import load_dotenv

load_dotenv()  # Loads variables from .env if present

API_KEY = os.getenv("KVK_API_KEY_PROD")
API_KEY_PROD = API_KEY
API_KEY_TEST = os.getenv("KVK_API_KEY_TEST")
KVK_MUTATIE_ABONNEMENT_ID = os.getenv("KVK_MUTATIE_ABONNEMENT_ID")

SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI")

KVK_TEST_NR = 83929223
KVK_VESTIGINGSNUMMER = "000050074695"

# Input en output directories
OUTPUT_DIR = "../../output/"
INPUT_DIR = "../../input/"

# Zoeken v2
PROD_BASE_URL = "https://api.kvk.nl/api"
TEST_BASE_URL = "https://developers.kvk.nl/test/api"
BASE_URL = PROD_BASE_URL
KVK_ZOEKEN_URL = BASE_URL + "/v2/zoeken"

# Basisprofielen
KVK_BASISPROFIELEN_URL = BASE_URL + "/v1/basisprofielen/{kvkNummer}"
KVK_BASISPROFIELEN_EIGENAAR_URL = BASE_URL + "/v1/basisprofielen/{kvkNummer}/eigenaar"
KVK_BASISPROFIELEN_HOOFDVESTIGING_URL = BASE_URL + "/v1/basisprofielen/{kvkNummer}/hoofdvestiging"
KVK_BASISPROFIELEN_VESTIGINGEN_URL = BASE_URL + "/v1/basisprofielen/{kvkNummer}/vestigingen"

# Vestigingsprofielen
KVK_VESTIGINGSPROFIEL_URL = BASE_URL + "/v1/vestigingsprofielen/{vestigingsnummer}"

# Naamgeving API
# noinspection SpellCheckingInspection
KVK_NAAMGEGING_URL = BASE_URL + "/v1/naamgevingen/kvknummer/{kvkNummer}"

# MUTATIESERVICE
KVK_MUTATIESERVICE_ABONNEMENTEN_URL = BASE_URL + "/v1/abonnementen/"
