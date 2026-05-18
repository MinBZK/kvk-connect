from enum import StrEnum


class KVKStatus(StrEnum):
    ACTIEF = "actief"
    UITGESCHREVEN = "uitgeschreven"
    TIJDELIJK_NIET_BESCHIKBAAR = "tijdelijk_niet_beschikbaar"
