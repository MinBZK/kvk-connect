class KVKPermanentError(Exception):
    """KVK nummer kan permanent niet worden geleverd (bijv. IPD0005 - bedrijf uitgeschreven)."""

    def __init__(self, kvk_nummer: str, code: str, omschrijving: str) -> None:
        self.kvk_nummer = kvk_nummer
        self.code = code
        self.omschrijving = omschrijving
        super().__init__(f"KVK {kvk_nummer} permanent niet leverbaar [{code}]: {omschrijving}")


class KVKTemporaryError(Exception):
    """KVK data tijdelijk niet leverbaar (IPD1002/IPD1003 - in behandeling)."""

    def __init__(self, kvk_nummer: str, code: str, omschrijving: str) -> None:
        self.kvk_nummer = kvk_nummer
        self.code = code
        self.omschrijving = omschrijving
        super().__init__(f"KVK {kvk_nummer} tijdelijk niet leverbaar [{code}]: {omschrijving}")
