from __future__ import annotations

import json
import logging
import re
from datetime import UTC, date, datetime, timedelta

logger = logging.getLogger(__name__)


def parse_kvk_datum(datum_str: str | None) -> date | None:
    """Parse een KVK datum string naar een date object.

    Ondersteunt:
    - DD-MM-YYYY (standaard KVK formaat)
    - YYYYMMDD (8 cijfers)
    - YYYYMM00 (6 cijfers + 00, zet dag op 1)
    - YYYY0000 (4 cijfers + 0000, zet maand en dag op 1)

    Args:
        datum_str: Datum string of None

    Returns:
        date object of None bij ongeldige/lege input
    """
    if datum_str is None or str(datum_str).strip() in ("", "None"):
        return None

    datum_str = str(datum_str).strip()

    # Probeer standaard DD-MM-YYYY formaat
    try:
        return datetime.strptime(datum_str, "%d-%m-%Y").date()
    except ValueError:
        pass

    # Probeer YYYYMMDD formaat (8 cijfers)
    if datum_str.isdigit() and len(datum_str) == 8:
        try:
            year = int(datum_str[0:4])
            month = int(datum_str[4:6])
            day = int(datum_str[6:8])

            # Als maand of dag 0 is, zet op 1
            if month == 0:
                month = 1
            if day == 0:
                day = 1

            return date(year, month, day)
        except (ValueError, OverflowError) as e:
            logger.warning("Ongeldige datum conversie voor '%s': {%s}", datum_str, e)
            return None

    logger.warning("Ongeldige datum conversie voor '%s': geen geldig formaat", datum_str)
    return None


def truncate_float(value: float | None, digits: int = 5) -> str:
    """Truncate a float to a given number of digits after the decimal point and return as string.

    If value is None, return empty string.
    """
    if value is None or value == 0.0:
        return ""
    factor = 10**digits
    truncated = int(value * factor) / factor
    # Format with fixed number of digits en gebruik komma als decimaalteken
    return f"{truncated:.{digits}f}".replace(".", ",")


def clean_and_pad(s, fill=8):
    """Strip non-digit characters from start/end and pad to given length with leading zeros."""

    cleaned = re.sub(r"[^\d]", "", s)
    # Pad to 8 digits
    return cleaned.zfill(fill)


def formatteer_datum(datum_str):
    """Formatteer een datum string van YYYYMMDD naar DD-MM-YYYY. Retourneer None bij "None"."""

    if datum_str != "None":
        try:
            return datetime.strptime(datum_str, "%Y%m%d").strftime("%d-%m-%Y")
        except ValueError:
            return datum_str
    else:
        return None


def print_response(_response):
    """Prints the response from a requests call in a formatted way. Handles JSON and non-JSON responses."""

    print("url:", _response.url)
    print("Status code:", _response.status_code)
    try:
        data = _response.json()
        print("Response data (JSON):")
        print(json.dumps(data, indent=4))
    except ValueError:
        print("Response data (text):")
        print(_response.text)


def get_timeselector(selected_from: datetime, selected_to: datetime) -> list[dict[str, datetime]]:
    """Gegeven twee datetime timestamps.Returned een lijst met from-to dicts terug van maximaal een week.

    Wordt gebruikt om pagination van mutaties op te halen deze mogen maximaal per week opgehaald worden
    """

    def to_utc(dt: datetime) -> datetime:
        return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)

    def split_into_chunks(start: datetime, end: datetime, days: int = 7) -> list[dict[str, datetime]]:
        out: list[dict[str, datetime]] = []
        cur = start
        step = timedelta(days=days)
        while cur < end:
            nxt = min(cur + step, end)
            out.append({"from": cur, "to": nxt})
            cur = nxt
        return out

    # Normalize selection to UTC and ensure ascending order
    sf, st = to_utc(selected_from), to_utc(selected_to)
    if st < sf:
        logger.warning("Selected time from is farther into the future than the selected time to.")
        return []
    else:
        return split_into_chunks(sf, st)
