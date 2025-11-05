from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine

from config import config
from kvk_connect import KVKApiClient, logging_config
from kvk_connect.db.init import ensure_database_initialized
from kvk_connect.db.signaal_reader import SignaalReader
from kvk_connect.db.signaal_writer import SignaalWriter
from kvk_connect.models.api.mutatiesignalen_api import MutatiesAPI, MutatieSignaal
from kvk_connect.models.orm.base import Base
from kvk_connect.utils.tools import get_timeselector

FETCH_LIMIT = 500  # Page size for KVK API calls
BATCH_SIZE = 100  # DB upsert batch size

logger = logging.getLogger(__name__)
"""
Doel: Haalt mutaties op uit de KVK mutatie API en schrijft deze naar de database.
"""


def parse_iso_utc(value: str) -> datetime:
    """Parse ISO8601 into timezone-aware UTC. Accepts 'Z' and offsets."""
    s = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def fetch_all_mutaties(
    client: KVKApiClient, from_time: datetime, to_time: datetime, size: int
) -> Iterator[MutatieSignaal]:
    """We halen alle mutaties op in de opgegeven tijdsperiode.

    We moeten de eerte pagina(first) ophalen om te weten hoeveel pagina's er zijn.
    """
    first: MutatiesAPI = client.get_mutaties(config.KVK_MUTATIE_ABONNEMENT_ID, from_time, to_time, page=1, size=size)
    logger.info(
        "Mutaties: page=%s, count=%s, total=%s, pages=%s",
        first.pagina,
        len(first.signalen),
        first.totaal,
        first.totaal_paginas,
    )
    for s in first.signalen:
        yield s
    for page in range(2, first.totaal_paginas + 1):
        logger.info("fetching page %s/%s", page, first.totaal_paginas)
        nxt = client.get_mutaties(config.KVK_MUTATIE_ABONNEMENT_ID, from_time, to_time, page=page, size=size)
        if nxt is not None and hasattr(nxt, "signalen"):
            for s in nxt.signalen:
                yield s


def resolve_time_window_auto(repo: SignaalReader) -> tuple[datetime, datetime]:
    """Resolve time window for auto mode."""
    to_time = datetime.now(tz=UTC) - timedelta(minutes=1)
    repo_last = repo.get_last_timestamp()

    from_time = repo_last if repo_last else to_time - timedelta(days=1)
    return from_time, to_time


def resolve_time_window_manual(from_str: str, to_str: str) -> tuple[datetime, datetime]:
    """Resolve time window for manual mode."""
    sf, st = parse_iso_utc(from_str), parse_iso_utc(to_str)
    if st < sf:
        raise ValueError("--to must be equal or after --from")
    return sf, st


def run_sync(engine, client, args, repo):
    """Single sync run."""
    if args.auto:
        from_time, to_time = resolve_time_window_auto(repo)
        logger.info("Auto mode: from=%s to=%s", from_time, to_time)
    else:
        from_time, to_time = resolve_time_window_manual(args.from_time, args.to_time)
        logger.info("Manual mode: from=%s to=%s", from_time, to_time)

    ranges = get_timeselector(from_time, to_time)
    if not ranges:
        logger.info("No new data to fetch.")
        return

    with SignaalWriter(engine, batch_size=args.batch_size, upsert=True) as writer:
        for window in ranges:
            wf, wt = window["from"], window["to"]
            logger.info("Fetching mutaties from %s to %s", wf, wt)
            for api_signaal in fetch_all_mutaties(client, wf, wt, size=args.fetch_limit):
                writer.add(api_signaal)
        writer.flush()


def fetch_single_signaal(client: KVKApiClient, signaal_id: str) -> None:
    """Fetch and print single signaal to stdout."""
    logger.info("Fetching signaal %s", signaal_id)
    data = client.get_mutatie_signaal_raw(config.KVK_MUTATIE_ABONNEMENT_ID, signaal_id)

    if data:
        print(json.dumps(data, indent=2, ensure_ascii=False))
    else:
        logger.error("No data found for signaal %s", signaal_id)
        sys.exit(1)


def main():
    """Main entry point for mutatie-reader service."""
    parser = argparse.ArgumentParser(description="KvK mutatie service")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--signaalid", metavar="ID", help="Fetch single signaal by ID and print to stdout")
    mode.add_argument("--auto", action="store_true", help="Automatic mode: from=repo last, to=now-1m")
    mode.add_argument("--manual", action="store_true", help="Manual mode: requires --from and --to")

    parser.add_argument("--from", dest="from_time", help="Manual from datetime (ISO8601)")
    parser.add_argument("--to", dest="to_time", help="Manual to datetime (ISO8601)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Upsert batch size")
    parser.add_argument("--fetch-limit", type=int, default=FETCH_LIMIT, help="API page size")
    parser.add_argument("--interval", type=int, default=60, help="Auto mode interval in minutes (default: 60)")
    parser.add_argument("--daemon", action="store_true", help="Run in daemon mode with interval")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG log level.")
    args = parser.parse_args()

    # Configure logging based on --debug flag
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging_config.configure(level=log_level)

    if args.manual and (args.from_time is None or args.to_time is None):
        raise SystemExit("Manual mode requires both --from and --to")

    if args.daemon and not args.auto:
        raise SystemExit("Daemon mode only works with --auto")

    # Initialize client
    client = KVKApiClient(api_key=config.API_KEY)

    # Single signaal mode - no DB needed
    if args.signaalid:
        fetch_single_signaal(client, args.signaalid)
        return

    # Initialize database for sync modes
    engine = create_engine(config.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
    ensure_database_initialized(engine, Base)
    repo = SignaalReader(engine)

    # Daemon or single run
    if args.daemon:
        logger.info("Daemon mode: running every %s minutes", args.interval)
        while True:
            try:
                run_sync(engine, client, args, repo)
                logger.info(f"Sleeping for {args.interval} minutes...")
                time.sleep(args.interval * 60)
            except KeyboardInterrupt:
                logger.info("Shutting down daemon...")
                break
            except Exception as e:
                logger.error("Error in daemon loop: %s", e, exc_info=True)
                time.sleep(60)
    else:
        run_sync(engine, client, args, repo)


if __name__ == "__main__":
    main()
