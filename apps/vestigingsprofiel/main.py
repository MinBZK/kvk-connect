# ruff: noqa: D103

import argparse
import csv
import logging
import time
from datetime import datetime

from sqlalchemy import create_engine

from config import config
from kvk_connect import KVKApiClient, logging_config
from kvk_connect.db.init import ensure_database_initialized
from kvk_connect.db.vestigingenprofiel_reader import VestigingsProfielReader
from kvk_connect.db.vestigingsprofiel_writer import VestigingsProfielWriter
from kvk_connect.models.orm.base import Base
from kvk_connect.services import KVKRecordService

BATCH_SIZE = 1

"""
Doel: Haalt alle VestigingsProfielen op voor KVK nummers en schrijft deze naar de database.


# Update missing records in daemon mode
python apps/basisprofiel/main.py --update-missing --daemon --interval 3600

# Update known outdated records in daemon mode
python apps/basisprofiel/main.py --update-known --daemon --interval 1800

# One-time update of known outdated records
python apps/basisprofiel/main.py --update-known
"""
logger = logging.getLogger(__name__)


def process_vestigingen(
    vestiging_nummers: list[str], description: str, kvk_client: KVKApiClient, writer: VestigingsProfielWriter
) -> int:
    """Verwerk lijst van vestigingsnummers.

    Returns: Aantal verwerkte records
    """
    logger.info("Processing %s", description)

    count = 0
    for vestiging_nummer in vestiging_nummers:
        vestigings_profiel = KVKRecordService(kvk_client).get_vestigingsprofiel(vestiging_nummer)
        if vestigings_profiel:
            writer.add(vestigings_profiel)
            count += 1
            if count % 10 == 0:
                logger.info("Processed %s/%s records...", count, len(vestiging_nummers))

    return count


def process_single_kvk(kvk_nummer: str, kvk_client: KVKApiClient, writer: VestigingsProfielWriter) -> int:
    logger.info("Processing single KvK nr=%s", kvk_nummer)

    # Ophaal alle vestigingen voor dit KvK nummer
    kvk_vestingen = KVKRecordService(kvk_client).get_vestigingen(kvk_nummer)
    vestiging_nummers = kvk_vestingen.vestigingsnummers if kvk_vestingen else []

    return process_vestigingen(vestiging_nummers, f"KvK nr={kvk_nummer}", kvk_client, writer)


def process_single_vestiging(vestiging_nummer: str, kvk_client: KVKApiClient, writer: VestigingsProfielWriter) -> int:
    return process_vestigingen([vestiging_nummer], f"single Vestiging nr={vestiging_nummer}", kvk_client, writer)


def process_csv_kvk(csv_path: str, kvk_client: KVKApiClient, writer: VestigingsProfielWriter) -> int:
    logger.info("Reading CSV KvK file=%s", csv_path)

    count = 0
    with open(csv_path, encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            for kvk_nummer in row:
                if kvk_nummer.strip():
                    count += process_single_kvk(kvk_nummer.strip(), kvk_client, writer)

    return count


def process_csv_vestiging(csv_path: str, kvk_client: KVKApiClient, writer: VestigingsProfielWriter) -> int:
    logger.info("Reading CSV Vestigingen file=%s", csv_path)

    vestiging_nummers = []
    with open(csv_path, encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            vestiging_nummers.extend(value.strip() for value in row if value.strip())

    return process_vestigingen(vestiging_nummers, f"CSV file={csv_path}", kvk_client, writer)


def process_missing(kvk_client: KVKApiClient, writer: VestigingsProfielWriter, reader: VestigingsProfielReader) -> int:
    missing_profielen = reader.get_vestigingen_zonder_vestigingsprofielen()
    description = f"{len(missing_profielen)} vestigingen without VestigingsProfielen"
    return process_vestigingen(missing_profielen, description, kvk_client, writer)


def process_outdated(kvk_client: KVKApiClient, writer: VestigingsProfielWriter, reader: VestigingsProfielReader) -> int:
    outdated_vestigingen = reader.get_outdated_vestigingen()
    description = f"{len(outdated_vestigingen)} outdated vestigingsprofielen (from kvk vestigingen)"
    logger.info(description)

    outdated_vestigingen_signaal = reader.get_outdated_vestigingen_signaal()
    description = f"{len(outdated_vestigingen_signaal)} outdated vestigingsprofielen (from signaal)"
    logger.info(description)

    return process_vestigingen(outdated_vestigingen_signaal, description, kvk_client, writer)


def run_daemon(kvk_client: KVKApiClient, engine, batch_size: int, interval: int) -> None:
    logger.info("Starting daemon mode with interval of %s minutes", interval)

    while True:
        try:
            logger.info("[%s] Starting cycle...", datetime.now())

            count = 0
            with VestigingsProfielWriter(engine, batch_size=batch_size) as writer:
                reader = VestigingsProfielReader(engine)

                count += process_outdated(kvk_client, writer, reader)
                count += process_missing(kvk_client, writer, reader)
                writer.flush()

            logger.info("✅ Cycle completed: %s record(s) processed", count)
            logger.info("Sleeping for %s minutes until next cycle...", interval)
            time.sleep(interval * 60)

        except KeyboardInterrupt:
            logger.info("Daemon mode stopped by user")
            break
        except Exception as e:
            logger.error("Error in daemon cycle: %s", e, exc_info=True)
            logger.info("Retrying in %s minutes...", interval)
            time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verwerk KVK nummers naar BasisProfiel in de database.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--kvk", help="Enkel KVK nummer om te verwerken, bijv. --kvk 12345678")
    group.add_argument("--vestiging", help="Enkel Vestiging nummer om te verwerken, bijv. --vestiging 123456789123")

    group.add_argument("--csv-kvk", help="Pad naar CSV met KVK nummers, bijv. --csv C:\\pad\\input.csv")
    group.add_argument(
        "--csv-vestiging", help="Pad naar CSV met Vestiging nummers, bijv. --csv-vestiging C:\\pad\\input.csv"
    )

    group.add_argument(
        "--update-missing",
        action="store_true",
        help="Verwerk alle vestigingen die nog geen VestigingsProfiel in de DB hebben",
    )
    group.add_argument(
        "--update-known",
        action="store_true",
        help="Update bestaande KVK nummers waarvan de signaal timestamp nieuwer is",
    )
    group.add_argument("--daemon", action="store_true", help="Run in daemon mode with interval")

    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Batch size voor DB writes")
    parser.add_argument("--interval", type=int, default=60, help="Interval in minutes for daemon mode (default: 60)")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG log level.")
    args = parser.parse_args()

    # Configure logging based on --debug flag
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging_config.configure(level=log_level)

    kvk_client = KVKApiClient(api_key=config.API_KEY)
    engine = create_engine(config.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
    ensure_database_initialized(engine, Base)

    if args.daemon:
        run_daemon(kvk_client, engine, args.batch_size, args.interval)
    else:
        processed = 0
        with VestigingsProfielWriter(engine, batch_size=args.batch_size) as writer:
            if args.kvk:
                processed = process_single_kvk(args.kvk, kvk_client, writer)
            elif args.vestiging:
                processed = process_single_vestiging(args.vestiging, kvk_client, writer)
            elif args.csv_kvk:
                processed = process_csv_kvk(args.csv_kvk, kvk_client, writer)
            elif args.csv_vestiging:
                processed = process_csv_vestiging(args.csv_vestiging, kvk_client, writer)
            elif args.update_missing:
                reader = VestigingsProfielReader(engine)
                processed = process_missing(kvk_client, writer, reader)
            elif args.update_known:
                reader = VestigingsProfielReader(engine)
                processed = process_outdated(kvk_client, writer, reader)
            writer.flush()

        logger.info("✅ Verwerkt en weggeschreven: %s record(s).", processed)


if __name__ == "__main__":
    main()
