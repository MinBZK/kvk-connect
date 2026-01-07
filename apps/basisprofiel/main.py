# ruff: noqa: D103
from __future__ import annotations

import argparse
import csv
import logging
import time
from datetime import datetime

from sqlalchemy import create_engine

from config import config
from kvk_connect import KVKApiClient, logging_config
from kvk_connect.db.basisprofiel_reader import BasisProfielReader
from kvk_connect.db.basisprofiel_writer import BasisProfielWriter
from kvk_connect.db.init import ensure_database_initialized
from kvk_connect.models.orm.base import Base
from kvk_connect.services import KVKRecordService

# Laag default batch size op 1 om db locking te minimaliseren
BATCH_SIZE = 1
KVK_FETCH_LIMIT = 100

"""
Doel: Haalt alle BasisProfielen op voor KVK nummers en schrijft deze naar de database.

# Update missing records in daemon mode
python apps/basisprofiel/main.py --update-missing --daemon --interval 3600

# Update known outdated records in daemon mode
python apps/basisprofiel/main.py --update-known --daemon --interval 1800

# One-time update of known outdated records
python apps/basisprofiel/main.py --update-known

# Process CSV with existence check
python apps/basisprofiel/main.py --csv data.csv
"""
logger = logging.getLogger(__name__)


def process_kvk_nummers(
    kvk_nummers: list[str], description: str, kvk_client: KVKApiClient, writer: BasisProfielWriter
) -> int:
    """Verwerk lijst van KvK nummers.

    Returns: Aantal verwerkte records
    """
    logger.info("Processing %s", description)

    count = 0
    for kvk_nummer in kvk_nummers:
        kvk_record = KVKRecordService(kvk_client).get_basisprofiel(kvk_nummer)
        if kvk_record:
            writer.add(kvk_record)
            count += 1
            if count % 10 == 0:
                logger.info("Processed %s/%s records...", count, len(kvk_nummers))

    return count


def process_single(kvk_nummer: str, kvk_client: KVKApiClient, writer: BasisProfielWriter) -> int:
    return process_kvk_nummers([kvk_nummer], f"single KvK nr={kvk_nummer}", kvk_client, writer)


def process_csv(csv_path: str, kvk_client: KVKApiClient, writer: BasisProfielWriter, reader: BasisProfielReader) -> int:
    """Process CSV file with existence check to handle large files efficiently.

    Checks if each KVK number exists in database before fetching from API.
    Suitable for processing millions of records.

    Args:
        csv_path: Path to CSV file.
        kvk_client: KVK API client.
        writer: Database writer.
        reader: Database reader for existence checks.

    Returns:
        Number of records processed.
    """
    logger.info("Reading CSV file=%s", csv_path)

    count_processed = 0
    count_skipped = 0
    count_total = 0

    try:
        with open(csv_path, encoding="utf-8") as file:
            csv_reader = csv.reader(file)

            for row in csv_reader:
                for kvk_nummer in (value.strip() for value in row if value.strip()):
                    count_total += 1

                    logger.debug("Processing record %s: KVK nummer=%s", count_total, kvk_nummer)
                    # Check if KVK number already exists in database
                    if reader.kvk_nummer_exists(kvk_nummer):
                        count_skipped += 1
                        if count_total % 1000 == 0:
                            logger.info(
                                "CSV progress: %s records processed, %s skipped (already exist)",
                                count_total,
                                count_skipped,
                            )
                        continue

                    # Fetch and store new records
                    kvk_record = KVKRecordService(kvk_client).get_basisprofiel(kvk_nummer)
                    if kvk_record:
                        writer.add(kvk_record)
                        count_processed += 1

                    if count_total % 1000 == 0:
                        logger.info(
                            "CSV progress: %s records processed, %s new, %s skipped",
                            count_total,
                            count_processed,
                            count_skipped,
                        )

    except FileNotFoundError:
        logger.error("CSV file not found: %s", csv_path)
        raise
    except Exception as e:
        logger.error("Error processing CSV file: %s", e, exc_info=True)
        raise

    logger.info(
        "CSV processing complete: %s total records, %s new, %s skipped (already exist)",
        count_total,
        count_processed,
        count_skipped,
    )

    return count_processed


def process_missing(kvk_client: KVKApiClient, writer: BasisProfielWriter, reader: BasisProfielReader) -> int:
    logger.info("Finding missing KvK nummers...")

    count_missing = reader.get_missing_kvk_nummers_count()
    logger.info("Total missing KvK nummers: %s", count_missing)

    missing_kvk_nummers = reader.get_missing_kvk_nummers(KVK_FETCH_LIMIT)
    description = f"{len(missing_kvk_nummers)} missing KvK nummer(s) (limit: {KVK_FETCH_LIMIT})"

    return process_kvk_nummers(missing_kvk_nummers, description, kvk_client, writer)


def process_outdated(kvk_client: KVKApiClient, writer: BasisProfielWriter, reader: BasisProfielReader) -> int:
    logger.info("Finding outdated KvK nummers...")
    outdated_kvk_nummers = reader.get_outdated_kvk_nummers()
    logger.debug("Found outdated kvk records: %s", outdated_kvk_nummers)

    description = f"{len(outdated_kvk_nummers)} outdated KvK nummer(s)"
    return process_kvk_nummers(outdated_kvk_nummers, description, kvk_client, writer)


def run_daemon(kvk_client: KVKApiClient, engine, batch_size: int, interval: int) -> None:
    logger.info("Starting daemon mode with interval of %s minutes", interval)

    while True:
        try:
            logger.info("[%s] Starting cycle...", datetime.now())

            count = 0
            with BasisProfielWriter(engine, batch_size=batch_size) as writer:
                reader = BasisProfielReader(engine)
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
            time.sleep(interval * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verwerk KVK nummers naar BasisProfiel in de database.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--kvk", help="Enkel KVK nummer om te verwerken, bijv. --kvk 12345678")
    group.add_argument("--csv", help="Pad naar CSV met KVK nummers, bijv. --csv C:\\pad\\input.csv")

    group.add_argument(
        "--update-missing",
        action="store_true",
        help="Automatisch alle ontbrekende KVK nummers uit signalen tabel verwerken",
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
    print(f"Using KVK API endpoint: {config.SQLALCHEMY_DATABASE_URI}")
    engine = create_engine(config.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True, connect_args={"timeout": 30})

    ensure_database_initialized(engine, Base)

    reader = BasisProfielReader(engine)

    if args.daemon:
        run_daemon(kvk_client, engine, args.batch_size, args.interval)
    else:
        processed = 0
        with BasisProfielWriter(engine, batch_size=args.batch_size) as writer:
            if args.kvk:
                processed = process_single(args.kvk, kvk_client, writer)
            elif args.csv:
                processed = process_csv(args.csv, kvk_client, writer, reader)
            elif args.update_missing:
                processed = process_missing(kvk_client, writer, reader)
            elif args.update_known:
                processed = process_outdated(kvk_client, writer, reader)
            writer.flush()

        logger.info("✅ Verwerkt en weggeschreven: %s record(s).", processed)


if __name__ == "__main__":
    main()
