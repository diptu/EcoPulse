import logging
from datetime import datetime

from sqlalchemy import select, tuple_
from sqlalchemy.orm import Session

from app.models.facility import Facility

logger = logging.getLogger(__name__)


def make_hashable(value):
    """
    Recursively convert lists/dicts to tuples for hashing. (Unchanged)
    """
    if isinstance(value, list):
        return tuple(make_hashable(v) for v in value)
    elif isinstance(value, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
    return value


def load_facilities(facilities: list[dict], db: Session, batch_size: int = 100):
    """
    Performs batch upsert (update/insert) of facilities.

    Key for Duplication/Update: (code, name, network_id, network_region).
    """
    # Define the key columns for comparison
    KEY_COLUMNS = ("code", "name", "network_id", "network_region")

    # Define the data columns that trigger an update if they differ
    DATA_COLUMNS = ["description", "npi_id", "latitude", "longitude", "units"]

    total = len(facilities)
    logger.info(f"[LOAD] Processing {total} unique facilities from Transform step...")

    total_inserted = 0
    total_updated = 0
    total_skipped = 0

    for i in range(0, total, batch_size):
        batch = facilities[i : i + batch_size]
        unique_batch = batch  # Input is assumed to be globally unique by all fields (from transform)

        # 1. Fetch existing rows based on the KEY_COLUMNS
        key_tuples = [tuple(row[col] for col in KEY_COLUMNS) for row in unique_batch]

        existing_rows = (
            db.execute(
                select(Facility).filter(
                    tuple_(
                        Facility.code,
                        Facility.name,
                        Facility.network_id,
                        Facility.network_region,
                    ).in_(key_tuples)
                )
            )
            .scalars()
            .all()
        )

        # Map existing rows by key tuple
        existing_map = {
            tuple(getattr(fac, col) for col in KEY_COLUMNS): fac
            for fac in existing_rows
        }

        # 2. Upsert Logic: Insert or Update
        inserted_count = 0
        updated_count = 0
        skipped_count = 0

        for row in unique_batch:
            key = tuple(row[col] for col in KEY_COLUMNS)
            existing = existing_map.get(key)

            if existing:
                # Rule 1 & 2: Key matches (potential duplicate/update)
                updated = False

                # Check if any DATA_COLUMNS differ
                for col in DATA_COLUMNS:
                    # Note: We compare the DB value with the incoming row value
                    if getattr(existing, col) != row.get(col):
                        setattr(existing, col, row.get(col))
                        updated = True

                if updated:
                    # Data columns differ -> UPDATE
                    existing.updated_at = datetime.utcnow()
                    updated_count += 1
                else:
                    # Data columns are identical -> SKIP (exact duplicate of existing DB row)
                    skipped_count += 1
            else:
                # Rule 3: Key is new -> INSERT
                db.add(Facility(**row))
                inserted_count += 1

        db.commit()

        logger.info(
            f"[LOAD] Batch {i // batch_size + 1} processed: "
            f"Inserted={inserted_count}, Updated={updated_count}, Skipped={skipped_count}"
        )

        total_inserted += inserted_count
        total_updated += updated_count
        total_skipped += skipped_count

    logger.info(
        f"[LOAD] Completed: Total Inserted={total_inserted}, "
        f"Total Updated={total_updated}, Total Skipped={total_skipped}"
    )

    return total_inserted, total_updated, total_skipped
