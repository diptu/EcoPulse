import logging
from datetime import datetime

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.facility import Facility

logger = logging.getLogger(__name__)


def make_hashable(value):
    """
    Recursively convert lists/dicts to tuples for hashing.
    """
    if isinstance(value, list):
        return tuple(make_hashable(v) for v in value)
    elif isinstance(value, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
    return value


def load_facilities(facilities: list[dict], db: AsyncSession, batch_size: int = 100):
    """
    Insert or update facilities in batches.

    Rules:
    1. Skip exact duplicates (all columns except created_at/updated_at)
    2. Update existing row if code, name, network_id, network_region match but other columns differ
    3. Log counts: inserted, updated, skipped
    """
    total = len(facilities)
    logger.info(f"[LOAD] Processing {total} facilities...")

    total_inserted = 0
    total_updated = 0
    total_skipped = 0

    for i in range(0, total, batch_size):
        batch = facilities[i : i + batch_size]

        # Deduplicate exact rows in Python
        def row_hash(row):
            return tuple(
                sorted(
                    (k, make_hashable(v))
                    for k, v in row.items()
                    if k not in ("created_at", "updated_at")
                )
            )

        seen_hashes = set()
        unique_batch = []
        skipped_count = 0
        for row in batch:
            h = row_hash(row)
            if h not in seen_hashes:
                seen_hashes.add(h)
                unique_batch.append(row)
            else:
                skipped_count += 1

        # Fetch existing rows that match code, name, network_id, network_region
        key_tuples = [
            (row["code"], row["name"], row["network_id"], row.get("network_region"))
            for row in unique_batch
        ]

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
            (fac.code, fac.name, fac.network_id, fac.network_region): fac
            for fac in existing_rows
        }

        # Upsert
        inserted_count = 0
        updated_count = 0
        for row in unique_batch:
            key = (
                row["code"],
                row["name"],
                row["network_id"],
                row.get("network_region"),
            )
            existing = existing_map.get(key)

            if existing:
                # Check if any other column differs
                updated = False
                for col in ["description", "npi_id", "latitude", "longitude", "units"]:
                    if getattr(existing, col) != row.get(col):
                        setattr(existing, col, row.get(col))
                        updated = True
                if updated:
                    existing.updated_at = datetime.utcnow()
                    updated_count += 1
                else:
                    skipped_count += 1  # identical DB row, count as skipped
            else:
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
