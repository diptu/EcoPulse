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
    1. Skip exact duplicates (all columns except created_at/updated_at)
    2. Update existing row if code, name, network_id, network_region match but other columns differ
    """
    total = len(facilities)
    logger.info(f"[LOAD] Processing {total} facilities...")

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
        for row in batch:
            h = row_hash(row)
            if h not in seen_hashes:
                seen_hashes.add(h)
                unique_batch.append(row)

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
        existing_map = {}
        for fac in existing_rows:
            key = (fac.code, fac.name, fac.network_id, fac.network_region)
            existing_map[key] = fac

        # Upsert
        for row in unique_batch:
            key = (
                row["code"],
                row["name"],
                row["network_id"],
                row.get("network_region"),
            )
            existing = existing_map.get(key)

            if existing:
                # Check if any other column differs, if yes update
                updated = False
                for col in ["description", "npi_id", "latitude", "longitude", "units"]:
                    if getattr(existing, col) != row.get(col):
                        setattr(existing, col, row.get(col))
                        updated = True
                if updated:
                    existing.updated_at = datetime.utcnow()
            else:
                db.add(Facility(**row))

        db.commit()
        logger.info(f"[LOAD] Batch {i // batch_size + 1} processed.")
