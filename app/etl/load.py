import logging
from typing import Any, Dict, List

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.facility import Facility

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def make_hashable(value: Any) -> Any:
    """Convert lists/dicts recursively to tuples for hashing."""
    if isinstance(value, list):
        return tuple(make_hashable(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
    return value


def row_signature(row: Dict[str, Any]) -> tuple:
    """Hashable signature for detecting exact duplicates based on content (excluding timestamps)."""
    keys = [
        "code",
        "name",
        "network_id",
        "network_region",
        "description",
        "npi_id",
        "latitude",
        "longitude",
        "units",
    ]
    return tuple((k, make_hashable(row.get(k))) for k in keys)


async def load_facilities(
    batch: List[Dict[str, Any]], db: AsyncSession, batch_size: int = 100
):
    if not batch:
        return

    inserted_count = 0
    updated_count = 0
    duplicate_count = 0

    # Remove exact duplicates within batch
    seen_rows = set()
    unique_batch = []
    for row in batch:
        sig = row_signature(row)
        if sig in seen_rows:
            duplicate_count += 1
            continue
        seen_rows.add(sig)
        unique_batch.append(row)

    if not unique_batch:
        logger.info("All rows are duplicates within batch. Nothing to insert/update.")
        return

    # Pre-fetch existing facilities with all columns
    stmt = select(Facility)
    result = await db.execute(stmt)
    existing_facilities = result.scalars().all()

    # Build set of existing row signatures
    existing_signatures = {
        row_signature(
            {
                "code": f.code,
                "name": f.name,
                "network_id": f.network_id,
                "network_region": f.network_region,
                "description": f.description,
                "npi_id": f.npi_id,
                "latitude": f.latitude,
                "longitude": f.longitude,
                "units": f.units,
            }
        )
        for f in existing_facilities
    }

    # Split batch into insert vs update
    to_insert = []
    to_update = []
    for row in unique_batch:
        sig = row_signature(row)
        if sig in existing_signatures:
            duplicate_count += 1
        else:
            # If same code+region exists but content differs, insert new row
            to_insert.append(row)

    # Insert new rows
    if to_insert:
        stmt = insert(Facility).values(to_insert)
        await db.execute(stmt)
        await db.commit()
        inserted_count = len(to_insert)

    logger.info(
        f"Inserted: {inserted_count}, Duplicates skipped (within batch & DB): {duplicate_count}"
    )
