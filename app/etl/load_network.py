import logging
from datetime import datetime

from sqlalchemy import select, tuple_
from sqlalchemy.orm import Session

from app.models.network import NetworkData

logger = logging.getLogger(__name__)


def load_network_data(rows: list[dict], db: Session, batch_size: int = 200):
    """
    Batch upsert for NetworkData.

    Unique key for dedupe/update: (timestamp, metric)
    Data columns that trigger UPDATE if changed: unit, name, fuel_group, value
    """

    KEY_COLUMNS = ("timestamp", "metric")
    DATA_COLUMNS = ["unit", "name", "fuel_group", "value"]

    total = len(rows)
    logger.info(f"[LOAD] Processing {total} network records...")

    total_inserted = 0
    total_updated = 0
    total_skipped = 0

    # ---------------------------------------------------------
    # 0. Deduplicate by DB key (timestamp, metric)
    # ---------------------------------------------------------
    unique_rows_map = {}
    for row in rows:
        key = (row["timestamp"], row["metric"])
        # Keep the latest row for this key
        unique_rows_map[key] = row
    unique_rows = list(unique_rows_map.values())

    for i in range(0, len(unique_rows), batch_size):
        batch = unique_rows[i : i + batch_size]

        # ---------------------------------------------------------
        # 1. Preload existing rows from DB
        # ---------------------------------------------------------
        key_tuples = [(row["timestamp"], row["metric"]) for row in batch]

        existing_rows = (
            db.execute(
                select(NetworkData).filter(
                    tuple_(NetworkData.timestamp, NetworkData.metric).in_(key_tuples)
                )
            )
            .scalars()
            .all()
        )

        existing_map = {(obj.timestamp, obj.metric): obj for obj in existing_rows}

        inserted_count = 0
        updated_count = 0
        skipped_count = 0

        # ---------------------------------------------------------
        # 2. Upsert logic
        # ---------------------------------------------------------
        for row in batch:
            key = (row["timestamp"], row["metric"])
            existing = existing_map.get(key)

            if existing:
                updated = False
                for col in DATA_COLUMNS:
                    if getattr(existing, col) != row.get(col):
                        setattr(existing, col, row.get(col))
                        updated = True
                if updated:
                    existing.updated_at = datetime.utcnow()
                    updated_count += 1
                else:
                    skipped_count += 1
            else:
                db.add(NetworkData(**row))
                inserted_count += 1

        db.commit()

        logger.info(
            f"[LOAD] Batch {i // batch_size + 1} processed — "
            f"Inserted={inserted_count}, Updated={updated_count}, Skipped={skipped_count}"
        )

        total_inserted += inserted_count
        total_updated += updated_count
        total_skipped += skipped_count

    logger.info(
        f"[LOAD] Completed NetworkData Load — "
        f"Inserted={total_inserted}, Updated={total_updated}, Skipped={total_skipped}"
    )

    return total_inserted, total_updated, total_skipped
