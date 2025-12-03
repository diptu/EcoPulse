# app/etl/base.py
import csv
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ValidationError
from sqlalchemy import select, tuple_
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger("openelectricity.client").setLevel(logging.ERROR)

from app.core.config import settings

API_KEY = os.environ.get("OPENELECTRICITY_API_KEY") or getattr(
    settings, "OPENELECTRICITY_API_KEY", None
)
if not API_KEY:
    raise ValueError(
        "❌ OPENELECTRICITY_API_KEY is missing! Set it in environment or settings."
    )

os.environ["OPENELECTRICITY_API_KEY"] = API_KEY

# -------------------------------
# API KEY handling
# -------------------------------
API_KEY: Optional[str] = os.environ.get("OPENELECTRICITY_API_KEY")
try:
    if not API_KEY:
        from app.core.config import settings

        API_KEY = getattr(settings, "OPENELECTRICITY_API_KEY", None)
except ImportError:
    pass

os.environ["OPENELECTRICITY_API_KEY"] = API_KEY or ""


# -------------------------------
# Utilities
# -------------------------------
def make_hashable(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(make_hashable(v) for v in value)
    elif isinstance(value, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
    return value


def serialize_datetime(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


class BaseETL:
    """
    Base ETL class to handle Extract, Transform, Load.
    Child classes must implement entity-specific extract, transform, load methods.
    """

    def __init__(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        interval: str = "1h",
    ):
        if not API_KEY:
            raise ValueError("❌ OPENELECTRICITY_API_KEY is missing!")
        self.start_time = start_time
        self.end_time = end_time
        self.interval = interval

    # -------------------------------
    # Extract
    # -------------------------------
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from source. Must be implemented by child class.
        """
        raise NotImplementedError

    # -------------------------------
    # Transform
    # -------------------------------
    def transform(
        self,
        rows: List[Dict[str, Any]],
        schema: BaseModel,
        csv_path: str,
    ) -> List[Dict[str, Any]]:
        """
        Generic transform:
        - Validate rows with Pydantic schema
        - Deduplicate exact duplicates
        - Export CSV
        """
        transformed: List[Dict[str, Any]] = []
        seen: set[Tuple] = set()
        skipped_count = 0

        for raw in rows:
            try:
                validated = schema(**raw).model_dump()
            except ValidationError as e:
                logger.warning(f"[WARNING] Invalid row: {raw}\n{e}")
                continue

            row_hash = make_hashable(validated)
            if row_hash in seen:
                skipped_count += 1
                continue
            seen.add(row_hash)
            transformed.append(validated)

        # Export CSV
        if transformed:
            csv_rows = []
            for row in transformed:
                row_copy = {k: serialize_datetime(v) for k, v in row.items()}
                csv_rows.append(row_copy)

            fieldnames = list(csv_rows[0].keys())
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(csv_rows)

            logger.info(f"[INFO] Saved {len(csv_rows)} unique rows → {csv_path}")
            if skipped_count:
                logger.info(f"[INFO] Skipped {skipped_count} duplicates.")

        return transformed

    # -------------------------------
    # Load
    # -------------------------------
    def load(
        self,
        rows: List[Dict[str, Any]],
        db: Session,
        model_class,
        key_columns: List[str],
        data_columns: List[str],
        batch_size: int = 100,
    ):
        """
        Generic batch upsert loader
        """
        total_inserted = total_updated = total_skipped = 0
        total = len(rows)
        logger.info(f"[LOAD] Processing {total} rows...")

        for i in range(0, total, batch_size):
            batch = rows[i : i + batch_size]
            key_tuples = [tuple(row[k] for k in key_columns) for row in batch]

            existing_rows = (
                db.execute(
                    select(model_class).filter(
                        tuple_(*[getattr(model_class, k) for k in key_columns]).in_(
                            key_tuples
                        )
                    )
                )
                .scalars()
                .all()
            )
            existing_map = {
                tuple(getattr(r, k) for k in key_columns): r for r in existing_rows
            }

            inserted_count = updated_count = skipped_count = 0
            for row in batch:
                key = tuple(row[k] for k in key_columns)
                existing = existing_map.get(key)
                if existing:
                    updated = False
                    for col in data_columns:
                        if getattr(existing, col) != row.get(col):
                            setattr(existing, col, row.get(col))
                            updated = True
                    if updated:
                        existing.updated_at = datetime.utcnow()
                        updated_count += 1
                    else:
                        skipped_count += 1
                else:
                    db.add(model_class(**row))
                    inserted_count += 1

            db.commit()
            logger.info(
                f"[LOAD] Batch {i // batch_size + 1}: Inserted={inserted_count}, Updated={updated_count}, Skipped={skipped_count}"
            )

            total_inserted += inserted_count
            total_updated += updated_count
            total_skipped += skipped_count

        logger.info(
            f"[LOAD] Completed — Total Inserted={total_inserted}, Total Updated={total_updated}, Total Skipped={total_skipped}"
        )
        return total_inserted, total_updated, total_skipped
