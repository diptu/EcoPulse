import csv
from datetime import datetime
from typing import Any, Dict, List, Tuple

from pydantic import ValidationError

from app.schemas.network import NetworkDataCreate


# -------------------------------------------------------------------
# Make any structure hashable (list/dict → deeply immutable)
# -------------------------------------------------------------------
def make_hashable(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(make_hashable(v) for v in value)
    elif isinstance(value, dict):
        # dicts → tuple of (key, hashable-value) sorted for stability
        return tuple((k, make_hashable(v)) for k, v in sorted(value.items()))
    return value


# -------------------------------------------------------------------
# Main Transformer
# -------------------------------------------------------------------
def transform_network(
    serialized_rows: List[Dict[str, Any]],
    csv_path: str = "transformed_network.csv",
) -> List[Dict[str, Any]]:
    """
    Transform Network Timeseries:
      - validate using Pydantic
      - deduplicate based on entire row payload
      - export unique rows to CSV
      - return only unique transformed rows
    """

    transformed: List[Dict[str, Any]] = []
    seen: set[Tuple] = set()
    skipped_duplicates = 0

    # --------------------------------------------------------------
    # 1. Validate & Deduplicate
    # --------------------------------------------------------------
    for raw in serialized_rows:
        try:
            validated = NetworkDataCreate(**raw).model_dump()
        except ValidationError as e:
            print(f"[WARNING] Invalid row timestamp={raw.get('timestamp')}: {e}")
            continue

        # Build deterministic hash
        row_hash = make_hashable(validated)

        if row_hash in seen:
            skipped_duplicates += 1
            continue

        seen.add(row_hash)
        transformed.append(validated)

    # --------------------------------------------------------------
    # 2. Export to CSV
    # --------------------------------------------------------------
    if transformed:
        csv_rows = []

        for row in transformed:
            row_copy = dict(row)
            if isinstance(row_copy.get("timestamp"), datetime):
                row_copy["timestamp"] = row_copy["timestamp"].isoformat()
            csv_rows.append(row_copy)

        fieldnames = list(csv_rows[0].keys())

        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)

        print(f"[INFO] Saved unique network rows → {csv_path}")
        if skipped_duplicates:
            print(f"[INFO] Skipped {skipped_duplicates} duplicate rows.")

    # --------------------------------------------------------------
    # 3. Return unique validated rows
    # --------------------------------------------------------------
    return transformed
