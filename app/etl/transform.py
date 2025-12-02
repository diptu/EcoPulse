import csv
import json
from datetime import datetime
from typing import Any, Dict, List

from pydantic import ValidationError

from app.schemas.facility import FacilityCreate


def serialize_unit(unit: Any) -> Dict[str, Any]:
    """
    Convert unit object to JSON-serializable dict.
    (This function remains here as it's imported by extract.py)
    """
    unit_dict = {}
    for key, value in vars(unit).items():
        if isinstance(value, datetime):
            unit_dict[key] = value.isoformat()
        elif hasattr(value, "value"):  # Enum -> string
            unit_dict[key] = value.value
        else:
            unit_dict[key] = value
    return unit_dict


def make_hashable(value: Any) -> Any:
    """Convert lists and dicts into hashable tuples."""
    if isinstance(value, list):
        return tuple(make_hashable(v) for v in value)
    elif isinstance(value, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
    return value


def transform_facilities(
    serialized_facilities: List[Dict[str, Any]],
    csv_path: str = "transformed_facilities.csv",
) -> List[Dict[str, Any]]:
    """
    - Takes JSON-serializable dicts from XCom (pre-transformed in extract).
    - Validates dicts against FacilityCreate schemas (Core Transform).
    - Removes exact duplicates (all fields).
    - Saves final transformed list to CSV file.
    - Returns list of dicts (JSON-serializable) for Airflow XCom.
    """
    transformed: List[Dict[str, Any]] = []
    seen_rows = set()
    skipped_count = 0

    # 1. Validation and Deduplication
    for raw_row in serialized_facilities:
        # Pydantic validation: Ensure data conforms to the schema
        try:
            # The .dict() output is used for validation and subsequent steps
            validated = FacilityCreate(**raw_row).dict()
        except ValidationError as e:
            print(f"[WARNING] Invalid Facility (code={raw_row.get('code')}):\n{e}\n")
            continue

        # Deduplication hash: Ensures exact record duplicates are dropped
        row_hashable = tuple(
            sorted((k, make_hashable(v)) for k, v in validated.items())
        )
        if row_hashable in seen_rows:
            skipped_count += 1
            continue

        seen_rows.add(row_hashable)
        # 'transformed' contains validated, deduplicated dictionaries with 'units' as a list
        transformed.append(validated)

    # 2. CSV SAVE SECTION (Side Effect)
    if transformed:
        # Create a deep copy for CSV export to prevent mutation of the 'transformed' list
        # intended for XCom/Load step.
        csv_export_list = [dict(obj) for obj in transformed]
        fieldnames = list(csv_export_list[0].keys())

        # Convert units list → JSON string for CSV file
        for obj in csv_export_list:
            # Mutate the units column ONLY in the CSV list
            obj["units"] = json.dumps(obj["units"], ensure_ascii=False)

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_export_list)

        print(f"[INFO] Saved transformed facilities to CSV → {csv_path}")
        if skipped_count > 0:
            print(
                f"[INFO] Skipped {skipped_count} duplicate facilities during transform"
            )

    # 3. Return Deduplicated, Non-Mutated Data
    # 'transformed' contains the clean, deduplicated list with 'units' as a list of dicts,
    # ready for the 'load' step.
    return transformed
