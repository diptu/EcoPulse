import csv
import json
from datetime import datetime
from typing import Any, Dict, List

from pydantic import ValidationError

from app.schemas.facility import FacilityCreate


def serialize_unit(unit: Any) -> Dict[str, Any]:
    """
    Convert unit object to JSON-serializable dict.
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
    raw_facilities: List[Any], csv_path: str = "transformed_facilities.csv"
) -> List[Dict[str, Any]]:
    """
    - Convert raw OEClient data → validated FacilityCreate schemas
    - Remove exact duplicates (all fields)
    - Save final transformed list to CSV file
    """
    transformed: List[Dict[str, Any]] = []
    seen_rows = set()

    for fac in raw_facilities:
        fac_dict = vars(fac)

        # Extract coordinates
        loc = fac_dict.get("location")
        latitude = getattr(loc, "lat", None) if loc else None
        longitude = getattr(loc, "lng", None) if loc else None

        # JSON-serializable units
        units_list = [serialize_unit(u) for u in getattr(fac, "units", [])]

        raw_row = {
            "code": fac_dict.get("code"),
            "name": fac_dict.get("name"),
            "network_id": fac_dict.get("network_id"),
            "network_region": fac_dict.get("network_region"),
            "description": fac_dict.get("description"),
            "npi_id": fac_dict.get("npi_id"),
            "latitude": latitude,
            "longitude": longitude,
            "units": units_list,
        }

        # Pydantic validation
        try:
            validated = FacilityCreate(**raw_row).dict()
        except ValidationError as e:
            print(f"[WARNING] Invalid Facility (code={raw_row.get('code')}):\n{e}\n")
            continue

        # Deduplication hash
        row_hashable = tuple(
            sorted((k, make_hashable(v)) for k, v in validated.items())
        )
        if row_hashable in seen_rows:
            continue

        seen_rows.add(row_hashable)
        transformed.append(validated)

    # --- CSV SAVE SECTION ---
    if transformed:
        fieldnames = list(transformed[0].keys())

        # Convert units list → JSON string for CSV
        for obj in transformed:
            obj["units"] = json.dumps(obj["units"], ensure_ascii=False)

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(transformed)

        print(f"[INFO] Saved transformed facilities to CSV → {csv_path}")

    return transformed
