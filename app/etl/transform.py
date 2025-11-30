from datetime import datetime
from typing import Any, Dict, List


def serialize_unit(unit: Any) -> Dict[str, Any]:
    """
    Convert unit object to JSON-serializable dictionary.
    Enums -> string, datetime -> ISO string, None stays None.
    """
    unit_dict = {}
    for key, value in vars(unit).items():
        if isinstance(value, datetime):
            unit_dict[key] = value.isoformat()
        elif hasattr(value, "value"):  # Enum
            unit_dict[key] = value.value
        else:
            unit_dict[key] = value
    return unit_dict


def make_hashable(value: Any) -> Any:
    """
    Recursively convert lists/dicts to tuples for hashing.
    """
    if isinstance(value, list):
        return tuple(make_hashable(v) for v in value)
    elif isinstance(value, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
    return value


def transform_facilities(raw_facilities: List[Any]) -> List[Dict[str, Any]]:
    """
    Transform raw OEClient facility objects into DB-ready dictionaries.
    Serialize units for JSONB storage.
    Removes duplicates only if the entire row is identical.
    """
    transformed = []
    seen_rows = set()  # to track serialized rows

    for fac in raw_facilities:
        fac_dict = vars(fac)

        # Extract latitude & longitude
        location_obj = fac_dict.get("location")
        latitude = getattr(location_obj, "lat", None) if location_obj else None
        longitude = getattr(location_obj, "lng", None) if location_obj else None

        # Serialize units
        units_list = [serialize_unit(u) for u in getattr(fac, "units", [])]

        row_dict = {
            "code": fac_dict.get("code"),
            "name": fac_dict.get("name"),
            "network_id": fac_dict.get("network_id"),
            "network_region": fac_dict.get("network_region"),
            "description": fac_dict.get("description"),
            "npi_id": fac_dict.get("npi_id"),
            "latitude": latitude,
            "longitude": longitude,
            "units": units_list,  # JSON-serializable now
        }

        # Convert row to hashable form
        row_hashable = tuple(sorted((k, make_hashable(v)) for k, v in row_dict.items()))
        if row_hashable in seen_rows:
            continue  # skip exact duplicate
        seen_rows.add(row_hashable)

        transformed.append(row_dict)

    return transformed
