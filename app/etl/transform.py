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


def transform_facilities(raw_facilities: List[Any]) -> List[Dict[str, Any]]:
    """
    Transform raw OEClient facility objects into DB-ready dictionaries.
    Serialize units for JSONB storage.
    """
    transformed = []

    for fac in raw_facilities:
        fac_dict = vars(fac)

        # Extract latitude & longitude
        location_obj = fac_dict.get("location")
        latitude = getattr(location_obj, "lat", None) if location_obj else None
        longitude = getattr(location_obj, "lng", None) if location_obj else None

        # Serialize units
        units_list = [serialize_unit(u) for u in getattr(fac, "units", [])]

        transformed.append(
            {
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
        )

    return transformed
