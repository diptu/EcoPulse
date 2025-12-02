import logging
import os
from typing import Any, Dict, List, Optional

# --- CRITICAL CHANGE: Import serialization helper from transform ---
# This links the necessary conversion logic.
from app.etl.transform import serialize_unit

# -------------------------------
# Configuration / API Key Setup
# -------------------------------
API_KEY: Optional[str] = os.environ.get("OPENELECTRICITY_API_KEY")
try:
    if not API_KEY:
        from app.core.config import settings

        API_KEY = getattr(settings, "OPENELECTRICITY_API_KEY", None)
except ImportError:
    pass

# Make API_KEY available as env var (empty string if missing)
os.environ["OPENELECTRICITY_API_KEY"] = API_KEY or ""

# -------------------------------
# Imports
# -------------------------------
from openelectricity import OEClient
from openelectricity.client import APIError
from openelectricity.types import UnitFueltechType, UnitStatusType

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logging.getLogger("openelectricity.client").setLevel(logging.ERROR)


# -------------------------------
# Serialization Helper
# -------------------------------
def serialize_facility_for_xcom(fac: Any) -> Dict[str, Any]:
    """
    Convert custom OE Facility object (openelectricity.models.facilities.Facility)
    into a standard dictionary for safe XCom transfer.
    """
    # Use vars() to get the attributes of the custom object
    fac_dict = vars(fac)

    # Extract coordinates from the nested 'location' object
    loc = fac_dict.get("location")
    latitude = getattr(loc, "lat", None) if loc else None
    longitude = getattr(loc, "lng", None) if loc else None

    # JSON-serializable units (using the imported serialize_unit helper)
    units_list = [serialize_unit(u) for u in getattr(fac, "units", [])]

    return {
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


# -------------------------------
# Synchronous Fetch
# -------------------------------
def fetch_facilities_single(
    client: OEClient,
    network_id: str,
    status_ids: list[UnitStatusType],
    fueltech_id: UnitFueltechType,
) -> List[Any]:
    """Fetch facilities synchronously for a single network/fueltech combination. (Unchanged)"""
    print(f"  -> Fetching: {network_id} | {fueltech_id.value}")
    try:
        response = client.get_facilities(
            network_id=[network_id],
            status_id=status_ids,
            fueltech_id=[fueltech_id],
        )
        return response.data
    except APIError as e:
        status_code = getattr(e, "status_code", None) or (e.args[0] if e.args else None)
        if status_code == 403:
            print(f"⚠️ Skipping {network_id} | {fueltech_id.value} (**403 Forbidden**)")
            return []
        print(f"❌ API Error {status_code} for {network_id} | {fueltech_id.value}: {e}")
        return []


def extract_facilities() -> List[Dict[str, Any]]:
    """
    Fetch all facilities synchronously using OEClient and **convert them to
    JSON-safe dictionaries** before returning.
    """
    if not API_KEY:
        raise ValueError(
            "❌ OPENELECTRICITY_API_KEY is missing! "
            "Set it as an environment variable or in settings."
        )

    networks = ["NEM", "WEM"]
    status_ids = [UnitStatusType.OPERATING]
    fueltech_ids = [
        UnitFueltechType.SOLAR_UTILITY,
        UnitFueltechType.WIND,
        UnitFueltechType.SOLAR_ROOFTOP,
        UnitFueltechType.HYDRO,
        UnitFueltechType.BATTERY_DISCHARGING,
        UnitFueltechType.BATTERY_CHARGING,
        UnitFueltechType.COAL_BLACK,
        UnitFueltechType.COAL_BROWN,
        UnitFueltechType.GAS_CCGT,
        UnitFueltechType.GAS_OCGT,
        UnitFueltechType.DISTILLATE,
        UnitFueltechType.BIOENERGY_BIOMASS,
        UnitFueltechType.BIOENERGY_BIOGAS,
    ]

    all_facilities: List[Any] = []  # Stores the raw openelectricity objects

    with OEClient() as client:
        for network in networks:
            for fueltech in fueltech_ids:
                all_facilities.extend(
                    fetch_facilities_single(client, network, status_ids, fueltech)
                )

    # --- CRITICAL STEP: Serialize raw objects to dictionaries for XCom safety ---
    return [serialize_facility_for_xcom(f) for f in all_facilities]


# -------------------------------
# Example Usage (Optional)
# -------------------------------
if __name__ == "__main__":
    try:
        facilities = extract_facilities()
        # print(f"Fetched {len(facilities)} facilities")
    except ValueError as e:
        print(e)
