import asyncio
import os
import sys
from typing import Any, List

# -------------------------------
# Load API key from settings
# -------------------------------
try:
    from app.core.config import settings

    API_KEY = settings.OPENELECTRICITY_API_KEY
except ImportError:
    API_KEY = os.environ.get("OPENELECTRICITY_API_KEY")

if not API_KEY:
    print(
        "\n❌ OPENELECTRICITY_API_KEY is missing! Set it as an environment variable or in config.\n"
    )
    sys.exit(1)

os.environ["OPENELECTRICITY_API_KEY"] = API_KEY

# ------------------------------------------------------------------------------
# Async OEClient Imports
# ------------------------------------------------------------------------------
from openelectricity import AsyncOEClient
from openelectricity.client import APIError
from openelectricity.types import UnitFueltechType, UnitStatusType


async def fetch_facilities_single(
    client: AsyncOEClient,
    network_id: str,
    status_ids: list[UnitStatusType],
    fueltech_id: UnitFueltechType,
) -> List[Any]:
    """Fetch facilities for a single network/fueltech combination, skip 403."""
    print(f"  -> Fetching: {network_id} | {fueltech_id.value}")
    try:
        response = await client.get_facilities(
            network_id=[network_id],
            status_id=status_ids,
            fueltech_id=[fueltech_id],
        )
        return response.data
    except APIError as e:
        status_code = getattr(e, "status_code", None) or (e.args[0] if e.args else None)
        if status_code == 403:
            print(f"⚠️ Skipping {network_id} | {fueltech_id.value} (403 Forbidden)")
            return []
        print(f"❌ API Error {status_code} for {network_id} | {fueltech_id.value}: {e}")
        return []


async def extract_facilities() -> List[Any]:
    """Fetch all facilities from all networks/fueltech combinations asynchronously."""
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

    all_facilities: List[Any] = []

    async with AsyncOEClient() as client:
        tasks = []
        for network in networks:
            for fueltech in fueltech_ids:
                tasks.append(
                    fetch_facilities_single(client, network, status_ids, fueltech)
                )

        # Run all requests concurrently
        results = await asyncio.gather(*tasks)

        # Flatten the list of lists
        for fac_list in results:
            all_facilities.extend(fac_list)

    return all_facilities
