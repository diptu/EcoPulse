from typing import Any, Dict, List

from openelectricity import OEClient
from openelectricity.types import UnitFueltechType, UnitStatusType

from app.etl.base import BaseETL
from app.etl.transform import serialize_unit
from app.models.facility import Facility
from app.schemas.facility import FacilityCreate


class FacilityETL(BaseETL):
    def extract(self) -> List[Dict[str, Any]]:
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
        all_facilities = []
        with OEClient() as client:
            for network in networks:
                for fueltech in fueltech_ids:
                    try:
                        data = client.get_facilities(
                            network_id=[network],
                            status_id=status_ids,
                            fueltech_id=[fueltech],
                        )
                        all_facilities.extend(data.data)
                    except Exception as e:
                        print(f"Skipping {network}|{fueltech}: {e}")
        # Serialize
        return [
            {
                "code": f.code,
                "name": f.name,
                "network_id": f.network_id,
                "network_region": f.network_region,
                "description": f.description,
                "npi_id": f.npi_id,
                "latitude": getattr(f.location, "lat", None) if f.location else None,
                "longitude": getattr(f.location, "lng", None) if f.location else None,
                "units": [serialize_unit(u) for u in f.units],
            }
            for f in all_facilities
        ]

    def transform(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return super().transform(
            rows, schema=FacilityCreate, csv_path="transformed_facilities.csv"
        )

    def load(self, rows: List[Dict[str, Any]], db, batch_size: int = 100, **kwargs):
        # Forward batch_size and other kwargs to BaseETL
        return super().load(
            rows,
            db,
            model_class=Facility,
            key_columns=["code", "name", "network_id", "network_region"],
            data_columns=["description", "npi_id", "latitude", "longitude", "units"],
            batch_size=batch_size,
            **kwargs,
        )
