# app/etl/common.py
"""fetch/transform helpers"""

from typing import Dict, List

API_URL = "https://api.example.com/facilities"


# ------------------- Extract -------------------
async def extract_facility_data() -> List[Dict]:
    """Fetch raw data from API."""
    pass


# ------------------- Transform -------------------
def transform_facility_data(raw_data: List[Dict]) -> List[Dict]:
    """
    Convert raw API data into DB-ready dicts.
    Example: normalize field names, filter unwanted fields, parse dates.
    """
    pass
