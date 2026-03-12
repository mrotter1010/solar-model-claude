"""Ground station registry for NSRDB bias correction.

SURFRAD (7 stations), SOLRAD (7 active stations), and MIDC (6 stations)
networks providing research-grade GHI/DNI measurements for comparison
against NSRDB satellite data.
"""

from typing import TypedDict


class StationRecord(TypedDict):
    """Type definition for a ground station entry."""

    name: str
    network: str
    station_id: str
    latitude: float
    longitude: float
    state: str
    elevation_m: float


STATIONS: list[StationRecord] = [
    # --- SURFRAD network (7 stations) ---
    {
        "name": "Bondville",
        "network": "SURFRAD",
        "station_id": "bon",
        "latitude": 40.05,
        "longitude": -88.37,
        "state": "IL",
        "elevation_m": 213.0,
    },
    {
        "name": "Fort Peck",
        "network": "SURFRAD",
        "station_id": "fpk",
        "latitude": 48.31,
        "longitude": -105.10,
        "state": "MT",
        "elevation_m": 634.0,
    },
    {
        "name": "Goodwin Creek",
        "network": "SURFRAD",
        "station_id": "gwn",
        "latitude": 34.25,
        "longitude": -89.87,
        "state": "MS",
        "elevation_m": 98.0,
    },
    {
        "name": "Table Mountain",
        "network": "SURFRAD",
        "station_id": "tbl",
        "latitude": 40.13,
        "longitude": -105.24,
        "state": "CO",
        "elevation_m": 1689.0,
    },
    {
        "name": "Desert Rock",
        "network": "SURFRAD",
        "station_id": "dra",
        "latitude": 36.62,
        "longitude": -116.02,
        "state": "NV",
        "elevation_m": 1007.0,
    },
    {
        "name": "Penn State",
        "network": "SURFRAD",
        "station_id": "psu",
        "latitude": 40.72,
        "longitude": -77.93,
        "state": "PA",
        "elevation_m": 376.0,
    },
    {
        "name": "Sioux Falls",
        "network": "SURFRAD",
        "station_id": "sxf",
        "latitude": 43.73,
        "longitude": -96.62,
        "state": "SD",
        "elevation_m": 473.0,
    },
    # --- SOLRAD network (7 active stations) ---
    {
        "name": "Albuquerque",
        "network": "SOLRAD",
        "station_id": "abq",
        "latitude": 35.038,
        "longitude": -106.622,
        "state": "NM",
        "elevation_m": 1617.0,
    },
    {
        "name": "Bismarck",
        "network": "SOLRAD",
        "station_id": "bis",
        "latitude": 46.772,
        "longitude": -100.760,
        "state": "ND",
        "elevation_m": 503.0,
    },
    {
        "name": "Hanford",
        "network": "SOLRAD",
        "station_id": "hnx",
        "latitude": 36.314,
        "longitude": -119.632,
        "state": "CA",
        "elevation_m": 73.0,
    },
    {
        "name": "Madison",
        "network": "SOLRAD",
        "station_id": "msn",
        "latitude": 43.073,
        "longitude": -89.411,
        "state": "WI",
        "elevation_m": 271.0,
    },
    {
        "name": "Salt Lake City",
        "network": "SOLRAD",
        "station_id": "slc",
        "latitude": 40.772,
        "longitude": -111.955,
        "state": "UT",
        "elevation_m": 1288.0,
    },
    {
        "name": "Seattle",
        "network": "SOLRAD",
        "station_id": "sea",
        "latitude": 47.687,
        "longitude": -122.257,
        "state": "WA",
        "elevation_m": 20.0,
    },
    {
        "name": "Sterling",
        "network": "SOLRAD",
        "station_id": "ste",
        "latitude": 38.972,
        "longitude": -77.487,
        "state": "VA",
        "elevation_m": 85.0,
    },
    # --- MIDC network (6 stations) ---
    {
        "name": "NREL SRRL",
        "network": "MIDC",
        "station_id": "BMS",
        "latitude": 39.74,
        "longitude": -105.18,
        "state": "CO",
        "elevation_m": 1829.0,
    },
    {
        "name": "Univ of Arizona",
        "network": "MIDC",
        "station_id": "UAT",
        "latitude": 32.23,
        "longitude": -110.95,
        "state": "AZ",
        "elevation_m": 786.0,
    },
    {
        "name": "Univ of Oregon",
        "network": "MIDC",
        "station_id": "UOSMRL",
        "latitude": 44.05,
        "longitude": -123.07,
        "state": "OR",
        "elevation_m": 150.0,
    },
    {
        "name": "Univ of Nevada LV",
        "network": "MIDC",
        "station_id": "UNLV",
        "latitude": 36.11,
        "longitude": -115.14,
        "state": "NV",
        "elevation_m": 615.0,
    },
    {
        "name": "Univ of Louisiana",
        "network": "MIDC",
        "station_id": "ULL",
        "latitude": 30.21,
        "longitude": -92.02,
        "state": "LA",
        "elevation_m": 12.0,
    },
    {
        "name": "UT Rio Grande Valley",
        "network": "MIDC",
        "station_id": "UTPASRL",
        "latitude": 26.31,
        "longitude": -98.17,
        "state": "TX",
        "elevation_m": 30.0,
    },
]
