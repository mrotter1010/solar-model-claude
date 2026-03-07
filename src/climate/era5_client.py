"""Backward-compatible re-export. Actual implementation in open_meteo_client.py."""

from src.climate.open_meteo_client import fetch_era5_land_data

__all__ = ["fetch_era5_land_data"]
