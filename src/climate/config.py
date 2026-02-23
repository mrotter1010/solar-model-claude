"""Climate-specific configuration settings."""

import os
from pathlib import Path

from pydantic import BaseModel, model_validator


class ClimateConfig(BaseModel):
    """Configuration for climate data retrieval.

    Args:
        api_key: NREL API key. Overridden by NSRDB_API_KEY env var.
        api_email: Email for NREL API. Overridden by NSRDB_API_EMAIL env var.
        default_year: Default weather data year.
        cache_max_age_days: Maximum age for cached files before re-fetch.
        cache_dir: Directory for weather data cache files.
        max_cache_distance_km: Maximum distance for nearest-cache fallback.
    """

    api_key: str = "DEMO_KEY"
    api_email: str = "demo@example.com"
    default_year: int = 2024
    cache_max_age_days: int = 365
    cache_dir: Path = Path("data/climate")
    max_cache_distance_km: float = 50.0

    @model_validator(mode="after")
    def load_env_overrides(self) -> "ClimateConfig":
        """Override fields from environment variables if set."""
        if env_key := os.environ.get("NSRDB_API_KEY"):
            self.api_key = env_key
        if env_email := os.environ.get("NSRDB_API_EMAIL"):
            self.api_email = env_email
        return self
