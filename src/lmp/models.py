"""Pydantic models for LMP (Locational Marginal Pricing) data."""

import statistics
from datetime import datetime

from pydantic import BaseModel, field_validator


class LMPData(BaseModel):
    """Hourly LMP prices for a zone/node over a year.

    Args:
        iso: ISO/RTO identifier (e.g., "pjm", "ercot", "caiso").
        zone: Pricing zone or node name.
        market: Market type (e.g., "DAY_AHEAD_HOURLY", "REAL_TIME_HOURLY").
        year: Calendar year of the price data.
        prices: 8760 hourly prices in $/MWh.
        timestamps: Optional hourly timestamps for reference.
        currency: Currency code for prices.
    """

    iso: str
    zone: str
    market: str
    year: int
    prices: list[float]
    timestamps: list[datetime] | None = None
    currency: str = "USD"

    @field_validator("prices")
    @classmethod
    def validate_prices_length(cls, v: list[float]) -> list[float]:
        """Validate that prices has exactly 8760 hourly values."""
        if len(v) != 8760:
            raise ValueError(
                f"prices must have exactly 8760 hourly values, got {len(v)}."
            )
        return v

    @property
    def mean_price(self) -> float:
        """Average $/MWh across all hours."""
        return statistics.mean(self.prices)

    @property
    def min_price(self) -> float:
        """Minimum $/MWh across all hours."""
        return min(self.prices)

    @property
    def max_price(self) -> float:
        """Maximum $/MWh across all hours."""
        return max(self.prices)

    @property
    def price_std(self) -> float:
        """Standard deviation of $/MWh across all hours."""
        return statistics.stdev(self.prices)


class PricingZone(BaseModel):
    """A pricing zone within an ISO/RTO.

    Args:
        iso: ISO/RTO identifier (e.g., "pjm", "ercot", "caiso").
        zone_name: Zone, hub, or node name.
        zone_type: Type of pricing location ("zone", "hub", "node").
        description: Optional human-readable description.
    """

    iso: str
    zone_name: str
    zone_type: str
    description: str | None = None
