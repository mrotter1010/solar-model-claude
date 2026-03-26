"""Tests for LMP data models (M14e)."""

import statistics
from datetime import datetime

import pytest

from src.lmp.models import LMPData, PricingZone


def _make_prices(base: float = 30.0, n: int = 8760) -> list[float]:
    """Generate synthetic hourly prices with a simple diurnal pattern."""
    return [base + (i % 24) * 0.5 for i in range(n)]


class TestLMPDataCreation:
    """LMPData model creation and field access."""

    def test_create_valid_lmp_data(self) -> None:
        """LMPData should accept valid 8760-length prices."""
        prices = _make_prices()
        data = LMPData(
            iso="pjm",
            zone="AEP",
            market="DAY_AHEAD_HOURLY",
            year=2024,
            prices=prices,
        )
        assert data.iso == "pjm"
        assert data.zone == "AEP"
        assert data.market == "DAY_AHEAD_HOURLY"
        assert data.year == 2024
        assert len(data.prices) == 8760
        assert data.currency == "USD"
        assert data.timestamps is None

    def test_create_with_timestamps(self) -> None:
        """LMPData should accept optional timestamps."""
        prices = _make_prices()
        ts = [datetime(2024, 1, 1, h % 24) for h in range(8760)]
        data = LMPData(
            iso="ercot",
            zone="LZ_HOUSTON",
            market="REAL_TIME_HOURLY",
            year=2024,
            prices=prices,
            timestamps=ts,
        )
        assert data.timestamps is not None
        assert len(data.timestamps) == 8760

    def test_custom_currency(self) -> None:
        """LMPData should accept custom currency code."""
        data = LMPData(
            iso="caiso",
            zone="SP15",
            market="DAY_AHEAD_HOURLY",
            year=2024,
            prices=_make_prices(),
            currency="CAD",
        )
        assert data.currency == "CAD"


class TestLMPDataValidation:
    """LMPData validation rules."""

    def test_reject_short_prices(self) -> None:
        """Prices list shorter than 8760 should be rejected."""
        with pytest.raises(ValueError, match="8760"):
            LMPData(
                iso="pjm",
                zone="AEP",
                market="DAY_AHEAD_HOURLY",
                year=2024,
                prices=[30.0] * 100,
            )

    def test_reject_long_prices(self) -> None:
        """Prices list longer than 8760 should be rejected."""
        with pytest.raises(ValueError, match="8760"):
            LMPData(
                iso="pjm",
                zone="AEP",
                market="DAY_AHEAD_HOURLY",
                year=2024,
                prices=[30.0] * 8761,
            )

    def test_reject_empty_prices(self) -> None:
        """Empty prices list should be rejected."""
        with pytest.raises(ValueError, match="8760"):
            LMPData(
                iso="pjm",
                zone="AEP",
                market="DAY_AHEAD_HOURLY",
                year=2024,
                prices=[],
            )


class TestLMPDataComputedProperties:
    """LMPData computed properties (mean, min, max, std)."""

    def test_mean_price(self) -> None:
        """mean_price should return the average of all hourly prices."""
        prices = _make_prices()
        data = LMPData(
            iso="pjm", zone="AEP", market="DAY_AHEAD_HOURLY",
            year=2024, prices=prices,
        )
        assert data.mean_price == pytest.approx(statistics.mean(prices))

    def test_min_price(self) -> None:
        """min_price should return the minimum hourly price."""
        prices = _make_prices()
        data = LMPData(
            iso="pjm", zone="AEP", market="DAY_AHEAD_HOURLY",
            year=2024, prices=prices,
        )
        assert data.min_price == min(prices)

    def test_max_price(self) -> None:
        """max_price should return the maximum hourly price."""
        prices = _make_prices()
        data = LMPData(
            iso="pjm", zone="AEP", market="DAY_AHEAD_HOURLY",
            year=2024, prices=prices,
        )
        assert data.max_price == max(prices)

    def test_price_std(self) -> None:
        """price_std should return the standard deviation."""
        prices = _make_prices()
        data = LMPData(
            iso="pjm", zone="AEP", market="DAY_AHEAD_HOURLY",
            year=2024, prices=prices,
        )
        assert data.price_std == pytest.approx(statistics.stdev(prices))

    def test_uniform_prices_zero_std(self) -> None:
        """Uniform prices should have zero standard deviation."""
        prices = [50.0] * 8760
        data = LMPData(
            iso="ercot", zone="LZ_NORTH", market="DAY_AHEAD_HOURLY",
            year=2024, prices=prices,
        )
        assert data.mean_price == 50.0
        assert data.min_price == 50.0
        assert data.max_price == 50.0
        assert data.price_std == 0.0


class TestPricingZone:
    """PricingZone model creation."""

    def test_create_zone(self) -> None:
        """PricingZone should accept valid zone data."""
        zone = PricingZone(
            iso="pjm",
            zone_name="AEP",
            zone_type="zone",
            description="American Electric Power zone",
        )
        assert zone.iso == "pjm"
        assert zone.zone_name == "AEP"
        assert zone.zone_type == "zone"
        assert zone.description == "American Electric Power zone"

    def test_create_hub(self) -> None:
        """PricingZone should accept hub type."""
        zone = PricingZone(
            iso="caiso",
            zone_name="NP15",
            zone_type="hub",
        )
        assert zone.zone_type == "hub"
        assert zone.description is None

    def test_create_node(self) -> None:
        """PricingZone should accept node type."""
        zone = PricingZone(
            iso="ercot",
            zone_name="HB_WEST",
            zone_type="node",
        )
        assert zone.zone_type == "node"
