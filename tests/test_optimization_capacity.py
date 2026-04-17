"""Tests for src/optimization/capacity.py — capacity_from_acreage()."""

import math

import pytest

from src.optimization.capacity import capacity_from_acreage
from src.optimization.defaults import DEFAULT_MODULE_AREA_M2, DEFAULT_MODULE_POWER_W


# ---------------------------------------------------------------------------
# Constants for manual verification
# ---------------------------------------------------------------------------
SQ_M_PER_ACRE = 4046.86
LONGI_POWER_W = DEFAULT_MODULE_POWER_W  # 551.0
LONGI_AREA_M2 = DEFAULT_MODULE_AREA_M2  # 2.51


class TestBasicCalculation:
    """Verify against a hand-computed example."""

    def test_100_acres_gcr040_longi_defaults(self) -> None:
        # Arrange — manual calculation
        # usable_area = 100 * 4046.86 * 0.75 = 303_514.5 m²
        # footprint  = 2.51 / 0.40 = 6.275 m²
        # modules    = floor(303514.5 / 6.275) = floor(48_370.598...) = 48_370
        # kw_dc      = 48_370 * 551.0 / 1000 = 26_651.87
        # mw_dc      = 26_651.87 / 1000 = 26.65187
        usable_area = 100 * SQ_M_PER_ACRE * 0.75
        footprint = LONGI_AREA_M2 / 0.40
        expected_modules = math.floor(usable_area / footprint)
        expected_kw = expected_modules * LONGI_POWER_W / 1000
        expected_mw = expected_kw / 1000

        # Act
        result = capacity_from_acreage(100, 0.40, LONGI_AREA_M2, LONGI_POWER_W)

        # Assert — exact integer module count
        assert result["num_modules"] == expected_modules
        assert result["mw_dc"] == round(expected_mw, 3)
        assert result["kw_dc"] == round(expected_kw, 1)
        assert result["usable_acres"] == 75.0  # 100 * 0.75
        assert result["modules_per_mw"] == round(1_000_000 / LONGI_POWER_W)
        assert result["acres_per_mw"] == round(75.0 / expected_mw, 2)


class TestAcresPerMwIndustryRange:
    """Verify acres/MW falls in realistic ranges for modern high-efficiency panels.

    Note: The traditional "5 acres/MW" rule of thumb was based on ~350W panels
    at ~17% efficiency. Modern 550W panels at ~22% efficiency achieve much
    higher density: ~2.5-4 acres/MW with 75% utilization.
    """

    def test_tracker_gcr040_in_range(self) -> None:
        # Single-axis tracker at GCR 0.40 with 550W panels: expect ~2.5-3.5 acres/MW
        result = capacity_from_acreage(100, 0.40, LONGI_AREA_M2, LONGI_POWER_W)
        assert 2.0 <= result["acres_per_mw"] <= 4.0, (
            f"acres_per_mw={result['acres_per_mw']} outside 2-4 range for tracker"
        )

    def test_fixed_tilt_gcr060_in_range(self) -> None:
        # Fixed-tilt at GCR 0.60 with 550W panels: expect ~1.5-2.5 acres/MW
        result = capacity_from_acreage(100, 0.60, LONGI_AREA_M2, LONGI_POWER_W)
        assert 1.5 <= result["acres_per_mw"] <= 2.5, (
            f"acres_per_mw={result['acres_per_mw']} outside 1.5-2.5 range for fixed"
        )


class TestGcrSensitivity:
    """Higher GCR → more modules → more MW on same acreage."""

    def test_higher_gcr_yields_more_mw(self) -> None:
        low = capacity_from_acreage(100, 0.30, LONGI_AREA_M2, LONGI_POWER_W)
        mid = capacity_from_acreage(100, 0.40, LONGI_AREA_M2, LONGI_POWER_W)
        high = capacity_from_acreage(100, 0.60, LONGI_AREA_M2, LONGI_POWER_W)

        assert low["mw_dc"] < mid["mw_dc"] < high["mw_dc"]
        assert low["num_modules"] < mid["num_modules"] < high["num_modules"]
        # Higher GCR → fewer acres per MW (denser)
        assert high["acres_per_mw"] < mid["acres_per_mw"] < low["acres_per_mw"]


class TestInputValidation:
    """Confirm ValueError for invalid inputs."""

    def test_zero_acres_raises(self) -> None:
        with pytest.raises(ValueError, match="buildable_acres must be > 0"):
            capacity_from_acreage(0, 0.40, LONGI_AREA_M2, LONGI_POWER_W)

    def test_negative_acres_raises(self) -> None:
        with pytest.raises(ValueError, match="buildable_acres must be > 0"):
            capacity_from_acreage(-10, 0.40, LONGI_AREA_M2, LONGI_POWER_W)

    def test_zero_gcr_raises(self) -> None:
        with pytest.raises(ValueError, match="gcr must be > 0"):
            capacity_from_acreage(100, 0, LONGI_AREA_M2, LONGI_POWER_W)

    def test_negative_gcr_raises(self) -> None:
        with pytest.raises(ValueError, match="gcr must be > 0"):
            capacity_from_acreage(100, -0.1, LONGI_AREA_M2, LONGI_POWER_W)

    def test_zero_module_area_raises(self) -> None:
        with pytest.raises(ValueError, match="module_area_m2 must be > 0"):
            capacity_from_acreage(100, 0.40, 0, LONGI_POWER_W)

    def test_zero_module_power_raises(self) -> None:
        with pytest.raises(ValueError, match="module_power_w must be > 0"):
            capacity_from_acreage(100, 0.40, LONGI_AREA_M2, 0)

    def test_utilization_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="utilization_factor must be > 0"):
            capacity_from_acreage(100, 0.40, LONGI_AREA_M2, LONGI_POWER_W, 0)

    def test_utilization_over_1_raises(self) -> None:
        with pytest.raises(ValueError, match="utilization_factor must be > 0"):
            capacity_from_acreage(100, 0.40, LONGI_AREA_M2, LONGI_POWER_W, 1.1)

    def test_utilization_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="utilization_factor must be > 0"):
            capacity_from_acreage(100, 0.40, LONGI_AREA_M2, LONGI_POWER_W, -0.5)


class TestEdgeCases:
    """Edge cases: full utilization, very small acreage."""

    def test_utilization_factor_1(self) -> None:
        # No roads/setbacks — all buildable land used
        result = capacity_from_acreage(
            100, 0.40, LONGI_AREA_M2, LONGI_POWER_W, utilization_factor=1.0
        )
        assert result["usable_acres"] == 100.0
        # More MW than default 0.75 utilization
        default = capacity_from_acreage(100, 0.40, LONGI_AREA_M2, LONGI_POWER_W)
        assert result["mw_dc"] > default["mw_dc"]

    def test_very_small_acreage(self) -> None:
        # 0.5 acres — should produce valid output, even if small
        result = capacity_from_acreage(0.5, 0.40, LONGI_AREA_M2, LONGI_POWER_W)
        assert result["num_modules"] > 0
        assert result["mw_dc"] > 0
        assert result["kw_dc"] > 0
        assert result["acres_per_mw"] > 0


class TestRuleOfThumb:
    """Compare against industry benchmarks for modern high-efficiency panels.

    Traditional "5 acres/MW" rule was based on ~350W panels (~17% eff).
    Modern 550W panels at ~22% efficiency achieve ~2.5-4 acres/MW of
    usable (post-utilization-factor) land. To reconcile with the 5 acres/MW
    rule, compare total (pre-utilization) acres: 2.81 / 0.75 ≈ 3.75 total
    acres/MW — still denser due to higher efficiency panels.
    """

    def test_modern_panel_density_at_gcr_035_040(self) -> None:
        # GCR 0.35: wider row spacing
        r035 = capacity_from_acreage(100, 0.35, LONGI_AREA_M2, LONGI_POWER_W)
        # GCR 0.40: typical tracker
        r040 = capacity_from_acreage(100, 0.40, LONGI_AREA_M2, LONGI_POWER_W)

        # Modern 550W panels: ~2.5-4.0 usable acres/MW
        assert 2.5 <= r035["acres_per_mw"] <= 4.0, (
            f"GCR=0.35: acres_per_mw={r035['acres_per_mw']} outside 2.5-4.0"
        )
        assert 2.0 <= r040["acres_per_mw"] <= 3.5, (
            f"GCR=0.40: acres_per_mw={r040['acres_per_mw']} outside 2.0-3.5"
        )

    def test_total_acres_per_mw_closer_to_traditional(self) -> None:
        """Total (pre-utilization) acres/MW should be closer to ~4 acres/MW."""
        r040 = capacity_from_acreage(100, 0.40, LONGI_AREA_M2, LONGI_POWER_W)
        # Total acres = usable acres / utilization_factor
        total_acres_per_mw = r040["acres_per_mw"] / 0.75
        assert 3.0 <= total_acres_per_mw <= 5.0, (
            f"Total acres/MW={total_acres_per_mw:.2f} outside 3-5 range"
        )
