"""Tests for subhourly timeseries adjustment module.

Tests apply_correction for positive/negative/zero corrections, edge cases
(very small correction, insufficient clipping hours), boundary constraints
(no hour below 0 or above AC capacity), and recalculate_summary_metrics.
"""

import json
from pathlib import Path

import numpy as np
import pytest

from src.models.timeseries_adjustment import (
    apply_correction,
    recalculate_summary_metrics,
)


# ---------------------------------------------------------------------------
# Helpers to build realistic 8760 timeseries
# ---------------------------------------------------------------------------


def _make_solar_profile(
    ac_capacity_kw: float = 10_000.0,
    peak_fraction: float = 0.95,
    dc_ac_ratio: float = 1.30,
) -> tuple[list[float], list[float]]:
    """Generate a realistic 8760 solar generation profile.

    Creates a simplified daily pattern: zero at night, bell curve during day
    with peak near noon. DC generation exceeds AC capacity during midday
    (simulating clipping).

    Returns:
        Tuple of (ac_hourly_kwh, dc_hourly_kwh).
    """
    ac = np.zeros(8760)
    dc = np.zeros(8760)
    dc_peak = ac_capacity_kw * dc_ac_ratio

    for day in range(365):
        base_hour = day * 24
        for h in range(24):
            hour_idx = base_hour + h
            if hour_idx >= 8760:
                break

            if 6 <= h <= 18:
                # Bell curve: peak at h=12
                solar_fraction = max(0, 1 - ((h - 12) / 6.5) ** 2)
                # Seasonal variation: summer (day ~180) has higher output
                seasonal = 0.7 + 0.3 * np.sin(
                    2 * np.pi * (day - 80) / 365
                )
                dc_kw = dc_peak * solar_fraction * seasonal
                ac_kw = min(dc_kw, ac_capacity_kw * peak_fraction)
                dc[hour_idx] = dc_kw
                ac[hour_idx] = ac_kw

    return ac.tolist(), dc.tolist()


def _make_flat_clipping_profile(
    n_hours: int = 8760,
    ac_capacity_kw: float = 10_000.0,
    clipping_hours: int = 2000,
    clipping_dc_ratio: float = 1.2,
    non_clipping_gen: float = 5000.0,
) -> tuple[list[float], list[float]]:
    """Generate a profile with controlled clipping hours for predictable tests.

    First `clipping_hours` have DC at clipping ratio and AC at capacity.
    Remaining generating hours have lower, uniform generation.

    Returns:
        Tuple of (ac_hourly, dc_hourly).
    """
    ac = np.zeros(n_hours)
    dc = np.zeros(n_hours)

    for i in range(clipping_hours):
        ac[i] = ac_capacity_kw
        dc[i] = ac_capacity_kw * clipping_dc_ratio

    # Some non-clipping generating hours
    non_clip_hours = min(2000, n_hours - clipping_hours)
    for i in range(clipping_hours, clipping_hours + non_clip_hours):
        ac[i] = non_clipping_gen
        dc[i] = non_clipping_gen * 0.8  # DC/AC ~ 0.4 (well below 0.9)

    return ac.tolist(), dc.tolist()


# ---------------------------------------------------------------------------
# Positive correction tests (remove energy)
# ---------------------------------------------------------------------------


class TestPositiveCorrection:
    """Tests for positive correction (hourly overestimates, remove energy)."""

    def test_total_energy_removed_matches_target(self) -> None:
        """Total energy removed should match correction_pct within 0.1%."""
        ac_cap = 10_000.0
        ac, dc = _make_solar_profile(ac_capacity_kw=ac_cap)
        original_total = sum(ac)

        correction_pct = 0.5  # Remove 0.5%
        adjusted = apply_correction(ac, correction_pct, dc, ac_cap)
        adjusted_total = sum(adjusted)

        expected_total = original_total * (1 - correction_pct / 100)
        relative_error = abs(adjusted_total - expected_total) / expected_total

        assert relative_error < 0.001, (
            f"Energy removed error {relative_error:.4%} exceeds 0.1%. "
            f"Expected {expected_total:.1f}, got {adjusted_total:.1f}"
        )

    def test_only_clipping_hours_affected(self) -> None:
        """Non-clipping hours should be unchanged when clipping hours suffice."""
        ac_cap = 10_000.0
        ac, dc = _make_flat_clipping_profile(ac_capacity_kw=ac_cap)

        correction_pct = 0.3  # Small correction
        adjusted = apply_correction(ac, correction_pct, dc, ac_cap)

        # Non-clipping, non-generating hours (index >= 4000) should be 0
        for i in range(4000, 8760):
            assert adjusted[i] == 0.0

        # Non-clipping generating hours (index 2000-3999) should be unchanged
        for i in range(2000, 4000):
            assert adjusted[i] == ac[i], (
                f"Hour {i} changed from {ac[i]} to {adjusted[i]}"
            )

    def test_no_hour_goes_below_zero(self) -> None:
        """No adjusted hour should be negative."""
        ac_cap = 10_000.0
        ac, dc = _make_solar_profile(ac_capacity_kw=ac_cap)

        adjusted = apply_correction(ac, 0.5, dc, ac_cap)

        assert all(h >= 0 for h in adjusted), "Found negative hour(s)"

    def test_shave_peaks_highest_first(self) -> None:
        """Hours with highest DC/AC ratio should lose the most energy."""
        ac_cap = 10_000.0
        # Create profile with varying clipping levels
        ac = [0.0] * 8760
        dc = [0.0] * 8760
        # 3 clipping hours with different DC levels
        ac[0], dc[0] = 9500.0, 15000.0  # DC/AC = 1.50 (highest)
        ac[1], dc[1] = 9500.0, 13000.0  # DC/AC = 1.30
        ac[2], dc[2] = 9500.0, 11000.0  # DC/AC = 1.10
        # Non-clipping
        ac[3], dc[3] = 5000.0, 5000.0

        # Remove a small amount
        adjusted = apply_correction(ac, 0.1, dc, ac_cap)

        # Hour 0 (highest DC/AC) should lose the most
        loss_0 = ac[0] - adjusted[0]
        loss_1 = ac[1] - adjusted[1]
        loss_2 = ac[2] - adjusted[2]

        assert loss_0 >= loss_1 >= loss_2, (
            f"Expected decreasing losses: {loss_0:.2f} >= {loss_1:.2f} >= {loss_2:.2f}"
        )


class TestNegativeCorrection:
    """Tests for negative correction (hourly underestimates, add energy)."""

    def test_total_energy_added_matches_target(self) -> None:
        """Total energy added should match correction_pct within 0.1%."""
        ac_cap = 10_000.0
        ac, dc = _make_solar_profile(ac_capacity_kw=ac_cap)
        original_total = sum(ac)

        correction_pct = -1.0  # Add 1%
        adjusted = apply_correction(ac, correction_pct, dc, ac_cap)
        adjusted_total = sum(adjusted)

        expected_total = original_total * (1 + abs(correction_pct) / 100)
        relative_error = abs(adjusted_total - expected_total) / expected_total

        assert relative_error < 0.001, (
            f"Energy added error {relative_error:.4%} exceeds 0.1%. "
            f"Expected {expected_total:.1f}, got {adjusted_total:.1f}"
        )

    def test_no_hour_exceeds_ac_capacity(self) -> None:
        """No adjusted hour should exceed AC capacity."""
        ac_cap = 10_000.0
        ac, dc = _make_solar_profile(ac_capacity_kw=ac_cap)

        adjusted = apply_correction(ac, -1.5, dc, ac_cap)

        assert all(h <= ac_cap for h in adjusted), (
            f"Found hour(s) exceeding AC capacity {ac_cap}"
        )

    def test_higher_dc_ratio_gets_more_boost(self) -> None:
        """Hours with higher DC/AC ratio should receive more energy."""
        ac_cap = 10_000.0
        ac = [0.0] * 8760
        dc = [0.0] * 8760
        # Two high-irradiance hours with different DC ratios
        ac[0], dc[0] = 8000.0, 12000.0  # DC/AC = 1.20 (higher)
        ac[1], dc[1] = 8000.0, 8000.0   # DC/AC = 0.80

        adjusted = apply_correction(ac, -1.0, dc, ac_cap)

        gain_0 = adjusted[0] - ac[0]
        gain_1 = adjusted[1] - ac[1]

        assert gain_0 > gain_1, (
            f"Higher DC/AC hour should get more: {gain_0:.2f} vs {gain_1:.2f}"
        )

    def test_night_hours_unchanged(self) -> None:
        """Hours with zero generation should stay zero."""
        ac_cap = 10_000.0
        ac, dc = _make_solar_profile(ac_capacity_kw=ac_cap)

        adjusted = apply_correction(ac, -1.0, dc, ac_cap)

        # Night hours (DC = 0, hence DC/AC = 0 < 0.70) should stay zero
        for i in range(8760):
            if dc[i] == 0.0:
                assert adjusted[i] == 0.0, (
                    f"Night hour {i} changed to {adjusted[i]}"
                )


# ---------------------------------------------------------------------------
# Zero and edge case tests
# ---------------------------------------------------------------------------


class TestZeroAndEdgeCases:
    """Tests for zero correction and edge cases."""

    def test_zero_correction_returns_input(self) -> None:
        """Zero correction should return exact copy of input."""
        ac_cap = 10_000.0
        ac, dc = _make_solar_profile(ac_capacity_kw=ac_cap)

        adjusted = apply_correction(ac, 0.0, dc, ac_cap)

        assert adjusted == ac

    def test_very_small_correction(self) -> None:
        """Very small correction (0.01%) should still apply cleanly."""
        ac_cap = 10_000.0
        ac, dc = _make_solar_profile(ac_capacity_kw=ac_cap)
        original_total = sum(ac)

        correction_pct = 0.01
        adjusted = apply_correction(ac, correction_pct, dc, ac_cap)
        adjusted_total = sum(adjusted)

        # Should be different from original
        assert adjusted_total != original_total
        # But only by ~0.01%
        expected_total = original_total * (1 - correction_pct / 100)
        relative_error = abs(adjusted_total - expected_total) / expected_total
        assert relative_error < 0.001

    def test_very_small_negative_correction(self) -> None:
        """Very small negative correction (-0.01%) should apply cleanly."""
        ac_cap = 10_000.0
        ac, dc = _make_solar_profile(ac_capacity_kw=ac_cap)
        original_total = sum(ac)

        correction_pct = -0.01
        adjusted = apply_correction(ac, correction_pct, dc, ac_cap)
        adjusted_total = sum(adjusted)

        assert adjusted_total != original_total
        expected_total = original_total * (1 + abs(correction_pct) / 100)
        relative_error = abs(adjusted_total - expected_total) / expected_total
        assert relative_error < 0.001

    def test_fallback_when_clipping_insufficient(self) -> None:
        """When clipping-prone hours can't absorb full correction, spread remainder.

        Creates a profile with very few clipping hours and a large correction
        to force the proportional fallback.
        """
        ac_cap = 10_000.0
        ac = [0.0] * 8760
        dc = [0.0] * 8760

        # Only 2 clipping-prone hours with small generation
        ac[0], dc[0] = 100.0, 9500.0  # DC/AC = 0.95 (clipping-prone)
        ac[1], dc[1] = 100.0, 9200.0  # DC/AC = 0.92

        # Many non-clipping generating hours
        for i in range(2, 4002):
            ac[i] = 5000.0
            dc[i] = 4000.0  # DC/AC = 0.40

        original_total = sum(ac)
        correction_pct = 0.5  # Remove 0.5%

        adjusted = apply_correction(ac, correction_pct, dc, ac_cap)
        adjusted_total = sum(adjusted)

        # Should still achieve the target (via proportional fallback)
        expected_total = original_total * (1 - correction_pct / 100)
        relative_error = abs(adjusted_total - expected_total) / expected_total
        assert relative_error < 0.001, (
            f"Fallback failed: error {relative_error:.4%}"
        )

    def test_no_generation_returns_input(self) -> None:
        """All-zero timeseries should return unchanged."""
        ac = [0.0] * 8760
        dc = [0.0] * 8760

        adjusted = apply_correction(ac, -1.0, dc, 10_000.0)

        assert adjusted == ac

    def test_all_hours_stay_within_bounds(self) -> None:
        """All hours should be in [0, ac_capacity_kw] after any correction."""
        ac_cap = 10_000.0
        ac, dc = _make_solar_profile(ac_capacity_kw=ac_cap)

        for pct in [-1.5, -0.5, 0.0, 0.3, 0.6]:
            adjusted = apply_correction(ac, pct, dc, ac_cap)

            for i, val in enumerate(adjusted):
                assert 0.0 <= val <= ac_cap, (
                    f"Hour {i} = {val} out of bounds [0, {ac_cap}] "
                    f"at correction {pct}%"
                )


# ---------------------------------------------------------------------------
# recalculate_summary_metrics tests
# ---------------------------------------------------------------------------


class TestRecalculateSummaryMetrics:
    """Tests for recalculate_summary_metrics with known values."""

    def test_known_values(self) -> None:
        """Verify metrics with a simple known timeseries."""
        # 8760 hours at constant 5000 kW = 43,800,000 kWh
        gen = [5000.0] * 8760
        dc_cap = 13_000.0
        ac_cap = 10_000.0

        metrics = recalculate_summary_metrics(gen, dc_cap, ac_cap)

        assert metrics["total_generation_kwh"] == 43_800_000.0
        # CF = 43.8M / (10000 * 8760) = 0.5
        assert abs(metrics["net_capacity_factor"] - 0.5) < 0.0001
        # Yield = 43.8M / 13000 = 3369.23 kWh/kWp
        assert abs(metrics["yield_kwh_per_kwp"] - 3369.23) < 0.1
        # PR = 43.8M / (13000 * 8760) = 0.384615
        assert abs(metrics["performance_ratio"] - 0.384615) < 0.001

    def test_returns_all_required_keys(self) -> None:
        """Verify returned dict has all expected keys."""
        gen = [1000.0] * 8760
        metrics = recalculate_summary_metrics(gen, 10_000.0, 8_000.0)

        expected_keys = {
            "total_generation_kwh",
            "net_capacity_factor",
            "yield_kwh_per_kwp",
            "performance_ratio",
        }
        assert set(metrics.keys()) == expected_keys

    def test_zero_generation(self) -> None:
        """Zero generation should produce zero metrics."""
        gen = [0.0] * 8760
        metrics = recalculate_summary_metrics(gen, 10_000.0, 8_000.0)

        assert metrics["total_generation_kwh"] == 0.0
        assert metrics["net_capacity_factor"] == 0.0
        assert metrics["yield_kwh_per_kwp"] == 0.0
        assert metrics["performance_ratio"] == 0.0

    def test_realistic_values(self) -> None:
        """Verify metrics with a realistic solar profile."""
        ac_cap = 10_000.0
        dc_cap = 13_000.0
        ac, _ = _make_solar_profile(ac_capacity_kw=ac_cap)

        metrics = recalculate_summary_metrics(ac, dc_cap, ac_cap)

        # Typical utility-scale CF is 15-30%
        assert 0.10 < metrics["net_capacity_factor"] < 0.35
        # Typical specific yield is 1200-2200 kWh/kWp
        assert 800 < metrics["yield_kwh_per_kwp"] < 2500
        assert metrics["total_generation_kwh"] > 0


# ---------------------------------------------------------------------------
# Output for manual inspection
# ---------------------------------------------------------------------------


class TestOutputs:
    """Write test outputs to test_results/ for manual inspection."""

    def test_save_adjustment_summary(self, test_results_dir: Path) -> None:
        """Save before/after metrics for several correction levels."""
        ac_cap = 10_000.0
        dc_cap = 13_000.0
        ac, dc = _make_solar_profile(ac_capacity_kw=ac_cap)
        original_total = sum(ac)

        results = []
        for pct in [-1.5, -1.0, -0.5, -0.01, 0.0, 0.01, 0.3, 0.5, 0.6]:
            adjusted = apply_correction(ac, pct, dc, ac_cap)
            adjusted_total = sum(adjusted)
            metrics = recalculate_summary_metrics(adjusted, dc_cap, ac_cap)

            results.append({
                "correction_pct": pct,
                "original_total_kwh": round(original_total, 1),
                "adjusted_total_kwh": round(adjusted_total, 1),
                "delta_kwh": round(adjusted_total - original_total, 1),
                "actual_change_pct": round(
                    (adjusted_total - original_total) / original_total * 100, 4
                ),
                **metrics,
            })

        output_path = test_results_dir / "test_timeseries_adjustment.json"
        output_path.write_text(json.dumps(results, indent=2))

        assert output_path.exists()
        assert len(results) == 9
