"""Tests for optimization chart builders (heatmap and winner comparison)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.optimization.chart_builder import (
    build_optimization_heatmap,
    build_winner_comparison_chart,
)


# ---------------------------------------------------------------------------
# Fixtures — synthetic sweep data
# ---------------------------------------------------------------------------

def _build_sweep_grid(
    gcr_range: tuple[float, float, float] = (0.30, 0.55, 0.05),
    dcac_range: tuple[float, float, float] = (1.10, 1.40, 0.10),
    mode: str = "lcoe",
) -> list[dict]:
    """Build a synthetic sweep_results list for testing.

    Each point gets realistic-ish annual_energy_mwh and capacity_factor_ac
    values that vary with GCR and DC/AC.  LCOE and NPV are generated when
    the mode requires them.
    """
    import numpy as np

    gcr_vals = np.arange(gcr_range[0], gcr_range[1] + 0.001, gcr_range[2])
    dcac_vals = np.arange(dcac_range[0], dcac_range[1] + 0.001, dcac_range[2])

    results: list[dict] = []
    for gcr in gcr_vals:
        for dcac in dcac_vals:
            gcr_r = round(float(gcr), 2)
            dcac_r = round(float(dcac), 2)

            # Higher GCR → more MW → more energy but lower CF
            mw_dc = round(gcr_r * 60, 1)
            mw_ac = round(mw_dc / dcac_r, 1)
            energy = round(mw_dc * 1700 + gcr_r * 1000 - dcac_r * 500, 1)
            cf = round(28.0 - gcr_r * 5 + dcac_r * 2, 2)

            point: dict = {
                "gcr": gcr_r,
                "dcac_ratio": dcac_r,
                "mw_dc": mw_dc,
                "mw_ac": mw_ac,
                "annual_energy_kwh": energy * 1000,
                "annual_energy_mwh": energy,
                "capacity_factor_ac": cf,
                "clipping_correction_pct": 0.3,
                "failed": False,
                "error": None,
                "capex_total": None,
                "opex_per_year": None,
                "net_capex": None,
                "lcoe_per_mwh": None,
                "lcoe_per_kwh": None,
                "annual_revenue_year1": None,
                "npv": None,
                "simple_payback_years": None,
                "irr": None,
            }

            if mode in ("lcoe", "npv"):
                point["lcoe_per_mwh"] = round(45 - gcr_r * 5 + dcac_r * 3, 2)
                point["lcoe_per_kwh"] = round(point["lcoe_per_mwh"] / 1000, 5)
                point["net_capex"] = round(mw_dc * 900_000, 2)

            if mode == "npv":
                point["npv"] = round(energy * 20 - mw_dc * 15000, 2)
                point["irr"] = round(7.0 + gcr_r * 3 - dcac_r * 1, 2)
                point["simple_payback_years"] = round(12.0 - gcr_r * 2, 1)

            results.append(point)

    return results


def _build_bess_sweep_grid() -> list[dict]:
    """Build synthetic bess_sweep_results for solar_bess mode testing."""
    results: list[dict] = []
    for gcr in (0.35, 0.40, 0.45, 0.50):
        for dcac in (1.20, 1.30):
            for power_frac in (0.25, 0.50):
                for dur in (2, 4):
                    combined_npv = round(
                        500_000 + gcr * 1_000_000
                        - dcac * 100_000
                        + power_frac * 200_000
                        + dur * 50_000,
                        2,
                    )
                    results.append({
                        "solar_gcr": gcr,
                        "solar_dcac": dcac,
                        "bess_power_mw": round(gcr * 60 * power_frac / dcac, 1),
                        "bess_duration_hr": dur,
                        "bess_capacity_mwh": round(
                            gcr * 60 * power_frac / dcac * dur, 1
                        ),
                        "combined_npv": combined_npv,
                        "solar_npv": round(combined_npv * 0.7, 2),
                        "bess_npv": round(combined_npv * 0.3, 2),
                        "failed": False,
                    })
    return results


def _production_winners() -> dict:
    """Minimal winners dict for production mode."""
    return {
        "max_production": {
            "gcr": 0.50, "dcac_ratio": 1.20,
            "mw_dc": 30.0, "mw_ac": 25.0,
            "annual_energy_mwh": 52_000.0,
            "capacity_factor_ac": 26.5,
            "lcoe_per_mwh": None, "npv": None,
        },
        "max_yield": {
            "gcr": 0.30, "dcac_ratio": 1.10,
            "mw_dc": 18.0, "mw_ac": 16.4,
            "annual_energy_mwh": 31_500.0,
            "capacity_factor_ac": 28.8,
            "lcoe_per_mwh": None, "npv": None,
        },
    }


def _lcoe_winners() -> dict:
    """Winners dict for LCOE mode."""
    base = _production_winners()
    base["lcoe"] = {
        "gcr": 0.40, "dcac_ratio": 1.20,
        "mw_dc": 24.0, "mw_ac": 20.0,
        "annual_energy_mwh": 41_200.0,
        "capacity_factor_ac": 27.5,
        "lcoe_per_mwh": 42.31,
        "net_capex": 21_600_000.0,
    }
    # Add LCOE to production/yield winners for comparison chart
    base["max_production"]["lcoe_per_mwh"] = 44.50
    base["max_yield"]["lcoe_per_mwh"] = 46.20
    return base


def _npv_winners() -> dict:
    """Winners dict for NPV mode."""
    base = _lcoe_winners()
    base["npv"] = {
        "gcr": 0.45, "dcac_ratio": 1.30,
        "mw_dc": 27.0, "mw_ac": 20.8,
        "annual_energy_mwh": 46_100.0,
        "capacity_factor_ac": 27.0,
        "lcoe_per_mwh": 43.10,
        "npv": 4_200_000.0,
        "irr": 8.2,
        "simple_payback_years": 10.5,
    }
    return base


def _solar_bess_winners() -> dict:
    """Winners dict for solar_bess mode."""
    return {
        "max_production": _production_winners()["max_production"],
        "max_yield": _production_winners()["max_yield"],
        "solar_only_npv": {
            "gcr": 0.45, "dcac_ratio": 1.30,
            "mw_dc": 27.0, "mw_ac": 20.8,
            "annual_energy_mwh": 46_100.0,
            "capacity_factor_ac": 27.0,
            "npv": 4_200_000.0,
            "irr": 8.2,
            "simple_payback_years": 10.5,
        },
        "solar_bess_npv": {
            "solar_gcr": 0.50, "solar_dcac": 1.20,
            "combined_npv": 5_800_000.0,
            "failed": False,
        },
    }


# ---------------------------------------------------------------------------
# Tests — build_optimization_heatmap
# ---------------------------------------------------------------------------

class TestOptimizationHeatmap:
    """Tests for the GCR x DC/AC heatmap chart."""

    def test_lcoe_mode_creates_png(self, tmp_path: Path):
        """LCOE mode should produce a non-empty PNG file."""
        out = tmp_path / "heatmap.png"
        sweep = _build_sweep_grid(mode="lcoe")
        result = build_optimization_heatmap(sweep, _lcoe_winners(), "lcoe", out)

        assert result == out
        assert out.exists()
        assert out.stat().st_size > 0

    def test_npv_mode_creates_png(self, tmp_path: Path):
        """NPV mode should produce a non-empty PNG file."""
        out = tmp_path / "heatmap.png"
        sweep = _build_sweep_grid(mode="npv")
        result = build_optimization_heatmap(sweep, _npv_winners(), "npv", out)

        assert result == out
        assert out.exists()
        assert out.stat().st_size > 0

    def test_solar_bess_mode_creates_png(self, tmp_path: Path):
        """solar_bess mode should produce a heatmap over combined_npv."""
        out = tmp_path / "heatmap.png"
        sweep = _build_bess_sweep_grid()
        result = build_optimization_heatmap(
            sweep, _solar_bess_winners(), "solar_bess", out,
        )

        assert result == out
        assert out.exists()
        assert out.stat().st_size > 0

    def test_production_mode_returns_none(self, tmp_path: Path):
        """Production mode has no heatmap — should return None."""
        out = tmp_path / "heatmap.png"
        sweep = _build_sweep_grid(mode="production")
        # Remove lcoe keys to simulate production mode
        for p in sweep:
            p["lcoe_per_mwh"] = None

        result = build_optimization_heatmap(
            sweep, _production_winners(), "production", out,
        )

        assert result is None
        assert not out.exists()

    def test_all_failed_returns_none(self, tmp_path: Path):
        """If all sweep points are failed, should return None gracefully."""
        out = tmp_path / "heatmap.png"
        sweep = _build_sweep_grid(mode="lcoe")
        for p in sweep:
            p["failed"] = True

        result = build_optimization_heatmap(sweep, _lcoe_winners(), "lcoe", out)

        assert result is None

    def test_empty_sweep_returns_none(self, tmp_path: Path):
        """Empty sweep_results should return None."""
        out = tmp_path / "heatmap.png"
        result = build_optimization_heatmap([], _lcoe_winners(), "lcoe", out)
        assert result is None

    def test_single_row_returns_none(self, tmp_path: Path):
        """A single GCR value (1D line) is not enough for a 2D heatmap."""
        out = tmp_path / "heatmap.png"
        sweep = _build_sweep_grid(
            gcr_range=(0.40, 0.40, 0.05),
            dcac_range=(1.10, 1.40, 0.10),
            mode="lcoe",
        )
        result = build_optimization_heatmap(sweep, _lcoe_winners(), "lcoe", out)
        assert result is None

    def test_file_size_reasonable(self, tmp_path: Path):
        """PNG at 150 DPI should be between 10 KB and 2 MB."""
        out = tmp_path / "heatmap.png"
        sweep = _build_sweep_grid(mode="lcoe")
        build_optimization_heatmap(sweep, _lcoe_winners(), "lcoe", out)

        size_kb = out.stat().st_size / 1024
        assert 10 < size_kb < 2048


# ---------------------------------------------------------------------------
# Tests — build_winner_comparison_chart
# ---------------------------------------------------------------------------

class TestWinnerComparisonChart:
    """Tests for the grouped winner comparison bar chart."""

    def test_production_mode_creates_png(self, tmp_path: Path):
        """Production mode (2 winners) should create a PNG."""
        out = tmp_path / "comparison.png"
        result = build_winner_comparison_chart(
            _production_winners(), "production", out,
        )

        assert result == out
        assert out.exists()
        assert out.stat().st_size > 0

    def test_lcoe_mode_creates_png(self, tmp_path: Path):
        """LCOE mode (3 winners) should create a PNG."""
        out = tmp_path / "comparison.png"
        result = build_winner_comparison_chart(
            _lcoe_winners(), "lcoe", out,
        )

        assert result == out
        assert out.exists()

    def test_npv_mode_creates_png(self, tmp_path: Path):
        """NPV mode (4 winners) should create a PNG."""
        out = tmp_path / "comparison.png"
        result = build_winner_comparison_chart(
            _npv_winners(), "npv", out,
        )

        assert result == out
        assert out.exists()

    def test_solar_bess_mode_creates_png(self, tmp_path: Path):
        """solar_bess mode should create a comparison chart for the
        solar-only winners (max_production, max_yield, solar_only_npv)."""
        out = tmp_path / "comparison.png"
        result = build_winner_comparison_chart(
            _solar_bess_winners(), "solar_bess", out,
        )

        assert result == out
        assert out.exists()

    def test_empty_winners_returns_none(self, tmp_path: Path):
        """No valid winners should return None."""
        out = tmp_path / "comparison.png"
        result = build_winner_comparison_chart({}, "production", out)
        assert result is None

    def test_file_size_reasonable(self, tmp_path: Path):
        """PNG at 150 DPI should be between 10 KB and 2 MB."""
        out = tmp_path / "comparison.png"
        build_winner_comparison_chart(_npv_winners(), "npv", out)

        size_kb = out.stat().st_size / 1024
        assert 10 < size_kb < 2048

    def test_winners_with_none_energy_skipped(self, tmp_path: Path):
        """Winners where annual_energy_mwh is None should be skipped."""
        out = tmp_path / "comparison.png"
        winners = {
            "max_production": {
                "gcr": 0.50, "dcac_ratio": 1.20,
                "annual_energy_mwh": 52_000.0,
                "capacity_factor_ac": 26.5,
            },
            "max_yield": {
                "gcr": 0.30, "dcac_ratio": 1.10,
                "annual_energy_mwh": None,  # broken point
                "capacity_factor_ac": None,
            },
        }
        result = build_winner_comparison_chart(winners, "production", out)
        # Should still create a chart with the one valid winner
        assert result == out
        assert out.exists()
