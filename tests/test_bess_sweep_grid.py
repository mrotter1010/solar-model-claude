"""Tests for BESS sizing sweep grid generation."""

import json
from pathlib import Path

import pytest

from src.bess.sizing_optimizer import (
    DURATION_STEP_HR,
    POWER_STEP_MW,
    build_sweep_grid,
)
from src.config.schema import SiteConfig

# Output directory for manual inspection
_OUTPUT_DIR = Path("outputs/test_results/m14c_sweep")
_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helper: build minimal SiteConfig for sweep grid tests
# ---------------------------------------------------------------------------


def _base_site_kwargs() -> dict:
    """Return minimal valid SiteConfig kwargs (no BESS/billing)."""
    return {
        "run_name": "SweepTest",
        "site_name": "SweepTestSite",
        "customer": "TestCustomer",
        "latitude": 33.45,
        "longitude": -112.07,
        "dc_size_mw": 6.25,
        "ac_installed_mw": 5.0,
        "ac_poi_mw": 5.0,
        "racking": "tracker",
        "tilt": 55.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
        "bifacial": False,
        "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "gcr": 0.35,
        "shading_percent": 0.0,
        "dc_wiring_loss_percent": 2.0,
        "ac_wiring_loss_percent": 1.0,
        "transformer_losses_percent": 1.0,
        "degradation_percent": 0.5,
        "availability_percent": 2.5,
        "module_mismatch_percent": 1.0,
        "lid_percent": 1.5,
    }


def _sweep_site(**overrides: object) -> SiteConfig:
    """Build a SiteConfig for sweep grid tests.

    By default creates a plain site without BESS dispatch/optimization
    enabled, so sizing fields can be set freely without validator
    constraints (e.g., min == max).
    """
    kw = _base_site_kwargs()
    kw.update(overrides)
    return SiteConfig(**kw)


def _optimization_site(**overrides: object) -> SiteConfig:
    """Build a SiteConfig with full optimization fields enabled.

    Enables bess_dispatch_required, bess_optimization_required, and
    bill_calculation with all required supporting fields.
    """
    kw = _base_site_kwargs()
    kw.update({
        "bess_dispatch_required": True,
        "bess_optimization_required": True,
        "bill_calculation": True,
        "utility_name": "Test Utility",
        "tariff_name": "Test Rate",
        "load_type": "MediumOffice",
        "annual_consumption_kwh": 500000.0,
        "solar_cost_per_kw_dc": 1200.0,
        "solar_cost_per_kw_ac": 200.0,
        "bess_opex_per_kw_year": 10.0,
    })
    kw.update(overrides)
    return SiteConfig(**kw)


# ===========================================================================
# Test cases
# ===========================================================================


class TestBuildSweepGrid:
    """Tests for build_sweep_grid() sweep grid generation."""

    def test_default_solar_bess_5mw(self) -> None:
        """(a) Default solar+BESS: 5 MW AC, no explicit power bounds.

        Power range: 10%-30% of 5.0 = 0.5–1.5 MW → 11 points (0.1 step).
        Duration range: 2.0–5.0 hr → 7 points (0.5 step).
        Total: 11 × 7 = 77 combos.
        """
        site = _optimization_site(ac_installed_mw=5.0, dc_size_mw=6.25, ac_poi_mw=5.0)
        grid = build_sweep_grid(site)

        power_values = sorted({p for p, _ in grid})
        duration_values = sorted({d for _, d in grid})

        assert len(power_values) == 11
        assert power_values[0] == pytest.approx(0.5)
        assert power_values[-1] == pytest.approx(1.5)
        assert len(duration_values) == 7
        assert duration_values[0] == pytest.approx(2.0)
        assert duration_values[-1] == pytest.approx(5.0)
        assert len(grid) == 77

    def test_custom_power_range(self) -> None:
        """(b) Custom power range: 1.0–2.0 MW → 11 power points."""
        site = _optimization_site(
            bess_power_min_mw=1.0,
            bess_power_max_mw=2.0,
        )
        grid = build_sweep_grid(site)

        power_values = sorted({p for p, _ in grid})
        assert len(power_values) == 11
        assert power_values[0] == pytest.approx(1.0)
        assert power_values[-1] == pytest.approx(2.0)

    def test_custom_duration_range(self) -> None:
        """(c) Custom duration range: 3.0–4.0 hr → 3 duration points."""
        site = _optimization_site(
            bess_duration_min_hr=3.0,
            bess_duration_max_hr=4.0,
        )
        grid = build_sweep_grid(site)

        duration_values = sorted({d for _, d in grid})
        assert len(duration_values) == 3
        assert duration_values == [pytest.approx(3.0), pytest.approx(3.5), pytest.approx(4.0)]

    def test_single_power_value(self) -> None:
        """(d) Single power value: min=max=1.0 → 1 power point.

        Uses non-optimization SiteConfig since the validator rejects
        min >= max when optimization is required.
        """
        site = _sweep_site(
            bess_power_min_mw=1.0,
            bess_power_max_mw=1.0,
        )
        grid = build_sweep_grid(site)

        power_values = sorted({p for p, _ in grid})
        duration_values = sorted({d for _, d in grid})
        assert len(power_values) == 1
        assert power_values[0] == pytest.approx(1.0)
        # Default duration: 2.0–5.0 → 7 points
        assert len(duration_values) == 7
        assert len(grid) == 7

    def test_single_duration_value(self) -> None:
        """(e) Single duration value: min=max=4.0 → 1 duration point.

        Uses non-optimization SiteConfig since the validator rejects
        min >= max when optimization is required.
        """
        site = _sweep_site(
            bess_duration_min_hr=4.0,
            bess_duration_max_hr=4.0,
        )
        grid = build_sweep_grid(site)

        power_values = sorted({p for p, _ in grid})
        duration_values = sorted({d for _, d in grid})
        assert len(duration_values) == 1
        assert duration_values[0] == pytest.approx(4.0)
        # Default power: 10%-30% of 5.0 MW = 0.5–1.5 → 11 points
        assert len(power_values) == 11
        assert len(grid) == 11

    def test_grid_only_explicit_bounds(self) -> None:
        """(f) Grid-only with explicit bounds: 0.5–2.0 MW.

        Power: (2.0-0.5)/0.1 + 1 = 16 points.
        Duration: default 2.0–5.0 → 7 points.
        Total: 16 × 7 = 112 combos.
        """
        site = _optimization_site(
            bess_grid_only_charging=True,
            bess_solar_only_charging=False,
            solar_cost_per_kw_dc=None,
            solar_cost_per_kw_ac=None,
            bess_power_min_mw=0.5,
            bess_power_max_mw=2.0,
        )
        grid = build_sweep_grid(site)

        power_values = sorted({p for p, _ in grid})
        duration_values = sorted({d for _, d in grid})
        assert len(power_values) == 16
        assert power_values[0] == pytest.approx(0.5)
        assert power_values[-1] == pytest.approx(2.0)
        assert len(duration_values) == 7
        assert len(grid) == 112

    def test_small_ac_site_1mw(self) -> None:
        """(g) Small AC site (1 MW): default power range 0.1–0.3 MW → 3 points."""
        site = _sweep_site(
            ac_installed_mw=1.0,
            dc_size_mw=1.25,
            ac_poi_mw=1.0,
        )
        grid = build_sweep_grid(site)

        power_values = sorted({p for p, _ in grid})
        assert len(power_values) == 3
        assert power_values == [pytest.approx(0.1), pytest.approx(0.2), pytest.approx(0.3)]

    def test_large_ac_site_50mw(self) -> None:
        """(h) Large AC site (50 MW): default power range 5.0–15.0 MW.

        Range span = 10 MW → adaptive step = 0.5 MW → 21 power points.
        Duration: default 2.0–5.0 → 7 points.
        Total: 21 × 7 = 147 combos.
        """
        site = _sweep_site(
            ac_installed_mw=50.0,
            dc_size_mw=62.5,
            ac_poi_mw=50.0,
        )
        grid = build_sweep_grid(site)

        power_values = sorted({p for p, _ in grid})
        duration_values = sorted({d for _, d in grid})
        assert len(power_values) == 21
        assert power_values[0] == pytest.approx(5.0)
        assert power_values[-1] == pytest.approx(15.0)
        assert len(duration_values) == 7
        assert len(grid) == 147

    def test_very_large_ac_site_200mw(self) -> None:
        """Very large AC site (200 MW): default power range 20.0–60.0 MW.

        Range span = 40 MW → adaptive step = 1.0 MW → 41 power points.
        Duration: default 2.0–5.0 → 7 points.
        Total: 41 × 7 = 287 combos.
        """
        site = _sweep_site(
            ac_installed_mw=200.0,
            dc_size_mw=250.0,
            ac_poi_mw=200.0,
        )
        grid = build_sweep_grid(site)

        power_values = sorted({p for p, _ in grid})
        duration_values = sorted({d for _, d in grid})
        assert len(power_values) == 41
        assert power_values[0] == pytest.approx(20.0)
        assert power_values[-1] == pytest.approx(60.0)
        assert len(duration_values) == 7
        assert len(grid) == 287

    def test_all_positive_values(self) -> None:
        """(i) Verify all tuples have power > 0 and duration > 0."""
        site = _optimization_site(ac_installed_mw=5.0, dc_size_mw=6.25, ac_poi_mw=5.0)
        grid = build_sweep_grid(site)

        for power, duration in grid:
            assert power > 0, f"Power must be > 0, got {power}"
            assert duration > 0, f"Duration must be > 0, got {duration}"

    def test_no_duplicate_tuples(self) -> None:
        """(j) Verify no duplicate tuples in output."""
        site = _optimization_site(ac_installed_mw=5.0, dc_size_mw=6.25, ac_poi_mw=5.0)
        grid = build_sweep_grid(site)

        assert len(grid) == len(set(grid)), (
            f"Duplicate tuples found: {len(grid)} total, {len(set(grid))} unique"
        )

    def test_write_grid_dimensions(self) -> None:
        """(k) Write grid dimensions to test_results for inspection."""
        test_cases = {
            "default_5mw": _optimization_site(
                ac_installed_mw=5.0, dc_size_mw=6.25, ac_poi_mw=5.0,
            ),
            "custom_power_1_2mw": _optimization_site(
                bess_power_min_mw=1.0, bess_power_max_mw=2.0,
            ),
            "custom_duration_3_4hr": _optimization_site(
                bess_duration_min_hr=3.0, bess_duration_max_hr=4.0,
            ),
            "single_power_1mw": _sweep_site(
                bess_power_min_mw=1.0, bess_power_max_mw=1.0,
            ),
            "single_duration_4hr": _sweep_site(
                bess_duration_min_hr=4.0, bess_duration_max_hr=4.0,
            ),
            "grid_only_0.5_2mw": _optimization_site(
                bess_grid_only_charging=True,
                bess_solar_only_charging=False,
                solar_cost_per_kw_dc=None,
                solar_cost_per_kw_ac=None,
                bess_power_min_mw=0.5,
                bess_power_max_mw=2.0,
            ),
            "small_1mw": _sweep_site(
                ac_installed_mw=1.0, dc_size_mw=1.25, ac_poi_mw=1.0,
            ),
            "large_50mw": _sweep_site(
                ac_installed_mw=50.0, dc_size_mw=62.5, ac_poi_mw=50.0,
            ),
            "very_large_200mw": _sweep_site(
                ac_installed_mw=200.0, dc_size_mw=250.0, ac_poi_mw=200.0,
            ),
        }

        results = {}
        for name, site in test_cases.items():
            grid = build_sweep_grid(site)
            power_values = sorted({p for p, _ in grid})
            duration_values = sorted({d for _, d in grid})
            results[name] = {
                "power_points": len(power_values),
                "power_range": [power_values[0], power_values[-1]],
                "duration_points": len(duration_values),
                "duration_range": [duration_values[0], duration_values[-1]],
                "total_combos": len(grid),
            }

        output = {
            "step_sizes": {
                "power_step_mw": POWER_STEP_MW,
                "duration_step_hr": DURATION_STEP_HR,
            },
            "cases": results,
        }
        (_OUTPUT_DIR / "sweep_grid_dimensions.json").write_text(
            json.dumps(output, indent=2)
        )

        # Sanity check: output was written
        assert (_OUTPUT_DIR / "sweep_grid_dimensions.json").exists()
