"""Tests for src/optimization/solar_optimizer.py helpers.

Covers:
- build_sweep_grid: default tracker/fixed grids, custom ranges, rounding,
  dedup, and validation.
- _run_single_pysam: mocked ModelConfigurator + PySAMSimulator with and
  without the subhourly clipping correction.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.config.loader import load_config
from src.optimization.solar_optimizer import (
    _run_single_pysam,
    build_sweep_grid,
)
from src.pysam_integration.simulator import SimulationResult


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SINGLE_ROW_CSV = FIXTURES_DIR / "single_row_test.csv"


def _make_site_config():
    """Load the single-row test fixture and return the SiteConfig."""
    sites = load_config(SINGLE_ROW_CSV)
    return sites[0]


def _make_hourly_data(ac_value: float = 1000.0, dc_value: float = 1200.0):
    """Build synthetic 8760-hour DataFrame with AC and DC columns."""
    timestamps = pd.date_range(start="2024-01-01", periods=8760, freq="h")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "ac_gross": [ac_value] * 8760,
            "ac_net": [ac_value] * 8760,
            "dc_net": [dc_value] * 8760,
            "poa_irradiance": [500.0] * 8760,
            "poa_nominal": [520.0] * 8760,
        }
    )


def _make_loss_data() -> dict:
    """Minimal loss_data dict with the fields _run_single_pysam needs."""
    return {
        "annual_energy": 28_935_577.0,
        "capacity_factor_ac": 32.34,
    }


def _make_simulation_result(site):
    """Build a fake successful SimulationResult."""
    return SimulationResult(
        site_name=site.site_name,
        run_name=site.run_name,
        customer=site.customer,
        success=True,
        hourly_data=_make_hourly_data(),
        loss_data=_make_loss_data(),
        weather_year="2024",
    )


# ---------------------------------------------------------------------------
# build_sweep_grid tests
# ---------------------------------------------------------------------------


class TestBuildSweepGridDefaults:
    """Default grid shape for tracker and fixed racking."""

    def test_tracker_default_grid_exact_values(self) -> None:
        """Tracker default: 7 GCR (0.30-0.60) × 10 DCAC (1.15-1.60) = 70."""
        # Act
        grid = build_sweep_grid("tracker")

        # Assert — size
        assert len(grid) == 70

        # Assert — unique GCRs and DCACs
        gcrs = sorted({pair[0] for pair in grid})
        dcacs = sorted({pair[1] for pair in grid})
        assert gcrs == [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60]
        assert dcacs == [
            1.15, 1.20, 1.25, 1.30, 1.35, 1.40, 1.45, 1.50, 1.55, 1.60,
        ]

    def test_fixed_default_grid_exact_values(self) -> None:
        """Fixed default: 7 GCR (0.40-0.70) × 10 DCAC (1.15-1.60) = 70."""
        # Act
        grid = build_sweep_grid("fixed")

        # Assert — size
        assert len(grid) == 70

        # Assert — unique GCRs and DCACs
        gcrs = sorted({pair[0] for pair in grid})
        dcacs = sorted({pair[1] for pair in grid})
        assert gcrs == [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]
        assert dcacs == [
            1.15, 1.20, 1.25, 1.30, 1.35, 1.40, 1.45, 1.50, 1.55, 1.60,
        ]


class TestBuildSweepGridCustomRanges:
    """Custom ranges and step sizes are respected."""

    def test_custom_gcr_range(self) -> None:
        """Custom gcr_range overrides the racking default."""
        # Act — narrow GCR range, default DCAC
        grid = build_sweep_grid("tracker", gcr_range=(0.35, 0.45), gcr_step=0.05)

        # Assert — 3 GCRs × 10 DCACs = 30
        gcrs = sorted({pair[0] for pair in grid})
        assert gcrs == [0.35, 0.40, 0.45]
        assert len(grid) == 30

    def test_custom_dcac_range_and_step(self) -> None:
        """Custom dcac_range and step produce the expected DCAC values."""
        # Act — default GCRs, custom DCAC 1.20-1.40 step 0.10
        grid = build_sweep_grid(
            "tracker", dcac_range=(1.20, 1.40), dcac_step=0.10
        )

        # Assert — DCAC values are 1.20, 1.30, 1.40
        dcacs = sorted({pair[1] for pair in grid})
        assert dcacs == [1.20, 1.30, 1.40]

    def test_single_point_ranges(self) -> None:
        """Range of (x, x) with any step yields exactly one value."""
        # Act — degenerate single-point ranges
        grid = build_sweep_grid(
            "tracker",
            gcr_range=(0.40, 0.40),
            gcr_step=0.05,
            dcac_range=(1.30, 1.30),
            dcac_step=0.05,
        )

        # Assert — exactly one (gcr, dcac) pair
        assert grid == [(0.40, 1.30)]


class TestBuildSweepGridRoundingAndDedup:
    """All values rounded to 2 decimals; no duplicate pairs."""

    def test_all_values_rounded_to_two_decimals(self) -> None:
        """Every value in the grid has at most 2 decimal places."""
        # Act — default tracker grid
        grid = build_sweep_grid("tracker")

        # Assert — each value equals round(value, 2)
        for gcr, dcac in grid:
            assert gcr == round(gcr, 2), f"GCR {gcr} not rounded to 2 decimals"
            assert dcac == round(dcac, 2), f"DCAC {dcac} not rounded to 2 decimals"

    def test_no_duplicate_pairs(self) -> None:
        """No (gcr, dcac) pair appears more than once."""
        # Act
        grid = build_sweep_grid("tracker")

        # Assert — count of pairs equals count of unique pairs
        assert len(grid) == len(set(grid))

    def test_tracker_first_five_tuples(self) -> None:
        """First 5 tuples match expected ordering (gcr outer, dcac inner)."""
        # Act
        grid = build_sweep_grid("tracker")

        # Assert — GCR 0.30 paired with first 5 DCACs
        assert grid[:5] == [
            (0.30, 1.15),
            (0.30, 1.20),
            (0.30, 1.25),
            (0.30, 1.30),
            (0.30, 1.35),
        ]


class TestBuildSweepGridValidation:
    """Invalid inputs raise ValueError."""

    def test_invalid_racking_raises(self) -> None:
        """Unknown racking string raises ValueError."""
        with pytest.raises(ValueError, match="Invalid racking"):
            build_sweep_grid("bifacial")  # not a supported value

    def test_zero_gcr_step_raises(self) -> None:
        """gcr_step == 0 raises ValueError."""
        with pytest.raises(ValueError, match="gcr_step must be > 0"):
            build_sweep_grid("tracker", gcr_step=0.0)

    def test_negative_gcr_step_raises(self) -> None:
        """gcr_step < 0 raises ValueError."""
        with pytest.raises(ValueError, match="gcr_step must be > 0"):
            build_sweep_grid("tracker", gcr_step=-0.05)

    def test_zero_dcac_step_raises(self) -> None:
        """dcac_step == 0 raises ValueError."""
        with pytest.raises(ValueError, match="dcac_step must be > 0"):
            build_sweep_grid("tracker", dcac_step=0.0)

    def test_inverted_gcr_range_raises(self) -> None:
        """gcr_range with min > max raises ValueError."""
        with pytest.raises(ValueError, match="gcr_range inverted"):
            build_sweep_grid("tracker", gcr_range=(0.60, 0.30))

    def test_inverted_dcac_range_raises(self) -> None:
        """dcac_range with min > max raises ValueError."""
        with pytest.raises(ValueError, match="dcac_range inverted"):
            build_sweep_grid("tracker", dcac_range=(1.60, 1.15))


# ---------------------------------------------------------------------------
# _run_single_pysam tests (mocked, no real PySAM)
# ---------------------------------------------------------------------------


class TestRunSinglePysam:
    """Mocked PySAM execution path for _run_single_pysam."""

    def test_returned_dict_shape_and_values(self) -> None:
        """_run_single_pysam returns the documented 4-key dict."""
        # Arrange
        site = _make_site_config()
        fake_result = _make_simulation_result(site)

        mock_configurator = MagicMock()
        mock_configurator.configure_model.return_value = "fake-model-config"
        mock_simulator = MagicMock()
        mock_simulator.execute_simulation.return_value = fake_result

        # Act — disable correction so no mocking of the correction fn needed
        out = _run_single_pysam(
            site_config=site,
            configurator=mock_configurator,
            simulator=mock_simulator,
            apply_clipping_correction=False,
        )

        # Assert — configurator and simulator were called correctly
        mock_configurator.configure_model.assert_called_once_with(site)
        mock_simulator.execute_simulation.assert_called_once_with(
            "fake-model-config"
        )

        # Assert — dict has exactly the four documented keys
        assert set(out.keys()) == {
            "annual_energy_kwh",
            "capacity_factor_ac",
            "clipping_correction_pct",
            "ac_hourly_kw",
        }

        # Assert — values match loss_data and hourly_data
        assert out["annual_energy_kwh"] == pytest.approx(28_935_577.0)
        assert out["capacity_factor_ac"] == pytest.approx(32.34)
        assert out["clipping_correction_pct"] == 0.0
        assert isinstance(out["ac_hourly_kw"], list)
        assert len(out["ac_hourly_kw"]) == 8760
        assert out["ac_hourly_kw"][0] == pytest.approx(1000.0)

    def test_correction_disabled_skips_helper(self) -> None:
        """apply_clipping_correction=False means the helper is never called."""
        # Arrange
        site = _make_site_config()
        fake_result = _make_simulation_result(site)

        mock_configurator = MagicMock()
        mock_configurator.configure_model.return_value = "fake-model-config"
        mock_simulator = MagicMock()
        mock_simulator.execute_simulation.return_value = fake_result

        # Act — patch the shared helper at its import site in solar_optimizer
        with patch(
            "src.optimization.solar_optimizer.apply_subhourly_clipping_correction"
        ) as mock_helper:
            out = _run_single_pysam(
                site_config=site,
                configurator=mock_configurator,
                simulator=mock_simulator,
                apply_clipping_correction=False,
            )

        # Assert — helper never invoked, correction_pct is 0.0
        mock_helper.assert_not_called()
        assert out["clipping_correction_pct"] == 0.0

    def test_correction_enabled_invokes_helper_with_correct_args(self) -> None:
        """apply_clipping_correction=True calls the helper with (result, site)."""
        # Arrange
        site = _make_site_config()
        fake_result = _make_simulation_result(site)

        mock_configurator = MagicMock()
        mock_configurator.configure_model.return_value = "fake-model-config"
        mock_simulator = MagicMock()
        mock_simulator.execute_simulation.return_value = fake_result

        # Act — patch the helper to return a known correction metadata dict
        with patch(
            "src.optimization.solar_optimizer.apply_subhourly_clipping_correction"
        ) as mock_helper:
            mock_helper.return_value = {
                "subhourly_correction_pct": 0.42,
                "subhourly_model_version": "v2",
                "raw_annual_energy_mwh": 28935.577,
            }

            out = _run_single_pysam(
                site_config=site,
                configurator=mock_configurator,
                simulator=mock_simulator,
                apply_clipping_correction=True,
            )

        # Assert — helper called exactly once with the result and the site
        mock_helper.assert_called_once_with(fake_result, site)

        # Assert — the correction pct flows into the returned dict
        assert out["clipping_correction_pct"] == pytest.approx(0.42)

    def test_correction_enabled_empty_metadata_yields_zero_pct(self) -> None:
        """If the helper returns {} (skipped / failed), clipping pct is 0.0."""
        # Arrange
        site = _make_site_config()
        fake_result = _make_simulation_result(site)

        mock_configurator = MagicMock()
        mock_configurator.configure_model.return_value = "fake-model-config"
        mock_simulator = MagicMock()
        mock_simulator.execute_simulation.return_value = fake_result

        # Act — helper returns empty dict (simulates skip path)
        with patch(
            "src.optimization.solar_optimizer.apply_subhourly_clipping_correction",
            return_value={},
        ):
            out = _run_single_pysam(
                site_config=site,
                configurator=mock_configurator,
                simulator=mock_simulator,
                apply_clipping_correction=True,
            )

        # Assert — defaults to 0.0 when metadata has no key
        assert out["clipping_correction_pct"] == 0.0
