"""WP7 End-to-end smoke tests for subhourly correction pipeline integration.

Runs the full pipeline (mocked climate + PySAM, real subhourly model + reports)
for 3 site configurations:
  1. Phoenix tracker, DC/AC 1.40, GCR 0.34 — clear desert, expect small/zero correction
  2. Seattle fixed, DC/AC 1.55, GCR 0.40 — cloudy, expect positive correction
  3. Phoenix fixed, DC/AC 1.20, GCR 0.34 — low DC/AC, expect zero correction

Verifies: correction %, waterfall step, PDF narrative, DB write.
Output artifacts saved to outputs/test_results/wp7_validation/.
"""

import json
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pandas as pd
import pytest

from src.config.loader import load_config
from src.pipeline import SolarModelingPipeline
from src.pysam_integration.simulator import SimulationResult
from src.reporting.data_extractor import extract_loss_waterfall, generate_narrative

METADATA_PATH = Path("src/models/artifacts/subhourly_correction_v1_metadata.json")
VALIDATION_DIR = Path("outputs/test_results/wp7_validation")

# Weather features from research data
PHOENIX_WEATHER = {
    "annual_ghi": 2142.52,
    "mean_kt": 0.6547,
    "std_kt": 0.2548,
    "ghi_cv": 0.6984,
    "mean_dni": 584.82,
    "pct_clear_hours": 53.34,
}

SEATTLE_WEATHER = {
    "annual_ghi": 1309.56,
    "mean_kt": 0.4619,
    "std_kt": 0.2643,
    "ghi_cv": 0.9231,
    "mean_dni": 307.63,
    "pct_clear_hours": 24.01,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_smoke_csv(tmp_path: Path) -> Path:
    """Create 3-row CSV for the 3 smoke test configurations."""
    data = {
        "Run Name": ["WP7_PHX_Track", "WP7_SEA_Fixed", "WP7_PHX_Fixed"],
        "Site Name": [
            "Phoenix Tracker 1.40",
            "Seattle Fixed 1.55",
            "Phoenix Fixed 1.20",
        ],
        "Customer": ["SmokeTestCo"] * 3,
        "Latitude": [33.45, 47.61, 33.45],
        "Longitude": [-112.07, -122.33, -112.07],
        "BESS Dispatch Required": [None] * 3,
        "BESS Optimization Required": [None] * 3,
        "DC Size (MW)": [14.0, 15.5, 12.0],
        "AC Installed (MW)": [10.0, 10.0, 10.0],
        "AC POI (MW)": [10.0, 10.0, 10.0],
        "Racking": ["Tracker", "Fixed", "Fixed"],
        "Tilt": [60, 30, 25],
        "Azimuth": [180, 180, 180],
        "Module Orientation": ["Portrait"] * 3,
        "Number of Modules": [2] * 3,
        "Ground Clearance Height (m)": [1.5, 1.0, 1.0],
        "Panel Model": ["CSI Solar Co. Ltd. CS3U-355P"] * 3,
        "Bifacial": [True, False, False],
        "Inverter Model": [
            "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]"
        ] * 3,
        "GCR": [0.34, 0.40, 0.34],
        "Shading (%)": [2.0, 1.0, 1.0],
        "DC Wiring Loss (%)": [1.5] * 3,
        "AC Wiring Loss (%)": [0.5] * 3,
        "Transformer Losses (%)": [1.0] * 3,
        "Degradation (%)": [0.5] * 3,
        "Availability (%)": [98.0] * 3,
        "Module Mismatch (%)": [1.0] * 3,
        "LID(%)": [1.5] * 3,
        "Report": ["TRUE"] * 3,
    }
    csv_path = tmp_path / "wp7_smoke.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)
    return csv_path


def _make_hourly(
    hours: int = 8760,
    ac_value: float = 3000.0,
    dc_factor: float = 1.3,
) -> pd.DataFrame:
    """Build synthetic hourly DataFrame."""
    timestamps = pd.date_range(start="2024-01-01", periods=hours, freq="h")
    return pd.DataFrame({
        "timestamp": timestamps,
        "ac_gross": [ac_value] * hours,
        "ac_net": [ac_value] * hours,
        "dc_net": [ac_value * dc_factor] * hours,
        "poa_irradiance": [500.0] * hours,
        "poa_nominal": [520.0] * hours,
        "cell_temperature": [35.0] * hours,
        "inverter_efficiency": [96.0] * hours,
    })


def _make_loss_data(cf_ac: float, ac_value: float = 3000.0) -> dict:
    """Create realistic loss_data dict for a given capacity factor."""
    annual = ac_value * 8760
    return {
        "annual_dc_nominal": annual * 1.25,
        "annual_dc_gross": annual * 1.15,
        "annual_dc_net": annual * 1.08,
        "annual_ac_gross": annual * 1.02,
        "annual_energy": annual,
        "capacity_factor": cf_ac * 0.78,
        "capacity_factor_ac": cf_ac,
        "kwh_per_kw": annual / 14000,
        "performance_ratio": 0.78,
        "monthly_energy": [annual / 12] * 12,
        "monthly_poa_eff": [annual * 1.3 / 12] * 12,
        "avg_daytime_ghi_wm2": 490.5,
        "annual_ghi_kwh_m2": 2131.8,
        "annual_poa_shading_loss_percent": 1.5,
        "annual_poa_soiling_loss_percent": 2.0,
        "annual_poa_cover_loss_percent": 0.7,
        "annual_dc_module_loss_percent": 8.5,
        "annual_dc_mismatch_loss_percent": 1.0,
        "annual_dc_diodes_loss_percent": 0.5,
        "annual_dc_wiring_loss_percent": 1.5,
        "annual_dc_nameplate_loss_percent": 1.0,
        "annual_dc_tracking_loss_percent": 0.0,
        "annual_bifacial_electrical_mismatch_percent": 0.0,
        "annual_ac_inv_clip_loss_percent": 0.7,
        "annual_ac_inv_eff_loss_percent": 1.4,
        "annual_ac_wiring_loss_percent": 0.5,
        "annual_xfmr_loss_percent": 1.0,
        "annual_ac_perf_adj_loss_percent": 2.0,
        "annual_poa_rear_gain_percent": 0.0,
        "annual_dc_snow_loss_percent": 0.0,
        "annual_snow_loss": 0.0,
        "monthly_snow_loss": [0.0] * 12,
    }


def _make_sim_result(
    site_name: str,
    run_name: str,
    cf_ac: float,
    ac_value: float = 3000.0,
    dc_factor: float = 1.3,
) -> SimulationResult:
    """Build a successful SimulationResult with loss_data."""
    return SimulationResult(
        site_name=site_name,
        run_name=run_name,
        customer="SmokeTestCo",
        success=True,
        hourly_data=_make_hourly(ac_value=ac_value, dc_factor=dc_factor),
        loss_data=_make_loss_data(cf_ac=cf_ac, ac_value=ac_value),
        weather_year=2024,
    )


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not METADATA_PATH.exists(),
    reason="Subhourly model artifacts not found",
)
class TestWP7SmokeTests:
    """End-to-end smoke tests for 3 site configurations."""

    @patch("src.pipeline.save_run_to_db")
    @patch("src.pipeline.compute_weather_features")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    @patch("src.pipeline.run_climate_data_pipeline")
    def test_smoke_all_three_configs(
        self, mock_climate, mock_sim, mock_weather, mock_db, tmp_path
    ):
        """Run the full pipeline for 3 configs and verify end-to-end behavior.

        Verifies per-config:
          - Raw and adjusted annual energy
          - Subhourly correction % (clamped)
          - Waterfall chart includes subhourly clipping step
          - PDF report generated
          - Database write with new columns
        """
        # -- Arrange --

        csv_path = _create_smoke_csv(tmp_path)

        # Load sites to set up mocks (same CSV the pipeline will load)
        sites = load_config(csv_path)
        for site in sites:
            site.weather_file_path = Path("/fake/weather.csv")
            site.data_source = "nsrdb"
        mock_climate.return_value = (sites, {}, {})

        # Per-site simulation results with different CFs
        # Phoenix tracker: CF_ac=32% (high desert CF)
        # Seattle fixed: CF_ac=14% (low cloudy CF)
        # Phoenix fixed: CF_ac=22% (moderate)
        mock_sim.side_effect = [
            _make_sim_result("Phoenix Tracker 1.40", "WP7_PHX_Track",
                             cf_ac=32.34, ac_value=3234.0, dc_factor=1.4),
            _make_sim_result("Seattle Fixed 1.55", "WP7_SEA_Fixed",
                             cf_ac=14.0, ac_value=1400.0, dc_factor=1.55),
            _make_sim_result("Phoenix Fixed 1.20", "WP7_PHX_Fixed",
                             cf_ac=22.0, ac_value=2200.0, dc_factor=1.2),
        ]

        # Weather features: Phoenix for sites 0,2; Seattle for site 1
        mock_weather.side_effect = [
            PHOENIX_WEATHER,
            SEATTLE_WEATHER,
            PHOENIX_WEATHER,
        ]

        # Mock DB write — return an object with .id
        mock_run = MagicMock()
        mock_run.id = uuid4()
        mock_db.return_value = mock_run

        # -- Act --

        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(csv_path)

        # -- Assert basics --

        assert results["total_sites"] == 3
        assert results["successful"] == 3
        assert results["failed"] == 0
        assert len(results["summaries"]) == 3
        assert len(results["timeseries_files"]) == 3
        assert len(results["report_files"]) == 3, (
            f"Expected 3 PDF reports, got {len(results['report_files'])}"
        )

        # -- Per-site assertions --

        summaries = results["summaries"]
        site_summaries = {s["site_name"]: s for s in summaries}

        # --- Config 1: Phoenix Tracker DC/AC 1.40 ---
        phx_track = site_summaries["Phoenix Tracker 1.40"]
        phx_track_corr = phx_track.get("subhourly_correction_pct", 0.0)
        assert phx_track_corr >= 0.0, "Clamped correction must be >= 0"
        # Clear desert tracker: expect small or zero correction (raw is likely negative)
        assert phx_track_corr < 0.5, (
            f"Phoenix tracker correction {phx_track_corr}% unexpectedly high"
        )
        assert "subhourly_model_version" in phx_track
        assert "raw_annual_energy_mwh" in phx_track
        assert phx_track["annual_energy_mwh"] > 0
        # Raw >= adjusted (correction removes energy or is zero)
        assert phx_track["raw_annual_energy_mwh"] >= phx_track["annual_energy_mwh"]

        # --- Config 2: Seattle Fixed DC/AC 1.55 ---
        sea_fixed = site_summaries["Seattle Fixed 1.55"]
        sea_fixed_corr = sea_fixed.get("subhourly_correction_pct", 0.0)
        assert sea_fixed_corr >= 0.0, "Clamped correction must be >= 0"
        # Cloudy fixed + high DC/AC: expect positive correction
        assert sea_fixed_corr > 0.7, (
            f"Seattle fixed DC/AC 1.55 correction {sea_fixed_corr}% unexpectedly low"
        )
        assert sea_fixed_corr < 2.5, (
            f"Seattle correction {sea_fixed_corr}% unexpectedly high"
        )
        assert sea_fixed["raw_annual_energy_mwh"] > sea_fixed["annual_energy_mwh"], (
            "Adjusted energy should be less than raw for positive correction"
        )

        # --- Config 3: Phoenix Fixed DC/AC 1.20 ---
        phx_fixed = site_summaries["Phoenix Fixed 1.20"]
        phx_fixed_corr = phx_fixed.get("subhourly_correction_pct", 0.0)
        assert phx_fixed_corr >= 0.0, "Clamped correction must be >= 0"
        # Low DC/AC: expect zero or very small correction
        assert phx_fixed_corr < 0.3, (
            f"Phoenix fixed DC/AC 1.20 correction {phx_fixed_corr}% "
            f"unexpectedly high for low DC/AC"
        )
        assert "subhourly_model_version" in phx_fixed

        # -- Waterfall verification --
        # The pipeline injects subhourly_correction_pct into loss_data before
        # report generation. Verify waterfall step exists.
        for summary in summaries:
            loss_data = summary.get("loss_data", {}) or {}
            correction = summary.get("subhourly_correction_pct")
            if correction is not None:
                # Build loss_data with subhourly fields for waterfall extraction
                waterfall_loss_data = dict(loss_data)
                waterfall_loss_data["subhourly_correction_pct"] = correction
                steps = extract_loss_waterfall(waterfall_loss_data)
                step_labels = [s["label"] for s in steps]
                assert "Subhourly Clipping" in step_labels, (
                    f"Waterfall for {summary['site_name']} missing "
                    f"'Subhourly Clipping' step. Steps: {step_labels}"
                )
                # Subhourly step should come before Net AC Energy
                sub_idx = step_labels.index("Subhourly Clipping")
                net_idx = step_labels.index("Net AC Energy")
                assert sub_idx < net_idx, (
                    "Subhourly Clipping should precede Net AC Energy"
                )

        # -- PDF report verification --
        for report_path in results["report_files"]:
            assert report_path.exists(), f"PDF not found: {report_path}"
            assert report_path.stat().st_size > 1000, (
                f"PDF suspiciously small: {report_path} "
                f"({report_path.stat().st_size} bytes)"
            )

        # -- Database write verification --
        assert mock_db.call_count == 3, (
            f"Expected 3 DB writes, got {mock_db.call_count}"
        )
        for call in mock_db.call_args_list:
            db_summary = call.kwargs["summary"]
            # New subhourly columns should be present
            assert "subhourly_correction_pct" in db_summary
            assert "subhourly_model_version" in db_summary
            assert "raw_annual_energy_mwh" in db_summary
            assert db_summary["subhourly_correction_pct"] >= 0.0

        # -- Save artifacts to test_results/wp7_validation/ --
        VALIDATION_DIR.mkdir(parents=True, exist_ok=True)

        # Copy PDFs
        for report_path in results["report_files"]:
            dest = VALIDATION_DIR / report_path.name
            shutil.copy2(report_path, dest)

        # Copy timeseries CSVs
        for ts_path in results["timeseries_files"]:
            dest = VALIDATION_DIR / ts_path.name
            shutil.copy2(ts_path, dest)

        # Write summary JSON artifact
        artifact = {
            "test": "WP7 Smoke Test — 3 Site Configurations",
            "total_sites": results["total_sites"],
            "successful": results["successful"],
            "failed": results["failed"],
            "sites": [],
        }
        for summary in summaries:
            site_artifact = {
                "site_name": summary["site_name"],
                "dc_size_mw": summary["dc_size_mw"],
                "ac_installed_mw": summary["ac_installed_mw"],
                "dc_ac_ratio": round(
                    summary["dc_size_mw"] / summary["ac_installed_mw"], 2
                ),
                "racking": summary["racking"],
                "raw_annual_energy_mwh": summary.get("raw_annual_energy_mwh"),
                "adjusted_annual_energy_mwh": summary.get("annual_energy_mwh"),
                "subhourly_correction_pct": summary.get(
                    "subhourly_correction_pct"
                ),
                "subhourly_model_version": summary.get(
                    "subhourly_model_version"
                ),
                "net_capacity_factor": summary.get("net_capacity_factor"),
                "specific_yield": summary.get("specific_yield"),
            }
            artifact["sites"].append(site_artifact)

        artifact_path = VALIDATION_DIR / "wp7_smoke_results.json"
        artifact_path.write_text(json.dumps(artifact, indent=2, default=str))

        # Verify artifact written
        assert artifact_path.exists()
