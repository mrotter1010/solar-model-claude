"""Tests for report generator — end-to-end PDF report orchestration."""

from pathlib import Path

import pytest

from src.reporting.report_generator import generate_report


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_loss_data(**overrides: object) -> dict:
    """Create a loss_data dict matching Phoenix inventory values."""
    data = {
        # Annual energy totals (kWh)
        "annual_dc_nominal": 35795452.0,
        "annual_dc_gross": 32400224.0,
        "annual_dc_net": 30965547.0,
        "annual_ac_gross": 30284794.0,
        "annual_energy": 28935577.0,
        # Annual loss percentages (%)
        "annual_poa_shading_loss_percent": 1.53,
        "annual_poa_soiling_loss_percent": 5.0,
        "annual_poa_cover_loss_percent": 0.68,
        "annual_dc_module_loss_percent": 9.48,
        "annual_dc_mismatch_loss_percent": 1.5,
        "annual_dc_diodes_loss_percent": 0.5,
        "annual_dc_wiring_loss_percent": 1.5,
        "annual_dc_nameplate_loss_percent": 1.0,
        "annual_dc_tracking_loss_percent": 0.0,
        "annual_bifacial_electrical_mismatch_percent": 0.17,
        "annual_ac_inv_clip_loss_percent": 0.69,
        "annual_ac_inv_eff_loss_percent": 1.41,
        "annual_ac_wiring_loss_percent": 1.5,
        "annual_xfmr_loss_percent": 0.0,
        "annual_ac_perf_adj_loss_percent": 3.0,
        "annual_poa_rear_gain_percent": 3.32,
        # Key scalars
        "capacity_factor": 25.41,
        "capacity_factor_ac": 32.34,
        "kwh_per_kw": 2225.81,
        "performance_ratio": 0.7676,
        # Monthly arrays (12-element lists, approximate kWh)
        "monthly_energy": [
            1800000.0, 2000000.0, 2400000.0, 2600000.0,
            2900000.0, 3100000.0, 3200000.0, 3000000.0,
            2700000.0, 2400000.0, 1900000.0, 1800000.0,
        ],
        "monthly_poa_eff": [
            8800000.0, 10100000.0, 15100000.0, 20500000.0,
            23000000.0, 20900000.0, 21200000.0, 18900000.0,
            18200000.0, 15100000.0, 11200000.0, 9800000.0,
        ],
        # GHI metrics
        "avg_daytime_ghi_wm2": 490.5,
        "annual_ghi_kwh_m2": 2131.8,
    }
    data.update(overrides)
    return data


def _make_site_config(**overrides: object) -> dict:
    """Create a site_config dict matching Phoenix test site."""
    config = {
        "site_name": "SiteTest_Phoenix",
        "customer": "TestCustomer",
        "latitude": 33.483,
        "longitude": -112.073,
        "dc_size_mw": 13.0,
        "ac_installed_mw": 10.0,
        "ac_poi_mw": 10.0,
        "racking": "tracker",
        "tilt": 60.0,
        "azimuth": 180.0,
        "gcr": 0.34,
        "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
        "number_of_modules": 2,
        "bifacial": True,
        "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
    }
    config.update(overrides)
    return config


# ---------------------------------------------------------------------------
# generate_report
# ---------------------------------------------------------------------------


class TestGenerateReport:
    """Tests for end-to-end report generation."""

    def test_produces_pdf_with_valid_data(self, tmp_path: Path) -> None:
        """generate_report creates a PDF file when given valid inputs."""
        result = generate_report(
            site_config=_make_site_config(),
            loss_data=_make_loss_data(),
            output_dir=tmp_path,
        )

        assert result is not None
        assert result.exists()
        assert result.stat().st_size > 0
        assert result.suffix == ".pdf"

    def test_pdf_filename_contains_site_name(self, tmp_path: Path) -> None:
        """Output filename includes the site name."""
        result = generate_report(
            site_config=_make_site_config(),
            loss_data=_make_loss_data(),
            output_dir=tmp_path,
        )

        assert result is not None
        assert "SiteTest_Phoenix" in result.name

    def test_returns_none_on_empty_loss_data(self, tmp_path: Path) -> None:
        """Returns None when loss_data is empty (no monthly_energy)."""
        result = generate_report(
            site_config=_make_site_config(),
            loss_data={},
            output_dir=tmp_path,
        )

        assert result is None

    def test_returns_none_on_missing_monthly_energy(self, tmp_path: Path) -> None:
        """Returns None when monthly_energy is missing from loss_data."""
        loss_data = _make_loss_data()
        del loss_data["monthly_energy"]

        result = generate_report(
            site_config=_make_site_config(),
            loss_data=loss_data,
            output_dir=tmp_path,
        )

        assert result is None

    def test_persists_chart_pngs_in_figures_dir(self, tmp_path: Path) -> None:
        """Chart PNGs are saved to a sibling figures/ directory."""
        # output_dir is treated as reports dir; figures go to parent/figures
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()

        result = generate_report(
            site_config=_make_site_config(),
            loss_data=_make_loss_data(),
            output_dir=reports_dir,
        )

        assert result is not None
        figures_dir = tmp_path / "figures"
        assert figures_dir.exists()
        png_files = list(figures_dir.glob("*.png"))
        assert len(png_files) >= 2  # monthly_production.png + loss_waterfall.png


class TestSampleReportArtifact:
    """Generate a sample PDF report for manual review."""

    def test_generate_sample_report(self) -> None:
        """Save a sample PDF to outputs/test_results/ for manual inspection."""
        output_dir = Path("outputs/test_results")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Remove previous sample if present
        sample_path = output_dir / "SiteTest_Phoenix_report.pdf"
        if sample_path.exists():
            sample_path.unlink()

        result = generate_report(
            site_config=_make_site_config(),
            loss_data=_make_loss_data(),
            output_dir=output_dir,
        )

        assert result is not None
        assert result.exists()
        assert result.stat().st_size > 0
