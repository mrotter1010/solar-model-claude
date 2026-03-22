"""Tests for BESS report section: chart generation and PDF page integration."""

import math
import tempfile
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pytest

from src.bess.report_section import (
    generate_dispatch_profile_chart,
    generate_heatmap_chart,
)
from src.reporting.pdf_builder import build_pdf

RESULTS_DIR = Path("outputs/test_results/bess_report")


@pytest.fixture(scope="module")
def results_dir() -> Path:
    """Ensure test results directory exists."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    return RESULTS_DIR


def _make_synthetic_heatmap() -> list[list[float]]:
    """Create a realistic 12x24 heatmap with seasonal/hourly patterns.

    Summer months have stronger charge (solar hours) and discharge (peak hours).
    Winter months are weaker. Night hours are near zero.
    """
    heatmap: list[list[float]] = []
    for month in range(12):
        row: list[float] = []
        summer_factor = 1.0 + 0.5 * math.sin((month - 3) * math.pi / 6)
        for hour in range(24):
            if 10 <= hour <= 15:
                val = -200 * summer_factor * math.sin(
                    (hour - 10) * math.pi / 5
                )
            elif 16 <= hour <= 21:
                val = 300 * summer_factor * math.sin(
                    (hour - 16) * math.pi / 5
                )
            else:
                val = 0.0
            row.append(round(val, 1))
        heatmap.append(row)
    return heatmap


def _make_synthetic_dispatch_profile() -> (
    tuple[list[float], list[float], list[float]]
):
    """Create realistic 24-hour load, solar, and battery profiles."""
    load: list[float] = []
    solar: list[float] = []
    battery: list[float] = []
    for h in range(24):
        # Commercial load: low at night, high during business hours
        if 6 <= h <= 20:
            load.append(800 + 400 * math.sin((h - 6) * math.pi / 14))
        else:
            load.append(300)

        # Solar: bell curve peaking around noon
        if 6 <= h <= 18:
            solar.append(600 * math.sin((h - 6) * math.pi / 12))
        else:
            solar.append(0)

        # Battery: charge during solar peak, discharge during evening peak
        if 10 <= h <= 14:
            battery.append(-250 * math.sin((h - 10) * math.pi / 4))
        elif 17 <= h <= 21:
            battery.append(200 * math.sin((h - 17) * math.pi / 4))
        else:
            battery.append(0)

    return load, solar, battery


def _make_bess_dispatch_dict() -> dict:
    """Build a realistic bess_dispatch summary dict for testing."""
    return {
        "bess_power_mw": 5.0,
        "bess_duration_hr": 4,
        "bess_capacity_kwh": 20000,
        "bess_strategy": "peak_shaving",
        "solar_only_annual_bill": 285000.0,
        "solar_plus_bess_annual_bill": 242000.0,
        "bess_incremental_savings": 43000.0,
        "bess_demand_savings": 35000.0,
        "bess_energy_savings": 8000.0,
        "annual_cycles": 312.5,
        "annual_throughput_kwh": 5000000.0,
        "average_daily_cycles": 0.86,
        "capacity_utilization_pct": 85.6,
        "total_curtailed_kwh": 1250.0,
        "estimated_annual_degradation_pct": 6.25,
        "monthly_solver_status": ["Optimal"] * 12,
        "heatmap_data": _make_synthetic_heatmap(),
    }


def _make_dummy_chart(path: Path) -> Path:
    """Create a minimal PNG chart file for testing build_pdf."""
    fig, ax = plt.subplots(figsize=(4, 2))
    ax.plot([0, 1], [0, 1])
    ax.set_title("Dummy Chart")
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(path), dpi=72)
    plt.close(fig)
    return path


# --- Heatmap chart tests ---


class TestHeatmapChart:
    """Tests for the 12x24 heatmap chart generation."""

    def test_generates_png(self, results_dir: Path) -> None:
        """Heatmap chart should be created as a valid PNG."""
        heatmap = _make_synthetic_heatmap()
        output = results_dir / "bess_heatmap.png"
        result = generate_heatmap_chart(heatmap, output)
        assert result == output
        assert output.exists()
        assert output.stat().st_size > 1000  # non-trivial PNG

    def test_all_zero_data(self, results_dir: Path) -> None:
        """Heatmap should handle all-zero data without error."""
        heatmap = [[0.0] * 24 for _ in range(12)]
        output = results_dir / "bess_heatmap_zeros.png"
        result = generate_heatmap_chart(heatmap, output)
        assert result == output
        assert output.exists()

    def test_correct_dimensions(self) -> None:
        """Verify heatmap data dimensions are 12x24."""
        heatmap = _make_synthetic_heatmap()
        assert len(heatmap) == 12
        for row in heatmap:
            assert len(row) == 24


# --- Dispatch profile chart tests ---


class TestDispatchProfileChart:
    """Tests for the 24-hour dispatch profile chart generation."""

    def test_generates_png(self, results_dir: Path) -> None:
        """Dispatch profile chart should be created as a valid PNG."""
        load, solar, battery = _make_synthetic_dispatch_profile()
        output = results_dir / "bess_dispatch_profile_july.png"
        result = generate_dispatch_profile_chart(
            load_kwh=load,
            solar_kwh=solar,
            battery_kw=battery,
            month_name="July",
            output_path=output,
        )
        assert result == output
        assert output.exists()
        assert output.stat().st_size > 1000

    def test_winter_month(self, results_dir: Path) -> None:
        """Chart should work with different month names."""
        load, solar, battery = _make_synthetic_dispatch_profile()
        # Scale down solar for winter
        solar = [s * 0.5 for s in solar]
        output = results_dir / "bess_dispatch_profile_december.png"
        result = generate_dispatch_profile_chart(
            load_kwh=load,
            solar_kwh=solar,
            battery_kw=battery,
            month_name="December",
            output_path=output,
        )
        assert result == output
        assert output.exists()

    def test_no_battery_activity(self, results_dir: Path) -> None:
        """Chart should handle zero battery dispatch without error."""
        load = [500.0] * 24
        solar = [0.0] * 24
        battery = [0.0] * 24
        output = results_dir / "bess_dispatch_profile_idle.png"
        result = generate_dispatch_profile_chart(
            load_kwh=load,
            solar_kwh=solar,
            battery_kw=battery,
            month_name="January",
            output_path=output,
        )
        assert result == output
        assert output.exists()


# --- PDF page integration tests ---


class TestBessPageIntegration:
    """Tests for BESS page integration into build_pdf."""

    def test_page_generates_without_error(self, results_dir: Path) -> None:
        """Full PDF with BESS page should build without error.

        Creates dummy base charts and real BESS charts, then calls build_pdf
        with all params including bess_dispatch.
        """
        with tempfile.TemporaryDirectory(prefix="bess_pdf_") as tmp:
            tmp_path = Path(tmp)

            # Generate required base chart dummies
            monthly_chart = _make_dummy_chart(tmp_path / "monthly.png")
            waterfall_chart = _make_dummy_chart(tmp_path / "waterfall.png")

            # Generate real BESS charts
            heatmap_data = _make_synthetic_heatmap()
            heatmap_path = tmp_path / "bess_heatmap.png"
            generate_heatmap_chart(heatmap_data, heatmap_path)

            load, solar, battery = _make_synthetic_dispatch_profile()
            dispatch_path = tmp_path / "bess_dispatch.png"
            generate_dispatch_profile_chart(
                load, solar, battery, "July", dispatch_path,
            )

            # Minimal site summary
            site_summary = {
                "site_name": "Test BESS Site",
                "customer": "Test Customer",
                "state": "AZ",
                "latitude": 33.45,
                "longitude": -112.07,
                "dc_capacity_mw": 13.0,
                "ac_capacity_mw": 10.0,
                "dc_ac_ratio": 1.3,
                "racking": "tracker",
                "tilt": 0,
                "azimuth": 180,
                "gcr": 0.35,
                "module_model": "Test Module",
                "inverter_model": "Test Inverter",
                "bifacial": False,
                "annual_energy_mwh": 25000.0,
                "capacity_factor_pct": 25.4,
                "specific_yield_kwh_kwp": 1750.0,
                "performance_ratio_pct": 82.5,
                "annual_ghi_kwh_m2": 2100.0,
                "weather_year": "tmy",
            }

            narrative = {
                "site_overview": "Test site overview.",
                "solar_resource": "Test solar resource.",
                "production_summary": "Test production summary.",
                "loss_summary": "Test loss analysis.",
            }

            output_pdf = results_dir / "bess_full_report.pdf"
            result = build_pdf(
                output_path=output_pdf,
                site_summary=site_summary,
                narrative=narrative,
                monthly_chart_path=monthly_chart,
                waterfall_chart_path=waterfall_chart,
                bess_dispatch=_make_bess_dispatch_dict(),
                bess_heatmap_path=heatmap_path,
                bess_dispatch_chart_path=dispatch_path,
            )

            assert result is not None
            assert output_pdf.exists()
            assert output_pdf.stat().st_size > 5000

    def test_heatmap_only_no_dispatch_chart(
        self, results_dir: Path
    ) -> None:
        """BESS page should work with heatmap but no dispatch profile chart."""
        with tempfile.TemporaryDirectory(prefix="bess_pdf_") as tmp:
            tmp_path = Path(tmp)

            monthly_chart = _make_dummy_chart(tmp_path / "monthly.png")
            waterfall_chart = _make_dummy_chart(tmp_path / "waterfall.png")

            heatmap_path = tmp_path / "bess_heatmap.png"
            generate_heatmap_chart(_make_synthetic_heatmap(), heatmap_path)

            site_summary = {
                "site_name": "Heatmap Only Site",
                "customer": "Test",
                "state": "AZ",
                "latitude": 33.45,
                "longitude": -112.07,
                "dc_capacity_mw": 13.0,
                "ac_capacity_mw": 10.0,
                "dc_ac_ratio": 1.3,
                "racking": "tracker",
                "tilt": 0,
                "azimuth": 180,
                "gcr": 0.35,
                "module_model": "Test",
                "inverter_model": "Test",
                "bifacial": False,
                "annual_energy_mwh": 25000.0,
                "capacity_factor_pct": 25.0,
                "specific_yield_kwh_kwp": 1750.0,
                "performance_ratio_pct": 82.0,
                "annual_ghi_kwh_m2": 2100.0,
                "weather_year": "tmy",
            }

            narrative = {
                "site_overview": "Test.",
                "solar_resource": "Test.",
                "production_summary": "Test.",
                "loss_summary": "Test.",
            }

            output_pdf = results_dir / "bess_heatmap_only_report.pdf"
            result = build_pdf(
                output_path=output_pdf,
                site_summary=site_summary,
                narrative=narrative,
                monthly_chart_path=monthly_chart,
                waterfall_chart_path=waterfall_chart,
                bess_dispatch=_make_bess_dispatch_dict(),
                bess_heatmap_path=heatmap_path,
                bess_dispatch_chart_path=None,
            )

            assert result is not None
            assert output_pdf.exists()


class TestMissingBessDispatch:
    """Verify BESS page is skipped when bess_dispatch data is absent."""

    def test_no_bess_dispatch_key(self, results_dir: Path) -> None:
        """PDF should build without BESS page when bess_dispatch is None."""
        with tempfile.TemporaryDirectory(prefix="bess_pdf_") as tmp:
            tmp_path = Path(tmp)

            monthly_chart = _make_dummy_chart(tmp_path / "monthly.png")
            waterfall_chart = _make_dummy_chart(tmp_path / "waterfall.png")

            site_summary = {
                "site_name": "No BESS Site",
                "customer": "Test",
                "state": "AZ",
                "latitude": 33.45,
                "longitude": -112.07,
                "dc_capacity_mw": 13.0,
                "ac_capacity_mw": 10.0,
                "dc_ac_ratio": 1.3,
                "racking": "tracker",
                "tilt": 0,
                "azimuth": 180,
                "gcr": 0.35,
                "module_model": "Test",
                "inverter_model": "Test",
                "bifacial": False,
                "annual_energy_mwh": 25000.0,
                "capacity_factor_pct": 25.0,
                "specific_yield_kwh_kwp": 1750.0,
                "performance_ratio_pct": 82.0,
                "annual_ghi_kwh_m2": 2100.0,
                "weather_year": "tmy",
            }

            narrative = {
                "site_overview": "Test.",
                "solar_resource": "Test.",
                "production_summary": "Test.",
                "loss_summary": "Test.",
            }

            output_pdf = results_dir / "no_bess_report.pdf"
            result = build_pdf(
                output_path=output_pdf,
                site_summary=site_summary,
                narrative=narrative,
                monthly_chart_path=monthly_chart,
                waterfall_chart_path=waterfall_chart,
                bess_dispatch=None,
                bess_heatmap_path=None,
            )

            assert result is not None
            assert output_pdf.exists()

    def test_bess_dispatch_without_heatmap(
        self, results_dir: Path
    ) -> None:
        """BESS page should be skipped when heatmap chart is missing."""
        with tempfile.TemporaryDirectory(prefix="bess_pdf_") as tmp:
            tmp_path = Path(tmp)

            monthly_chart = _make_dummy_chart(tmp_path / "monthly.png")
            waterfall_chart = _make_dummy_chart(tmp_path / "waterfall.png")

            site_summary = {
                "site_name": "Missing Heatmap Site",
                "customer": "Test",
                "state": "AZ",
                "latitude": 33.45,
                "longitude": -112.07,
                "dc_capacity_mw": 13.0,
                "ac_capacity_mw": 10.0,
                "dc_ac_ratio": 1.3,
                "racking": "tracker",
                "tilt": 0,
                "azimuth": 180,
                "gcr": 0.35,
                "module_model": "Test",
                "inverter_model": "Test",
                "bifacial": False,
                "annual_energy_mwh": 25000.0,
                "capacity_factor_pct": 25.0,
                "specific_yield_kwh_kwp": 1750.0,
                "performance_ratio_pct": 82.0,
                "annual_ghi_kwh_m2": 2100.0,
                "weather_year": "tmy",
            }

            narrative = {
                "site_overview": "Test.",
                "solar_resource": "Test.",
                "production_summary": "Test.",
                "loss_summary": "Test.",
            }

            output_pdf = results_dir / "missing_heatmap_report.pdf"
            result = build_pdf(
                output_path=output_pdf,
                site_summary=site_summary,
                narrative=narrative,
                monthly_chart_path=monthly_chart,
                waterfall_chart_path=waterfall_chart,
                bess_dispatch=_make_bess_dispatch_dict(),
                bess_heatmap_path=None,
            )

            # Should succeed but without BESS page
            assert result is not None
            assert output_pdf.exists()
