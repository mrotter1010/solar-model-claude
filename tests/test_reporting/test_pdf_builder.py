"""Tests for PDF builder — assembles reportlab PDF from extracted data."""

from pathlib import Path

import pytest

from src.reporting.chart_builder import (
    generate_loss_waterfall_chart,
    generate_monthly_production_chart,
)
from src.reporting.pdf_builder import build_pdf


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_site_summary() -> dict:
    """Site summary matching Phoenix test site (output of extract_site_summary)."""
    return {
        "site_name": "SiteTest_Phoenix",
        "customer": "TestCustomer",
        "latitude": 33.483,
        "longitude": -112.073,
        "state": "Arizona",
        "dc_capacity_mw": 13.0,
        "ac_capacity_mw": 10.0,
        "dc_ac_ratio": 1.3,
        "racking": "tracker",
        "tilt": 60.0,
        "azimuth": 180.0,
        "gcr": 0.34,
        "module_model": "CSI Solar Co. Ltd. CS3U-355P",
        "module_count": 2,
        "bifacial": True,
        "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "annual_energy_mwh": 28935.6,
        "capacity_factor_pct": 25.41,
        "specific_yield_kwh_kwp": 2225.81,
        "performance_ratio_pct": 76.76,
        "avg_daytime_ghi_wm2": 490.5,
        "annual_ghi_kwh_m2": 2131.8,
        "weather_year": "2023",
    }


def _make_narrative() -> dict:
    """Narrative text matching Phoenix test site."""
    return {
        "site_overview": (
            "SiteTest_Phoenix is a proposed 13.0 MW-DC / 10.0 MW-AC "
            "solar PV facility located in Arizona."
        ),
        "solar_resource": (
            "Based on 2023 NSRDB satellite-derived weather data, the site "
            "receives an annual GHI of 2,132 kWh/m\u00b2."
        ),
        "production_summary": (
            "The system is projected to produce 28,936 MWh of net AC energy "
            "annually, corresponding to a capacity factor of 25.4%."
        ),
        "loss_summary": (
            "From a nominal DC energy of 35,795 MWh, the system delivers "
            "28,936 MWh after accounting for system losses.\n"
            "POA & Module Losses accounts for 3,395 MWh (9.5%).\n"
            "DC Losses contributes 1,435 MWh (4.4%)."
        ),
    }


def _make_monthly_data() -> dict:
    """Monthly production data matching Phoenix test site."""
    return {
        "months": [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ],
        "energy_mwh": [
            1441.2, 1630.5, 2378.1, 3041.2, 3342.3, 3010.2,
            3051.4, 2740.3, 2661.8, 2275.1, 1787.1, 1576.5,
        ],
    }


def _make_waterfall_data() -> list[dict]:
    """Waterfall data matching Phoenix test site."""
    return [
        {
            "label": "Nominal DC Energy",
            "energy_kwh": 35795452.0,
            "loss_percent": 0.0,
            "type": "start",
            "sub_losses": [],
        },
        {
            "label": "POA & Module Losses",
            "energy_kwh": 3395228.0,
            "loss_percent": 9.49,
            "type": "loss",
            "sub_losses": [
                {"label": "Shading", "percent": 1.53},
                {"label": "Soiling", "percent": 5.0},
                {"label": "Module Efficiency", "percent": 9.48},
                {"label": "Bifacial Gain", "percent": -3.32},
            ],
        },
        {
            "label": "Net AC Energy",
            "energy_kwh": 28935577.0,
            "loss_percent": 0.0,
            "type": "end",
            "sub_losses": [],
        },
    ]


def _generate_test_charts(tmp_path: Path) -> tuple[Path, Path]:
    """Generate chart PNGs for testing PDF builder.

    Args:
        tmp_path: pytest tmp_path fixture directory.

    Returns:
        Tuple of (monthly_chart_path, waterfall_chart_path).
    """
    monthly_path = tmp_path / "monthly.png"
    generate_monthly_production_chart(_make_monthly_data(), monthly_path)

    waterfall_path = tmp_path / "waterfall.png"
    generate_loss_waterfall_chart(_make_waterfall_data(), waterfall_path)

    return monthly_path, waterfall_path


# ---------------------------------------------------------------------------
# build_pdf
# ---------------------------------------------------------------------------


class TestBuildPdf:
    """Tests for PDF report generation."""

    def test_produces_pdf_file(self, tmp_path: Path) -> None:
        """build_pdf creates a PDF file at the specified path with size > 0."""
        monthly_path, waterfall_path = _generate_test_charts(tmp_path)
        output_path = tmp_path / "report.pdf"

        result = build_pdf(
            output_path=output_path,
            site_summary=_make_site_summary(),
            narrative=_make_narrative(),
            monthly_chart_path=monthly_path,
            waterfall_chart_path=waterfall_path,
        )

        assert result == output_path
        assert output_path.exists()
        assert output_path.stat().st_size > 0

    def test_returns_none_on_missing_monthly_chart(self, tmp_path: Path) -> None:
        """Returns None when monthly chart file does not exist."""
        _, waterfall_path = _generate_test_charts(tmp_path)
        output_path = tmp_path / "report.pdf"
        missing_path = tmp_path / "nonexistent.png"

        result = build_pdf(
            output_path=output_path,
            site_summary=_make_site_summary(),
            narrative=_make_narrative(),
            monthly_chart_path=missing_path,
            waterfall_chart_path=waterfall_path,
        )

        assert result is None

    def test_returns_none_on_missing_waterfall_chart(self, tmp_path: Path) -> None:
        """Returns None when waterfall chart file does not exist."""
        monthly_path, _ = _generate_test_charts(tmp_path)
        output_path = tmp_path / "report.pdf"
        missing_path = tmp_path / "nonexistent.png"

        result = build_pdf(
            output_path=output_path,
            site_summary=_make_site_summary(),
            narrative=_make_narrative(),
            monthly_chart_path=monthly_path,
            waterfall_chart_path=missing_path,
        )

        assert result is None

    def test_pdf_file_starts_with_pdf_header(self, tmp_path: Path) -> None:
        """Generated file is a valid PDF (starts with %PDF magic bytes)."""
        monthly_path, waterfall_path = _generate_test_charts(tmp_path)
        output_path = tmp_path / "report.pdf"

        build_pdf(
            output_path=output_path,
            site_summary=_make_site_summary(),
            narrative=_make_narrative(),
            monthly_chart_path=monthly_path,
            waterfall_chart_path=waterfall_path,
        )

        header = output_path.read_bytes()[:5]
        assert header == b"%PDF-"
