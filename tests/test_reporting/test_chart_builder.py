"""Tests for chart builder — generates PNG charts from extracted data."""

from pathlib import Path

import pytest

from src.reporting.chart_builder import (
    generate_loss_waterfall_chart,
    generate_monthly_production_chart,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_monthly_data() -> dict:
    """Monthly production data matching Phoenix test site."""
    return {
        "months": ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
        "energy_mwh": [
            1441.2, 1630.5, 2378.1, 3041.2, 3342.3, 3010.2,
            3051.4, 2740.3, 2661.8, 2275.1, 1787.1, 1576.5,
        ],
    }


def _make_waterfall_data() -> list[dict]:
    """Waterfall data matching Phoenix test site loss structure."""
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
            "label": "DC Losses",
            "energy_kwh": 1434677.0,
            "loss_percent": 4.43,
            "type": "loss",
            "sub_losses": [
                {"label": "Mismatch", "percent": 1.5},
                {"label": "DC Wiring", "percent": 1.5},
                {"label": "Nameplate/LID", "percent": 1.0},
            ],
        },
        {
            "label": "Inverter Losses",
            "energy_kwh": 680753.0,
            "loss_percent": 2.20,
            "type": "loss",
            "sub_losses": [
                {"label": "Inverter Clipping", "percent": 0.69},
                {"label": "Inverter Efficiency", "percent": 1.41},
            ],
        },
        {
            "label": "AC Losses & Availability",
            "energy_kwh": 1349217.0,
            "loss_percent": 4.46,
            "type": "loss",
            "sub_losses": [
                {"label": "AC Wiring", "percent": 1.5},
                {"label": "Availability", "percent": 3.0},
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


# ---------------------------------------------------------------------------
# generate_monthly_production_chart
# ---------------------------------------------------------------------------


class TestMonthlyProductionChart:
    """Tests for monthly production bar chart generation."""

    def test_produces_png_file(self, tmp_path: Path) -> None:
        """Chart function creates a PNG file at the specified path."""
        output_path = tmp_path / "monthly.png"

        result = generate_monthly_production_chart(_make_monthly_data(), output_path)

        assert result == output_path
        assert output_path.exists()
        assert output_path.stat().st_size > 0

    def test_returns_none_on_invalid_input(self, tmp_path: Path) -> None:
        """Returns None (no exception) for missing keys."""
        output_path = tmp_path / "bad.png"

        result = generate_monthly_production_chart({}, output_path)

        assert result is None
        assert not output_path.exists()

    def test_returns_none_on_none_input(self, tmp_path: Path) -> None:
        """Returns None (no exception) for None input."""
        output_path = tmp_path / "bad.png"

        result = generate_monthly_production_chart(None, output_path)

        assert result is None


# ---------------------------------------------------------------------------
# generate_loss_waterfall_chart
# ---------------------------------------------------------------------------


class TestLossWaterfallChart:
    """Tests for loss waterfall bar chart generation."""

    def test_produces_png_file(self, tmp_path: Path) -> None:
        """Chart function creates a PNG file at the specified path."""
        output_path = tmp_path / "waterfall.png"

        result = generate_loss_waterfall_chart(_make_waterfall_data(), output_path)

        assert result == output_path
        assert output_path.exists()
        assert output_path.stat().st_size > 0

    def test_returns_none_on_empty_input(self, tmp_path: Path) -> None:
        """Returns None (no exception) for empty list."""
        output_path = tmp_path / "bad.png"

        result = generate_loss_waterfall_chart([], output_path)

        assert result is None
        assert not output_path.exists()

    def test_returns_none_on_none_input(self, tmp_path: Path) -> None:
        """Returns None (no exception) for None input."""
        output_path = tmp_path / "bad.png"

        result = generate_loss_waterfall_chart(None, output_path)

        assert result is None


# ---------------------------------------------------------------------------
# Sample chart artifacts for manual review
# ---------------------------------------------------------------------------


class TestSampleChartArtifacts:
    """Generate sample charts to outputs/test_results/ for visual inspection."""

    def test_generate_sample_monthly_chart(self) -> None:
        """Save a sample monthly production chart for manual review."""
        output_dir = Path("outputs/test_results")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "sample_monthly_production.png"

        result = generate_monthly_production_chart(_make_monthly_data(), output_path)

        assert result is not None
        assert output_path.exists()

    def test_generate_sample_waterfall_chart(self) -> None:
        """Save a sample loss waterfall chart for manual review."""
        output_dir = Path("outputs/test_results")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "sample_loss_waterfall.png"

        result = generate_loss_waterfall_chart(_make_waterfall_data(), output_path)

        assert result is not None
        assert output_path.exists()
