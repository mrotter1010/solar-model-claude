"""Tests for NEM configuration in the rate builder CLI."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.rates.rate_builder_cli import run_cli
from src.rates.rate_parser import load_rate_file

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "outputs" / "test_results" / "m14b_prompt2"


def _flat_rate_base_inputs(output_file: Path) -> list[str]:
    """Return the standard flat-rate input sequence up through output path.

    This is the common prefix for all NEM CLI tests: builds a minimal
    flat energy-only rate and specifies the output file path. NEM-specific
    inputs should be appended after this list.
    """
    return [
        "Test Utility",        # utility name
        "NEM Test Rate",       # tariff name
        "",                    # sector (default: commercial)
        "n",                   # not TOU
        "0.10",                # energy rate
        "n",                   # no demand
        "15.00",               # fixed charge
        str(output_file),      # output path
    ]


class TestNemDeclined:
    """Test CLI flow when NEM is declined."""

    def test_nem_declined_defaults_to_none(self, tmp_path: Path) -> None:
        """Answering 'n' to NEM prompt leaves mode=none."""
        output_file = tmp_path / "no_nem.json"
        inputs = _flat_rate_base_inputs(output_file) + [
            "n",  # decline NEM
        ]

        with patch("builtins.input", side_effect=inputs):
            run_cli()

        rate = load_rate_file(output_file)
        assert rate.net_metering.mode == "none"
        assert rate.net_metering.export_rate is None
        assert rate.net_metering.true_up_rate == 0.0

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        (OUTPUT_DIR / "nem_declined.json").write_text(
            json.dumps(rate.model_dump(), indent=2, default=str)
        )


class TestNemFlatRate:
    """Test CLI flow for flat_rate NEM mode."""

    def test_flat_rate_nem(self, tmp_path: Path) -> None:
        """flat_rate NEM with export_rate and true_up_rate."""
        output_file = tmp_path / "flat_nem.json"
        inputs = _flat_rate_base_inputs(output_file) + [
            "y",      # configure NEM
            "1",      # flat_rate mode
            "0.04",   # export rate
            "0.02",   # true_up_rate
        ]

        with patch("builtins.input", side_effect=inputs):
            run_cli()

        rate = load_rate_file(output_file)
        assert rate.net_metering.mode == "flat_rate"
        assert rate.net_metering.export_rate == 0.04
        assert rate.net_metering.true_up_rate == 0.02

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        (OUTPUT_DIR / "flat_rate_nem.json").write_text(
            json.dumps(rate.model_dump(), indent=2, default=str)
        )

    def test_flat_rate_default_true_up(self, tmp_path: Path) -> None:
        """flat_rate NEM with empty input for true_up_rate uses default 0.00."""
        output_file = tmp_path / "flat_nem_default_tu.json"
        inputs = _flat_rate_base_inputs(output_file) + [
            "y",      # configure NEM
            "1",      # flat_rate mode
            "0.05",   # export rate
            "",       # true_up_rate (default 0.00)
        ]

        with patch("builtins.input", side_effect=inputs):
            run_cli()

        rate = load_rate_file(output_file)
        assert rate.net_metering.mode == "flat_rate"
        assert rate.net_metering.export_rate == 0.05
        assert rate.net_metering.true_up_rate == 0.00

    def test_flat_rate_invalid_export_rate_reprompts(self, tmp_path: Path) -> None:
        """Invalid export rates (0, negative) trigger re-prompt until valid."""
        output_file = tmp_path / "flat_nem_retry.json"
        inputs = _flat_rate_base_inputs(output_file) + [
            "y",       # configure NEM
            "1",       # flat_rate mode
            "0",       # invalid: zero
            "-0.05",   # invalid: negative
            "0.04",    # valid
            "0.00",    # true_up_rate
        ]

        with patch("builtins.input", side_effect=inputs):
            run_cli()

        rate = load_rate_file(output_file)
        assert rate.net_metering.mode == "flat_rate"
        assert rate.net_metering.export_rate == 0.04


class TestNemMatchImport:
    """Test CLI flow for match_import NEM mode."""

    def test_match_import_nem(self, tmp_path: Path) -> None:
        """match_import NEM with true_up_rate."""
        output_file = tmp_path / "match_nem.json"
        inputs = _flat_rate_base_inputs(output_file) + [
            "y",      # configure NEM
            "2",      # match_import mode
            "0.01",   # true_up_rate
        ]

        with patch("builtins.input", side_effect=inputs):
            run_cli()

        rate = load_rate_file(output_file)
        assert rate.net_metering.mode == "match_import"
        assert rate.net_metering.export_rate is None
        assert rate.net_metering.true_up_rate == 0.01


class TestNemPreservesRateData:
    """Test that NEM configuration doesn't alter the underlying rate."""

    def test_rate_data_unchanged(self, tmp_path: Path) -> None:
        """Energy, demand, and fixed charge data survive NEM application."""
        output_file = tmp_path / "nem_preserves.json"
        inputs = _flat_rate_base_inputs(output_file) + [
            "y",      # configure NEM
            "1",      # flat_rate mode
            "0.06",   # export rate
            "",       # true_up_rate default
        ]

        with patch("builtins.input", side_effect=inputs):
            run_cli()

        rate = load_rate_file(output_file)
        # Original rate data preserved
        assert rate.utility_name == "Test Utility"
        assert rate.tariff_name == "NEM Test Rate"
        assert rate.energyratestructure[0][0].rate == 0.10
        assert rate.fixed_charges.fixed_charge_first_meter == 15.00
        # NEM applied
        assert rate.net_metering.mode == "flat_rate"


class TestWriteOutputs:
    """Write test artifacts for manual inspection."""

    def test_write_summary(self, tmp_path: Path) -> None:
        """Write a summary of NEM CLI test results."""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        summary = {
            "test_file": "tests/test_rates/test_nem_cli.py",
            "tests": [
                "nem_declined_defaults_to_none",
                "flat_rate_nem",
                "flat_rate_default_true_up",
                "flat_rate_invalid_export_rate_reprompts",
                "match_import_nem",
                "rate_data_unchanged",
            ],
            "design_notes": {
                "nem_placement": "After output path prompt, before save_rate_file()",
                "backward_compat": "try/except (StopIteration, EOFError) around NEM collection",
                "detailed_mode_reuse": "_collect_tou_periods() + _build_schedule_matrix()",
            },
        }
        (OUTPUT_DIR / "test_summary.json").write_text(
            json.dumps(summary, indent=2)
        )
        assert (OUTPUT_DIR / "test_summary.json").exists()


# ---------------------------------------------------------------------------
# validate_nem_export_vs_import tests (M14b prompt 5b)
# ---------------------------------------------------------------------------

from src.rates.models import (
    FixedCharges,
    NetMeteringConfig,
    RateSchedule,
    RateTier,
)
from src.rates.rate_builder_cli import validate_nem_export_vs_import

OUTPUT_DIR_5B = Path(__file__).resolve().parents[2] / "outputs" / "test_results" / "m14b_prompt5b"


def _make_rate_schedule(
    energy_rate: float = 0.15,
    nem_mode: str = "none",
    export_rate: float | None = None,
    export_schedule: list[list[int]] | None = None,
    export_weekend_schedule: list[list[int]] | None = None,
    export_rate_structure: list[list[RateTier]] | None = None,
) -> RateSchedule:
    """Build a minimal RateSchedule for testing validation.

    Flat energy rate across all months/hours unless otherwise specified.
    """
    # Single energy period with flat rate
    structure = [[RateTier(rate=energy_rate)]]
    schedule = [[0] * 24 for _ in range(12)]

    nem = NetMeteringConfig(
        mode=nem_mode,
        export_rate=export_rate,
        export_schedule=export_schedule,
        export_weekend_schedule=export_weekend_schedule,
        export_rate_structure=export_rate_structure,
    )

    return RateSchedule(
        utility_name="Test",
        tariff_name="Test",
        energyratestructure=structure,
        energyweekdayschedule=[list(row) for row in schedule],
        energyweekendschedule=[list(row) for row in schedule],
        net_metering=nem,
    )


def _make_tou_rate_schedule(
    peak_rate: float = 0.30,
    offpeak_rate: float = 0.10,
    peak_hours: tuple[int, int] = (16, 21),
    nem_mode: str = "none",
    export_rate: float | None = None,
) -> RateSchedule:
    """Build a TOU rate schedule with peak/off-peak periods.

    Peak period runs from peak_hours[0] to peak_hours[1] (exclusive).
    """
    # Period 0 = off-peak, period 1 = peak
    structure = [
        [RateTier(rate=offpeak_rate)],
        [RateTier(rate=peak_rate)],
    ]
    # Build 12x24 schedule: peak hours get period 1, others get period 0
    schedule = []
    for _ in range(12):
        row = [0] * 24
        for h in range(peak_hours[0], peak_hours[1]):
            row[h] = 1
        schedule.append(row)

    nem = NetMeteringConfig(
        mode=nem_mode,
        export_rate=export_rate,
    )

    return RateSchedule(
        utility_name="TOU Test",
        tariff_name="TOU Test",
        energyratestructure=structure,
        energyweekdayschedule=[list(row) for row in schedule],
        energyweekendschedule=[list(row) for row in schedule],
        net_metering=nem,
    )


class TestValidateNemExportVsImportMatchImport:
    """match_import mode always returns no warnings."""

    def test_match_import_no_warnings(self) -> None:
        """match_import mode returns empty list (no check needed)."""
        rate = _make_rate_schedule(
            energy_rate=0.15,
            nem_mode="match_import",
        )
        warnings = validate_nem_export_vs_import(rate)
        assert warnings == []


class TestValidateNemExportVsImportNone:
    """none mode always returns no warnings."""

    def test_none_mode_no_warnings(self) -> None:
        """none mode returns empty list."""
        rate = _make_rate_schedule(energy_rate=0.15, nem_mode="none")
        warnings = validate_nem_export_vs_import(rate)
        assert warnings == []


class TestValidateNemExportVsImportFlatRate:
    """flat_rate mode: compare flat export rate vs all import rates."""

    def test_flat_rate_export_below_import_no_warnings(self) -> None:
        """Export rate below all import rates produces no warnings."""
        rate = _make_rate_schedule(
            energy_rate=0.15,
            nem_mode="flat_rate",
            export_rate=0.08,
        )
        warnings = validate_nem_export_vs_import(rate)
        assert warnings == []

    def test_flat_rate_export_above_import_produces_warnings(self) -> None:
        """Export rate above import rate produces warnings for every cell."""
        rate = _make_rate_schedule(
            energy_rate=0.10,
            nem_mode="flat_rate",
            export_rate=0.15,
        )
        warnings = validate_nem_export_vs_import(rate)
        # 12 months * 24 hours * 2 (weekday + weekend) = 576 warnings
        assert len(warnings) == 576
        assert all("exceeds import rate" in w for w in warnings)

    def test_flat_rate_export_above_some_import_rates(self) -> None:
        """Export rate exceeds only off-peak import rate with TOU schedule."""
        # off-peak $0.10, peak $0.30 — export $0.15 exceeds off-peak only
        rate = _make_tou_rate_schedule(
            peak_rate=0.30,
            offpeak_rate=0.10,
            peak_hours=(16, 21),
            nem_mode="flat_rate",
            export_rate=0.15,
        )
        warnings = validate_nem_export_vs_import(rate)
        # Warnings only for off-peak hours (0-15, 21-23 = 19 hours per day)
        # 12 months * 19 hours * 2 (weekday + weekend) = 456
        assert len(warnings) == 456
        # Confirm no warnings mention the peak hours
        for w in warnings:
            assert "$0.3000" not in w  # peak rate never appears as exceeded

    def test_flat_rate_export_equals_import_no_warning(self) -> None:
        """Export rate == import rate is not a violation (only > triggers)."""
        rate = _make_rate_schedule(
            energy_rate=0.15,
            nem_mode="flat_rate",
            export_rate=0.15,
        )
        warnings = validate_nem_export_vs_import(rate)
        assert warnings == []


class TestValidateNemExportVsImportDetailed:
    """detailed mode: compare per-cell export rates vs import rates."""

    def test_detailed_export_below_import_no_warnings(self) -> None:
        """Detailed export rates all below import produces no warnings."""
        export_schedule = [[0] * 24 for _ in range(12)]
        export_rate_struct = [[RateTier(rate=0.05)]]

        rate = _make_rate_schedule(
            energy_rate=0.15,
            nem_mode="detailed",
            export_schedule=export_schedule,
            export_weekend_schedule=[list(row) for row in export_schedule],
            export_rate_structure=export_rate_struct,
        )
        warnings = validate_nem_export_vs_import(rate)
        assert warnings == []

    def test_detailed_export_above_import_produces_warnings(self) -> None:
        """Detailed export rates above import produces warnings."""
        export_schedule = [[0] * 24 for _ in range(12)]
        export_rate_struct = [[RateTier(rate=0.25)]]

        rate = _make_rate_schedule(
            energy_rate=0.15,
            nem_mode="detailed",
            export_schedule=export_schedule,
            export_weekend_schedule=[list(row) for row in export_schedule],
            export_rate_structure=export_rate_struct,
        )
        warnings = validate_nem_export_vs_import(rate)
        assert len(warnings) == 576  # 12 * 24 * 2
        assert all("exceeds import rate" in w for w in warnings)


class TestValidateNemWriteOutputs:
    """Write test outputs for manual inspection."""

    def test_write_validation_summary(self) -> None:
        """Write summary of validation test results."""
        OUTPUT_DIR_5B.mkdir(parents=True, exist_ok=True)

        # No warnings case
        rate_ok = _make_rate_schedule(
            energy_rate=0.15, nem_mode="flat_rate", export_rate=0.08
        )
        warnings_ok = validate_nem_export_vs_import(rate_ok)

        # Warnings case
        rate_bad = _make_tou_rate_schedule(
            peak_rate=0.30, offpeak_rate=0.10,
            nem_mode="flat_rate", export_rate=0.15,
        )
        warnings_bad = validate_nem_export_vs_import(rate_bad)

        summary = {
            "no_warnings": {"export_rate": 0.08, "import_rate": 0.15, "warnings": len(warnings_ok)},
            "with_warnings": {
                "export_rate": 0.15,
                "peak_import": 0.30,
                "offpeak_import": 0.10,
                "total_warnings": len(warnings_bad),
                "sample_warnings": warnings_bad[:5],
            },
        }

        output_path = OUTPUT_DIR_5B / "nem_validation_summary.json"
        output_path.write_text(json.dumps(summary, indent=2))
        assert output_path.exists()
