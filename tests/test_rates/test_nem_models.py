"""Tests for NetMeteringConfig model, RateSchedule integration, rate_parser, and rate_builder."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.rates.models import NetMeteringConfig, RateSchedule, RateTier
from src.rates.rate_builder import build_rate_schedule, set_net_metering
from src.rates.rate_parser import load_rate_file

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "outputs" / "test_results" / "m14b_prompt1"


def _make_schedule_12x24(value: int = 0) -> list[list[int]]:
    """Build a 12x24 matrix filled with a single value."""
    return [[value] * 24 for _ in range(12)]


def _make_energy_only_rate_dict() -> dict:
    """Minimal energy-only rate schedule dict for RateSchedule construction."""
    return {
        "utility_name": "Test Utility",
        "tariff_name": "Flat Rate",
        "energyratestructure": [[{"rate": 0.10}]],
        "energyweekdayschedule": _make_schedule_12x24(0),
        "energyweekendschedule": _make_schedule_12x24(0),
    }


def _make_minimal_rate_schedule() -> RateSchedule:
    """Build a minimal RateSchedule for tests that need one."""
    return RateSchedule(**_make_energy_only_rate_dict())


def _make_sample_rate_json_with_nem(tmp_path: Path) -> Path:
    """Write a minimal rate JSON file that includes a net_metering section."""
    data = _make_energy_only_rate_dict()
    data["net_metering"] = {
        "mode": "flat_rate",
        "export_rate": 0.04,
        "true_up_rate": 0.02,
    }
    path = tmp_path / "rate_with_nem.json"
    path.write_text(json.dumps(data, indent=2))
    return path


def _make_sample_rate_json_without_nem(tmp_path: Path) -> Path:
    """Write a minimal rate JSON file with no net_metering section."""
    data = _make_energy_only_rate_dict()
    path = tmp_path / "rate_no_nem.json"
    path.write_text(json.dumps(data, indent=2))
    return path


# ---------------------------------------------------------------------------
# NetMeteringConfig defaults
# ---------------------------------------------------------------------------


class TestNetMeteringConfigDefaults:
    """Test NetMeteringConfig default values."""

    def test_defaults(self) -> None:
        """Default config has mode=none and true_up_rate=0.0."""
        nem = NetMeteringConfig()
        assert nem.mode == "none"
        assert nem.export_rate is None
        assert nem.export_schedule is None
        assert nem.export_weekend_schedule is None
        assert nem.export_rate_structure is None
        assert nem.true_up_rate == 0.0


# ---------------------------------------------------------------------------
# flat_rate mode
# ---------------------------------------------------------------------------


class TestFlatRateMode:
    """Test flat_rate NEM mode validation."""

    def test_valid_flat_rate(self) -> None:
        """Valid flat_rate config with export_rate > 0."""
        nem = NetMeteringConfig(mode="flat_rate", export_rate=0.05)
        assert nem.mode == "flat_rate"
        assert nem.export_rate == 0.05

    def test_flat_rate_with_true_up(self) -> None:
        """flat_rate can also set a true_up_rate."""
        nem = NetMeteringConfig(mode="flat_rate", export_rate=0.05, true_up_rate=0.03)
        assert nem.true_up_rate == 0.03

    def test_flat_rate_missing_export_rate_raises(self) -> None:
        """flat_rate without export_rate raises ValidationError."""
        with pytest.raises(ValidationError, match="export_rate is required"):
            NetMeteringConfig(mode="flat_rate")

    def test_flat_rate_zero_export_rate_raises(self) -> None:
        """flat_rate with export_rate=0 raises ValidationError."""
        with pytest.raises(ValidationError, match="export_rate must be > 0"):
            NetMeteringConfig(mode="flat_rate", export_rate=0.0)

    def test_flat_rate_negative_export_rate_raises(self) -> None:
        """flat_rate with export_rate < 0 raises ValidationError."""
        with pytest.raises(ValidationError, match="export_rate must be > 0"):
            NetMeteringConfig(mode="flat_rate", export_rate=-0.01)


# ---------------------------------------------------------------------------
# match_import mode
# ---------------------------------------------------------------------------


class TestMatchImportMode:
    """Test match_import NEM mode validation."""

    def test_valid_match_import(self) -> None:
        """match_import only needs the mode field."""
        nem = NetMeteringConfig(mode="match_import")
        assert nem.mode == "match_import"
        assert nem.export_rate is None

    def test_match_import_ignores_extra_fields(self) -> None:
        """match_import with extra fields set does not error."""
        nem = NetMeteringConfig(mode="match_import", export_rate=0.05)
        assert nem.export_rate == 0.05


# ---------------------------------------------------------------------------
# detailed mode
# ---------------------------------------------------------------------------


class TestDetailedMode:
    """Test detailed NEM mode validation."""

    def test_valid_detailed(self) -> None:
        """Valid detailed config with all required schedules."""
        nem = NetMeteringConfig(
            mode="detailed",
            export_schedule=_make_schedule_12x24(0),
            export_weekend_schedule=_make_schedule_12x24(0),
            export_rate_structure=[[RateTier(rate=0.04)]],
        )
        assert nem.mode == "detailed"
        assert len(nem.export_schedule) == 12
        assert len(nem.export_rate_structure) == 1

    def test_detailed_missing_export_schedule_raises(self) -> None:
        """detailed without export_schedule raises ValidationError."""
        with pytest.raises(ValidationError, match="export_schedule is required"):
            NetMeteringConfig(
                mode="detailed",
                export_weekend_schedule=_make_schedule_12x24(0),
                export_rate_structure=[[RateTier(rate=0.04)]],
            )

    def test_detailed_missing_export_weekend_schedule_raises(self) -> None:
        """detailed without export_weekend_schedule raises ValidationError."""
        with pytest.raises(ValidationError, match="export_weekend_schedule is required"):
            NetMeteringConfig(
                mode="detailed",
                export_schedule=_make_schedule_12x24(0),
                export_rate_structure=[[RateTier(rate=0.04)]],
            )

    def test_detailed_missing_export_rate_structure_raises(self) -> None:
        """detailed without export_rate_structure raises ValidationError."""
        with pytest.raises(ValidationError, match="export_rate_structure is required"):
            NetMeteringConfig(
                mode="detailed",
                export_schedule=_make_schedule_12x24(0),
                export_weekend_schedule=_make_schedule_12x24(0),
            )

    def test_detailed_wrong_schedule_rows_raises(self) -> None:
        """detailed with non-12-row schedule raises ValidationError."""
        bad_schedule = [[0] * 24 for _ in range(10)]  # 10 rows, not 12
        with pytest.raises(ValidationError, match="must have 12 rows"):
            NetMeteringConfig(
                mode="detailed",
                export_schedule=bad_schedule,
                export_weekend_schedule=_make_schedule_12x24(0),
                export_rate_structure=[[RateTier(rate=0.04)]],
            )

    def test_detailed_wrong_schedule_columns_raises(self) -> None:
        """detailed with non-24-column schedule row raises ValidationError."""
        bad_schedule = [[0] * 20 for _ in range(12)]  # 20 cols, not 24
        with pytest.raises(ValidationError, match="must have 24 columns"):
            NetMeteringConfig(
                mode="detailed",
                export_schedule=bad_schedule,
                export_weekend_schedule=_make_schedule_12x24(0),
                export_rate_structure=[[RateTier(rate=0.04)]],
            )

    def test_detailed_weekend_wrong_dimensions_raises(self) -> None:
        """detailed with bad weekend schedule dimensions raises ValidationError."""
        bad_weekend = [[0] * 24 for _ in range(11)]  # 11 rows
        with pytest.raises(ValidationError, match="must have 12 rows"):
            NetMeteringConfig(
                mode="detailed",
                export_schedule=_make_schedule_12x24(0),
                export_weekend_schedule=bad_weekend,
                export_rate_structure=[[RateTier(rate=0.04)]],
            )


# ---------------------------------------------------------------------------
# Invalid mode
# ---------------------------------------------------------------------------


class TestInvalidMode:
    """Test invalid NEM mode rejection."""

    def test_invalid_mode_raises(self) -> None:
        """Unrecognized mode string raises ValidationError."""
        with pytest.raises(ValidationError, match="mode must be one of"):
            NetMeteringConfig(mode="wholesale")

    def test_empty_string_mode_raises(self) -> None:
        """Empty string mode is coerced to '' which is invalid."""
        with pytest.raises(ValidationError, match="mode must be one of"):
            NetMeteringConfig(mode="  ")


# ---------------------------------------------------------------------------
# Mode coercion
# ---------------------------------------------------------------------------


class TestModeCoercion:
    """Test mode string normalization to lowercase."""

    def test_uppercase_coerced(self) -> None:
        """Uppercase mode string is lowercased."""
        nem = NetMeteringConfig(mode="FLAT_RATE", export_rate=0.05)
        assert nem.mode == "flat_rate"

    def test_mixed_case_coerced(self) -> None:
        """Mixed case mode string is lowercased."""
        nem = NetMeteringConfig(mode="Match_Import")
        assert nem.mode == "match_import"

    def test_leading_trailing_whitespace_stripped(self) -> None:
        """Whitespace around mode string is stripped."""
        nem = NetMeteringConfig(mode="  none  ")
        assert nem.mode == "none"


# ---------------------------------------------------------------------------
# RateSchedule integration
# ---------------------------------------------------------------------------


class TestRateScheduleIntegration:
    """Test that RateSchedule includes net_metering with correct default."""

    def test_rate_schedule_default_net_metering(self) -> None:
        """RateSchedule has net_metering=NetMeteringConfig() by default."""
        rate = _make_minimal_rate_schedule()
        assert isinstance(rate.net_metering, NetMeteringConfig)
        assert rate.net_metering.mode == "none"

    def test_rate_schedule_with_explicit_nem(self) -> None:
        """RateSchedule accepts explicit NetMeteringConfig."""
        data = _make_energy_only_rate_dict()
        data["net_metering"] = {"mode": "flat_rate", "export_rate": 0.06}
        rate = RateSchedule(**data)
        assert rate.net_metering.mode == "flat_rate"
        assert rate.net_metering.export_rate == 0.06

    def test_rate_schedule_serializes_net_metering(self) -> None:
        """RateSchedule.model_dump() includes net_metering."""
        data = _make_energy_only_rate_dict()
        data["net_metering"] = {"mode": "flat_rate", "export_rate": 0.06}
        rate = RateSchedule(**data)
        dumped = rate.model_dump()
        assert "net_metering" in dumped
        assert dumped["net_metering"]["mode"] == "flat_rate"
        assert dumped["net_metering"]["export_rate"] == 0.06


# ---------------------------------------------------------------------------
# rate_parser integration
# ---------------------------------------------------------------------------


class TestRateParserNem:
    """Test that rate_parser handles net_metering in JSON files."""

    def test_load_json_with_net_metering(self, tmp_path: Path) -> None:
        """load_rate_file parses net_metering section from JSON."""
        path = _make_sample_rate_json_with_nem(tmp_path)
        rate = load_rate_file(path)
        assert rate.net_metering.mode == "flat_rate"
        assert rate.net_metering.export_rate == 0.04
        assert rate.net_metering.true_up_rate == 0.02

    def test_load_json_without_net_metering(self, tmp_path: Path) -> None:
        """load_rate_file defaults to mode=none when net_metering is absent."""
        path = _make_sample_rate_json_without_nem(tmp_path)
        rate = load_rate_file(path)
        assert rate.net_metering.mode == "none"
        assert rate.net_metering.export_rate is None


# ---------------------------------------------------------------------------
# rate_builder set_net_metering
# ---------------------------------------------------------------------------


class TestRateBuilderSetNetMetering:
    """Test set_net_metering() in rate_builder."""

    def test_set_flat_rate(self) -> None:
        """set_net_metering with flat_rate mode sets fields correctly."""
        rate = _make_minimal_rate_schedule()
        updated = set_net_metering(rate, mode="flat_rate", export_rate=0.05)
        assert updated.net_metering.mode == "flat_rate"
        assert updated.net_metering.export_rate == 0.05
        # Original rate is unchanged (immutable copy)
        assert rate.net_metering.mode == "none"

    def test_set_match_import(self) -> None:
        """set_net_metering with match_import mode."""
        rate = _make_minimal_rate_schedule()
        updated = set_net_metering(rate, mode="match_import")
        assert updated.net_metering.mode == "match_import"

    def test_set_detailed(self) -> None:
        """set_net_metering with detailed mode sets all schedule fields."""
        rate = _make_minimal_rate_schedule()
        sched = _make_schedule_12x24(0)
        updated = set_net_metering(
            rate,
            mode="detailed",
            export_schedule=sched,
            export_weekend_schedule=sched,
            export_rate_structure=[[RateTier(rate=0.03)]],
            true_up_rate=0.01,
        )
        assert updated.net_metering.mode == "detailed"
        assert updated.net_metering.export_schedule == sched
        assert updated.net_metering.true_up_rate == 0.01

    def test_set_net_metering_preserves_rate_data(self) -> None:
        """set_net_metering does not alter the underlying rate schedule data."""
        rate = _make_minimal_rate_schedule()
        updated = set_net_metering(rate, mode="flat_rate", export_rate=0.05)
        assert updated.utility_name == "Test Utility"
        assert updated.tariff_name == "Flat Rate"
        assert updated.energyratestructure == rate.energyratestructure

    def test_set_net_metering_invalid_mode_raises(self) -> None:
        """set_net_metering with invalid mode raises ValidationError."""
        rate = _make_minimal_rate_schedule()
        with pytest.raises(ValidationError, match="mode must be one of"):
            set_net_metering(rate, mode="bogus")


# ---------------------------------------------------------------------------
# Output test results for manual inspection
# ---------------------------------------------------------------------------


class TestWriteOutputs:
    """Write test artifacts to outputs/test_results/m14b_prompt1/."""

    def test_write_nem_config_samples(self) -> None:
        """Write sample NEM configs as JSON for manual inspection."""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        samples = {
            "none": NetMeteringConfig(),
            "flat_rate": NetMeteringConfig(mode="flat_rate", export_rate=0.04),
            "match_import": NetMeteringConfig(mode="match_import"),
            "detailed": NetMeteringConfig(
                mode="detailed",
                export_schedule=_make_schedule_12x24(0),
                export_weekend_schedule=_make_schedule_12x24(0),
                export_rate_structure=[[RateTier(rate=0.04)]],
                true_up_rate=0.02,
            ),
        }

        output_path = OUTPUT_DIR / "nem_config_samples.json"
        output_path.write_text(
            json.dumps(
                {k: v.model_dump() for k, v in samples.items()},
                indent=2,
            )
        )
        assert output_path.exists()

    def test_write_rate_schedule_with_nem(self) -> None:
        """Write a full rate schedule with NEM for manual inspection."""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        data = _make_energy_only_rate_dict()
        data["net_metering"] = {"mode": "flat_rate", "export_rate": 0.06}
        rate = RateSchedule(**data)

        output_path = OUTPUT_DIR / "rate_schedule_with_nem.json"
        output_path.write_text(json.dumps(rate.model_dump(), indent=2))
        assert output_path.exists()
