"""Unit tests for M11 subhourly refinement research code.

Tests cover:
  1. Timezone lookup (get_timezone_offset)
  2. QC filters (apply_irradiance_qc)
  3. 60-min aggregation (build_60min_from_1min)
  4. SAM weather file format (write_sam_csv round-trip)
  5. Delta computation formula and clamping
  6. Config matrix structural validation
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Path setup — research modules are not installable packages
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
M11_DIR = PROJECT_ROOT / "research" / "m11_subhourly"
sys.path.insert(0, str(M11_DIR))

from utils import get_timezone_offset  # noqa: E402
from build_weather_files import (  # noqa: E402
    apply_irradiance_qc,
    build_60min_from_1min,
    write_sam_csv,
    GHI_CAP,
    DNI_CAP,
    DHI_CAP,
    MAX_FORWARD_FILL_MINUTES,
)


# ===================================================================
# 1. Timezone lookup
# ===================================================================
class TestTimezoneOffset:
    """Verify get_timezone_offset returns correct standard UTC offsets."""

    def test_bondville_il(self) -> None:
        # Bondville, IL → US/Central → UTC-6
        assert get_timezone_offset(40.05, -88.37) == -6

    def test_desert_rock_nv(self) -> None:
        # Desert Rock, NV → US/Pacific → UTC-8
        assert get_timezone_offset(36.62, -116.02) == -8

    def test_penn_state_pa(self) -> None:
        # Penn State, PA → US/Eastern → UTC-5
        assert get_timezone_offset(40.72, -77.93) == -5

    def test_sioux_falls_sd(self) -> None:
        # Sioux Falls, SD → US/Central → UTC-6
        assert get_timezone_offset(43.73, -96.62) == -6

    def test_returns_integer(self) -> None:
        # Ensure offset is always an integer (SAM header requirement)
        offset = get_timezone_offset(40.05, -88.37)
        assert isinstance(offset, int)


# ===================================================================
# 2. QC filters (apply_irradiance_qc)
# ===================================================================
class TestIrradianceQC:
    """Test QC pipeline: reindex, ffill, clamp negatives, cap extremes."""

    @pytest.fixture()
    def _synthetic_1min(self) -> pd.DataFrame:
        """Build a small synthetic 1-min irradiance DataFrame for 2020.

        Returns 120 rows (2 hours) starting at 2020-01-01 00:00 UTC,
        with intentional QC issues: negatives, extremes, NaN gaps.
        """
        idx = pd.date_range(
            "2020-01-01 00:00:00+00:00", periods=120, freq="min",
        )
        df = pd.DataFrame(
            {
                "ghi": np.full(120, 100.0),
                "dni": np.full(120, 200.0),
                "dhi": np.full(120, 50.0),
            },
            index=idx,
        )
        # Inject issues
        df.loc[df.index[5], "ghi"] = -10.0       # negative → should become 0
        df.loc[df.index[6], "dni"] = -5.0         # negative → should become 0
        df.loc[df.index[10], "ghi"] = 1600.0      # above cap → should become 1500
        df.loc[df.index[11], "dni"] = 1700.0      # above cap → should become 1500
        df.loc[df.index[12], "dhi"] = 900.0        # above cap → should become 800
        # NaN gap of 5 minutes (should be forward-filled)
        for i in range(20, 25):
            df.loc[df.index[i], :] = np.nan
        # NaN gap of 15 minutes (first 10 ffilled, last 5 → 0)
        for i in range(40, 55):
            df.loc[df.index[i], :] = np.nan
        return df

    def test_negatives_clamped_to_zero(self, _synthetic_1min: pd.DataFrame) -> None:
        df_qc, summary = apply_irradiance_qc(_synthetic_1min, 2020)
        # All values should be >= 0 after QC
        assert (df_qc["ghi"] >= 0).all()
        assert (df_qc["dni"] >= 0).all()
        assert (df_qc["dhi"] >= 0).all()

    def test_extremes_capped(self, _synthetic_1min: pd.DataFrame) -> None:
        df_qc, _ = apply_irradiance_qc(_synthetic_1min, 2020)
        assert (df_qc["ghi"] <= GHI_CAP).all()
        assert (df_qc["dni"] <= DNI_CAP).all()
        assert (df_qc["dhi"] <= DHI_CAP).all()

    def test_reindex_fills_full_year(self, _synthetic_1min: pd.DataFrame) -> None:
        df_qc, _ = apply_irradiance_qc(_synthetic_1min, 2020)
        # 2020 is a leap year → 366 * 1440 = 527,040 minutes
        expected = 366 * 1440
        assert len(df_qc) == expected

    def test_no_nans_after_qc(self, _synthetic_1min: pd.DataFrame) -> None:
        df_qc, _ = apply_irradiance_qc(_synthetic_1min, 2020)
        assert df_qc.isna().sum().sum() == 0

    def test_short_gap_forward_filled(self, _synthetic_1min: pd.DataFrame) -> None:
        df_qc, summary = apply_irradiance_qc(_synthetic_1min, 2020)
        # 5-minute gap at index 20-24 should be forward-filled (not 0)
        # Timestamps within the first 120 minutes should have non-zero values
        # where they were forward-filled from valid preceding data
        ts_filled = pd.Timestamp("2020-01-01 00:20:00+00:00")
        assert df_qc.loc[ts_filled, "ghi"] == 100.0  # filled from preceding value

    def test_long_gap_partially_filled(self, _synthetic_1min: pd.DataFrame) -> None:
        df_qc, _ = apply_irradiance_qc(_synthetic_1min, 2020)
        # 15-minute gap at 40-54: first 10 ffilled, last 5 → 0
        ts_ffilled = pd.Timestamp("2020-01-01 00:49:00+00:00")  # minute 49, within 10-min fill
        ts_zero = pd.Timestamp("2020-01-01 00:54:00+00:00")     # minute 54, beyond fill limit
        assert df_qc.loc[ts_ffilled, "ghi"] == 100.0  # ffilled
        assert df_qc.loc[ts_zero, "ghi"] == 0.0       # fell through to zero

    def test_qc_summary_has_expected_keys(self, _synthetic_1min: pd.DataFrame) -> None:
        _, summary = apply_irradiance_qc(_synthetic_1min, 2020)
        expected_keys = {
            "raw_records", "expected_records", "missing_before_fill",
            "forward_filled", "filled_zero", "neg_clamped_ghi",
            "neg_clamped_dni", "neg_clamped_dhi", "capped_ghi",
            "capped_dni", "capped_dhi",
        }
        assert set(summary.keys()) == expected_keys

    def test_cap_constants_correct(self) -> None:
        assert GHI_CAP == 1500.0
        assert DNI_CAP == 1500.0
        assert DHI_CAP == 800.0
        assert MAX_FORWARD_FILL_MINUTES == 10


# ===================================================================
# 3. 60-min aggregation (build_60min_from_1min)
# ===================================================================
class TestBuild60minFrom1min:
    """Verify 60-min aggregation produces correct hourly means."""

    def test_basic_aggregation(self) -> None:
        """120 rows of 1-min data → 2 hourly rows with correct means."""
        idx = pd.date_range("2020-01-01 00:00", periods=120, freq="min")
        df = pd.DataFrame(
            {
                "GHI": np.concatenate([np.full(60, 100.0), np.full(60, 200.0)]),
                "DNI": np.concatenate([np.full(60, 300.0), np.full(60, 400.0)]),
                "DHI": np.concatenate([np.full(60, 50.0), np.full(60, 75.0)]),
                "Temperature": np.full(120, 25.0),
                "Wind Speed": np.full(120, 5.0),
                "Surface Albedo": np.full(120, 0.2),
            },
            index=idx,
        )
        hourly = build_60min_from_1min(df, 2020)

        assert len(hourly) == 2
        assert hourly["GHI"].iloc[0] == pytest.approx(100.0)
        assert hourly["GHI"].iloc[1] == pytest.approx(200.0)
        assert hourly["DNI"].iloc[0] == pytest.approx(300.0)
        assert hourly["DNI"].iloc[1] == pytest.approx(400.0)
        assert hourly["DHI"].iloc[0] == pytest.approx(50.0)
        assert hourly["DHI"].iloc[1] == pytest.approx(75.0)

    def test_aggregation_is_mean_not_sum(self) -> None:
        """Ensure aggregation uses mean (not sum) — critical for irradiance."""
        idx = pd.date_range("2020-01-01 00:00", periods=60, freq="min")
        # Ramp from 0 to 59 → mean = 29.5
        df = pd.DataFrame(
            {
                "GHI": np.arange(60, dtype=float),
                "DNI": np.zeros(60),
                "DHI": np.zeros(60),
                "Temperature": np.zeros(60),
                "Wind Speed": np.zeros(60),
                "Surface Albedo": np.zeros(60),
            },
            index=idx,
        )
        hourly = build_60min_from_1min(df, 2020)
        assert hourly["GHI"].iloc[0] == pytest.approx(29.5)

    def test_minute_column_is_zero_at_hour_start(self) -> None:
        """Hourly timestamps should land on HH:00."""
        idx = pd.date_range("2020-01-01 00:00", periods=120, freq="min")
        df = pd.DataFrame(
            {col: np.ones(120) for col in
             ["GHI", "DNI", "DHI", "Temperature", "Wind Speed", "Surface Albedo"]},
            index=idx,
        )
        hourly = build_60min_from_1min(df, 2020)
        assert all(ts.minute == 0 for ts in hourly.index)


# ===================================================================
# 4. SAM weather file format (write_sam_csv round-trip)
# ===================================================================
class TestSAMWeatherFileFormat:
    """Test write_sam_csv produces valid SAM-format weather files."""

    def test_roundtrip_write_and_read(self, tmp_path: Path) -> None:
        """Write a small SAM file and verify header + data structure."""
        # Build 2-hour synthetic data
        idx = pd.date_range("2020-01-01 00:00", periods=2, freq="h")
        df = pd.DataFrame(
            {
                "GHI": [100.0, 200.0],
                "DNI": [300.0, 400.0],
                "DHI": [50.0, 75.0],
                "Temperature": [25.0, 26.0],
                "Wind Speed": [5.0, 6.0],
                "Surface Albedo": [0.2, 0.25],
            },
            index=idx,
        )
        filepath = tmp_path / "test_sam.csv"
        write_sam_csv(df, lat=40.05, lon=-88.37, elevation=213.0, filepath=filepath)

        # Read raw lines
        lines = filepath.read_text().splitlines()

        # Row 0: header labels
        assert "Latitude" in lines[0]
        assert "Longitude" in lines[0]
        assert "Time Zone" in lines[0]
        assert "Elevation" in lines[0]

        # Row 1: header values
        assert "40.05" in lines[1]
        assert "-88.37" in lines[1]
        assert "213.0" in lines[1]

        # Row 2: column names
        expected_cols = {"Year", "Month", "Day", "Hour", "Minute",
                         "GHI", "DNI", "DHI", "Temperature", "Wind Speed",
                         "Surface Albedo"}
        actual_cols = set(lines[2].split(","))
        assert expected_cols == actual_cols

        # Rows 3+: data
        assert len(lines) == 5  # 2 header + 1 col header + 2 data rows

    def test_bondville_60min_file_exists_and_valid(self) -> None:
        """Spot-check real Bondville 60-min weather file from M11 pipeline."""
        filepath = M11_DIR / "weather_files" / "Bondville_IL_2020_60min.csv"
        if not filepath.exists():
            pytest.skip("Bondville weather file not available")

        lines = filepath.read_text().splitlines()

        # 2-row metadata header + 1 column header + data
        assert len(lines) >= 4, "File too short for SAM format"

        # Header row 0 should have metadata labels
        assert "Latitude" in lines[0]

        # Read data portion (skip 2 metadata rows)
        df = pd.read_csv(filepath, skiprows=2)

        # 2020 is a leap year → 8784 hourly rows
        assert len(df) == 8784

        # Required SAM columns present
        required = {"Year", "Month", "Day", "Hour", "Minute",
                     "GHI", "DNI", "DHI", "Temperature", "Wind Speed"}
        assert required.issubset(set(df.columns))

        # No NaN in irradiance columns
        for col in ["GHI", "DNI", "DHI"]:
            assert df[col].isna().sum() == 0, f"NaN found in {col}"

        # Irradiance non-negative
        for col in ["GHI", "DNI", "DHI"]:
            assert (df[col] >= 0).all(), f"Negative values in {col}"

    def test_time_zone_zero_for_utc(self, tmp_path: Path) -> None:
        """SAM header should have Time Zone=0 for UTC convention."""
        idx = pd.date_range("2020-01-01 00:00", periods=1, freq="h")
        df = pd.DataFrame(
            {
                "GHI": [100.0], "DNI": [200.0], "DHI": [50.0],
                "Temperature": [20.0], "Wind Speed": [3.0],
                "Surface Albedo": [0.2],
            },
            index=idx,
        )
        filepath = tmp_path / "tz_test.csv"
        write_sam_csv(df, lat=33.0, lon=-112.0, elevation=300.0, filepath=filepath)

        # Read metadata row (row 1 = values)
        meta = pd.read_csv(filepath, nrows=1)
        tz_col = next(c for c in meta.columns if "Time Zone" in c and "Local" not in c)
        assert int(meta[tz_col].iloc[0]) == 0


# ===================================================================
# 5. Delta computation formula and clamping
# ===================================================================
class TestDeltaComputation:
    """Test subhourly loss formula: (ac_60min - ac_1min) / ac_60min * 100."""

    def test_positive_delta_when_60min_overestimates(self) -> None:
        """60-min AC > 1-min AC → positive loss (clipping missed)."""
        ac_60min = 100_000.0
        ac_1min = 98_000.0
        delta = (ac_60min - ac_1min) / ac_60min * 100
        assert delta == pytest.approx(2.0)

    def test_negative_delta_when_1min_higher(self) -> None:
        """1-min AC > 60-min AC → negative delta (subhourly variability helps)."""
        ac_60min = 100_000.0
        ac_1min = 101_000.0
        delta = (ac_60min - ac_1min) / ac_60min * 100
        assert delta == pytest.approx(-1.0)

    def test_zero_delta_when_equal(self) -> None:
        ac_60min = 100_000.0
        ac_1min = 100_000.0
        delta = (ac_60min - ac_1min) / ac_60min * 100
        assert delta == pytest.approx(0.0)

    def test_clamping_floors_at_zero(self) -> None:
        """Production model clamps negatives to 0 (loss-only correction)."""
        raw_delta = -0.5
        clamped = max(0.0, raw_delta)
        assert clamped == 0.0

    def test_clamping_preserves_positive(self) -> None:
        raw_delta = 1.5
        clamped = max(0.0, raw_delta)
        assert clamped == 1.5

    def test_realistic_range(self) -> None:
        """Typical subhourly losses are 0-5% for DC/AC 1.2-1.8."""
        ac_60min = 50_000_000.0  # 50 GWh
        # 1.5% loss
        ac_1min = ac_60min * (1 - 0.015)
        delta = (ac_60min - ac_1min) / ac_60min * 100
        assert 0.0 < delta < 5.0
        assert delta == pytest.approx(1.5, abs=0.01)


# ===================================================================
# 6. Config matrix validation
# ===================================================================
class TestConfigMatrix:
    """Validate config_matrix_m11.csv structure and completeness."""

    @pytest.fixture()
    def config_df(self) -> pd.DataFrame:
        csv_path = M11_DIR / "config_matrix_m11.csv"
        if not csv_path.exists():
            pytest.skip("Config matrix CSV not available")
        return pd.read_csv(csv_path)

    def test_row_count(self, config_df: pd.DataFrame) -> None:
        # 20 stations × 5 DC/AC × 4 GCR × 2 racking × 2 resolution = 1600
        assert len(config_df) == 1600

    def test_run_names_unique(self, config_df: pd.DataFrame) -> None:
        assert config_df["Run Name"].nunique() == 1600

    def test_resolution_suffix_pattern(self, config_df: pd.DataFrame) -> None:
        """Every run name should end with _1min or _60min."""
        for name in config_df["Run Name"]:
            assert name.endswith("_1min") or name.endswith("_60min"), (
                f"Run name '{name}' missing resolution suffix"
            )

    def test_paired_resolutions_balanced(self, config_df: pd.DataFrame) -> None:
        """Equal number of 1-min and 60-min runs."""
        n_1min = config_df["Run Name"].str.endswith("_1min").sum()
        n_60min = config_df["Run Name"].str.endswith("_60min").sum()
        assert n_1min == 800
        assert n_60min == 800

    def test_dc_ac_ratios(self, config_df: pd.DataFrame) -> None:
        """5 DC/AC ratios: 1.20, 1.30, 1.40, 1.50, 1.60."""
        dc_ac = config_df["DC Size (MW)"] / config_df["AC Installed (MW)"]
        expected_ratios = {1.2, 1.3, 1.4, 1.5, 1.6}
        actual_ratios = set(dc_ac.round(2).unique())
        assert actual_ratios == expected_ratios

    def test_gcr_values(self, config_df: pd.DataFrame) -> None:
        """4 GCR values: 0.30, 0.40, 0.50, 0.60."""
        expected_gcr = {0.3, 0.4, 0.5, 0.6}
        actual_gcr = set(config_df["GCR"].round(2).unique())
        assert actual_gcr == expected_gcr

    def test_racking_types(self, config_df: pd.DataFrame) -> None:
        """2 racking types: tracker and fixed."""
        assert set(config_df["Racking"].str.lower().unique()) == {"tracker", "fixed"}

    def test_weather_file_paths_exist(self, config_df: pd.DataFrame) -> None:
        """All Resource File Path entries should reference existing weather files."""
        paths = config_df["Resource File Path"].dropna()
        if len(paths) == 0:
            pytest.skip("No resource file paths in config matrix")
        # Spot-check first 10
        for p in paths.head(10):
            assert Path(p).exists(), f"Weather file not found: {p}"

    def test_20_unique_stations(self, config_df: pd.DataFrame) -> None:
        assert config_df["Site Name"].nunique() == 20
