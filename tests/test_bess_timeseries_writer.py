"""Tests for src.bess.timeseries_writer — combined BESS + solar CSV."""

import pandas as pd
import pytest

from src.bess.models import BESSConfig, BatteryMetrics, DispatchResult, HourlyDispatch
from src.bess.timeseries_writer import write_bess_timeseries


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config() -> BESSConfig:
    """Minimal BESSConfig for test fixtures."""
    return BESSConfig(
        bess_power_kw=1000.0,
        bess_duration_hr=4.0,
        capacity_kwh=4000.0,
        usable_capacity_kwh=3200.0,
        min_soc_kwh=400.0,
        max_soc_kwh=3600.0,
        charge_efficiency=0.92,
        discharge_efficiency=0.92,
        degradation_cost_per_kwh=0.01,
        strategy="global",
    )


def _make_metrics() -> BatteryMetrics:
    """Minimal BatteryMetrics for test fixtures."""
    return BatteryMetrics(
        annual_throughput_kwh=100.0,
        annual_cycles=1.0,
        average_daily_cycles=0.003,
        capacity_utilization_pct=10.0,
        average_discharge_depth_pct=50.0,
        max_discharge_depth_pct=80.0,
        total_curtailed_kwh=0.0,
        hours_charging=10,
        hours_discharging=10,
        hours_idle=8740,
        estimated_annual_degradation_pct=1.0,
    )


def _make_hourly_dispatch(
    n: int,
    *,
    charge: float = 0.0,
    discharge: float = 0.0,
    soc: float = 400.0,
    net_load: float = 50.0,
    curtailed: float = 0.0,
    export: float = 0.0,
    solar_export: float = 0.0,
    grid_charge: float = 0.0,
) -> list[HourlyDispatch]:
    """Build a list of n identical HourlyDispatch objects."""
    return [
        HourlyDispatch(
            hour=i,
            charge_kw=charge,
            discharge_kw=discharge,
            soc_kwh=soc,
            net_load_kw=net_load,
            curtailed_kw=curtailed,
            export_kw=export,
            solar_export_kw=solar_export,
            grid_charge_kw=grid_charge,
        )
        for i in range(n)
    ]


def _make_dispatch_result(hourly: list[HourlyDispatch]) -> DispatchResult:
    """Wrap hourly dispatch in a DispatchResult."""
    return DispatchResult(
        config=_make_config(),
        hourly_dispatch=hourly,
        monthly_solve_status=["Optimal"] * 12,
        metrics=_make_metrics(),
    )


def _make_hourly_data(n: int) -> pd.DataFrame:
    """PySAM hourly DataFrame with timestamp and all diagnostic columns."""
    return pd.DataFrame({
        "timestamp": pd.date_range("2023-01-01", periods=n, freq="h"),
        "ac_gross": [100.0] * n,
        "ac_net": [95.0] * n,
        "dc_net": [110.0] * n,
        "poa_irradiance": [800.0] * n,
        "poa_nominal": [850.0] * n,
        "cell_temperature": [45.0] * n,
        "inverter_efficiency": [97.5] * n,
    })


# ---------------------------------------------------------------------------
# BTM column tests
# ---------------------------------------------------------------------------

class TestBTMColumns:
    """Verify BTM CSV has the correct columns in the correct order."""

    def test_btm_column_names_and_order(self, tmp_path: "Path") -> None:
        """BTM CSV contains exactly the 19 expected columns in order."""
        n = 24
        hourly = _make_hourly_dispatch(n, charge=50.0, discharge=0.0, net_load=80.0)
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test_8760.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[200.0] * n,
            load_kwh=[150.0] * n,
            dispatch_result=dr,
            output_path=out,
            dispatch_mode="btm",
        )

        df = pd.read_csv(out)
        expected = [
            "timestamp",
            "ac_gross",
            "ac_net",
            "dc_net",
            "poa_irradiance",
            "poa_nominal",
            "cell_temperature",
            "inverter_efficiency",
            "solar_production_kw",
            "load_kw",
            "battery_charge_kw",
            "battery_discharge_kw",
            "battery_soc_kwh",
            "solar_to_load_kw",
            "solar_to_battery_kw",
            "battery_to_load_kw",
            "grid_import_kw",
            "solar_curtailed_kw",
            "net_load_kw",
        ]
        assert list(df.columns) == expected

    def test_btm_row_count(self, tmp_path: "Path") -> None:
        """BTM CSV has one row per hour."""
        n = 48
        hourly = _make_hourly_dispatch(n)
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[100.0] * n,
            load_kwh=[80.0] * n,
            dispatch_result=dr,
            output_path=out,
        )

        df = pd.read_csv(out)
        assert len(df) == n


# ---------------------------------------------------------------------------
# FTM column tests
# ---------------------------------------------------------------------------

class TestFTMColumns:
    """Verify FTM CSV has the correct columns in the correct order."""

    def test_ftm_column_names_and_order(self, tmp_path: "Path") -> None:
        """FTM CSV contains exactly the 17 expected columns in order."""
        n = 24
        hourly = _make_hourly_dispatch(
            n, solar_export=100.0, grid_charge=20.0, charge=50.0,
        )
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test_ftm.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[300.0] * n,
            load_kwh=None,
            dispatch_result=dr,
            output_path=out,
            dispatch_mode="ftm",
        )

        df = pd.read_csv(out)
        expected = [
            "timestamp",
            "ac_gross",
            "ac_net",
            "dc_net",
            "poa_irradiance",
            "poa_nominal",
            "cell_temperature",
            "inverter_efficiency",
            "solar_production_kw",
            "battery_charge_kw",
            "battery_discharge_kw",
            "battery_soc_kwh",
            "solar_to_battery_kw",
            "solar_export_kw",
            "grid_charge_kw",
            "solar_curtailed_kw",
            "net_load_kw",
        ]
        assert list(df.columns) == expected

    def test_ftm_no_load_column(self, tmp_path: "Path") -> None:
        """FTM CSV must not contain a load_kw column."""
        n = 24
        hourly = _make_hourly_dispatch(n)
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test_ftm.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[100.0] * n,
            load_kwh=None,
            dispatch_result=dr,
            output_path=out,
            dispatch_mode="ftm",
        )

        df = pd.read_csv(out)
        assert "load_kw" not in df.columns
        assert "battery_to_load_kw" not in df.columns
        assert "grid_import_kw" not in df.columns
        assert "solar_to_load_kw" not in df.columns


# ---------------------------------------------------------------------------
# Derived column correctness
# ---------------------------------------------------------------------------

class TestDerivedColumns:
    """Verify derived columns are mathematically correct."""

    def test_grid_import_equals_max_zero_net_load(self, tmp_path: "Path") -> None:
        """grid_import_kw == max(0, net_load_kw) for each row."""
        n = 24
        # Mix positive and negative net_load values
        hourly = []
        for i in range(n):
            nl = 100.0 - i * 10  # ranges from 100 down to -130
            hourly.append(HourlyDispatch(
                hour=i, charge_kw=0.0, discharge_kw=0.0, soc_kwh=400.0,
                net_load_kw=nl, curtailed_kw=0.0,
            ))
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[200.0] * n,
            load_kwh=[200.0] * n,
            dispatch_result=dr,
            output_path=out,
        )

        df = pd.read_csv(out)
        for _, row in df.iterrows():
            assert row["grid_import_kw"] == pytest.approx(
                max(0.0, row["net_load_kw"]), abs=1e-4
            )

    def test_solar_to_battery_equals_charge_minus_grid_charge(
        self, tmp_path: "Path"
    ) -> None:
        """solar_to_battery_kw == max(0, charge_kw - grid_charge_kw)."""
        n = 24
        hourly = []
        expected_stb = []
        for i in range(n):
            # Vary charge and grid_charge
            c = float(i * 10)
            gc = float(min(i * 5, c))  # grid_charge <= charge
            hourly.append(HourlyDispatch(
                hour=i, charge_kw=c, discharge_kw=0.0, soc_kwh=400.0,
                net_load_kw=50.0, curtailed_kw=0.0, grid_charge_kw=gc,
            ))
            expected_stb.append(max(0.0, c - gc))
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[500.0] * n,
            load_kwh=[200.0] * n,
            dispatch_result=dr,
            output_path=out,
        )

        df = pd.read_csv(out)
        assert df["solar_to_battery_kw"].tolist() == pytest.approx(
            expected_stb, abs=1e-4
        )

    def test_solar_to_battery_clamped_to_zero(self, tmp_path: "Path") -> None:
        """solar_to_battery_kw >= 0 even when grid_charge > charge (edge case)."""
        n = 4
        # grid_charge_kw == charge_kw (all charging from grid)
        hourly = _make_hourly_dispatch(n, charge=100.0, grid_charge=100.0)
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[0.0] * n,
            load_kwh=[100.0] * n,
            dispatch_result=dr,
            output_path=out,
        )

        df = pd.read_csv(out)
        assert (df["solar_to_battery_kw"] >= -1e-10).all()

    def test_solar_to_load_clamped_and_capped(self, tmp_path: "Path") -> None:
        """solar_to_load_kw is >= 0 and <= load_kw for each row."""
        n = 24
        hourly = []
        for i in range(n):
            hourly.append(HourlyDispatch(
                hour=i, charge_kw=float(i * 5), discharge_kw=0.0,
                soc_kwh=400.0, net_load_kw=50.0,
                curtailed_kw=float(max(0, i - 15) * 10),
            ))
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[300.0] * n,
            load_kwh=[150.0] * n,
            dispatch_result=dr,
            output_path=out,
        )

        df = pd.read_csv(out)
        assert (df["solar_to_load_kw"] >= -1e-10).all()
        assert (df["solar_to_load_kw"] <= df["load_kw"] + 1e-10).all()

    def test_battery_to_load_equals_discharge(self, tmp_path: "Path") -> None:
        """battery_to_load_kw equals discharge_kw for BTM."""
        n = 12
        hourly = _make_hourly_dispatch(n, discharge=75.0, net_load=25.0)
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[100.0] * n,
            load_kwh=[100.0] * n,
            dispatch_result=dr,
            output_path=out,
        )

        df = pd.read_csv(out)
        assert (df["battery_to_load_kw"] == df["battery_discharge_kw"]).all()


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Test edge conditions: zero solar, full battery, curtailment."""

    def test_zero_solar_hour(self, tmp_path: "Path") -> None:
        """When solar is zero, solar_to_load and solar_to_battery are zero."""
        n = 4
        hourly = _make_hourly_dispatch(n, net_load=100.0)
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[0.0] * n,
            load_kwh=[100.0] * n,
            dispatch_result=dr,
            output_path=out,
        )

        df = pd.read_csv(out)
        assert (df["solar_to_load_kw"] == 0.0).all()
        assert (df["solar_to_battery_kw"] == 0.0).all()
        assert (df["solar_curtailed_kw"] == 0.0).all()

    def test_full_battery_with_curtailment(self, tmp_path: "Path") -> None:
        """When battery is full and load is met, excess is curtailed."""
        n = 4
        # Solar 500 kW, load 100 kW, no charging, curtailed 400 kW
        hourly = _make_hourly_dispatch(
            n, charge=0.0, discharge=0.0, soc=3600.0,
            net_load=0.0, curtailed=400.0,
        )
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[500.0] * n,
            load_kwh=[100.0] * n,
            dispatch_result=dr,
            output_path=out,
        )

        df = pd.read_csv(out)
        assert (df["solar_curtailed_kw"] == 400.0).all()
        assert (df["solar_to_load_kw"] == 100.0).all()
        assert (df["solar_to_battery_kw"] == 0.0).all()

    def test_discharge_serving_load(self, tmp_path: "Path") -> None:
        """During nighttime, battery discharge serves load and grid covers rest."""
        n = 4
        # No solar, load 200 kW, discharge 150 kW, net_load 50 kW (grid)
        hourly = _make_hourly_dispatch(
            n, charge=0.0, discharge=150.0, soc=2000.0,
            net_load=50.0,
        )
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[0.0] * n,
            load_kwh=[200.0] * n,
            dispatch_result=dr,
            output_path=out,
        )

        df = pd.read_csv(out)
        assert (df["battery_to_load_kw"] == 150.0).all()
        assert (df["grid_import_kw"] == 50.0).all()
        assert (df["solar_to_load_kw"] == 0.0).all()

    def test_ftm_with_none_load(self, tmp_path: "Path") -> None:
        """FTM mode works with load_kwh=None (no parasitic load)."""
        n = 8
        hourly = _make_hourly_dispatch(n, solar_export=200.0, charge=50.0)
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test.csv"

        result = write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[300.0] * n,
            load_kwh=None,
            dispatch_result=dr,
            output_path=out,
            dispatch_mode="ftm",
        )

        assert result == out
        df = pd.read_csv(out)
        assert len(df) == n
        assert "load_kw" not in df.columns

    def test_overwrites_existing_file(self, tmp_path: "Path") -> None:
        """write_bess_timeseries overwrites an existing file at output_path."""
        out = tmp_path / "existing.csv"
        out.write_text("old,data\n1,2\n")

        n = 4
        hourly = _make_hourly_dispatch(n)
        dr = _make_dispatch_result(hourly)

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[100.0] * n,
            load_kwh=[80.0] * n,
            dispatch_result=dr,
            output_path=out,
        )

        df = pd.read_csv(out)
        assert "solar_production_kw" in df.columns
        assert "old" not in df.columns

    def test_ftm_solar_to_battery_with_grid_charge(self, tmp_path: "Path") -> None:
        """FTM solar_to_battery correctly subtracts grid_charge from total charge."""
        n = 4
        # charge=100, grid_charge=40 → solar_to_battery=60
        hourly = _make_hourly_dispatch(
            n, charge=100.0, grid_charge=40.0, solar_export=50.0,
        )
        dr = _make_dispatch_result(hourly)
        out = tmp_path / "test.csv"

        write_bess_timeseries(
            hourly_data=_make_hourly_data(n),
            production_kwh=[200.0] * n,
            load_kwh=None,
            dispatch_result=dr,
            output_path=out,
            dispatch_mode="ftm",
        )

        df = pd.read_csv(out)
        assert df["solar_to_battery_kw"].tolist() == pytest.approx(
            [60.0] * n, abs=1e-4
        )
        assert df["grid_charge_kw"].tolist() == pytest.approx(
            [40.0] * n, abs=1e-4
        )
