"""Combined BESS + solar timeseries CSV writer.

Builds a single DataFrame from in-memory solar production, load profile,
and BESS dispatch results, then writes it as a CSV.  No disk reads — all
data arrives via function parameters.
"""

from pathlib import Path

import pandas as pd

from src.bess.models import DispatchResult
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Original PySAM diagnostic columns carried through from hourly_data
_SOLAR_DIAG_COLUMNS = [
    "ac_gross",
    "ac_net",
    "dc_net",
    "poa_irradiance",
    "poa_nominal",
    "cell_temperature",
    "inverter_efficiency",
]

# Column order for BTM (behind-the-meter) combined timeseries
_BTM_COLUMNS = [
    "timestamp",
    *_SOLAR_DIAG_COLUMNS,
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

# Column order for FTM (front-of-meter) combined timeseries
_FTM_COLUMNS = [
    "timestamp",
    *_SOLAR_DIAG_COLUMNS,
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


def write_bess_timeseries(
    hourly_data: pd.DataFrame,
    production_kwh: list[float],
    load_kwh: list[float] | None,
    dispatch_result: DispatchResult,
    output_path: Path,
    dispatch_mode: str = "btm",
) -> Path:
    """Write a combined solar + BESS timeseries CSV from in-memory data.

    Merges solar production, load profile, and hourly BESS dispatch into
    a single DataFrame and writes it as CSV, overwriting any existing file
    at output_path.

    Args:
        hourly_data: PySAM hourly DataFrame (must contain 'timestamp' column).
        production_kwh: 8760 hourly solar production in kW (shading-adjusted).
        load_kwh: 8760 hourly load in kW.  Required for BTM, may be None
            for FTM (zeros used if absent).
        dispatch_result: Full-year BESS dispatch result with .hourly_dispatch.
        output_path: File path to write the CSV to.
        dispatch_mode: 'btm' or 'ftm'.

    Returns:
        Path to the written CSV file.
    """
    hourly = dispatch_result.hourly_dispatch

    # Base columns from dispatch
    charge = [h.charge_kw for h in hourly]
    discharge = [h.discharge_kw for h in hourly]
    soc = [h.soc_kwh for h in hourly]
    curtailed = [h.curtailed_kw for h in hourly]
    net_load = [h.net_load_kw for h in hourly]
    grid_charge = [h.grid_charge_kw for h in hourly]

    # Derived: solar_to_battery = charge - grid_charge, clamped >= 0
    solar_to_battery = [max(0.0, c - gc) for c, gc in zip(charge, grid_charge)]

    # Solar diagnostic columns from PySAM hourly DataFrame
    diag = {
        col: hourly_data[col].values
        for col in _SOLAR_DIAG_COLUMNS
        if col in hourly_data.columns
    }

    if dispatch_mode == "ftm":
        solar_export = [h.solar_export_kw for h in hourly]

        df = pd.DataFrame({
            "timestamp": hourly_data["timestamp"].values,
            **diag,
            "solar_production_kw": production_kwh,
            "battery_charge_kw": charge,
            "battery_discharge_kw": discharge,
            "battery_soc_kwh": soc,
            "solar_to_battery_kw": solar_to_battery,
            "solar_export_kw": solar_export,
            "grid_charge_kw": grid_charge,
            "solar_curtailed_kw": curtailed,
            "net_load_kw": net_load,
        })
        df = df[[c for c in _FTM_COLUMNS if c in df.columns]]
    else:
        load = load_kwh if load_kwh is not None else [0.0] * len(hourly)

        # Derived: solar_to_load = solar - curtailed - (charge - grid_charge)
        #          clamped >= 0, capped at load
        solar_to_load = [
            min(ld, max(0.0, sp - cur - max(0.0, c - gc)))
            for sp, ld, cur, c, gc in zip(
                production_kwh, load, curtailed, charge, grid_charge
            )
        ]

        # grid_import = max(0, net_load)
        grid_import = [max(0.0, nl) for nl in net_load]

        df = pd.DataFrame({
            "timestamp": hourly_data["timestamp"].values,
            **diag,
            "solar_production_kw": production_kwh,
            "load_kw": load,
            "battery_charge_kw": charge,
            "battery_discharge_kw": discharge,
            "battery_soc_kwh": soc,
            "solar_to_load_kw": solar_to_load,
            "solar_to_battery_kw": solar_to_battery,
            "battery_to_load_kw": discharge,
            "grid_import_kw": grid_import,
            "solar_curtailed_kw": curtailed,
            "net_load_kw": net_load,
        })
        df = df[[c for c in _BTM_COLUMNS if c in df.columns]]

    df.to_csv(output_path, index=False, float_format="%.4f")
    logger.info("BESS combined timeseries written: %s (%d rows)", output_path, len(df))
    return output_path
