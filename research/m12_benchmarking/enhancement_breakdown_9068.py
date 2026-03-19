"""Enhancement layer breakdown for PVDAQ 9068 (SR_CO, Kersey CO).

Shows the cumulative impact of each model correction layer on prediction
accuracy vs measured production. Each step builds on the previous one.

Steps:
    1. RAW:           Unmodified NSRDB TMY → PySAM (no corrections)
    2. + BIAS:        Apply NSRDB bias correction to weather data
    3. + SOILING:     Add dynamic soiling from ERA5 precipitation
    4. + SNOW:        Add ERA5 snow depth to weather file for PySAM snow model
    5. + SUBHOURLY:   Apply subhourly clipping correction after PySAM
    6. + AVAILABILITY: Apply 2.5% availability reduction

Measured baseline: 8,239 MWh/yr (5-year average 2018-2022, meter data)

Site: SR_CO, Kersey CO (40.3864, -104.5512), 4.738 MW DC tracker
Module: First Solar Inc. FS-4117-3 (CdTe, NOT bifacial)
Inverter: Sichuan Clou Energy Electric Co - Ltd : NEPCS-2000 [690V]
GCR: 0.38
"""

from pathlib import Path

import pandas as pd

from src.climate.open_meteo_client import fetch_era5_land_data
from src.climate.soiling_calculator import calculate_monthly_soiling
from src.climate.weather_formatter import WeatherFormatter
from src.config.schema import SiteConfig
from src.models.nsrdb_bias_correction import apply_bias_correction
from src.models.subhourly_correction import compute_weather_features, predict_correction
from src.pysam_integration.model_configurator import ModelConfigurator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TMY_CSV_PATH = Path("research/m12_benchmarking/output/40.3864_-104.5512_tmy.csv")
OUTPUT_DIR = Path("research/m12_benchmarking/output")

MEASURED_MWH = 8239.0

LAT = 40.3864
LON = -104.5512
ELEVATION_M = 1407.0
GCR = 0.38

# ERA5 year for TMY (production pipeline uses 2024 for TMY)
ERA5_YEAR = 2024

# Availability for final step
AVAILABILITY_PCT = 2.5

BASE_CONFIG = {
    "run_name": "9068_enhancement",
    "site_name": "SR_CO",
    "customer": "M12_Benchmarking",
    "latitude": LAT,
    "longitude": LON,
    "dc_size_mw": 4.738,
    "ac_installed_mw": 3.820,
    "ac_poi_mw": 3.820,
    "racking": "tracker",
    "tilt": 60,
    "azimuth": 180,
    "gcr": GCR,
    "module_orientation": "portrait",
    "number_of_modules": 2,
    "ground_clearance_height_m": 1.5,
    "panel_model": "First Solar Inc. FS-4117-3",
    "bifacial": False,
    "inverter_model": "Sichuan Clou Energy Electric Co - Ltd : NEPCS-2000 [690V]",
    "shading_percent": 0.0,
    "dc_wiring_loss_percent": 2.0,
    "ac_wiring_loss_percent": 1.0,
    "transformer_losses_percent": 0.0,
    "degradation_percent": 0.0,
    "availability_percent": 0.0,
    "module_mismatch_percent": 1.0,
    "lid_percent": 0.0,
    "report": False,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def run_pysam(
    weather_file: Path,
    monthly_soiling: list[float],
    availability_pct: float = 0.0,
) -> dict:
    """Run a single PySAM simulation.

    Args:
        weather_file: Path to PySAM-format weather CSV.
        monthly_soiling: 12-element list of monthly soiling loss percentages.
        availability_pct: Availability reduction percentage (0 = no downtime).

    Returns:
        Dict with annual_energy_mwh, cf_ac, dc_ac_ratio.
    """
    config = {**BASE_CONFIG, "availability_percent": availability_pct}
    site_config = SiteConfig(**config, weather_file_path=weather_file)

    configurator = ModelConfigurator()
    model_config = configurator.configure_model(
        site_config, monthly_soiling=monthly_soiling
    )

    model = model_config.model

    # PySAM quirk: cec_bifacial_ground_clearance_height must be > 0 even when
    # bifacial is disabled (cec_is_bifacial=0).
    cec = model.CECPerformanceModelWithModuleDatabase
    if cec.cec_is_bifacial == 0 and cec.cec_bifacial_ground_clearance_height <= 0:
        cec.cec_bifacial_ground_clearance_height = 0.001

    model.execute()

    outputs = model.Outputs
    return {
        "annual_energy_mwh": outputs.annual_energy / 1000.0,
        "cf_ac": outputs.capacity_factor_ac,
        "dc_ac_ratio": model_config.dc_ac_ratio,
    }


def fmt_error(mwh: float) -> str:
    """Format error vs measured as signed percentage string."""
    err = (mwh - MEASURED_MWH) / MEASURED_MWH * 100
    return f"{err:+.1f}%"


def fmt_delta(current_mwh: float, prev_mwh: float) -> str:
    """Format delta from previous step."""
    delta = current_mwh - prev_mwh
    return f"{delta:+.1f} MWh"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------------
    # Step 0: Prepare weather data and ERA5 data
    # -----------------------------------------------------------------------
    print("=" * 75)
    print("ENHANCEMENT LAYER BREAKDOWN — PVDAQ 9068 (SR_CO, Kersey CO)")
    print(f"Measured production: {MEASURED_MWH:,.0f} MWh/yr (2018-2022 average)")
    print(f"GCR: {GCR}")
    print("=" * 75)

    # Load and format raw TMY weather
    print("\n--- Preparing weather data ---")
    raw_csv = TMY_CSV_PATH.read_text()
    formatter = WeatherFormatter()
    df_weather_raw, metadata = formatter.format_for_pysam(
        nsrdb_csv=raw_csv, lat=LAT, lon=LON
    )
    print(f"  TMY source: {TMY_CSV_PATH}")
    print(f"  Weather rows: {len(df_weather_raw)}")
    print(f"  Raw GHI mean: {df_weather_raw['GHI'].mean():.1f} W/m²")
    print(f"  Raw DNI mean: {df_weather_raw['DNI'].mean():.1f} W/m²")

    # Apply bias correction
    df_weather_bias, bias_meta = apply_bias_correction(
        df_weather_raw, LAT, LON, ELEVATION_M
    )
    print(f"  Bias-corrected GHI mean: {df_weather_bias['GHI'].mean():.1f} W/m²")
    print(f"  Bias-corrected DNI mean: {df_weather_bias['DNI'].mean():.1f} W/m²")
    print(f"  Mean GHI CF: {bias_meta['mean_ghi_correction_factor']:.4f}")
    print(f"  Mean DNI CF: {bias_meta['mean_dni_correction_factor']:.4f}")

    # Fetch ERA5 data
    print(f"\n--- Fetching ERA5 data (year={ERA5_YEAR}) ---")
    era5_data = fetch_era5_land_data(LAT, LON, ERA5_YEAR)

    snow_depth_cm = None
    monthly_soiling_era5 = None

    if era5_data is not None:
        # Snow depth
        snow_raw = era5_data["snow_depth_cm"]
        print(f"  ERA5 snow_depth_cm: {len(snow_raw)} hours")

        # Trim leap day for TMY (Feb 29 = hours 1416-1439)
        if len(snow_raw) == 8784:
            snow_depth_cm = snow_raw[:1416] + snow_raw[1440:]
            print(f"  Trimmed leap day: {len(snow_depth_cm)} hours")
        else:
            snow_depth_cm = snow_raw
            print(f"  No leap day trim needed: {len(snow_depth_cm)} hours")

        max_snow = max(snow_depth_cm)
        snow_hours = sum(1 for s in snow_depth_cm if s > 0)
        print(f"  Max snow depth: {max_snow:.1f} cm")
        print(f"  Hours with snow: {snow_hours}")

        # Soiling from precipitation
        monthly_precip_inches = era5_data["monthly_precip_inches"]
        monthly_soiling_era5 = calculate_monthly_soiling(monthly_precip_inches)
        print(f"\n  Monthly precip (inches): {[f'{p:.2f}' for p in monthly_precip_inches]}")
        print(f"  Monthly soiling (%):     {monthly_soiling_era5}")
    else:
        print("  WARNING: ERA5 fetch failed. Using fallback values.")
        print("  Snow depth: all zeros (no snow loss)")
        print("  Soiling: [0.0]*12 (no soiling)")
        snow_depth_cm = None
        monthly_soiling_era5 = [0.0] * 12

    # -----------------------------------------------------------------------
    # Create weather files for each configuration
    # -----------------------------------------------------------------------
    print("\n--- Creating weather files ---")

    # File A: RAW (no bias, no snow)
    path_raw = OUTPUT_DIR / "9068_enh_raw.csv"
    formatter.save_to_csv(df_weather_raw, path_raw, lat=LAT, lon=LON, metadata=metadata)
    print(f"  RAW weather: {path_raw}")

    # File B: BIAS-corrected (no snow) — used for steps 2, 3
    path_bias = OUTPUT_DIR / "9068_enh_bias.csv"
    formatter.save_to_csv(df_weather_bias, path_bias, lat=LAT, lon=LON, metadata=metadata)
    print(f"  BIAS weather: {path_bias}")

    # File C: BIAS-corrected + SNOW — used for step 4+
    if snow_depth_cm is not None and len(snow_depth_cm) == len(df_weather_bias):
        df_weather_bias_snow = df_weather_bias.copy()
        df_weather_bias_snow["Snow Depth"] = snow_depth_cm
    else:
        df_weather_bias_snow = df_weather_bias.copy()
        print("  WARNING: Snow depth length mismatch or missing. Using zeros.")

    path_bias_snow = OUTPUT_DIR / "9068_enh_bias_snow.csv"
    formatter.save_to_csv(
        df_weather_bias_snow, path_bias_snow, lat=LAT, lon=LON, metadata=metadata
    )
    print(f"  BIAS+SNOW weather: {path_bias_snow}")

    # Soiling configs
    no_soiling = [0.0] * 12
    soiling = monthly_soiling_era5 if monthly_soiling_era5 is not None else [0.0] * 12

    # -----------------------------------------------------------------------
    # Run 6 cumulative configurations
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 75}")
    print("RUNNING 6 CUMULATIVE CONFIGURATIONS")
    print("=" * 75)

    results = []

    # --- Step 1: RAW ---
    print("\n[1/6] RAW: unmodified NSRDB TMY → PySAM")
    print("       Weather: raw, Soiling: 0%, Snow: none, Avail: 0%")
    r1 = run_pysam(path_raw, monthly_soiling=no_soiling, availability_pct=0.0)
    mwh_1 = r1["annual_energy_mwh"]
    print(f"       → {mwh_1:,.1f} MWh  ({fmt_error(mwh_1)})")
    results.append(("1. Raw", mwh_1))

    # --- Step 2: + BIAS ---
    print("\n[2/6] + BIAS: NSRDB bias correction applied")
    print("       Weather: bias-corrected, Soiling: 0%, Snow: none, Avail: 0%")
    r2 = run_pysam(path_bias, monthly_soiling=no_soiling, availability_pct=0.0)
    mwh_2 = r2["annual_energy_mwh"]
    print(f"       → {mwh_2:,.1f} MWh  ({fmt_error(mwh_2)})  Δ = {fmt_delta(mwh_2, mwh_1)}")
    results.append(("2. + Bias Corr", mwh_2))

    # --- Step 3: + SOILING ---
    print("\n[3/6] + SOILING: ERA5 precipitation-based dynamic soiling")
    print(f"       Weather: bias-corrected, Soiling: {soiling}, Snow: none, Avail: 0%")
    r3 = run_pysam(path_bias, monthly_soiling=soiling, availability_pct=0.0)
    mwh_3 = r3["annual_energy_mwh"]
    print(f"       → {mwh_3:,.1f} MWh  ({fmt_error(mwh_3)})  Δ = {fmt_delta(mwh_3, mwh_2)}")
    results.append(("3. + Soiling", mwh_3))

    # --- Step 4: + SNOW ---
    print("\n[4/6] + SNOW: ERA5 snow depth in weather file for PySAM snow model")
    print("       Weather: bias-corrected + snow, Soiling: ERA5, Snow: ERA5, Avail: 0%")
    r4 = run_pysam(path_bias_snow, monthly_soiling=soiling, availability_pct=0.0)
    mwh_4 = r4["annual_energy_mwh"]
    print(f"       → {mwh_4:,.1f} MWh  ({fmt_error(mwh_4)})  Δ = {fmt_delta(mwh_4, mwh_3)}")
    results.append(("4. + Snow", mwh_4))

    # --- Step 5: + SUBHOURLY ---
    print("\n[5/6] + SUBHOURLY: subhourly clipping correction (post-PySAM)")
    # Compute weather features from the bias+snow weather file
    weather_features = compute_weather_features(path_bias_snow)
    subhourly_loss = predict_correction(
        dcac_ratio=r4["dc_ac_ratio"],
        gcr=GCR,
        racking="tracker",
        latitude=LAT,
        longitude=LON,
        cf_60min=r4["cf_ac"],
        weather_features=weather_features,
    )
    mwh_5 = mwh_4 * (1 - subhourly_loss / 100.0)
    print(f"       Subhourly correction: {subhourly_loss:.3f}%")
    print(f"       → {mwh_5:,.1f} MWh  ({fmt_error(mwh_5)})  Δ = {fmt_delta(mwh_5, mwh_4)}")
    results.append(("5. + Subhourly", mwh_5))

    # --- Step 6: + AVAILABILITY ---
    print(f"\n[6/6] + AVAILABILITY: {AVAILABILITY_PCT}% availability reduction")
    mwh_6 = mwh_5 * (1 - AVAILABILITY_PCT / 100.0)
    print(f"       → {mwh_6:,.1f} MWh  ({fmt_error(mwh_6)})  Δ = {fmt_delta(mwh_6, mwh_5)}")
    results.append(("6. + Availability", mwh_6))

    # -----------------------------------------------------------------------
    # Summary Table
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 75}")
    print("ENHANCEMENT LAYER WATERFALL — PVDAQ 9068 (SR_CO, Kersey CO)")
    print(f"Measured production: {MEASURED_MWH:,.0f} MWh/yr  |  GCR: {GCR}")
    print("=" * 75)

    header = (
        f"{'Step':<22} | {'Annual AC (MWh)':>15} | {'Error vs Measured':>17} "
        f"| {'Delta from Previous':>20}"
    )
    print(header)
    print("-" * len(header))

    prev_mwh = None
    for label, mwh in results:
        err = (mwh - MEASURED_MWH) / MEASURED_MWH * 100
        if prev_mwh is None:
            delta_str = "—"
        else:
            delta = mwh - prev_mwh
            delta_str = f"{delta:+.1f} MWh ({delta / MEASURED_MWH * 100:+.2f} pp)"
        print(
            f"{label:<22} | {mwh:>15,.1f} | {err:>+16.1f}% "
            f"| {delta_str:>20}"
        )
        prev_mwh = mwh

    # -----------------------------------------------------------------------
    # Details
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 75}")
    print("DETAILS")
    print("=" * 75)
    print(f"Bias correction model: v{bias_meta['bias_correction_model_version']}")
    print(f"  Monthly GHI factors: {bias_meta['monthly_ghi_factors']}")
    print(f"  Monthly DNI factors: {bias_meta['monthly_dni_factors']}")
    if monthly_soiling_era5 is not None:
        print(f"Soiling (precipitation threshold table from production pipeline):")
        print(f"  Monthly soiling losses: {monthly_soiling_era5}")
    print(f"Snow: ERA5 year={ERA5_YEAR}, trimmed leap day for TMY")
    if snow_depth_cm is not None:
        print(f"  Max depth: {max(snow_depth_cm):.1f} cm, snow hours: {sum(1 for s in snow_depth_cm if s > 0)}")
    print(f"Subhourly correction: {subhourly_loss:.3f}%")
    print(f"Availability: {AVAILABILITY_PCT}%")
    print(f"DC/AC ratio: {r4['dc_ac_ratio']:.3f}")

    # Biggest impact layer
    deltas = []
    for i in range(1, len(results)):
        label, mwh = results[i]
        _, prev = results[i - 1]
        deltas.append((label, mwh - prev))

    biggest = min(deltas, key=lambda x: abs(x[1]))  # Closest to zero = least impact
    biggest = max(deltas, key=lambda x: abs(x[1]))  # Farthest from zero = most impact
    print(f"\nLargest single-layer impact: {biggest[0]} ({biggest[1]:+.1f} MWh)")


if __name__ == "__main__":
    main()
