"""PVDAQ 9068 (SR_CO) Benchmark — PySAM vs Measured Production.

Runs 6 simulations: 3 GCR values × 2 modes (RAW / CORRECTED).
RAW: NSRDB TMY → PySAM simulation
CORRECTED: NSRDB TMY → bias correction → PySAM → subhourly correction

Measured baseline: 8,239 MWh/yr (2018-2022 average, meter data)

Site: SR_CO, Kersey CO (40.3864, -104.5512), 4.738 MW DC tracker
Module: First Solar Inc. FS-4117-3 (CdTe, 117.768W, NOT bifacial)
Inverter: Sichuan Clou Energy Electric Co - Ltd : NEPCS-2000 [690V] (proxy)

Assumptions documented:
- ground_clearance_height_m = 1.5 (assumed, not in PVDAQ metadata)
- number_of_modules = 2 (modules per rack, standard for portrait CdTe)
- availability_percent = 0 (schema field = downtime %; 0 = 100% available)
- monthly_soiling = [0.0]*12 (explicit; default is 5% which is not desired)
- degradation_percent = 0 (benchmarking year-1, no degradation)
- shading_percent = 0 (tracker with backtracking handles self-shading)
- GCR unknown from metadata — sensitivity at 0.30, 0.34, 0.38
"""

from pathlib import Path

import pandas as pd

from src.climate.weather_formatter import WeatherFormatter
from src.config.schema import SiteConfig
from src.models.nsrdb_bias_correction import apply_bias_correction
from src.models.subhourly_correction import compute_weather_features, predict_correction
from src.pysam_integration.model_configurator import ModelConfigurator

# Paths
TMY_CSV_PATH = Path("research/m12_benchmarking/output/40.3864_-104.5512_tmy.csv")
OUTPUT_DIR = Path("research/m12_benchmarking/output")

# Measured baseline (2018-2022 average from 9068_annual_summary.csv)
MEASURED_MWH = 8239.0

# Site coordinates and elevation
LAT = 40.3864
LON = -104.5512
ELEVATION_M = 1407.0

# GCR sensitivity values
GCR_VALUES = [0.30, 0.34, 0.38]

# Base config dict (GCR set per-run)
BASE_CONFIG = {
    "run_name": "9068_benchmark",
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
    "module_orientation": "portrait",
    "number_of_modules": 2,
    "ground_clearance_height_m": 1.5,
    "panel_model": "First Solar Inc. FS-4117-3",
    "bifacial": False,
    "inverter_model": "Sichuan Clou Energy Electric Co - Ltd : NEPCS-2000 [690V]",
    "shading_percent": 0.0,
    "dc_wiring_loss_percent": 2.0,
    "ac_wiring_loss_percent": 1.0,
    "transformer_losses_percent": 1.0,
    "degradation_percent": 0.0,
    # availability_percent = downtime %. 0 = no downtime = 100% available.
    # (Schema inverts: PySAM adjust_constant = availability_percent directly)
    "availability_percent": 0.0,
    "module_mismatch_percent": 1.0,
    "lid_percent": 0.0,
    "report": False,
}

# No soiling for benchmark (pipeline default would be 5%)
MONTHLY_SOILING = [0.0] * 12


def run_pysam(weather_file: Path, gcr: float) -> dict:
    """Run a single PySAM simulation and return results.

    Args:
        weather_file: Path to PySAM-format weather CSV.
        gcr: Ground coverage ratio for this run.

    Returns:
        Dict with annual_energy_kwh, annual_energy_mwh, cf_ac, cf_dc,
        string_config details.
    """
    config = {**BASE_CONFIG, "gcr": gcr}
    site_config = SiteConfig(**config, weather_file_path=weather_file)

    configurator = ModelConfigurator()
    model_config = configurator.configure_model(
        site_config, monthly_soiling=MONTHLY_SOILING
    )

    model = model_config.model

    # PySAM quirk: cec_bifacial_ground_clearance_height must be > 0 even when
    # bifacial is disabled (cec_is_bifacial=0). Set to small positive value
    # to pass validation. Has no effect on non-bifacial simulation.
    cec = model.CECPerformanceModelWithModuleDatabase
    if cec.cec_is_bifacial == 0 and cec.cec_bifacial_ground_clearance_height <= 0:
        cec.cec_bifacial_ground_clearance_height = 0.001

    model.execute()

    outputs = model.Outputs
    sc = model_config.string_config

    return {
        "annual_energy_kwh": outputs.annual_energy,
        "annual_energy_mwh": outputs.annual_energy / 1000.0,
        "cf_ac": outputs.capacity_factor_ac,
        "cf_dc": outputs.capacity_factor,
        "dc_ac_ratio": model_config.dc_ac_ratio,
        "inverter_count": model_config.inverter_count,
        "nstrings": sc.nstrings if sc else None,
        "modules_per_string": sc.modules_per_string if sc else None,
        "total_modules": sc.total_modules if sc else None,
        "dc_mw_actual": sc.dc_size_mw_actual if sc else None,
        "deviation_pct": sc.deviation_pct if sc else None,
    }


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Load and format TMY weather
    print("=" * 70)
    print("Step 1: Formatting TMY weather data")
    print("=" * 70)
    raw_csv = TMY_CSV_PATH.read_text()
    formatter = WeatherFormatter()
    df_weather_raw, metadata = formatter.format_for_pysam(
        nsrdb_csv=raw_csv, lat=LAT, lon=LON
    )
    print(f"  Weather rows: {len(df_weather_raw)}")
    print(f"  GHI mean: {df_weather_raw['GHI'].mean():.1f} W/m²")
    print(f"  DNI mean: {df_weather_raw['DNI'].mean():.1f} W/m²")

    # Save RAW weather file
    raw_weather_path = OUTPUT_DIR / "9068_tmy_raw_pysam.csv"
    formatter.save_to_csv(
        df_weather_raw, raw_weather_path, lat=LAT, lon=LON, metadata=metadata
    )
    print(f"  Saved RAW weather: {raw_weather_path}")

    # Step 2: Apply bias correction
    print(f"\n{'=' * 70}")
    print("Step 2: Applying NSRDB bias correction")
    print("=" * 70)
    df_weather_corrected, bias_metadata = apply_bias_correction(
        df_weather_raw, LAT, LON, ELEVATION_M
    )
    print(f"  Mean GHI CF: {bias_metadata['mean_ghi_correction_factor']:.4f}")
    print(f"  Mean DNI CF: {bias_metadata['mean_dni_correction_factor']:.4f}")
    print(f"  Monthly GHI factors: {bias_metadata['monthly_ghi_factors']}")
    print(f"  Monthly DNI factors: {bias_metadata['monthly_dni_factors']}")
    print(f"  Corrected GHI mean: {df_weather_corrected['GHI'].mean():.1f} W/m²")
    print(f"  Corrected DNI mean: {df_weather_corrected['DNI'].mean():.1f} W/m²")

    # Save CORRECTED weather file
    corrected_weather_path = OUTPUT_DIR / "9068_tmy_corrected_pysam.csv"
    formatter.save_to_csv(
        df_weather_corrected, corrected_weather_path, lat=LAT, lon=LON, metadata=metadata
    )
    print(f"  Saved CORRECTED weather: {corrected_weather_path}")

    # Step 3: Run 6 simulations (3 GCR × 2 modes)
    print(f"\n{'=' * 70}")
    print("Step 3: Running 6 PySAM simulations")
    print("=" * 70)

    results = []

    for gcr in GCR_VALUES:
        # --- RAW mode ---
        print(f"\n  Running GCR={gcr:.2f} RAW...")
        raw_result = run_pysam(raw_weather_path, gcr)
        raw_mwh = raw_result["annual_energy_mwh"]
        raw_cf = raw_result["cf_ac"]
        raw_error = (raw_mwh - MEASURED_MWH) / MEASURED_MWH * 100

        print(f"    AC MWh: {raw_mwh:,.1f}  CF_AC: {raw_cf:.2f}%  Error: {raw_error:+.1f}%")

        # Check CF bounds
        if raw_cf < 15 or raw_cf > 35:
            print(f"    STOP: CF_AC {raw_cf:.2f}% is outside 15-35% range!")
            return

        results.append({
            "gcr": gcr,
            "mode": "RAW",
            "annual_ac_mwh": round(raw_mwh, 1),
            "cf_ac_pct": round(raw_cf, 2),
            "subhourly_loss_pct": 0.0,
            "corrected_ac_mwh": round(raw_mwh, 1),
            "error_vs_measured_pct": round(raw_error, 1),
            "nstrings": raw_result["nstrings"],
            "modules_per_string": raw_result["modules_per_string"],
            "total_modules": raw_result["total_modules"],
            "dc_mw_actual": raw_result["dc_mw_actual"],
            "inverter_count": raw_result["inverter_count"],
        })

        # --- CORRECTED mode ---
        print(f"  Running GCR={gcr:.2f} CORRECTED...")
        corr_result = run_pysam(corrected_weather_path, gcr)
        corr_mwh_before = corr_result["annual_energy_mwh"]
        corr_cf = corr_result["cf_ac"]

        # Compute subhourly correction
        weather_features = compute_weather_features(corrected_weather_path)
        dcac_ratio = corr_result["dc_ac_ratio"]
        subhourly_loss = predict_correction(
            dcac_ratio=dcac_ratio,
            gcr=gcr,
            racking="tracker",
            latitude=LAT,
            longitude=LON,
            cf_60min=corr_cf,
            weather_features=weather_features,
        )

        corr_mwh_after = corr_mwh_before * (1 - subhourly_loss / 100.0)
        corr_error = (corr_mwh_after - MEASURED_MWH) / MEASURED_MWH * 100

        print(f"    AC MWh (pre-subhourly): {corr_mwh_before:,.1f}  CF_AC: {corr_cf:.2f}%")
        print(f"    Subhourly loss: {subhourly_loss:.3f}%")
        print(f"    AC MWh (final): {corr_mwh_after:,.1f}  Error: {corr_error:+.1f}%")

        # Check CF bounds
        if corr_cf < 15 or corr_cf > 35:
            print(f"    STOP: CF_AC {corr_cf:.2f}% is outside 15-35% range!")
            return

        results.append({
            "gcr": gcr,
            "mode": "CORRECTED",
            "annual_ac_mwh": round(corr_mwh_before, 1),
            "cf_ac_pct": round(corr_cf, 2),
            "subhourly_loss_pct": round(subhourly_loss, 3),
            "corrected_ac_mwh": round(corr_mwh_after, 1),
            "error_vs_measured_pct": round(corr_error, 1),
            "nstrings": corr_result["nstrings"],
            "modules_per_string": corr_result["modules_per_string"],
            "total_modules": corr_result["total_modules"],
            "dc_mw_actual": corr_result["dc_mw_actual"],
            "inverter_count": corr_result["inverter_count"],
        })

    # Step 4: Results summary
    print(f"\n{'=' * 70}")
    print("RESULTS SUMMARY")
    print(f"{'=' * 70}")
    print(f"Measured baseline: {MEASURED_MWH:,.0f} MWh/yr (2018-2022 average)")
    print(f"DC capacity: {BASE_CONFIG['dc_size_mw']} MW  AC capacity: {BASE_CONFIG['ac_installed_mw']} MW")
    print()

    # Print string sizing (same for all runs)
    r0 = results[0]
    print(f"String sizing: {r0['nstrings']} strings × {r0['modules_per_string']} modules/string")
    print(f"  Total modules: {r0['total_modules']:,}")
    print(f"  Actual DC: {r0['dc_mw_actual']:.4f} MW (target: {BASE_CONFIG['dc_size_mw']} MW)")
    print(f"  Inverters: {r0['inverter_count']}")
    print()

    header = f"{'GCR':>5} {'Mode':<10} {'PySAM MWh':>10} {'CF_AC%':>7} {'Sub-hr%':>8} {'Final MWh':>10} {'Error%':>8}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['gcr']:>5.2f} {r['mode']:<10} {r['annual_ac_mwh']:>10,.1f} "
            f"{r['cf_ac_pct']:>7.2f} {r['subhourly_loss_pct']:>8.3f} "
            f"{r['corrected_ac_mwh']:>10,.1f} {r['error_vs_measured_pct']:>+8.1f}"
        )

    # Find best match
    best = min(results, key=lambda r: abs(r["error_vs_measured_pct"]))
    print(f"\nClosest to measured: GCR={best['gcr']:.2f} {best['mode']} "
          f"— {best['corrected_ac_mwh']:,.1f} MWh ({best['error_vs_measured_pct']:+.1f}%)")

    # Step 5: Save results CSV
    results_df = pd.DataFrame(results)
    results_path = OUTPUT_DIR / "9068_benchmark_results.csv"
    results_df.to_csv(results_path, index=False)
    print(f"\nResults saved to: {results_path}")

    # Print bias correction details
    print(f"\n{'=' * 70}")
    print("BIAS CORRECTION DETAILS")
    print(f"{'=' * 70}")
    print(f"Model version: {bias_metadata['bias_correction_model_version']}")
    print(f"Mean GHI CF: {bias_metadata['mean_ghi_correction_factor']:.4f}")
    print(f"Mean DNI CF: {bias_metadata['mean_dni_correction_factor']:.4f}")
    print(f"Monthly GHI factors: {bias_metadata['monthly_ghi_factors']}")
    print(f"Monthly DNI factors: {bias_metadata['monthly_dni_factors']}")


if __name__ == "__main__":
    main()
