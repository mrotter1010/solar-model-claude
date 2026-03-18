"""Proof-of-concept: Run PySAM simulation with NSRDB TMY weather data.

Tests whether PySAM handles the mixed-year Year column in TMY data correctly.
Uses the same Phoenix system config as tests/fixtures/single_row_test.csv,
but swaps in the TMY weather file from nsrdb_tmy_fetcher.py.

Usage:
    python research/m12_benchmarking/tmy_pysam_test.py
"""

from pathlib import Path

import pandas as pd

from src.climate.weather_formatter import WeatherFormatter
from src.config.schema import SiteConfig
from src.pysam_integration.model_configurator import ModelConfigurator


# Phoenix system config — matches tests/fixtures/single_row_test.csv
PHOENIX_SITE = {
    "run_name": "TMY_Benchmark",
    "site_name": "Phoenix_TMY_Test",
    "customer": "M12Research",
    "latitude": 33.4484,
    "longitude": -112.074,
    "dc_size_mw": 250.0,
    "ac_installed_mw": 200.0,
    "ac_poi_mw": 195.0,
    "racking": "tracker",
    "tilt": 60,  # rotation limit for tracker (single_row_test.csv has 0 — unrealistic)
    "azimuth": 180,
    "module_orientation": "portrait",
    "number_of_modules": 2,
    "ground_clearance_height_m": 1.5,
    "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
    "bifacial": True,
    "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
    "gcr": 0.35,
    "shading_percent": 2.0,
    "dc_wiring_loss_percent": 1.5,
    "ac_wiring_loss_percent": 0.5,
    "transformer_losses_percent": 1.0,
    "degradation_percent": 0.5,
    "availability_percent": 0.5,
    "module_mismatch_percent": 2.0,
    "lid_percent": 1.5,
}

TMY_CSV_PATH = Path("research/m12_benchmarking/output/33.45_-111.95_tmy.csv")
OUTPUT_DIR = Path("research/m12_benchmarking/output")

# Baseline from production pipeline (2024 single-year NSRDB data)
BASELINE_AC_CF = 31.51


def main() -> None:
    # Step 1: Format TMY CSV for PySAM
    print("Step 1: Formatting TMY weather data for PySAM...")
    raw_csv = TMY_CSV_PATH.read_text()

    formatter = WeatherFormatter()
    df_weather, metadata = formatter.format_for_pysam(
        nsrdb_csv=raw_csv,
        lat=PHOENIX_SITE["latitude"],
        lon=PHOENIX_SITE["longitude"],
    )

    print(f"  Weather rows: {len(df_weather)}")
    print(f"  Columns: {list(df_weather.columns)}")
    print(f"  Metadata: {metadata}")
    print(f"  Year range: {df_weather['Year'].min()} - {df_weather['Year'].max()}")
    print(f"  Unique years: {sorted(df_weather['Year'].unique())}")

    # Save PySAM-formatted weather file
    pysam_weather_path = OUTPUT_DIR / "phoenix_tmy_pysam.csv"
    formatter.save_to_csv(
        df_weather,
        pysam_weather_path,
        lat=PHOENIX_SITE["latitude"],
        lon=PHOENIX_SITE["longitude"],
        metadata=metadata,
    )
    print(f"  Saved PySAM weather file: {pysam_weather_path}")

    # Step 2: Configure PySAM model
    print("\nStep 2: Configuring PySAM model...")
    site_config = SiteConfig(
        **PHOENIX_SITE,
        weather_file_path=pysam_weather_path,
    )

    configurator = ModelConfigurator()
    model_config = configurator.configure_model(site_config)
    print(f"  DC/AC ratio: {model_config.dc_ac_ratio:.2f}")
    print(f"  Inverter count: {model_config.inverter_count}")
    if model_config.string_config:
        sc = model_config.string_config
        print(f"  String config: {sc.nstrings} strings x {sc.modules_per_string} modules")

    # Step 3: Execute simulation
    print("\nStep 3: Executing PySAM simulation...")
    model = model_config.model
    model.execute()

    outputs = model.Outputs
    annual_energy_kwh = outputs.annual_energy
    annual_energy_mwh = annual_energy_kwh / 1000
    cf_dc = outputs.capacity_factor
    cf_ac = outputs.capacity_factor_ac

    print(f"\n{'='*60}")
    print(f"PySAM TMY Simulation Results")
    print(f"{'='*60}")
    print(f"  Annual AC Energy:     {annual_energy_mwh:,.1f} MWh")
    print(f"  Annual AC Energy:     {annual_energy_kwh:,.0f} kWh")
    print(f"  DC Capacity Factor:   {cf_dc:.2f}%")
    print(f"  AC Capacity Factor:   {cf_ac:.2f}%")
    print(f"  kWh/kW:              {outputs.kwh_per_kw:.1f}")

    # Step 4: Comparison with baseline
    delta = cf_ac - BASELINE_AC_CF
    print(f"\n{'='*60}")
    print(f"Comparison vs 2024 Single-Year Baseline")
    print(f"{'='*60}")
    print(f"  TMY AC CF:            {cf_ac:.2f}%")
    print(f"  2024 Baseline AC CF:  {BASELINE_AC_CF:.2f}%")
    print(f"  Delta:                {delta:+.2f} pp")

    if abs(delta) > 10:
        print(f"\n  WARNING: Delta exceeds ±10 pp — something may be wrong!")
    else:
        print(f"\n  Delta is within ±10 pp — TMY result is plausible.")

    # Step 5: Sanity checks
    print(f"\n{'='*60}")
    print(f"Sanity Checks")
    print(f"{'='*60}")

    gen = list(outputs.gen)
    print(f"  Hourly gen array length: {len(gen)}")
    print(f"  Gen sum (kWh):        {sum(gen):,.0f}")
    print(f"  Gen max (kW):         {max(gen):,.1f}")
    print(f"  Gen min (kW):         {min(gen):,.1f}")
    print(f"  Non-zero hours:       {sum(1 for g in gen if g > 0)}")

    # Monthly energy breakdown
    monthly = list(outputs.monthly_energy)
    if len(monthly) == 12:
        print(f"\n  Monthly Energy (MWh):")
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        for m, e in zip(months, monthly):
            print(f"    {m}: {e/1000:>10,.1f}")

    print(f"\nPySAM handled mixed-year TMY data: SUCCESS (no errors)")


if __name__ == "__main__":
    main()
