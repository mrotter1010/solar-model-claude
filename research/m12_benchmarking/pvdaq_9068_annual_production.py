"""
PVDAQ 9068 (SR_CO) — Annual AC Production Analysis

Loads the 9068 AC power data, identifies the meter-level column,
aggregates to hourly, and computes annual production (MWh) and
data completeness for each calendar year.

No quality filtering applied — raw totals and completeness only.
"""

import pandas as pd
from pathlib import Path

DATA_FILE = Path(__file__).parent / "pvdaq_data" / "9068" / "9068_ac_power_data.csv"
OUTPUT_FILE = Path(__file__).parent / "pvdaq_data" / "9068" / "9068_annual_summary.csv"


def main() -> None:
    # Load data
    print(f"Loading {DATA_FILE} ...")
    df = pd.read_csv(DATA_FILE, parse_dates=["measured_on"])
    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    print(f"Date range: {df['measured_on'].min()} to {df['measured_on'].max()}")
    print()

    # Print ALL column names for verification
    print("=== ALL COLUMNS ===")
    for i, col in enumerate(df.columns):
        print(f"  [{i:2d}] {col}")
    print()

    # Identify meter-level total AC power column
    meter_cols = [c for c in df.columns if "meter" in c.lower() and "power" in c.lower() and "phase" not in c.lower()]
    print(f"Meter-level AC power columns (excluding per-phase): {meter_cols}")

    if len(meter_cols) != 1:
        print(f"ERROR: Expected exactly 1 meter total power column, found {len(meter_cols)}")
        print("Cannot proceed — review columns above and specify manually.")
        return

    meter_col = meter_cols[0]
    print(f"Using column: '{meter_col}'")
    print()

    # Also report the inverter total columns for cross-check
    inv_total_cols = [c for c in df.columns if c.startswith("inverter_") and "ac_power" in c and "module" not in c]
    print(f"Inverter total power columns: {inv_total_cols}")
    print()

    # Basic stats on the meter column
    print(f"=== METER COLUMN STATS (raw) ===")
    print(f"  Non-null count: {df[meter_col].notna().sum():,} / {len(df):,}")
    print(f"  Null count:     {df[meter_col].isna().sum():,}")
    print(f"  Min:  {df[meter_col].min():.1f} kW")
    print(f"  Max:  {df[meter_col].max():.1f} kW")
    print(f"  Mean: {df[meter_col].mean():.1f} kW")
    print()

    # Detect time resolution
    dt = df["measured_on"].diff().dropna()
    mode_minutes = dt.mode()[0].total_seconds() / 60
    print(f"Time resolution (mode): {mode_minutes:.0f} minutes")
    print()

    # Set timestamp as index for resampling
    df = df.set_index("measured_on")

    # Resample to hourly: average power (kW) over each hour
    # For 5-min data, each hour has up to 12 readings
    # Energy (kWh) for the hour = mean power (kW) × 1 hour
    hourly = df[[meter_col]].resample("1h").agg(
        mean_power_kw=(meter_col, "mean"),
        valid_readings=(meter_col, "count"),
    )

    # Energy in kWh = mean_power_kw × 1 hour
    hourly["energy_kwh"] = hourly["mean_power_kw"]  # kW × 1h = kWh

    # Add year column
    hourly["year"] = hourly.index.year

    # Expected readings per hour based on resolution
    expected_per_hour = int(60 / mode_minutes)
    print(f"Expected readings per hour: {expected_per_hour}")
    print()

    # Annual aggregation
    print("=== ANNUAL PRODUCTION SUMMARY ===")
    print(f"{'Year':<6} {'AC MWh':>10} {'Hours w/data':>14} {'Hours in year':>14} {'Completeness':>13} {'Avg readings/hr':>16}")
    print("-" * 80)

    results = []
    for year, group in hourly.groupby("year"):
        hours_in_year = 8784 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 8760
        hours_with_data = (group["valid_readings"] > 0).sum()
        total_energy_kwh = group["energy_kwh"].sum()
        total_energy_mwh = total_energy_kwh / 1000.0
        completeness_pct = (hours_with_data / hours_in_year) * 100
        avg_readings = group.loc[group["valid_readings"] > 0, "valid_readings"].mean()

        print(f"{year:<6} {total_energy_mwh:>10.1f} {hours_with_data:>14,} {hours_in_year:>14,} {completeness_pct:>12.1f}% {avg_readings:>16.1f}")
        results.append({
            "year": year,
            "annual_ac_mwh": round(total_energy_mwh, 1),
            "hours_with_data": hours_with_data,
            "hours_in_year": hours_in_year,
            "completeness_pct": round(completeness_pct, 1),
            "avg_readings_per_hour": round(avg_readings, 1),
        })

    print()

    # Identify best year (highest completeness among full calendar years)
    # Exclude partial first/last years
    full_year_results = [r for r in results if r["completeness_pct"] > 50]
    if full_year_results:
        best = max(full_year_results, key=lambda r: r["completeness_pct"])
        print(f"Best year (highest completeness): {best['year']} — {best['completeness_pct']}% complete, {best['annual_ac_mwh']} MWh")
    print()

    # Save summary CSV
    summary_df = pd.DataFrame(results)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Summary saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
