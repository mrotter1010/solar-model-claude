"""PVDAQ 2107 annual production analysis.

Loads revenue-grade meter data (15-min intervals), aggregates to hourly,
and computes annual AC production (MWh) and data completeness per calendar year.

This is a research utility for M12 benchmarking — does NOT modify production code.

Usage:
    python research/m12_benchmarking/pvdaq_2107_annual_production.py
"""

from pathlib import Path

import pandas as pd

METER_CSV = Path(
    "research/m12_benchmarking/pvdaq_data/2107/2107_meter_15m_data.csv"
)
OUTPUT_CSV = Path(
    "research/m12_benchmarking/pvdaq_data/2107/2107_annual_summary.csv"
)


def load_meter_data(csv_path: Path) -> pd.DataFrame:
    """Load meter CSV and parse timestamps."""
    df = pd.read_csv(csv_path, parse_dates=["measured_on"])
    return df


def analyze_annual_production(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 15-min meter data to annual production summary.

    Returns:
        DataFrame with columns: year, annual_mwh, completeness_pct,
        hours_with_data, hours_expected, mean_kw, max_kw, min_kw.
    """
    # Identify the AC power column (everything except measured_on)
    power_col = [c for c in df.columns if c != "measured_on"][0]
    print(f"AC power column: {power_col}")

    # Set timestamp as index
    df = df.set_index("measured_on").sort_index()

    # Resample to hourly: average the 15-min readings within each hour
    # (up to 4 readings per hour). This gives hourly average kW.
    hourly = df[[power_col]].resample("1h").mean()
    hourly["year"] = hourly.index.year

    # Count valid (non-NaN) hours per year
    results = []
    for year, group in hourly.groupby("year"):
        valid_hours = group[power_col].notna().sum()

        # Expected hours in that calendar year
        year_start = pd.Timestamp(f"{year}-01-01")
        year_end = pd.Timestamp(f"{year + 1}-01-01")
        hours_expected = int((year_end - year_start).total_seconds() / 3600)

        completeness = valid_hours / hours_expected * 100

        # Annual energy: sum of hourly average kW * 1 hour = kWh, convert to MWh
        annual_kwh = group[power_col].sum()  # sum of kW * 1hr = kWh
        annual_mwh = annual_kwh / 1000.0

        mean_kw = group[power_col].mean()
        max_kw = group[power_col].max()
        min_kw = group[power_col].min()

        results.append({
            "year": int(year),
            "annual_mwh": round(annual_mwh, 2),
            "completeness_pct": round(completeness, 2),
            "hours_with_data": int(valid_hours),
            "hours_expected": hours_expected,
            "mean_kw": round(mean_kw, 2),
            "max_kw": round(max_kw, 2),
            "min_kw": round(min_kw, 2),
        })

    return pd.DataFrame(results)


def main() -> None:
    """Run the full analysis and save summary."""
    print(f"Loading: {METER_CSV}")
    df = load_meter_data(METER_CSV)

    print(f"\nFile size: {METER_CSV.stat().st_size / 1e6:.1f} MB")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {list(df.columns)}")
    print(f"Date range: {df['measured_on'].min()} to {df['measured_on'].max()}")

    # Power column stats (raw, before any aggregation)
    power_col = [c for c in df.columns if c != "measured_on"][0]
    print(f"\nRaw {power_col} stats (15-min data):")
    print(f"  Min:    {df[power_col].min():.2f} kW")
    print(f"  Max:    {df[power_col].max():.2f} kW")
    print(f"  Mean:   {df[power_col].mean():.2f} kW")
    print(f"  NaN:    {df[power_col].isna().sum():,} ({df[power_col].isna().mean()*100:.1f}%)")

    summary = analyze_annual_production(df)

    print(f"\n{'='*80}")
    print("Annual Production Summary — PVDAQ 2107 (Farm Solar Array, Arbuckle CA)")
    print(f"{'='*80}")
    print(f"{'Year':<6} {'MWh':>10} {'Complete%':>10} {'Hrs Valid':>10} {'Hrs Exp':>8} "
          f"{'Mean kW':>9} {'Max kW':>9} {'Min kW':>9}")
    print("-" * 80)
    for _, row in summary.iterrows():
        print(f"{row['year']:<6} {row['annual_mwh']:>10.1f} {row['completeness_pct']:>9.1f}% "
              f"{row['hours_with_data']:>10} {row['hours_expected']:>8} "
              f"{row['mean_kw']:>9.1f} {row['max_kw']:>9.1f} {row['min_kw']:>9.1f}")
    print("-" * 80)

    # Best year recommendation
    full_years = summary[summary["completeness_pct"] >= 95.0]
    if not full_years.empty:
        best = full_years.loc[full_years["annual_mwh"].idxmax()]
        print(f"\nBest year (>=95% complete, highest MWh): {int(best['year'])} "
              f"({best['annual_mwh']:.1f} MWh, {best['completeness_pct']:.1f}% complete)")
    else:
        best = summary.loc[summary["completeness_pct"].idxmax()]
        print(f"\nNo year >=95% complete. Most complete: {int(best['year'])} "
              f"({best['annual_mwh']:.1f} MWh, {best['completeness_pct']:.1f}% complete)")

    # Save
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved summary to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
