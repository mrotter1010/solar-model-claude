"""M8 Subhourly: Compute resolution correction deltas and build training dataset.

Pairs 5-min and 60-min PySAM results by original run name, computes the
resolution correction percentage, joins site metadata, and saves the
complete training dataset for the clipping loss regression model.

Usage:
    python compute_deltas.py
"""

from pathlib import Path

import pandas as pd

RESEARCH_DIR = Path(__file__).resolve().parent
RAW_RESULTS = RESEARCH_DIR / "raw_run_results.csv"
SITES_CSV = RESEARCH_DIR / "research_sites.csv"
OUTPUT_CSV = RESEARCH_DIR / "training_data.csv"


def main() -> None:
    """Load raw results, pair resolutions, compute deltas, save training data."""
    # --- Load raw results ---
    raw = pd.read_csv(RAW_RESULTS)
    print(f"Loaded {len(raw)} rows from {RAW_RESULTS.name}")

    if len(raw) < 6480:
        print(f"STOP: Expected 6,480 rows, got {len(raw)}. Missing {6480 - len(raw)}.")
        return

    # Check for any failed runs (error column present and non-null)
    if "error" in raw.columns:
        errors = raw[raw["error"].notna()]
        if len(errors) > 0:
            print(f"WARNING: {len(errors)} failed runs found:")
            for _, row in errors.head(10).iterrows():
                print(f"  {row['run_name']}: {row['error']}")

    # --- Strip resolution suffix to get base run name for pairing ---
    raw["base_run_name"] = raw["run_name"].str.replace(
        r"_(5min|60min)$", "", regex=True
    )

    # Split into 5-min and 60-min subsets
    df_5 = raw[raw["resolution"] == "5min"].copy()
    df_60 = raw[raw["resolution"] == "60min"].copy()

    print(f"  5-min runs: {len(df_5)}")
    print(f"  60-min runs: {len(df_60)}")

    # --- Check for unmatched configs ---
    base_5 = set(df_5["base_run_name"])
    base_60 = set(df_60["base_run_name"])

    missing_60 = base_5 - base_60
    missing_5 = base_60 - base_5

    if missing_60 or missing_5:
        print("STOP: Unmatched configs found!")
        if missing_60:
            print(f"  Missing 60-min counterpart ({len(missing_60)}):")
            for name in sorted(missing_60)[:20]:
                print(f"    {name}")
        if missing_5:
            print(f"  Missing 5-min counterpart ({len(missing_5)}):")
            for name in sorted(missing_5)[:20]:
                print(f"    {name}")
        return

    # --- Merge pairs on base_run_name ---
    paired = df_5.merge(
        df_60,
        on="base_run_name",
        suffixes=("_5min", "_60min"),
    )

    print(f"Paired configs: {len(paired)}")

    if len(paired) != 3240:
        print(
            f"WARNING: Expected 3,240 paired configs, got {len(paired)}. "
            f"Check for duplicate base_run_names."
        )

    # --- Compute resolution correction ---
    paired["resolution_correction_pct"] = (
        (paired["total_gen_kwh_60min"] - paired["total_gen_kwh_5min"])
        / paired["total_gen_kwh_60min"]
        * 100
    )

    # --- Load site metadata ---
    sites = pd.read_csv(SITES_CSV)
    print(f"Loaded {len(sites)} sites from {SITES_CSV.name}")

    # --- Build training dataset ---
    training = paired.merge(
        sites[["site_name", "latitude", "longitude", "latitude_band", "climate_type"]],
        left_on="site_name_5min",
        right_on="site_name",
    )

    training = training.rename(columns={
        "site_name": "site_name",
        "dcac_ratio_5min": "dcac_ratio",
        "gcr_5min": "gcr",
        "racking_5min": "racking",
        "total_gen_kwh_5min": "gen_5min_kwh",
        "total_gen_kwh_60min": "gen_60min_kwh",
        "capacity_factor_5min": "cf_5min",
        "capacity_factor_60min": "cf_60min",
    })

    output_cols = [
        "site_name",
        "latitude",
        "longitude",
        "latitude_band",
        "climate_type",
        "dcac_ratio",
        "gcr",
        "racking",
        "gen_5min_kwh",
        "gen_60min_kwh",
        "cf_5min",
        "cf_60min",
        "resolution_correction_pct",
    ]
    training = training[output_cols]

    # Save
    training.to_csv(OUTPUT_CSV, index=False)
    print(f"\nTraining dataset saved to {OUTPUT_CSV.name}: {len(training)} rows")

    # === Summary Statistics ===
    rc = training["resolution_correction_pct"]

    print(f"\n{'='*60}")
    print("SUMMARY STATISTICS")
    print(f"{'='*60}")
    print(f"\nTotal paired configs: {len(training)}")
    print(f"\nresolution_correction_pct:")
    print(f"  mean:  {rc.mean():+.4f}%")
    print(f"  std:   {rc.std():.4f}%")
    print(f"  min:   {rc.min():+.4f}%")
    print(f"  max:   {rc.max():+.4f}%")

    # --- Breakdown by racking ---
    print(f"\nBy racking type:")
    for racking, group in training.groupby("racking"):
        g = group["resolution_correction_pct"]
        print(
            f"  {racking:8s}: mean={g.mean():+.4f}%, "
            f"std={g.std():.4f}%, n={len(g)}"
        )

    # --- Breakdown by climate_type ---
    print(f"\nBy climate_type:")
    for climate, group in training.groupby("climate_type"):
        g = group["resolution_correction_pct"]
        print(
            f"  {climate:10s}: mean={g.mean():+.4f}%, "
            f"std={g.std():.4f}%, n={len(g)}"
        )

    # --- Breakdown by dcac_ratio ---
    print(f"\nBy dcac_ratio:")
    for ratio, group in training.sort_values("dcac_ratio").groupby("dcac_ratio"):
        g = group["resolution_correction_pct"]
        print(f"  {ratio:.2f}: mean={g.mean():+.4f}%, std={g.std():.4f}%, n={len(g)}")

    # --- Top 10 most positive (hourly overestimates most) ---
    print(f"\nTop 10 highest resolution_correction_pct (hourly overestimates):")
    top_pos = training.nlargest(10, "resolution_correction_pct")
    for _, row in top_pos.iterrows():
        print(
            f"  {row['site_name']:20s} DC/AC={row['dcac_ratio']:.2f} "
            f"GCR={row['gcr']:.2f} {row['racking']:7s} "
            f"correction={row['resolution_correction_pct']:+.4f}%"
        )

    # --- Top 10 most negative (hourly underestimates most) ---
    print(f"\nTop 10 most negative resolution_correction_pct (hourly underestimates):")
    top_neg = training.nsmallest(10, "resolution_correction_pct")
    for _, row in top_neg.iterrows():
        print(
            f"  {row['site_name']:20s} DC/AC={row['dcac_ratio']:.2f} "
            f"GCR={row['gcr']:.2f} {row['racking']:7s} "
            f"correction={row['resolution_correction_pct']:+.4f}%"
        )


if __name__ == "__main__":
    main()
