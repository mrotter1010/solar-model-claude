"""Compute NSRDB vs ground truth bias ratios and generate diagnostic plots.

Joins monthly ground truth (SURFRAD + SOLRAD) with co-located NSRDB data,
computes bias ratios and correction factors, and produces summary statistics
and diagnostic visualizations.
"""

import logging
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for saving plots
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Add project root so we can import the station registry
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
from stations import STATIONS  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Build station metadata lookup
STATION_META = {s["station_id"]: s for s in STATIONS}


def load_and_merge() -> pd.DataFrame:
    """Load ground truth and NSRDB monthly data, inner-join on (station_id, year, month).

    Returns:
        Merged DataFrame with ground truth and NSRDB columns side by side.
    """
    base_dir = Path(__file__).resolve().parent.parent
    gt_dir = base_dir / "cache" / "ground_truth"
    nsrdb_dir = base_dir / "cache" / "nsrdb"

    # Load ground truth
    surfrad = pd.read_csv(gt_dir / "surfrad_all_monthly.csv")
    solrad = pd.read_csv(gt_dir / "solrad_all_monthly.csv")
    gt = pd.concat([surfrad, solrad], ignore_index=True)
    logger.info("Ground truth: %d rows (SURFRAD=%d, SOLRAD=%d)", len(gt), len(surfrad), len(solrad))

    # Load NSRDB
    nsrdb = pd.read_csv(nsrdb_dir / "nsrdb_all_monthly.csv")
    logger.info("NSRDB: %d rows", len(nsrdb))

    # Inner join
    merged = gt.merge(
        nsrdb,
        on=["station_id", "year", "month"],
        how="inner",
        suffixes=("_gt", "_nsrdb"),
    )
    logger.info("Merged (inner join): %d matched rows", len(merged))

    # Report unmatched
    gt_keys = set(zip(gt["station_id"], gt["year"], gt["month"]))
    nsrdb_keys = set(zip(nsrdb["station_id"], nsrdb["year"], nsrdb["month"]))
    gt_only = gt_keys - nsrdb_keys
    nsrdb_only = nsrdb_keys - gt_keys

    if gt_only:
        logger.warning("Ground truth months with no NSRDB match: %d", len(gt_only))
        for sid, y, m in sorted(gt_only)[:10]:
            logger.warning("  %s %d/%02d", sid, y, m)
    if nsrdb_only:
        logger.info("NSRDB months with no ground truth match: %d", len(nsrdb_only))

    if len(merged) < 800:
        raise RuntimeError(
            f"STOP: Only {len(merged)} matched rows (expected ~962). "
            "Check join keys."
        )

    return merged


def compute_bias(df: pd.DataFrame) -> pd.DataFrame:
    """Compute bias ratios and correction factors.

    Args:
        df: Merged DataFrame with _gt and _nsrdb suffixed columns.

    Returns:
        DataFrame with bias ratio and correction factor columns added.
    """
    df = df.copy()

    df["bias_ratio_ghi"] = df["ghi_kwh_m2_day_nsrdb"] / df["ghi_kwh_m2_day_gt"]
    df["bias_ratio_dni"] = df["dni_kwh_m2_day_nsrdb"] / df["dni_kwh_m2_day_gt"]
    df["correction_factor_ghi"] = df["ghi_kwh_m2_day_gt"] / df["ghi_kwh_m2_day_nsrdb"]
    df["correction_factor_dni"] = df["dni_kwh_m2_day_gt"] / df["dni_kwh_m2_day_nsrdb"]

    # Add station metadata
    df["latitude"] = df["station_id"].map(lambda s: STATION_META[s]["latitude"])
    df["longitude"] = df["station_id"].map(lambda s: STATION_META[s]["longitude"])
    df["elevation_m"] = df["station_id"].map(lambda s: STATION_META[s]["elevation_m"])
    df["station_name"] = df["station_id"].map(lambda s: STATION_META[s]["name"])

    return df


def summary_statistics(df: pd.DataFrame, output_dir: Path) -> None:
    """Compute and save summary statistics."""
    lines: list[str] = []

    def section(title: str) -> None:
        lines.append("")
        lines.append(title)
        lines.append("=" * len(title))

    # --- Overall ---
    section("A. Overall Bias Statistics")
    for var in ["ghi", "dni"]:
        col = f"bias_ratio_{var}"
        lines.append(
            f"  {var.upper()} bias ratio: "
            f"mean={df[col].mean():.4f}, "
            f"median={df[col].median():.4f}, "
            f"std={df[col].std():.4f}, "
            f"min={df[col].min():.4f}, "
            f"max={df[col].max():.4f}"
        )

    # --- Per-station ---
    section("B. Per-Station Mean Annual Bias Ratios")
    station_stats = df.groupby(["station_id", "station_name"]).agg(
        ghi_bias=("bias_ratio_ghi", "mean"),
        dni_bias=("bias_ratio_dni", "mean"),
        n_months=("bias_ratio_ghi", "count"),
        lat=("latitude", "first"),
    ).reset_index().sort_values("ghi_bias", ascending=False)

    lines.append(
        f"  {'Station':<8} {'Name':<16} {'Lat':>6} {'N':>4} "
        f"{'GHI Bias':>10} {'DNI Bias':>10}"
    )
    lines.append("  " + "-" * 60)
    for _, row in station_stats.iterrows():
        lines.append(
            f"  {row['station_id']:<8} {row['station_name']:<16} "
            f"{row['lat']:>6.1f} {int(row['n_months']):>4} "
            f"{row['ghi_bias']:>10.4f} {row['dni_bias']:>10.4f}"
        )

    highest_ghi = station_stats.iloc[0]
    lowest_ghi = station_stats.iloc[-1]
    lines.append("")
    lines.append(
        f"  Highest GHI bias: {highest_ghi['station_id']} "
        f"({highest_ghi['station_name']}) = {highest_ghi['ghi_bias']:.4f}"
    )
    lines.append(
        f"  Lowest GHI bias:  {lowest_ghi['station_id']} "
        f"({lowest_ghi['station_name']}) = {lowest_ghi['ghi_bias']:.4f}"
    )

    # --- Per-month ---
    section("C. Monthly Bias Pattern (averaged across all stations)")
    month_stats = df.groupby("month").agg(
        ghi_bias=("bias_ratio_ghi", "mean"),
        dni_bias=("bias_ratio_dni", "mean"),
    ).reset_index()

    lines.append(f"  {'Month':>6} {'GHI Bias':>10} {'DNI Bias':>10}")
    lines.append("  " + "-" * 30)
    for _, row in month_stats.iterrows():
        lines.append(
            f"  {int(row['month']):>6} {row['ghi_bias']:>10.4f} {row['dni_bias']:>10.4f}"
        )

    seasonal_range_ghi = month_stats["ghi_bias"].max() - month_stats["ghi_bias"].min()
    seasonal_range_dni = month_stats["dni_bias"].max() - month_stats["dni_bias"].min()
    lines.append(f"  Seasonal range GHI: {seasonal_range_ghi:.4f}")
    lines.append(f"  Seasonal range DNI: {seasonal_range_dni:.4f}")

    # --- Correlations ---
    section("D. Bias Correlation with Geography")
    station_geo = df.groupby("station_id").agg(
        ghi_bias=("bias_ratio_ghi", "mean"),
        dni_bias=("bias_ratio_dni", "mean"),
        latitude=("latitude", "first"),
        longitude=("longitude", "first"),
        elevation_m=("elevation_m", "first"),
    )
    for geo_var in ["latitude", "longitude", "elevation_m"]:
        r_ghi = station_geo["ghi_bias"].corr(station_geo[geo_var])
        r_dni = station_geo["dni_bias"].corr(station_geo[geo_var])
        lines.append(f"  {geo_var:<14} vs GHI bias: r={r_ghi:+.3f}, vs DNI bias: r={r_dni:+.3f}")

    # --- GHI vs DNI bias ---
    section("E. GHI Bias vs DNI Bias Correlation")
    r_ghi_dni = station_geo["ghi_bias"].corr(station_geo["dni_bias"])
    lines.append(f"  Station-level correlation: r={r_ghi_dni:.3f}")
    if abs(r_ghi_dni) > 0.7:
        lines.append("  -> Strong correlation: stations biased in GHI tend to be biased in DNI too.")
    elif abs(r_ghi_dni) > 0.4:
        lines.append("  -> Moderate correlation.")
    else:
        lines.append("  -> Weak correlation: GHI and DNI biases are somewhat independent.")

    # Print and save
    summary_text = "\n".join(lines) + "\n"
    summary_path = output_dir / "bias_analysis_summary.txt"
    summary_path.write_text(summary_text)
    logger.info("Saved summary: %s", summary_path)
    print(summary_text)


def plot_bias_by_station(df: pd.DataFrame, output_dir: Path) -> None:
    """Bar charts of mean bias ratio per station for GHI and DNI."""
    station_stats = df.groupby(["station_id", "station_name"]).agg(
        ghi_bias=("bias_ratio_ghi", "mean"),
        dni_bias=("bias_ratio_dni", "mean"),
    ).reset_index()

    for var, label in [("ghi", "GHI"), ("dni", "DNI")]:
        col = f"{var}_bias"
        sorted_df = station_stats.sort_values(col, ascending=True)

        fig, ax = plt.subplots(figsize=(10, 6))
        colors = ["#2196F3" if v < 1.0 else "#FF5722" for v in sorted_df[col]]
        bars = ax.barh(
            range(len(sorted_df)),
            sorted_df[col].values,
            color=colors,
            edgecolor="white",
            linewidth=0.5,
        )
        ax.set_yticks(range(len(sorted_df)))
        ax.set_yticklabels(
            [f"{row['station_id']} ({row['station_name']})"
             for _, row in sorted_df.iterrows()],
            fontsize=9,
        )
        ax.axvline(x=1.0, color="red", linestyle="--", linewidth=1.5, label="No bias (1.0)")
        ax.set_xlabel(f"{label} Bias Ratio (NSRDB / Ground Truth)")
        ax.set_title(f"NSRDB {label} Bias by Station (2018-2023 mean)")
        ax.legend(loc="lower right")

        # Add value labels
        for i, (_, row) in enumerate(sorted_df.iterrows()):
            ax.text(
                row[col] + 0.005, i, f"{row[col]:.3f}",
                va="center", fontsize=8,
            )

        plt.tight_layout()
        path = output_dir / f"bias_by_station_{var}.png"
        fig.savefig(path, dpi=150)
        plt.close(fig)
        logger.info("Saved %s", path.name)


def plot_seasonal_pattern(df: pd.DataFrame, output_dir: Path) -> None:
    """Line plot of monthly bias ratios averaged across all stations."""
    month_stats = df.groupby("month").agg(
        ghi_bias=("bias_ratio_ghi", "mean"),
        ghi_std=("bias_ratio_ghi", "std"),
        dni_bias=("bias_ratio_dni", "mean"),
        dni_std=("bias_ratio_dni", "std"),
    ).reset_index()

    fig, ax = plt.subplots(figsize=(10, 6))
    months = month_stats["month"].values

    ax.plot(months, month_stats["ghi_bias"], "o-", color="#2196F3", label="GHI bias", linewidth=2)
    ax.fill_between(
        months,
        month_stats["ghi_bias"] - month_stats["ghi_std"],
        month_stats["ghi_bias"] + month_stats["ghi_std"],
        alpha=0.15, color="#2196F3",
    )

    ax.plot(months, month_stats["dni_bias"], "s-", color="#FF9800", label="DNI bias", linewidth=2)
    ax.fill_between(
        months,
        month_stats["dni_bias"] - month_stats["dni_std"],
        month_stats["dni_bias"] + month_stats["dni_std"],
        alpha=0.15, color="#FF9800",
    )

    ax.axhline(y=1.0, color="red", linestyle="--", linewidth=1, alpha=0.7, label="No bias")
    ax.set_xlabel("Month")
    ax.set_ylabel("Bias Ratio (NSRDB / Ground Truth)")
    ax.set_title("NSRDB Seasonal Bias Pattern (all stations, 2018-2023)")
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                         "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    path = output_dir / "bias_seasonal_pattern.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", path.name)


def plot_bias_vs_latitude(df: pd.DataFrame, output_dir: Path) -> None:
    """Scatter of station mean GHI bias vs latitude."""
    station_stats = df.groupby(["station_id", "station_name"]).agg(
        ghi_bias=("bias_ratio_ghi", "mean"),
        latitude=("latitude", "first"),
    ).reset_index()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(station_stats["latitude"], station_stats["ghi_bias"],
               s=80, color="#2196F3", edgecolors="white", zorder=5)

    # Label each point
    for _, row in station_stats.iterrows():
        ax.annotate(
            f" {row['station_id']}",
            (row["latitude"], row["ghi_bias"]),
            fontsize=8, ha="left",
        )

    # Trend line
    z = np.polyfit(station_stats["latitude"], station_stats["ghi_bias"], 1)
    p = np.poly1d(z)
    lat_range = np.linspace(station_stats["latitude"].min() - 1, station_stats["latitude"].max() + 1, 100)
    ax.plot(lat_range, p(lat_range), "--", color="gray", alpha=0.7, label=f"Trend (slope={z[0]:.4f}/deg)")

    r = station_stats["ghi_bias"].corr(station_stats["latitude"])
    ax.axhline(y=1.0, color="red", linestyle="--", linewidth=1, alpha=0.5)
    ax.set_xlabel("Latitude (degrees N)")
    ax.set_ylabel("Mean GHI Bias Ratio (NSRDB / Ground Truth)")
    ax.set_title(f"NSRDB GHI Bias vs Latitude (r={r:.3f})")
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    path = output_dir / "bias_vs_latitude.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", path.name)


def plot_ghi_vs_dni_bias(df: pd.DataFrame, output_dir: Path) -> None:
    """Scatter of station mean GHI bias vs DNI bias."""
    station_stats = df.groupby(["station_id", "station_name"]).agg(
        ghi_bias=("bias_ratio_ghi", "mean"),
        dni_bias=("bias_ratio_dni", "mean"),
    ).reset_index()

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.scatter(station_stats["ghi_bias"], station_stats["dni_bias"],
               s=80, color="#9C27B0", edgecolors="white", zorder=5)

    for _, row in station_stats.iterrows():
        ax.annotate(
            f" {row['station_id']}",
            (row["ghi_bias"], row["dni_bias"]),
            fontsize=8, ha="left",
        )

    # Diagonal reference
    lims = [
        min(station_stats["ghi_bias"].min(), station_stats["dni_bias"].min()) - 0.02,
        max(station_stats["ghi_bias"].max(), station_stats["dni_bias"].max()) + 0.02,
    ]
    ax.plot(lims, lims, "--", color="gray", alpha=0.5, label="1:1 line")
    ax.axhline(y=1.0, color="red", linestyle=":", alpha=0.3)
    ax.axvline(x=1.0, color="red", linestyle=":", alpha=0.3)

    r = station_stats["ghi_bias"].corr(station_stats["dni_bias"])
    ax.set_xlabel("Mean GHI Bias Ratio")
    ax.set_ylabel("Mean DNI Bias Ratio")
    ax.set_title(f"GHI vs DNI Bias by Station (r={r:.3f})")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_aspect("equal")

    plt.tight_layout()
    path = output_dir / "bias_scatter_ghi_vs_dni.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", path.name)


def plot_correction_factor_heatmap(df: pd.DataFrame, output_dir: Path) -> None:
    """Heatmap of GHI correction factors (stations x months)."""
    # Pivot to station x month
    pivot = df.groupby(["station_id", "month"])["correction_factor_ghi"].mean().unstack()

    # Sort stations by mean correction factor
    pivot = pivot.loc[pivot.mean(axis=1).sort_values().index]

    fig, ax = plt.subplots(figsize=(12, 8))
    im = ax.imshow(pivot.values, aspect="auto", cmap="RdBu_r", vmin=0.85, vmax=1.15)

    # Labels
    ax.set_xticks(range(12))
    ax.set_xticklabels(["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                         "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    station_labels = [
        f"{sid} ({STATION_META[sid]['name']})" for sid in pivot.index
    ]
    ax.set_yticks(range(len(station_labels)))
    ax.set_yticklabels(station_labels, fontsize=9)

    # Annotate cells
    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            val = pivot.iloc[i, j]
            if not np.isnan(val):
                ax.text(j, i, f"{val:.2f}", ha="center", va="center", fontsize=7,
                        color="white" if abs(val - 1.0) > 0.08 else "black")

    ax.set_title("GHI Correction Factor (Ground Truth / NSRDB) by Station and Month")
    ax.set_xlabel("Month")
    cbar = fig.colorbar(im, ax=ax, shrink=0.8, label="Correction Factor")
    cbar.ax.axhline(y=1.0, color="black", linewidth=1)

    plt.tight_layout()
    path = output_dir / "correction_factor_heatmap.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", path.name)


def main() -> None:
    """Run full bias analysis pipeline."""
    base_dir = Path(__file__).resolve().parent.parent
    output_dir = base_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Merge
    merged = load_and_merge()

    # 2. Compute bias
    df = compute_bias(merged)

    # 3. Summary statistics
    summary_statistics(df, output_dir)

    # 4. Diagnostic plots
    plot_bias_by_station(df, output_dir)
    plot_seasonal_pattern(df, output_dir)
    plot_bias_vs_latitude(df, output_dir)
    plot_ghi_vs_dni_bias(df, output_dir)
    plot_correction_factor_heatmap(df, output_dir)

    # 5. Save merged dataset
    dataset_path = output_dir / "bias_merged_dataset.csv"
    df.to_csv(dataset_path, index=False)
    logger.info("Saved merged dataset: %s (%d rows)", dataset_path, len(df))

    # Final summary
    print(f"\nDone. {len(df)} matched station-months analyzed.")
    print(f"Plots saved to: {output_dir}/")
    print("Files generated:")
    for f in sorted(output_dir.glob("bias_*")):
        print(f"  {f.name}")
    print(f"  correction_factor_heatmap.png")


if __name__ == "__main__":
    main()
