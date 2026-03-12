"""Re-run bias analysis and model training on the expanded 20-station dataset.

Combines SURFRAD (7) + SOLRAD (7) + MIDC (6) stations. All outputs saved
with _v2 suffix for comparison against the original 14-station results.
"""

import logging
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

_project_root = Path(__file__).resolve().parent.parent
_scripts_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_scripts_dir))

from stations import STATIONS  # noqa: E402
from train_correction_model import (  # noqa: E402
    MonthlyClimatology,
    StationMonthClimatology,
    build_features,
    get_sklearn_model,
    compute_metrics,
    GENERALIZABLE_MODELS,
    IRRADIANCE_VARS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_DIR = _project_root
OUTPUT_DIR = BASE_DIR / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

STATION_META = {s["station_id"]: s for s in STATIONS}


# ===================================================================
# STEP 1: Bias Analysis (v2)
# ===================================================================

def load_and_merge_v2() -> pd.DataFrame:
    """Load combined ground truth and NSRDB, inner-join on (station_id, year, month)."""
    gt_path = BASE_DIR / "cache" / "ground_truth" / "all_networks_monthly.csv"
    nsrdb_path = BASE_DIR / "cache" / "nsrdb" / "nsrdb_all_stations_monthly.csv"

    gt = pd.read_csv(gt_path)
    nsrdb = pd.read_csv(nsrdb_path)
    logger.info("Ground truth: %d rows (%d stations)", len(gt), gt["station_id"].nunique())
    logger.info("NSRDB: %d rows (%d stations)", len(nsrdb), nsrdb["station_id"].nunique())

    merged = gt.merge(
        nsrdb, on=["station_id", "year", "month"], how="inner", suffixes=("_gt", "_nsrdb"),
    )
    logger.info("Merged: %d matched rows (%d stations)", len(merged), merged["station_id"].nunique())

    if len(merged) < 1200:
        raise RuntimeError(f"STOP: Only {len(merged)} matched rows (expected ~1300+). Check join keys.")

    return merged


def compute_bias(df: pd.DataFrame) -> pd.DataFrame:
    """Compute bias ratios and correction factors."""
    df = df.copy()
    df["bias_ratio_ghi"] = df["ghi_kwh_m2_day_nsrdb"] / df["ghi_kwh_m2_day_gt"]
    df["bias_ratio_dni"] = df["dni_kwh_m2_day_nsrdb"] / df["dni_kwh_m2_day_gt"]
    df["correction_factor_ghi"] = df["ghi_kwh_m2_day_gt"] / df["ghi_kwh_m2_day_nsrdb"]
    df["correction_factor_dni"] = df["dni_kwh_m2_day_gt"] / df["dni_kwh_m2_day_nsrdb"]

    df["latitude"] = df["station_id"].map(lambda s: STATION_META[s]["latitude"])
    df["longitude"] = df["station_id"].map(lambda s: STATION_META[s]["longitude"])
    df["elevation_m"] = df["station_id"].map(lambda s: STATION_META[s]["elevation_m"])
    df["station_name"] = df["station_id"].map(lambda s: STATION_META[s]["name"])

    return df


def bias_summary(df: pd.DataFrame) -> str:
    """Generate bias summary statistics text."""
    lines = []

    lines.append("BIAS ANALYSIS SUMMARY (v2 — 20 stations)")
    lines.append("=" * 70)
    lines.append(f"Total matched station-months: {len(df)}")
    lines.append(f"Stations: {df['station_id'].nunique()}")
    lines.append(f"Years: {sorted(df['year'].unique())}")
    lines.append("")

    # Overall
    lines.append("A. Overall Bias Statistics")
    lines.append("-" * 40)
    for var in ["ghi", "dni"]:
        col = f"bias_ratio_{var}"
        lines.append(
            f"  {var.upper()} bias ratio: mean={df[col].mean():.4f}, "
            f"median={df[col].median():.4f}, std={df[col].std():.4f}"
        )
    lines.append("")

    # Per-station
    lines.append("B. Per-Station Mean Bias Ratios")
    lines.append("-" * 70)
    station_stats = df.groupby(["station_id", "station_name"]).agg(
        ghi_bias=("bias_ratio_ghi", "mean"),
        dni_bias=("bias_ratio_dni", "mean"),
        n_months=("bias_ratio_ghi", "count"),
        lat=("latitude", "first"),
        network=("network_gt", "first"),
    ).reset_index().sort_values("ghi_bias", ascending=False)

    lines.append(
        f"  {'Station':<10} {'Name':<22} {'Net':<8} {'Lat':>6} {'N':>4} "
        f"{'GHI Bias':>10} {'DNI Bias':>10}"
    )
    lines.append("  " + "-" * 75)
    for _, row in station_stats.iterrows():
        lines.append(
            f"  {row['station_id']:<10} {row['station_name']:<22} "
            f"{row['network']:<8} {row['lat']:>6.1f} {int(row['n_months']):>4} "
            f"{row['ghi_bias']:>10.4f} {row['dni_bias']:>10.4f}"
        )

    # Monthly pattern
    lines.append("")
    lines.append("C. Monthly Bias Pattern")
    lines.append("-" * 40)
    month_stats = df.groupby("month").agg(
        ghi_bias=("bias_ratio_ghi", "mean"),
        dni_bias=("bias_ratio_dni", "mean"),
    ).reset_index()
    lines.append(f"  {'Month':>6} {'GHI Bias':>10} {'DNI Bias':>10}")
    for _, row in month_stats.iterrows():
        lines.append(f"  {int(row['month']):>6} {row['ghi_bias']:>10.4f} {row['dni_bias']:>10.4f}")

    # Correlations
    lines.append("")
    lines.append("D. Bias Correlation with Geography")
    lines.append("-" * 40)
    station_geo = df.groupby("station_id").agg(
        ghi_bias=("bias_ratio_ghi", "mean"),
        latitude=("latitude", "first"),
        longitude=("longitude", "first"),
        elevation_m=("elevation_m", "first"),
    )
    for geo_var in ["latitude", "longitude", "elevation_m"]:
        r = station_geo["ghi_bias"].corr(station_geo[geo_var])
        lines.append(f"  {geo_var:<14} vs GHI bias: r={r:+.3f}")

    return "\n".join(lines) + "\n"


def plot_bias_by_station_v2(df: pd.DataFrame) -> None:
    """Bar chart of per-station GHI bias with network color coding."""
    station_stats = df.groupby(["station_id", "station_name"]).agg(
        ghi_bias=("bias_ratio_ghi", "mean"),
        network=("network_gt", "first"),
    ).reset_index().sort_values("ghi_bias", ascending=True)

    color_map = {"SURFRAD": "#2196F3", "SOLRAD": "#FF9800", "MIDC": "#4CAF50"}
    colors = [color_map.get(n, "#999") for n in station_stats["network"]]

    fig, ax = plt.subplots(figsize=(10, 8))
    bars = ax.barh(range(len(station_stats)), station_stats["ghi_bias"].values,
                   color=colors, edgecolor="white", linewidth=0.5)
    ax.set_yticks(range(len(station_stats)))
    ax.set_yticklabels(
        [f"{row['station_id']} ({row['station_name']})"
         for _, row in station_stats.iterrows()], fontsize=8,
    )
    ax.axvline(x=1.0, color="red", linestyle="--", linewidth=1.5, label="No bias")

    for i, val in enumerate(station_stats["ghi_bias"].values):
        ax.text(val + 0.005, i, f"{val:.3f}", va="center", fontsize=7)

    # Legend for networks
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor=c, label=n) for n, c in color_map.items()]
    legend_elements.append(plt.Line2D([0], [0], color="red", linestyle="--", label="No bias"))
    ax.legend(handles=legend_elements, loc="lower right", fontsize=9)

    ax.set_xlabel("GHI Bias Ratio (NSRDB / Ground Truth)")
    ax.set_title("NSRDB GHI Bias by Station — 20 Stations (v2)")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "bias_by_station_ghi_v2.png", dpi=150)
    plt.close(fig)
    logger.info("Saved bias_by_station_ghi_v2.png")


def plot_heatmap_v2(df: pd.DataFrame) -> None:
    """Correction factor heatmap for all 20 stations."""
    pivot = df.groupby(["station_id", "month"])["correction_factor_ghi"].mean().unstack()
    pivot = pivot.loc[pivot.mean(axis=1).sort_values().index]

    fig, ax = plt.subplots(figsize=(12, 10))
    im = ax.imshow(pivot.values, aspect="auto", cmap="RdBu_r", vmin=0.85, vmax=1.15)

    ax.set_xticks(range(12))
    ax.set_xticklabels(["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                         "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    labels = [f"{sid} ({STATION_META[sid]['name']})" for sid in pivot.index]
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=8)

    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            val = pivot.iloc[i, j]
            if not np.isnan(val):
                ax.text(j, i, f"{val:.2f}", ha="center", va="center", fontsize=6,
                        color="white" if abs(val - 1.0) > 0.08 else "black")

    ax.set_title("GHI Correction Factor by Station × Month — 20 Stations (v2)")
    fig.colorbar(im, ax=ax, shrink=0.7, label="Correction Factor")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "correction_factor_heatmap_v2.png", dpi=150)
    plt.close(fig)
    logger.info("Saved correction_factor_heatmap_v2.png")


# ===================================================================
# STEP 2: Model Training (v2)
# ===================================================================

def run_loso_cv_v2(df: pd.DataFrame) -> pd.DataFrame:
    """Run leave-one-station-out CV for all generalizable models on 20 stations."""
    stations = sorted(df["station_id"].unique())
    logger.info("Running LOSO CV across %d stations", len(stations))

    all_predictions: list[dict] = []

    for held_out in stations:
        train = df[df["station_id"] != held_out]
        test = df[df["station_id"] == held_out]

        for model_name in GENERALIZABLE_MODELS:
            preds: dict[str, np.ndarray] = {}
            for var in IRRADIANCE_VARS:
                target_col = f"correction_factor_{var}"

                if model_name == "monthly_climatology":
                    model = MonthlyClimatology()
                    model.fit(train, target_col)
                    preds[var] = model.predict(test)
                else:
                    X_train = build_features(train, var)
                    y_train = train[target_col].values
                    X_test = build_features(test, var)
                    sklearn_model = get_sklearn_model(model_name)
                    sklearn_model.fit(X_train, y_train)
                    preds[var] = sklearn_model.predict(X_test)

                if np.any(np.isnan(preds[var])) or np.any(np.isinf(preds[var])):
                    raise RuntimeError(
                        f"STOP: NaN/inf in {model_name} {var} for held_out={held_out}"
                    )

            for i, (_, row) in enumerate(test.iterrows()):
                all_predictions.append({
                    "station_id": row["station_id"],
                    "year": row["year"],
                    "month": row["month"],
                    "model_name": model_name,
                    "predicted_cf_ghi": preds["ghi"][i],
                    "actual_cf_ghi": row["correction_factor_ghi"],
                    "predicted_cf_dni": preds["dni"][i],
                    "actual_cf_dni": row["correction_factor_dni"],
                    "nsrdb_ghi": row["ghi_kwh_m2_day_nsrdb"],
                    "nsrdb_dni": row["dni_kwh_m2_day_nsrdb"],
                    "ground_truth_ghi": row["ghi_kwh_m2_day_gt"],
                    "ground_truth_dni": row["dni_kwh_m2_day_gt"],
                    "corrected_ghi_error": abs(
                        row["ghi_kwh_m2_day_nsrdb"] * preds["ghi"][i] - row["ghi_kwh_m2_day_gt"]
                    ),
                    "uncorrected_ghi_error": abs(
                        row["ghi_kwh_m2_day_nsrdb"] - row["ghi_kwh_m2_day_gt"]
                    ),
                    "corrected_dni_error": abs(
                        row["dni_kwh_m2_day_nsrdb"] * preds["dni"][i] - row["dni_kwh_m2_day_gt"]
                    ),
                    "uncorrected_dni_error": abs(
                        row["dni_kwh_m2_day_nsrdb"] - row["dni_kwh_m2_day_gt"]
                    ),
                })

    return pd.DataFrame(all_predictions)


def build_model_comparison(cv_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Build model comparison table from CV results."""
    rows = []

    for model_name in GENERALIZABLE_MODELS:
        model_preds = cv_df[cv_df["model_name"] == model_name]
        for var in IRRADIANCE_VARS:
            actual = model_preds[f"actual_cf_{var}"].values
            predicted = model_preds[f"predicted_cf_{var}"].values
            nsrdb = model_preds[f"nsrdb_{var}"].values
            gt = model_preds[f"ground_truth_{var}"].values
            metrics = compute_metrics(actual, predicted, nsrdb, gt)
            pct_improv = (
                (metrics["uncorrected_error_kwh"] - metrics["corrected_error_kwh"])
                / metrics["uncorrected_error_kwh"] * 100
            )
            rows.append({
                "model": model_name, "variable": var.upper(),
                "mae_cf": metrics["mae_cf"], "rmse_cf": metrics["rmse_cf"],
                "mean_bias_cf": metrics["mean_bias_cf"],
                "corrected_error_kwh": metrics["corrected_error_kwh"],
                "uncorrected_error_kwh": metrics["uncorrected_error_kwh"],
                "pct_improvement": pct_improv,
            })

    # Station-month climatology in-sample
    for var in IRRADIANCE_VARS:
        target_col = f"correction_factor_{var}"
        model = StationMonthClimatology()
        model.fit(df, target_col)
        predicted = model.predict(df)
        nsrdb_col = f"{var}_kwh_m2_day_nsrdb"
        gt_col = f"{var}_kwh_m2_day_gt"
        metrics = compute_metrics(
            df[target_col].values, predicted, df[nsrdb_col].values, df[gt_col].values,
        )
        pct_improv = (
            (metrics["uncorrected_error_kwh"] - metrics["corrected_error_kwh"])
            / metrics["uncorrected_error_kwh"] * 100
        )
        rows.append({
            "model": "station_month_climatology*", "variable": var.upper(),
            "mae_cf": metrics["mae_cf"], "rmse_cf": metrics["rmse_cf"],
            "mean_bias_cf": metrics["mean_bias_cf"],
            "corrected_error_kwh": metrics["corrected_error_kwh"],
            "uncorrected_error_kwh": metrics["uncorrected_error_kwh"],
            "pct_improvement": pct_improv,
        })

    return pd.DataFrame(rows)


def format_comparison(comp_df: pd.DataFrame) -> str:
    """Format model comparison as text table."""
    lines = []
    lines.append("MODEL COMPARISON TABLE (v2 — 20 stations, LOSO CV)")
    lines.append("=" * 115)
    lines.append(
        f"{'Model':<30} {'Var':>4} {'MAE CF':>8} {'RMSE CF':>9} "
        f"{'Bias CF':>9} {'Corr Err':>10} {'Uncorr Err':>11} {'% Improv':>9}"
    )
    lines.append("-" * 115)

    for _, row in comp_df.iterrows():
        lines.append(
            f"{row['model']:<30} {row['variable']:>4} "
            f"{row['mae_cf']:>8.4f} {row['rmse_cf']:>9.4f} "
            f"{row['mean_bias_cf']:>+9.4f} {row['corrected_error_kwh']:>10.4f} "
            f"{row['uncorrected_error_kwh']:>11.4f} {row['pct_improvement']:>+8.1f}%"
        )

    lines.append("-" * 115)
    lines.append("* station_month_climatology is IN-SAMPLE only")
    return "\n".join(lines)


# ===================================================================
# Model Training Plots (v2)
# ===================================================================

def plot_model_comparison_bar_v2(comp_df: pd.DataFrame) -> None:
    """Grouped bar chart comparing RMSE across all models."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    for ax, var in zip(axes, ["GHI", "DNI"]):
        var_data = comp_df[comp_df["variable"] == var].sort_values("rmse_cf")
        colors = ["#4CAF50" if "*" in m else "#2196F3" for m in var_data["model"]]
        ax.barh(range(len(var_data)), var_data["rmse_cf"].values, color=colors,
                edgecolor="white", linewidth=0.5)
        ax.set_yticks(range(len(var_data)))
        ax.set_yticklabels(var_data["model"].values, fontsize=9)
        for i, val in enumerate(var_data["rmse_cf"].values):
            ax.text(val + 0.001, i, f"{val:.4f}", va="center", fontsize=8)
        ax.set_xlabel("RMSE of Correction Factor")
        ax.set_title(f"{var} Correction Factor RMSE")
        ax.grid(axis="x", alpha=0.3)

    plt.suptitle("Model Comparison: RMSE (v2, 20 stations LOSO CV)", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "model_comparison_bar_v2.png", dpi=150)
    plt.close(fig)
    logger.info("Saved model_comparison_bar_v2.png")


def plot_scatter_v2(cv_df: pd.DataFrame) -> None:
    """Scatter of predicted vs actual CF — subplots per model."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))
    for ax, model_name in zip(axes.flat, GENERALIZABLE_MODELS):
        model_preds = cv_df[cv_df["model_name"] == model_name]
        actual = model_preds["actual_cf_ghi"].values
        predicted = model_preds["predicted_cf_ghi"].values
        ax.scatter(actual, predicted, alpha=0.25, s=8, color="#2196F3")
        lims = [min(actual.min(), predicted.min()) - 0.02,
                max(actual.max(), predicted.max()) + 0.02]
        ax.plot(lims, lims, "--", color="red", linewidth=1.5)
        r2 = r2_score(actual, predicted)
        rmse = np.sqrt(mean_squared_error(actual, predicted))
        ax.text(0.05, 0.95, f"R² = {r2:.3f}\nRMSE = {rmse:.4f}",
                transform=ax.transAxes, fontsize=9, va="top",
                bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.8))
        ax.set_xlabel("Actual CF (GHI)")
        ax.set_ylabel("Predicted CF (GHI)")
        ax.set_title(model_name.replace("_", " ").title(), fontsize=11)
        ax.grid(True, alpha=0.3)
        ax.set_aspect("equal")

    plt.suptitle("Predicted vs Actual GHI CF (v2, 20 stations LOSO)", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "scatter_predicted_vs_actual_v2.png", dpi=150)
    plt.close(fig)
    logger.info("Saved scatter_predicted_vs_actual_v2.png")


def plot_correction_by_station_v2(cv_df: pd.DataFrame) -> None:
    """Per-station GHI error before vs after correction — subplots per model."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 16))
    for ax, model_name in zip(axes.flat, GENERALIZABLE_MODELS):
        model_preds = cv_df[cv_df["model_name"] == model_name]
        station_errors = model_preds.groupby("station_id").agg(
            corrected=("corrected_ghi_error", "mean"),
            uncorrected=("uncorrected_ghi_error", "mean"),
        ).sort_values("uncorrected", ascending=True)

        x = np.arange(len(station_errors))
        w = 0.35
        ax.barh(x - w/2, station_errors["uncorrected"], w, label="Uncorrected",
                color="#FF5722", alpha=0.8)
        ax.barh(x + w/2, station_errors["corrected"], w, label="Corrected",
                color="#2196F3", alpha=0.8)
        ax.set_yticks(x)
        ax.set_yticklabels(station_errors.index, fontsize=7)
        ax.set_xlabel("MAE (kWh/m²/day)")
        ax.set_title(model_name.replace("_", " ").title(), fontsize=11)
        ax.legend(fontsize=8, loc="lower right")
        ax.grid(axis="x", alpha=0.3)

    plt.suptitle("GHI Error by Station: Before vs After (v2, 20 stations)", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "correction_by_station_v2.png", dpi=150)
    plt.close(fig)
    logger.info("Saved correction_by_station_v2.png")


def plot_seasonal_v2(cv_df: pd.DataFrame) -> None:
    """Monthly mean predicted vs actual CF — subplots per model."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    for ax, model_name in zip(axes.flat, GENERALIZABLE_MODELS):
        model_preds = cv_df[cv_df["model_name"] == model_name]
        monthly = model_preds.groupby("month").agg(
            predicted=("predicted_cf_ghi", "mean"),
            actual=("actual_cf_ghi", "mean"),
        )
        ax.plot(monthly.index, monthly["actual"], "o-", color="#FF5722", label="Actual", lw=2)
        ax.plot(monthly.index, monthly["predicted"], "s--", color="#2196F3", label="Predicted", lw=2)
        ax.axhline(y=1.0, color="gray", linestyle=":", alpha=0.5)
        ax.set_xticks(range(1, 13))
        ax.set_xticklabels(["J","F","M","A","M","J","J","A","S","O","N","D"])
        ax.set_ylabel("CF (GHI)")
        ax.set_title(model_name.replace("_", " ").title(), fontsize=11)
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    plt.suptitle("Seasonal Pattern: Predicted vs Actual GHI CF (v2)", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "seasonal_pattern_v2.png", dpi=150)
    plt.close(fig)
    logger.info("Saved seasonal_pattern_v2.png")


def plot_feature_importance_v2(df: pd.DataFrame) -> None:
    """Feature importance for RF and GB side by side."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    feature_names = ["latitude", "longitude", "elevation_m", "month", "nsrdb_ghi"]

    for ax, (label, ModelClass, params) in zip(axes, [
        ("Random Forest", RandomForestRegressor, dict(
            n_estimators=200, max_depth=10, min_samples_leaf=5, random_state=42, n_jobs=-1)),
        ("Gradient Boosting", GradientBoostingRegressor, dict(
            n_estimators=200, max_depth=4, learning_rate=0.1, min_samples_leaf=5, random_state=42)),
    ]):
        X = build_features(df, "ghi")
        y = df["correction_factor_ghi"].values
        model = ModelClass(**params)
        model.fit(X, y)
        importances = model.feature_importances_
        sorted_idx = np.argsort(importances)

        ax.barh(range(len(sorted_idx)), importances[sorted_idx], color="#FF9800", edgecolor="white")
        ax.set_yticks(range(len(sorted_idx)))
        ax.set_yticklabels([feature_names[i] for i in sorted_idx], fontsize=10)
        ax.set_xlabel("Feature Importance")
        ax.set_title(f"{label} (GHI)")
        for i, idx in enumerate(sorted_idx):
            ax.text(importances[idx] + 0.005, i, f"{importances[idx]:.3f}", va="center", fontsize=9)
        ax.grid(axis="x", alpha=0.3)

    plt.suptitle("Feature Importance (v2, 20 stations, GHI)", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "feature_importance_v2.png", dpi=150)
    plt.close(fig)
    logger.info("Saved feature_importance_v2.png")


# ===================================================================
# STEP 3: v1 vs v2 Comparison
# ===================================================================

def compare_v1_v2(comp_v2: pd.DataFrame) -> str:
    """Compare v1 (14 stations) vs v2 (20 stations) results side by side."""
    # Load v1 results
    v1_path = OUTPUT_DIR / "model_comparison.txt"
    if not v1_path.exists():
        return "v1 model_comparison.txt not found — cannot compare."

    # Parse v1 RMSE values from the text file
    v1_rmse: dict[tuple[str, str], float] = {}
    v1_improv: dict[tuple[str, str], float] = {}
    for line in v1_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("=") or line.startswith("-") or line.startswith("*") or line.startswith("CF"):
            continue
        parts = line.split()
        if len(parts) >= 7:
            model = parts[0]
            var = parts[1]
            try:
                rmse = float(parts[3])
                improv_str = parts[-1].rstrip("%")
                improv = float(improv_str)
                v1_rmse[(model, var)] = rmse
                v1_improv[(model, var)] = improv
            except (ValueError, IndexError):
                continue

    lines = []
    lines.append("\nv1 (14 STATIONS) vs v2 (20 STATIONS) COMPARISON")
    lines.append("=" * 100)
    lines.append(
        f"{'Model':<30} {'Var':>4} {'v1 RMSE':>9} {'v2 RMSE':>9} {'Change':>9} "
        f"{'v1 %Imp':>8} {'v2 %Imp':>8} {'Delta':>8}"
    )
    lines.append("-" * 100)

    for _, row in comp_v2.iterrows():
        model = row["model"]
        var = row["variable"]
        v2_rmse = row["rmse_cf"]
        v2_improv = row["pct_improvement"]

        v1_key = (model.rstrip("*"), var)
        if v1_key not in v1_rmse:
            # Try matching with asterisk
            v1_key = (model, var)
        if v1_key not in v1_rmse:
            continue

        v1_r = v1_rmse[v1_key]
        v1_i = v1_improv[v1_key]
        rmse_change = v2_rmse - v1_r
        improv_delta = v2_improv - v1_i

        better = "better" if rmse_change < 0 else "worse" if rmse_change > 0 else "same"
        lines.append(
            f"{model:<30} {var:>4} {v1_r:>9.4f} {v2_rmse:>9.4f} "
            f"{rmse_change:>+9.4f} {v1_i:>+7.1f}% {v2_improv:>+7.1f}% "
            f"{improv_delta:>+7.1f}%"
        )

    lines.append("-" * 100)

    # Analysis
    lines.append("")
    lines.append("ANALYSIS:")

    # Check if spatial models improved relative to monthly climatology
    v2_monthly_ghi = comp_v2[
        (comp_v2["model"] == "monthly_climatology") & (comp_v2["variable"] == "GHI")
    ]["rmse_cf"].values[0]

    for model_name in ["linear_regression", "random_forest", "gradient_boosting"]:
        v2_r = comp_v2[
            (comp_v2["model"] == model_name) & (comp_v2["variable"] == "GHI")
        ]["rmse_cf"].values[0]
        v1_r = v1_rmse.get((model_name, "GHI"), None)
        v1_monthly = v1_rmse.get(("monthly_climatology", "GHI"), None)

        if v1_r is not None and v1_monthly is not None:
            v1_gap = v1_r - v1_monthly
            v2_gap = v2_r - v2_monthly_ghi
            lines.append(
                f"  {model_name}: gap vs monthly clim: "
                f"v1={v1_gap:+.4f}, v2={v2_gap:+.4f} "
                f"({'narrowed' if v2_gap < v1_gap else 'widened'})"
            )

    return "\n".join(lines)


# ===================================================================
# Main
# ===================================================================

def main() -> None:
    """Run full v2 analysis pipeline."""
    logger.info("=" * 60)
    logger.info("V2 ANALYSIS — 20 stations (SURFRAD + SOLRAD + MIDC)")
    logger.info("=" * 60)

    # --- Step 1: Bias analysis ---
    logger.info("\n--- STEP 1: Bias Analysis ---")
    merged = load_and_merge_v2()
    df = compute_bias(merged)

    # Summary
    summary = bias_summary(df)
    summary_path = OUTPUT_DIR / "bias_analysis_summary_v2.txt"
    summary_path.write_text(summary)
    print("\n" + summary)

    # Save merged dataset
    dataset_path = OUTPUT_DIR / "bias_merged_dataset_v2.csv"
    df.to_csv(dataset_path, index=False)
    logger.info("Saved %s (%d rows)", dataset_path.name, len(df))

    # Plots
    plot_bias_by_station_v2(df)
    plot_heatmap_v2(df)

    # --- Step 2: Model training ---
    logger.info("\n--- STEP 2: Model Training ---")
    cv_df = run_loso_cv_v2(df)
    comp_df = build_model_comparison(cv_df, df)

    # Format and print comparison table
    comp_text = format_comparison(comp_df)
    print("\n" + comp_text)

    report_path = OUTPUT_DIR / "model_comparison_v2.txt"
    report_path.write_text(comp_text + "\n")
    logger.info("Saved %s", report_path.name)

    # Save CV predictions
    cv_out_cols = [
        "station_id", "year", "month", "model_name",
        "predicted_cf_ghi", "actual_cf_ghi", "predicted_cf_dni", "actual_cf_dni",
        "corrected_ghi_error", "uncorrected_ghi_error",
        "corrected_dni_error", "uncorrected_dni_error",
    ]
    cv_path = OUTPUT_DIR / "cv_predictions_v2.csv"
    cv_df[cv_out_cols].to_csv(cv_path, index=False)
    logger.info("Saved %s (%d rows)", cv_path.name, len(cv_df))

    # Plots
    plot_model_comparison_bar_v2(comp_df)
    plot_scatter_v2(cv_df)
    plot_correction_by_station_v2(cv_df)
    plot_seasonal_v2(cv_df)
    plot_feature_importance_v2(df)

    # --- Step 3: v1 vs v2 comparison ---
    logger.info("\n--- STEP 3: v1 vs v2 Comparison ---")
    comparison = compare_v1_v2(comp_df)
    print(comparison)

    # Append comparison to report
    full_report = comp_text + "\n\n" + comparison + "\n"
    report_path.write_text(full_report)

    # Files generated
    print("\n" + "=" * 60)
    print("FILES GENERATED")
    print("=" * 60)
    for f in sorted(OUTPUT_DIR.glob("*_v2*")):
        print(f"  {f.name}")


if __name__ == "__main__":
    main()
