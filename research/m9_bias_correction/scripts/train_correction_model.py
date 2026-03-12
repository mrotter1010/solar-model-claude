"""Train and evaluate NSRDB bias correction models with leave-one-station-out CV.

Models evaluated:
1. Monthly Climatology Baseline — mean correction factor per month
2. Station-Month Climatology — memorized per station×month (in-sample reference only)
3. Linear Regression — spatial + temporal + irradiance features
4. Random Forest — conservative hyperparameters (14 stations → avoid overfit)
5. Gradient Boosting — conservative hyperparameters

Evaluation: leave-one-station-out cross-validation (LOSO) for models 1, 3, 4, 5.
Model 2 reports in-sample fit only (cannot generalize to unseen stations).
"""

import logging
import sys
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FEATURE_COLS = ["latitude", "longitude", "elevation_m", "month", "nsrdb_value"]
GENERALIZABLE_MODELS = ["monthly_climatology", "linear_regression", "random_forest", "gradient_boosting"]
ALL_MODELS = GENERALIZABLE_MODELS + ["station_month_climatology"]

IRRADIANCE_VARS = ["ghi", "dni"]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_data() -> pd.DataFrame:
    """Load the merged bias dataset."""
    path = OUTPUT_DIR / "bias_merged_dataset.csv"
    df = pd.read_csv(path)
    logger.info("Loaded %d rows from %s", len(df), path.name)

    # Sanity checks
    n_stations = df["station_id"].nunique()
    logger.info("Stations: %d, Years: %s", n_stations, sorted(df["year"].unique()))

    for var in IRRADIANCE_VARS:
        cf_col = f"correction_factor_{var}"
        cf_min, cf_max = df[cf_col].min(), df[cf_col].max()
        if cf_min < 0.5 or cf_max > 1.5:
            logger.warning(
                "WARNING: %s has values outside [0.5, 1.5]: min=%.3f, max=%.3f",
                cf_col, cf_min, cf_max,
            )

    # Check for NaN/inf in critical columns
    critical_cols = [
        "correction_factor_ghi", "correction_factor_dni",
        "ghi_kwh_m2_day_gt", "dni_kwh_m2_day_gt",
        "ghi_kwh_m2_day_nsrdb", "dni_kwh_m2_day_nsrdb",
        "latitude", "longitude", "elevation_m",
    ]
    for col in critical_cols:
        n_bad = df[col].isna().sum() + np.isinf(df[col]).sum()
        if n_bad > 0:
            raise RuntimeError(f"STOP: {col} has {n_bad} NaN/inf values")

    return df


# ---------------------------------------------------------------------------
# Model definitions
# ---------------------------------------------------------------------------
class MonthlyClimatology:
    """Mean correction factor per month across training stations."""

    def __init__(self) -> None:
        self.monthly_means: dict[int, float] = {}

    def fit(self, df: pd.DataFrame, target_col: str) -> None:
        self.monthly_means = df.groupby("month")[target_col].mean().to_dict()

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        return df["month"].map(self.monthly_means).values


class StationMonthClimatology:
    """Mean correction factor per station×month (memorizes training data)."""

    def __init__(self) -> None:
        self.lookup: dict[tuple[str, int], float] = {}
        self.global_mean: float = 1.0

    def fit(self, df: pd.DataFrame, target_col: str) -> None:
        grouped = df.groupby(["station_id", "month"])[target_col].mean()
        self.lookup = grouped.to_dict()
        self.global_mean = df[target_col].mean()

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        return np.array([
            self.lookup.get((sid, m), self.global_mean)
            for sid, m in zip(df["station_id"], df["month"])
        ])


def build_features(df: pd.DataFrame, var: str) -> np.ndarray:
    """Build feature matrix for sklearn models.

    Args:
        df: DataFrame with latitude, longitude, elevation_m, month columns.
        var: 'ghi' or 'dni' — determines which NSRDB column to use.

    Returns:
        Feature array with columns: latitude, longitude, elevation_m, month, nsrdb_value.
    """
    nsrdb_col = f"{var}_kwh_m2_day_nsrdb"
    return df[["latitude", "longitude", "elevation_m", "month", nsrdb_col]].values


def get_sklearn_model(model_name: str) -> object:
    """Create a fresh sklearn model instance."""
    if model_name == "linear_regression":
        return LinearRegression()
    elif model_name == "random_forest":
        return RandomForestRegressor(
            n_estimators=200, max_depth=10, min_samples_leaf=5, random_state=42, n_jobs=-1,
        )
    elif model_name == "gradient_boosting":
        return GradientBoostingRegressor(
            n_estimators=200, max_depth=4, learning_rate=0.1, min_samples_leaf=5, random_state=42,
        )
    else:
        raise ValueError(f"Unknown sklearn model: {model_name}")


# ---------------------------------------------------------------------------
# Evaluation metrics
# ---------------------------------------------------------------------------
def compute_metrics(
    actual_cf: np.ndarray,
    predicted_cf: np.ndarray,
    nsrdb_values: np.ndarray,
    ground_truth_values: np.ndarray,
) -> dict[str, float]:
    """Compute evaluation metrics for a set of predictions.

    Args:
        actual_cf: Actual correction factors (ground_truth / nsrdb).
        predicted_cf: Predicted correction factors.
        nsrdb_values: Raw NSRDB irradiance values (kWh/m²/day).
        ground_truth_values: Ground truth irradiance values (kWh/m²/day).

    Returns:
        Dictionary with MAE, RMSE, mean_bias, corrected_error, uncorrected_error.
    """
    mae = mean_absolute_error(actual_cf, predicted_cf)
    rmse = np.sqrt(mean_squared_error(actual_cf, predicted_cf))
    mean_bias = float(np.mean(predicted_cf - actual_cf))

    # Corrected NSRDB = nsrdb * predicted_cf
    corrected_nsrdb = nsrdb_values * predicted_cf
    corrected_error = float(np.mean(np.abs(corrected_nsrdb - ground_truth_values)))

    # Uncorrected error
    uncorrected_error = float(np.mean(np.abs(nsrdb_values - ground_truth_values)))

    return {
        "mae_cf": mae,
        "rmse_cf": rmse,
        "mean_bias_cf": mean_bias,
        "corrected_error_kwh": corrected_error,
        "uncorrected_error_kwh": uncorrected_error,
    }


# ---------------------------------------------------------------------------
# Leave-one-station-out cross-validation
# ---------------------------------------------------------------------------
def run_loso_cv(df: pd.DataFrame) -> pd.DataFrame:
    """Run leave-one-station-out CV for all generalizable models.

    Returns:
        DataFrame of per-row predictions: station_id, year, month, model_name,
        predicted_cf_ghi, actual_cf_ghi, predicted_cf_dni, actual_cf_dni, etc.
    """
    stations = sorted(df["station_id"].unique())
    logger.info("Running LOSO CV across %d stations", len(stations))

    all_predictions: list[dict] = []

    for held_out in stations:
        train = df[df["station_id"] != held_out]
        test = df[df["station_id"] == held_out]
        logger.info("  Hold out: %s (%d test rows, %d train rows)", held_out, len(test), len(train))

        for model_name in GENERALIZABLE_MODELS:
            pred_row_template = {
                "station_id": None,
                "year": None,
                "month": None,
                "model_name": model_name,
            }

            # Train and predict for each irradiance variable
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

                # Check for NaN/inf
                if np.any(np.isnan(preds[var])) or np.any(np.isinf(preds[var])):
                    raise RuntimeError(
                        f"STOP: NaN/inf in predictions for model={model_name}, "
                        f"var={var}, held_out={held_out}"
                    )

            # Collect per-row predictions
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

    cv_df = pd.DataFrame(all_predictions)
    logger.info("LOSO CV complete: %d prediction rows", len(cv_df))
    return cv_df


def compute_station_month_insample(df: pd.DataFrame) -> dict[str, dict[str, float]]:
    """Compute in-sample fit metrics for station-month climatology (Model 2).

    Returns:
        Dict keyed by var ('ghi', 'dni') → metrics dict.
    """
    results = {}
    for var in IRRADIANCE_VARS:
        target_col = f"correction_factor_{var}"
        model = StationMonthClimatology()
        model.fit(df, target_col)
        predicted = model.predict(df)

        nsrdb_col = f"{var}_kwh_m2_day_nsrdb"
        gt_col = f"{var}_kwh_m2_day_gt"

        results[var] = compute_metrics(
            df[target_col].values, predicted, df[nsrdb_col].values, df[gt_col].values,
        )
    return results


# ---------------------------------------------------------------------------
# Results aggregation and reporting
# ---------------------------------------------------------------------------
def aggregate_model_comparison(cv_df: pd.DataFrame, insample: dict) -> pd.DataFrame:
    """Build the model comparison table.

    Args:
        cv_df: Cross-validation predictions DataFrame.
        insample: In-sample metrics for station-month climatology.

    Returns:
        DataFrame with one row per model×variable.
    """
    rows = []

    for model_name in GENERALIZABLE_MODELS:
        model_preds = cv_df[cv_df["model_name"] == model_name]

        for var in IRRADIANCE_VARS:
            actual = model_preds[f"actual_cf_{var}"].values
            predicted = model_preds[f"predicted_cf_{var}"].values
            nsrdb = model_preds[f"nsrdb_{var}"].values
            gt = model_preds[f"ground_truth_{var}"].values

            metrics = compute_metrics(actual, predicted, nsrdb, gt)
            pct_improvement = (
                (metrics["uncorrected_error_kwh"] - metrics["corrected_error_kwh"])
                / metrics["uncorrected_error_kwh"] * 100
            )

            rows.append({
                "model": model_name,
                "variable": var.upper(),
                "mae_cf": metrics["mae_cf"],
                "rmse_cf": metrics["rmse_cf"],
                "mean_bias_cf": metrics["mean_bias_cf"],
                "corrected_error_kwh": metrics["corrected_error_kwh"],
                "uncorrected_error_kwh": metrics["uncorrected_error_kwh"],
                "pct_improvement": pct_improvement,
            })

    # Add station-month climatology (in-sample)
    for var in IRRADIANCE_VARS:
        m = insample[var]
        pct_improvement = (
            (m["uncorrected_error_kwh"] - m["corrected_error_kwh"])
            / m["uncorrected_error_kwh"] * 100
        )
        rows.append({
            "model": "station_month_climatology*",
            "variable": var.upper(),
            "mae_cf": m["mae_cf"],
            "rmse_cf": m["rmse_cf"],
            "mean_bias_cf": m["mean_bias_cf"],
            "corrected_error_kwh": m["corrected_error_kwh"],
            "uncorrected_error_kwh": m["uncorrected_error_kwh"],
            "pct_improvement": pct_improvement,
        })

    return pd.DataFrame(rows)


def per_station_results(cv_df: pd.DataFrame) -> pd.DataFrame:
    """Per-station mean errors for each generalizable model.

    Returns:
        DataFrame: station_id × model_name × variable with corrected/uncorrected errors.
    """
    rows = []
    for model_name in GENERALIZABLE_MODELS:
        model_preds = cv_df[cv_df["model_name"] == model_name]

        for station_id, sdf in model_preds.groupby("station_id"):
            for var in IRRADIANCE_VARS:
                corrected_err = sdf[f"corrected_{var}_error"].mean()
                uncorrected_err = sdf[f"uncorrected_{var}_error"].mean()
                pct_improvement = (
                    (uncorrected_err - corrected_err) / uncorrected_err * 100
                    if uncorrected_err > 0 else 0.0
                )
                rows.append({
                    "model": model_name,
                    "station_id": station_id,
                    "variable": var.upper(),
                    "corrected_error_kwh": corrected_err,
                    "uncorrected_error_kwh": uncorrected_err,
                    "pct_improvement": pct_improvement,
                    "n_months": len(sdf),
                })

    return pd.DataFrame(rows)


def format_comparison_table(comp_df: pd.DataFrame) -> str:
    """Format model comparison as a printable text table."""
    lines = []
    lines.append("MODEL COMPARISON TABLE (Leave-One-Station-Out Cross-Validation)")
    lines.append("=" * 110)
    lines.append(
        f"{'Model':<30} {'Var':>4} {'MAE CF':>8} {'RMSE CF':>9} "
        f"{'Bias CF':>9} {'Corr Err':>10} {'Uncorr Err':>11} {'% Improv':>9}"
    )
    lines.append("-" * 110)

    for _, row in comp_df.iterrows():
        model_label = row["model"]
        lines.append(
            f"{model_label:<30} {row['variable']:>4} "
            f"{row['mae_cf']:>8.4f} {row['rmse_cf']:>9.4f} "
            f"{row['mean_bias_cf']:>+9.4f} {row['corrected_error_kwh']:>10.4f} "
            f"{row['uncorrected_error_kwh']:>11.4f} {row['pct_improvement']:>+8.1f}%"
        )

    lines.append("-" * 110)
    lines.append("* station_month_climatology is IN-SAMPLE only (cannot generalize to new stations)")
    lines.append("  CF = correction factor | Err = mean absolute error (kWh/m²/day)")
    lines.append("  % Improv = reduction in NSRDB error after applying predicted correction")
    return "\n".join(lines)


def format_per_station_table(station_df: pd.DataFrame) -> str:
    """Format per-station results as a printable text table."""
    lines = []
    lines.append("\nPER-STATION RESULTS (mean kWh/m²/day error)")
    lines.append("=" * 100)

    for model_name in GENERALIZABLE_MODELS:
        model_data = station_df[station_df["model"] == model_name]
        lines.append(f"\n--- {model_name} ---")
        lines.append(
            f"  {'Station':<8} {'Var':>4} {'Corr Err':>10} {'Uncorr Err':>11} "
            f"{'% Improv':>9} {'N months':>9}"
        )
        lines.append("  " + "-" * 60)

        for _, row in model_data.sort_values(["variable", "pct_improvement"], ascending=[True, False]).iterrows():
            lines.append(
                f"  {row['station_id']:<8} {row['variable']:>4} "
                f"{row['corrected_error_kwh']:>10.4f} {row['uncorrected_error_kwh']:>11.4f} "
                f"{row['pct_improvement']:>+8.1f}% {int(row['n_months']):>9}"
            )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------
def plot_model_comparison_bar(comp_df: pd.DataFrame) -> None:
    """Grouped bar chart comparing RMSE across all models for GHI and DNI."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    for ax, var in zip(axes, ["GHI", "DNI"]):
        var_data = comp_df[comp_df["variable"] == var].sort_values("rmse_cf")
        colors = ["#4CAF50" if "climatology*" in m else "#2196F3"
                   for m in var_data["model"]]

        bars = ax.barh(range(len(var_data)), var_data["rmse_cf"].values, color=colors,
                       edgecolor="white", linewidth=0.5)
        ax.set_yticks(range(len(var_data)))
        ax.set_yticklabels(var_data["model"].values, fontsize=9)

        for i, val in enumerate(var_data["rmse_cf"].values):
            ax.text(val + 0.001, i, f"{val:.4f}", va="center", fontsize=8)

        ax.set_xlabel("RMSE of Correction Factor")
        ax.set_title(f"{var} Correction Factor RMSE")
        ax.grid(axis="x", alpha=0.3)

    plt.suptitle("Model Comparison: Correction Factor RMSE (LOSO CV)", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "model_comparison_bar.png", dpi=150)
    plt.close(fig)
    logger.info("Saved model_comparison_bar.png")


def plot_correction_by_station(cv_df: pd.DataFrame) -> None:
    """Per-station GHI error before vs after correction — subplots per model."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 14))

    for ax, model_name in zip(axes.flat, GENERALIZABLE_MODELS):
        model_preds = cv_df[cv_df["model_name"] == model_name]
        station_errors = model_preds.groupby("station_id").agg(
            corrected=("corrected_ghi_error", "mean"),
            uncorrected=("uncorrected_ghi_error", "mean"),
        ).sort_values("uncorrected", ascending=True)

        x = np.arange(len(station_errors))
        w = 0.35
        ax.barh(x - w / 2, station_errors["uncorrected"], w, label="Uncorrected NSRDB",
                color="#FF5722", alpha=0.8)
        ax.barh(x + w / 2, station_errors["corrected"], w, label="Corrected NSRDB",
                color="#2196F3", alpha=0.8)
        ax.set_yticks(x)
        ax.set_yticklabels(station_errors.index, fontsize=8)
        ax.set_xlabel("Mean Absolute Error (kWh/m²/day)")
        ax.set_title(model_name.replace("_", " ").title(), fontsize=11)
        ax.legend(fontsize=8, loc="lower right")
        ax.grid(axis="x", alpha=0.3)

    plt.suptitle("GHI Error by Station: Before vs After Correction (LOSO CV)",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "correction_by_station_all_models.png", dpi=150)
    plt.close(fig)
    logger.info("Saved correction_by_station_all_models.png")


def plot_seasonal_pattern(cv_df: pd.DataFrame, df_full: pd.DataFrame) -> None:
    """Monthly mean predicted vs actual correction factor — subplots per model."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))

    for ax, model_name in zip(axes.flat, GENERALIZABLE_MODELS):
        model_preds = cv_df[cv_df["model_name"] == model_name]
        monthly = model_preds.groupby("month").agg(
            predicted=("predicted_cf_ghi", "mean"),
            actual=("actual_cf_ghi", "mean"),
        )

        months = monthly.index.values
        ax.plot(months, monthly["actual"], "o-", color="#FF5722", label="Actual CF", linewidth=2)
        ax.plot(months, monthly["predicted"], "s--", color="#2196F3", label="Predicted CF", linewidth=2)

        ax.axhline(y=1.0, color="gray", linestyle=":", alpha=0.5)
        ax.set_xlabel("Month")
        ax.set_ylabel("Correction Factor (GHI)")
        ax.set_title(model_name.replace("_", " ").title(), fontsize=11)
        ax.set_xticks(range(1, 13))
        ax.set_xticklabels(["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"])
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    plt.suptitle("Seasonal Pattern: Predicted vs Actual GHI Correction Factor (LOSO CV)",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "seasonal_pattern_all_models.png", dpi=150)
    plt.close(fig)
    logger.info("Saved seasonal_pattern_all_models.png")


def plot_scatter_predicted_vs_actual(cv_df: pd.DataFrame) -> None:
    """Scatter of predicted vs actual correction factor — subplots per model."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))

    for ax, model_name in zip(axes.flat, GENERALIZABLE_MODELS):
        model_preds = cv_df[cv_df["model_name"] == model_name]
        actual = model_preds["actual_cf_ghi"].values
        predicted = model_preds["predicted_cf_ghi"].values

        ax.scatter(actual, predicted, alpha=0.3, s=10, color="#2196F3")

        # 1:1 line
        lims = [
            min(actual.min(), predicted.min()) - 0.02,
            max(actual.max(), predicted.max()) + 0.02,
        ]
        ax.plot(lims, lims, "--", color="red", linewidth=1.5, label="1:1 line")

        r2 = r2_score(actual, predicted)
        rmse = np.sqrt(mean_squared_error(actual, predicted))
        ax.text(
            0.05, 0.95, f"R² = {r2:.3f}\nRMSE = {rmse:.4f}",
            transform=ax.transAxes, fontsize=9, verticalalignment="top",
            bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.8),
        )

        ax.set_xlabel("Actual Correction Factor (GHI)")
        ax.set_ylabel("Predicted Correction Factor (GHI)")
        ax.set_title(model_name.replace("_", " ").title(), fontsize=11)
        ax.legend(fontsize=8, loc="lower right")
        ax.grid(True, alpha=0.3)
        ax.set_aspect("equal")

    plt.suptitle("Predicted vs Actual GHI Correction Factor (LOSO CV)",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "scatter_predicted_vs_actual_all_models.png", dpi=150)
    plt.close(fig)
    logger.info("Saved scatter_predicted_vs_actual_all_models.png")


def plot_feature_importance(df: pd.DataFrame) -> None:
    """Feature importance for Random Forest and Gradient Boosting, side by side."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    feature_names = ["latitude", "longitude", "elevation_m", "month", "nsrdb_ghi"]

    for ax, (model_name, ModelClass, params) in zip(axes, [
        ("Random Forest", RandomForestRegressor, dict(
            n_estimators=200, max_depth=10, min_samples_leaf=5, random_state=42, n_jobs=-1,
        )),
        ("Gradient Boosting", GradientBoostingRegressor, dict(
            n_estimators=200, max_depth=4, learning_rate=0.1, min_samples_leaf=5, random_state=42,
        )),
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
        ax.set_title(f"{model_name} (GHI)")

        for i, idx in enumerate(sorted_idx):
            ax.text(importances[idx] + 0.005, i, f"{importances[idx]:.3f}", va="center", fontsize=9)

        ax.grid(axis="x", alpha=0.3)

    plt.suptitle("Feature Importance for Tree-Based Models (trained on all data, GHI)",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "feature_importance.png", dpi=150)
    plt.close(fig)
    logger.info("Saved feature_importance.png")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Train, evaluate, and report on all bias correction models."""
    logger.info("=" * 60)
    logger.info("NSRDB Bias Correction Model Training & Evaluation")
    logger.info("=" * 60)

    # 1. Load data
    df = load_data()

    # 2. Run LOSO CV for generalizable models
    cv_df = run_loso_cv(df)

    # 3. Compute in-sample metrics for station-month climatology
    insample = compute_station_month_insample(df)

    # 4. Aggregate results
    comp_df = aggregate_model_comparison(cv_df, insample)
    station_df = per_station_results(cv_df)

    # 5. Format and print tables
    comparison_text = format_comparison_table(comp_df)
    station_text = format_per_station_table(station_df)

    full_report = comparison_text + "\n\n" + station_text
    print("\n" + full_report)

    # Save text report
    report_path = OUTPUT_DIR / "model_comparison.txt"
    report_path.write_text(full_report + "\n")
    logger.info("Saved %s", report_path)

    # 6. Save CV predictions
    cv_out_cols = [
        "station_id", "year", "month", "model_name",
        "predicted_cf_ghi", "actual_cf_ghi", "predicted_cf_dni", "actual_cf_dni",
        "corrected_ghi_error", "uncorrected_ghi_error",
        "corrected_dni_error", "uncorrected_dni_error",
    ]
    cv_path = OUTPUT_DIR / "cv_predictions.csv"
    cv_df[cv_out_cols].to_csv(cv_path, index=False)
    logger.info("Saved %s (%d rows)", cv_path, len(cv_df))

    # 7. Generate plots
    logger.info("Generating diagnostic plots...")
    plot_model_comparison_bar(comp_df)
    plot_correction_by_station(cv_df)
    plot_seasonal_pattern(cv_df, df)
    plot_scatter_predicted_vs_actual(cv_df)
    plot_feature_importance(df)

    # 8. Final summary
    print("\n" + "=" * 60)
    print("FILES GENERATED")
    print("=" * 60)
    for f in sorted(OUTPUT_DIR.glob("model_*")) + sorted(OUTPUT_DIR.glob("cv_*")) + \
             sorted(OUTPUT_DIR.glob("correction_by*")) + sorted(OUTPUT_DIR.glob("seasonal_*")) + \
             sorted(OUTPUT_DIR.glob("scatter_*")) + sorted(OUTPUT_DIR.glob("feature_*")):
        print(f"  {f.name}")
    print()


if __name__ == "__main__":
    main()
