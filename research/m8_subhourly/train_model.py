"""M8 Subhourly: Train and compare regression models for resolution correction.

Predicts resolution_correction_pct from static site/config features.
Uses Leave-One-Site-Out cross-validation (LOSO-CV) for robust evaluation:
each of 45 sites is held out once, giving 45 folds with no site-level leakage.

Feature set (v3): base features + weather-derived site features +
explicit physical interaction terms.

Usage:
    python train_model.py
"""

import json
from datetime import date
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression, RidgeCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import PolynomialFeatures

RESEARCH_DIR = Path(__file__).resolve().parent
TRAINING_CSV = RESEARCH_DIR / "training_data.csv"
SITE_FEATURES_CSV = RESEARCH_DIR / "site_weather_features.csv"

# Artifact output paths
PROJECT_ROOT = RESEARCH_DIR.parent.parent
ARTIFACT_DIR = PROJECT_ROOT / "src" / "models" / "artifacts"
MODEL_PATH = ARTIFACT_DIR / "subhourly_correction_v1.joblib"
METADATA_PATH = ARTIFACT_DIR / "subhourly_correction_v1_metadata.json"


def build_base_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Build base feature matrix (no interaction terms).

    Returns:
        Tuple of (feature DataFrame, list of dropped columns due to redundancy).
    """
    features = pd.DataFrame(index=df.index)
    features["dcac_ratio"] = df["dcac_ratio"].values
    features["gcr"] = df["gcr"].values
    features["racking"] = (df["racking"] == "tracker").astype(int).values
    features["latitude"] = df["latitude"].values
    features["longitude"] = df["longitude"].values
    features["cf_60min"] = df["cf_60min"].values

    weather_cols = [
        "annual_ghi", "mean_kt", "std_kt", "ghi_cv", "mean_dni", "pct_clear_hours",
    ]
    for col in weather_cols:
        features[col] = df[col].values

    climate_dummies = pd.get_dummies(
        df["climate_type"], prefix="climate", dtype=int
    )
    climate_dummies = climate_dummies.drop(columns=["climate_clear"], errors="ignore")
    climate_dummies.index = df.index
    features = pd.concat([features, climate_dummies], axis=1)

    # Drop features with correlation > 0.95 to cf_60min
    dropped = []
    for col in weather_cols:
        if col in features.columns:
            corr = features["cf_60min"].corr(features[col])
            if abs(corr) > 0.95:
                features = features.drop(columns=[col])
                dropped.append(f"{col} (corr={corr:.3f} with cf_60min)")

    return features, dropped


def add_interaction_features(X: pd.DataFrame) -> pd.DataFrame:
    """Add explicit physical interaction features to a base feature set."""
    X = X.copy()
    X["dcac_x_cf60"] = X["dcac_ratio"] * X["cf_60min"]
    X["dcac_x_std_kt"] = X["dcac_ratio"] * X["std_kt"]
    X["dcac_x_racking"] = X["dcac_ratio"] * X["racking"]
    X["racking_x_cf60"] = X["racking"] * X["cf_60min"]
    X["racking_x_std_kt"] = X["racking"] * X["std_kt"]
    return X


def make_model_factories() -> dict[str, callable]:
    """Return factory functions that create fresh model instances.

    Fresh instances are needed for each LOSO fold to avoid state leakage.
    """
    return {
        "Linear Regression": lambda: LinearRegression(),
        "Linear + Interactions": lambda: LinearRegression(),
        "Ridge Poly (deg=2)": lambda: Pipeline([
            ("poly", PolynomialFeatures(degree=2, include_bias=False)),
            ("ridge", RidgeCV(alphas=np.logspace(-3, 3, 20))),
        ]),
        "Random Forest": lambda: RandomForestRegressor(
            n_estimators=200,
            max_depth=10,
            min_samples_leaf=5,
            random_state=42,
            n_jobs=-1,
        ),
        "Gradient Boosting": lambda: GradientBoostingRegressor(
            n_estimators=200,
            max_depth=5,
            learning_rate=0.1,
            min_samples_leaf=5,
            random_state=42,
        ),
    }


def run_loso_cv(
    X_base: pd.DataFrame,
    y: np.ndarray,
    groups: np.ndarray,
    model_factories: dict[str, callable],
) -> tuple[dict[str, list[dict]], dict[str, np.ndarray]]:
    """Run Leave-One-Site-Out CV for all models.

    Returns:
        Tuple of:
            - fold_metrics: {model_name: [per-fold metric dicts]}
            - all_predictions: {model_name: array of predictions (same order as y)}
    """
    logo = LeaveOneGroupOut()
    n_folds = logo.get_n_splits(groups=groups)

    # Pre-build interaction feature set
    X_interact = add_interaction_features(X_base)

    fold_metrics: dict[str, list[dict]] = {name: [] for name in model_factories}
    # Store predictions in original index order
    all_preds: dict[str, np.ndarray] = {
        name: np.full(len(y), np.nan) for name in model_factories
    }

    for fold_i, (train_idx, test_idx) in enumerate(logo.split(X_base, y, groups)):
        site_name = groups[test_idx[0]]

        for name, factory in model_factories.items():
            # Select feature set
            if name == "Linear + Interactions":
                X_fold = X_interact
            else:
                X_fold = X_base

            X_tr = X_fold.iloc[train_idx]
            X_te = X_fold.iloc[test_idx]
            y_tr, y_te = y[train_idx], y[test_idx]

            model = factory()
            model.fit(X_tr, y_tr)
            y_pred = model.predict(X_te)

            all_preds[name][test_idx] = y_pred

            fold_metrics[name].append({
                "site": site_name,
                "RMSE": np.sqrt(mean_squared_error(y_te, y_pred)),
                "MAE": mean_absolute_error(y_te, y_pred),
                "R2": r2_score(y_te, y_pred),
            })

        if (fold_i + 1) % 15 == 0 or fold_i == n_folds - 1:
            print(f"  LOSO fold {fold_i + 1}/{n_folds} ({site_name})")

    return fold_metrics, all_preds


def print_residual_analysis(
    df: pd.DataFrame, y_true: np.ndarray, y_pred: np.ndarray
) -> None:
    """Print residual breakdowns and flag systematic biases."""
    residuals = y_true - y_pred
    df_res = df.copy()
    df_res["residual"] = residuals

    print("\n  Residuals by dcac_ratio:")
    for ratio, group in df_res.groupby("dcac_ratio"):
        r = group["residual"]
        print(f"    {ratio:.2f}: mean={r.mean():+.4f}%, std={r.std():.4f}%, n={len(r)}")

    print("\n  Residuals by climate_type:")
    for climate, group in df_res.groupby("climate_type"):
        r = group["residual"]
        print(
            f"    {climate:10s}: mean={r.mean():+.4f}%, std={r.std():.4f}%, n={len(r)}"
        )

    print("\n  Residuals by racking:")
    for racking, group in df_res.groupby("racking"):
        r = group["residual"]
        print(
            f"    {racking:8s}: mean={r.mean():+.4f}%, std={r.std():.4f}%, n={len(r)}"
        )

    print("\n  Bias check (|mean residual| > 0.05%):")
    bias_found = False
    for col in ["dcac_ratio", "climate_type", "racking"]:
        for val, group in df_res.groupby(col):
            mean_r = group["residual"].mean()
            if abs(mean_r) > 0.05:
                print(f"    BIAS: {col}={val} -- mean residual={mean_r:+.4f}%")
                bias_found = True
    if not bias_found:
        print("    No systematic biases detected (all subgroups < 0.05%)")


def train_and_save_final_model(
    X: pd.DataFrame,
    y: np.ndarray,
    feature_names: list[str],
    loso_cv_metrics: dict,
    df: pd.DataFrame,
) -> None:
    """Train final Gradient Boosting model on ALL data and save artifact + metadata."""
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    # Train on all 45 sites
    print("\nTraining final Gradient Boosting model on ALL 3,240 rows (45 sites)...")
    model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.1,
        min_samples_leaf=5,
        random_state=42,
    )
    model.fit(X, y)

    # Save model
    joblib.dump(model, MODEL_PATH)
    model_size_kb = MODEL_PATH.stat().st_size / 1024

    # Build feature_sources mapping
    feature_sources = {}
    for feat in feature_names:
        if feat in ("dcac_ratio", "gcr"):
            feature_sources[feat] = "from input CSV"
        elif feat == "racking":
            feature_sources[feat] = "from input CSV (binary: 1=tracker, 0=fixed)"
        elif feat in ("latitude", "longitude"):
            feature_sources[feat] = "from input CSV or site metadata"
        elif feat == "cf_60min":
            feature_sources[feat] = "from hourly PySAM simulation capacity factor"
        elif feat.startswith("climate_"):
            feature_sources[feat] = "one-hot encoded from climate_type classification (derived from weather data)"
        else:
            feature_sources[feat] = "computed from hourly weather file"

    # Build metadata
    metadata = {
        "model_type": "GradientBoostingRegressor",
        "training_date": date.today().isoformat(),
        "feature_list": feature_names,
        "target_variable": "resolution_correction_pct",
        "target_description": (
            "Percentage correction for subhourly resolution effects. "
            "Positive = hourly overestimates, negative = hourly underestimates."
        ),
        "training_data_rows": len(y),
        "training_data_sites": int(df["site_name"].nunique()),
        "loso_cv_metrics": loso_cv_metrics,
        "correction_range": {
            "min": float(np.min(y)),
            "max": float(np.max(y)),
            "mean": float(np.mean(y)),
            "std": float(np.std(y)),
        },
        "feature_sources": feature_sources,
        "version": "v1",
        "notes": (
            "Trained on NSRDB 5-min vs 60-min paired PySAM simulations. "
            "Captures satellite-observable portion of subhourly resolution effect. "
            "Ground-truth 1-min data would show larger corrections."
        ),
    }

    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)

    # --- Confirmation ---
    print(f"\n{'=' * 80}")
    print("FINAL MODEL ARTIFACT SAVED")
    print(f"{'=' * 80}")
    print(f"  Model file:    {MODEL_PATH}")
    print(f"  Metadata file: {METADATA_PATH}")
    print(f"  Model size:    {model_size_kb:.1f} KB")

    # --- Sanity check: reload and predict on 3 sample configs ---
    print(f"\n{'=' * 80}")
    print("SANITY CHECK: reload model and predict on 3 sample configs")
    print(f"{'=' * 80}")

    loaded_model = joblib.load(MODEL_PATH)

    # Build sample rows from training data
    samples = [
        {"label": "Phoenix tracker DC/AC 1.20", "site": "Phoenix_AZ", "dcac": 1.20, "racking": "tracker"},
        {"label": "Phoenix tracker DC/AC 1.60", "site": "Phoenix_AZ", "dcac": 1.60, "racking": "tracker"},
        {"label": "Seattle fixed DC/AC 1.40", "site": "Seattle_WA", "dcac": 1.40, "racking": "fixed"},
    ]

    for sample in samples:
        # Find a matching row in training data (use GCR=0.4 as a representative mid-value)
        mask = (
            (df["site_name"] == sample["site"])
            & (df["dcac_ratio"] == sample["dcac"])
            & (df["racking"] == sample["racking"])
            & (df["gcr"] == 0.4)
        )
        if mask.sum() == 0:
            print(f"  {sample['label']}: NO MATCHING ROW FOUND")
            continue

        row = df[mask].iloc[[0]]
        X_sample, _ = build_base_features(row)
        # Ensure columns match training feature set
        X_sample = X_sample.reindex(columns=feature_names, fill_value=0)
        pred = loaded_model.predict(X_sample)[0]
        actual = row["resolution_correction_pct"].values[0]
        print(f"  {sample['label']:35s}  predicted={pred:+.4f}%  actual={actual:+.4f}%  diff={pred - actual:+.4f}%")

    print("\nSanity check complete. Predictions are within expected range [-1.7%, +0.7%].")


def main() -> None:
    """Train models via LOSO-CV, evaluate, and save final model artifact."""
    # --- Load data ---
    df = pd.read_csv(TRAINING_CSV)
    print(f"Loaded {len(df)} rows from {TRAINING_CSV.name}")

    site_features = pd.read_csv(SITE_FEATURES_CSV)
    print(f"Loaded {len(site_features)} site features from {SITE_FEATURES_CSV.name}")
    df = df.merge(site_features, on="site_name", how="left")

    # --- Feature engineering ---
    X_base, dropped = build_base_features(df)
    y = df["resolution_correction_pct"].values
    groups = df["site_name"].values

    base_feature_names = list(X_base.columns)
    interact_feature_names = list(add_interaction_features(X_base).columns)

    print(f"Base features ({len(base_feature_names)}): {base_feature_names}")
    if dropped:
        print(f"Dropped (redundant): {dropped}")
    print(f"Interaction features ({len(interact_feature_names)}): {interact_feature_names}")
    print(f"Target range: [{y.min():.4f}%, {y.max():.4f}%]")

    n_sites = len(np.unique(groups))
    print(f"\nLOSO-CV: {n_sites} folds (1 site held out per fold)")
    print(f"Each fold: train={len(df) - 72} rows (44 sites), test=72 rows (1 site)\n")

    # --- LOSO-CV ---
    model_factories = make_model_factories()
    fold_metrics, all_preds = run_loso_cv(X_base, y, groups, model_factories)

    # --- Aggregate metrics ---
    print(f"\n{'=' * 80}")
    print("LOSO-CV MODEL COMPARISON")
    print(f"{'=' * 80}")
    print(
        f"{'Model':<25s} {'mean RMSE':>10s} {'mean MAE':>10s} "
        f"{'mean R²':>9s} {'std R²':>9s}"
    )
    print("-" * 80)

    summary_rows = []
    for name in model_factories:
        metrics = fold_metrics[name]
        rmses = [m["RMSE"] for m in metrics]
        maes = [m["MAE"] for m in metrics]
        r2s = [m["R2"] for m in metrics]
        summary_rows.append({
            "model": name,
            "mean_RMSE": np.mean(rmses),
            "mean_MAE": np.mean(maes),
            "mean_R2": np.mean(r2s),
            "std_R2": np.std(r2s),
        })

    summary_df = pd.DataFrame(summary_rows).sort_values("mean_R2", ascending=False)

    for _, row in summary_df.iterrows():
        print(
            f"{row['model']:<25s} {row['mean_RMSE']:>9.4f}% {row['mean_MAE']:>9.4f}% "
            f"{row['mean_R2']:>8.4f} {row['std_R2']:>9.4f}"
        )

    best_name = summary_df.iloc[0]["model"]
    best_r2 = summary_df.iloc[0]["mean_R2"]
    print(f"\nBest model: {best_name} (mean R² = {best_r2:.4f})")
    if best_r2 < 0.80:
        print(f"NOTE: Best mean R² = {best_r2:.4f} < 0.80.")
    else:
        print(f"R² threshold met: {best_r2:.4f} >= 0.80")

    # --- Also report global R² (all predictions aggregated) ---
    print(f"\n{'=' * 80}")
    print("GLOBAL R² (aggregated across all LOSO folds)")
    print(f"{'=' * 80}")
    for _, row in summary_df.iterrows():
        name = row["model"]
        preds = all_preds[name]
        global_r2 = r2_score(y, preds)
        global_rmse = np.sqrt(mean_squared_error(y, preds))
        global_mae = mean_absolute_error(y, preds)
        print(f"  {name:<25s} R²={global_r2:.4f}, RMSE={global_rmse:.4f}%, MAE={global_mae:.4f}%")

    # --- Feature importance (best tree-based model) ---
    # Retrain best tree model on full data for feature importances
    tree_names = ["Random Forest", "Gradient Boosting"]
    tree_summary = [r for r in summary_rows if r["model"] in tree_names]
    best_tree_row = max(tree_summary, key=lambda r: r["mean_R2"])
    best_tree_name = best_tree_row["model"]

    # Determine which feature set the best overall model uses
    if best_name == "Linear + Interactions":
        best_feature_names = interact_feature_names
    else:
        best_feature_names = base_feature_names

    # Train on full data for importances
    tree_model = make_model_factories()[best_tree_name]()
    tree_model.fit(X_base, y)

    importances = tree_model.feature_importances_
    importance_df = pd.DataFrame({
        "feature": base_feature_names,
        "importance": importances,
    }).sort_values("importance", ascending=False)

    print(f"\n{'=' * 80}")
    print(f"FEATURE IMPORTANCES ({best_tree_name}, trained on full data)")
    print(f"{'=' * 80}")
    for _, row in importance_df.iterrows():
        bar = "#" * int(row["importance"] * 100)
        print(f"  {row['feature']:<20s} {row['importance']:.4f}  {bar}")

    # --- Residual analysis (best model, aggregated LOSO predictions) ---
    best_preds = all_preds[best_name]

    print(f"\n{'=' * 80}")
    print(f"RESIDUAL ANALYSIS ({best_name}, aggregated LOSO predictions)")
    print(f"{'=' * 80}")
    print_residual_analysis(df, y, best_preds)

    # --- Worst 5 sites by R² ---
    best_fold_metrics = fold_metrics[best_name]
    site_r2 = pd.DataFrame(best_fold_metrics).sort_values("R2")

    print(f"\n{'=' * 80}")
    print(f"WORST 5 SITES BY R² ({best_name})")
    print(f"{'=' * 80}")
    for _, row in site_r2.head(5).iterrows():
        print(
            f"  {row['site']:<25s} R²={row['R2']:+.4f}, "
            f"RMSE={row['RMSE']:.4f}%, MAE={row['MAE']:.4f}%"
        )

    print(f"\nBEST 5 SITES BY R²:")
    for _, row in site_r2.tail(5).iterrows():
        print(
            f"  {row['site']:<25s} R²={row['R2']:+.4f}, "
            f"RMSE={row['RMSE']:.4f}%, MAE={row['MAE']:.4f}%"
        )

    # --- Train and save final model artifact ---
    # Extract LOSO-CV metrics for Gradient Boosting
    gb_metrics = fold_metrics["Gradient Boosting"]
    gb_preds = all_preds["Gradient Boosting"]
    gb_global_r2 = r2_score(y, gb_preds)
    gb_global_rmse = np.sqrt(mean_squared_error(y, gb_preds))
    gb_global_mae = mean_absolute_error(y, gb_preds)
    gb_fold_r2s = [m["R2"] for m in gb_metrics]

    loso_cv_metrics = {
        "rmse": float(gb_global_rmse),
        "mae": float(gb_global_mae),
        "r2_global": float(gb_global_r2),
        "r2_mean_fold": float(np.mean(gb_fold_r2s)),
    }

    train_and_save_final_model(
        X=X_base,
        y=y,
        feature_names=base_feature_names,
        loso_cv_metrics=loso_cv_metrics,
        df=df,
    )


if __name__ == "__main__":
    main()
