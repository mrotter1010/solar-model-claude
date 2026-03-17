"""M11 Subhourly: Train v2 correction model from 1-min ground station data.

Trains a Gradient Boosting model on 760 paired configs from 19 CONUS
ground stations. Uses Leave-One-Station-Out CV (19 folds) for evaluation.
Target is clamped subhourly loss (>= 0, loss-only).

Saves v2 model artifacts alongside v1 (no overwrite).

Usage:
    python train_model_m11.py
"""

import json
from datetime import date
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import LeaveOneGroupOut

RESEARCH_DIR = Path(__file__).resolve().parent
TRAINING_CSV = RESEARCH_DIR / "training_data_m11.csv"

PROJECT_ROOT = RESEARCH_DIR.parent.parent
ARTIFACT_DIR = PROJECT_ROOT / "src" / "models" / "artifacts"
MODEL_PATH = ARTIFACT_DIR / "subhourly_correction_v2.joblib"
METADATA_PATH = ARTIFACT_DIR / "subhourly_correction_v2_metadata.json"

# Feature list — matches M8/v1 exactly (14 features)
FEATURE_COLS = [
    "dcac_ratio",
    "gcr",
    "racking",        # binary: tracker=1, fixed=0
    "latitude",
    "longitude",
    "cf_60min",
    "annual_ghi",
    "mean_kt",
    "std_kt",
    "ghi_cv",
    "mean_dni",
    "pct_clear_hours",
    "climate_cloudy",
    "climate_variable",
]

# Gradient Boosting hyperparameters — same as M8/v1
GB_PARAMS = {
    "n_estimators": 200,
    "max_depth": 5,
    "learning_rate": 0.1,
    "min_samples_leaf": 5,
    "random_state": 42,
}


def classify_climate(pct_clear: float) -> str:
    """Classify climate type from pct_clear_hours.

    Thresholds match src/models/subhourly_correction.py exactly.
    """
    if pct_clear < 30.0:
        return "cloudy"
    if pct_clear >= 45.0:
        return "clear"
    return "variable"


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build feature matrix matching M8/v1 feature set (14 features)."""
    X = pd.DataFrame(index=df.index)
    X["dcac_ratio"] = df["dcac_ratio"].values
    X["gcr"] = df["gcr"].values
    X["racking"] = df["racking_encoded"].values
    X["latitude"] = df["latitude"].values
    X["longitude"] = df["longitude"].values
    X["cf_60min"] = df["cf_60min"].values

    for col in ["annual_ghi", "mean_kt", "std_kt", "ghi_cv", "mean_dni", "pct_clear_hours"]:
        X[col] = df[col].values

    # Climate dummies from pct_clear_hours (same thresholds as v1)
    climate = df["pct_clear_hours"].apply(classify_climate)
    X["climate_cloudy"] = (climate == "cloudy").astype(int).values
    X["climate_variable"] = (climate == "variable").astype(int).values

    return X


def run_loso_cv(
    X: pd.DataFrame,
    y: np.ndarray,
    groups: np.ndarray,
) -> tuple[list[dict], np.ndarray]:
    """Run Leave-One-Station-Out CV with Gradient Boosting.

    Returns:
        Tuple of (per-fold metrics list, out-of-fold predictions array).
    """
    logo = LeaveOneGroupOut()
    n_folds = logo.get_n_splits(groups=groups)

    fold_metrics: list[dict] = []
    oof_preds = np.full(len(y), np.nan)

    for fold_i, (train_idx, test_idx) in enumerate(logo.split(X, y, groups)):
        site_name = groups[test_idx[0]]
        n_test = len(test_idx)

        X_tr, X_te = X.iloc[train_idx], X.iloc[test_idx]
        y_tr, y_te = y[train_idx], y[test_idx]

        model = GradientBoostingRegressor(**GB_PARAMS)
        model.fit(X_tr, y_tr)
        y_pred = model.predict(X_te)

        oof_preds[test_idx] = y_pred

        r2 = r2_score(y_te, y_pred) if np.std(y_te) > 0 else float("nan")
        rmse = np.sqrt(mean_squared_error(y_te, y_pred))
        mae = mean_absolute_error(y_te, y_pred)
        bias = float(np.mean(y_pred) - np.mean(y_te))

        fold_metrics.append({
            "site": site_name,
            "n": n_test,
            "R2": r2,
            "RMSE": rmse,
            "MAE": mae,
            "bias": bias,
            "actual_mean": float(np.mean(y_te)),
            "pred_mean": float(np.mean(y_pred)),
        })

        if (fold_i + 1) % 5 == 0 or fold_i == n_folds - 1:
            print(f"  Fold {fold_i + 1}/{n_folds}: {site_name:30s} R²={r2:.4f}  RMSE={rmse:.4f}%")

    return fold_metrics, oof_preds


def main() -> None:
    """Train v2 subhourly correction model and save artifacts."""
    # --- Load data ---
    df = pd.read_csv(TRAINING_CSV)
    print(f"Loaded {len(df)} rows from {TRAINING_CSV.name}")
    print(f"Stations: {df['site_name'].nunique()}")

    # --- Verify feature availability ---
    X = build_features(df)
    assert list(X.columns) == FEATURE_COLS, (
        f"Feature mismatch!\n  Expected: {FEATURE_COLS}\n  Got: {list(X.columns)}"
    )

    y = df["subhourly_loss_pct_clamped"].values
    groups = df["site_name"].values

    print(f"\nFeatures ({len(FEATURE_COLS)}): {FEATURE_COLS}")
    print(f"Target: subhourly_loss_pct_clamped")
    print(f"  range: [{y.min():.4f}%, {y.max():.4f}%]")
    print(f"  mean:  {y.mean():.4f}%")
    print(f"  zeros: {(y == 0).sum()} / {len(y)} ({(y == 0).sum()/len(y)*100:.1f}%)")

    n_sites = len(np.unique(groups))
    n_per_site = len(df) // n_sites
    print(f"\nLOSO-CV: {n_sites} folds")
    print(f"  Each fold: train ~{len(df) - n_per_site} rows, test ~{n_per_site} rows")

    # --- LOSO-CV ---
    print(f"\n{'='*70}")
    print("LEAVE-ONE-STATION-OUT CROSS-VALIDATION")
    print(f"{'='*70}")
    fold_metrics, oof_preds = run_loso_cv(X, y, groups)

    # --- Per-fold metrics table ---
    print(f"\n{'='*70}")
    print("PER-FOLD CV METRICS")
    print(f"{'='*70}")
    print(f"  {'Station':<30s} {'n':>3s} {'R²':>8s} {'RMSE':>8s} {'MAE':>8s} {'bias':>8s} {'actual':>8s} {'pred':>8s}")
    print("  " + "-" * 100)

    neg_r2_count = 0
    for m in sorted(fold_metrics, key=lambda x: x["R2"]):
        flag = " ***" if m["R2"] < 0 else ""
        if m["R2"] < 0:
            neg_r2_count += 1
        print(
            f"  {m['site']:<30s} {m['n']:>3d} {m['R2']:>8.4f} {m['RMSE']:>7.4f}% "
            f"{m['MAE']:>7.4f}% {m['bias']:>+7.4f}% {m['actual_mean']:>7.4f}% "
            f"{m['pred_mean']:>7.4f}%{flag}"
        )

    if neg_r2_count > 5:
        print(f"\nSTOP: {neg_r2_count} folds have negative R² (> 5). Model is fundamentally broken.")
        return

    # --- Overall CV metrics ---
    global_r2 = r2_score(y, oof_preds)
    global_rmse = np.sqrt(mean_squared_error(y, oof_preds))
    global_mae = mean_absolute_error(y, oof_preds)
    global_bias = float(np.mean(oof_preds) - np.mean(y))

    mean_fold_r2 = np.mean([m["R2"] for m in fold_metrics])
    mean_fold_rmse = np.mean([m["RMSE"] for m in fold_metrics])
    mean_fold_mae = np.mean([m["MAE"] for m in fold_metrics])

    if global_r2 < 0:
        print(f"\nSTOP: Global R² = {global_r2:.4f} < 0. Model is worse than predicting the mean.")
        return

    print(f"\n{'='*70}")
    print("OVERALL CV METRICS")
    print(f"{'='*70}")
    print(f"  Global R² (all OOF predictions):  {global_r2:.4f}")
    print(f"  Global RMSE:                      {global_rmse:.4f}%")
    print(f"  Global MAE:                       {global_mae:.4f}%")
    print(f"  Global bias:                      {global_bias:+.4f}%")
    print(f"  Mean fold R²:                     {mean_fold_r2:.4f}")
    print(f"  Mean fold RMSE:                   {mean_fold_rmse:.4f}%")
    print(f"  Mean fold MAE:                    {mean_fold_mae:.4f}%")
    print(f"  Folds with negative R²:           {neg_r2_count}")

    # --- Train final model on ALL data ---
    print(f"\n{'='*70}")
    print(f"TRAINING FINAL MODEL ON ALL {len(df)} SAMPLES")
    print(f"{'='*70}")

    final_model = GradientBoostingRegressor(**GB_PARAMS)
    final_model.fit(X, y)

    # --- Feature importance ---
    importances = final_model.feature_importances_
    imp_df = pd.DataFrame({
        "feature": FEATURE_COLS,
        "importance": importances,
    }).sort_values("importance", ascending=False)

    print(f"\n{'='*70}")
    print("FEATURE IMPORTANCES (final model)")
    print(f"{'='*70}")
    for _, row in imp_df.iterrows():
        bar = "#" * int(row["importance"] * 100)
        print(f"  {row['feature']:<20s} {row['importance']:.4f}  {bar}")

    # --- M8 vs M11 comparison ---
    print(f"\n{'='*70}")
    print("M8 (v1) vs M11 (v2) COMPARISON")
    print(f"{'='*70}")
    print(f"  {'Metric':<25s} {'M8/v1':>12s} {'M11/v2':>12s}")
    print("  " + "-" * 50)
    print(f"  {'Training data':<25s} {'5-min NSRDB':>12s} {'1-min ground':>12s}")
    print(f"  {'Stations':<25s} {'45':>12s} {f'{n_sites}':>12s}")
    print(f"  {'Samples':<25s} {'3,240':>12s} {f'{len(df):,}':>12s}")
    print(f"  {'Target':<25s} {'unclamped':>12s} {'clamped>=0':>12s}")
    print(f"  {'Global R²':<25s} {'0.8480':>12s} {f'{global_r2:.4f}':>12s}")
    print(f"  {'Global RMSE':<25s} {'0.1795%':>12s} {f'{global_rmse:.4f}%':>12s}")
    print(f"  {'Global MAE':<25s} {'0.1394%':>12s} {f'{global_mae:.4f}%':>12s}")
    print(f"  {'Mean fold R²':<25s} {'0.6446':>12s} {f'{mean_fold_r2:.4f}':>12s}")
    print(f"  {'Mean correction':<25s} {'~0.3%':>12s} {f'{y.mean():.4f}%':>12s}")

    # --- Save artifacts ---
    print(f"\n{'='*70}")
    print("SAVING MODEL ARTIFACTS")
    print(f"{'='*70}")

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    # Verify v1 files exist and won't be overwritten
    v1_model = ARTIFACT_DIR / "subhourly_correction_v1.joblib"
    v1_meta = ARTIFACT_DIR / "subhourly_correction_v1_metadata.json"
    assert v1_model.exists(), f"v1 model not found: {v1_model}"
    assert v1_meta.exists(), f"v1 metadata not found: {v1_meta}"
    print(f"  v1 artifacts verified (will NOT be overwritten)")

    # Save v2 model
    joblib.dump(final_model, MODEL_PATH)
    model_size_kb = MODEL_PATH.stat().st_size / 1024
    print(f"  Saved model:    {MODEL_PATH.name} ({model_size_kb:.1f} KB)")

    # Build metadata
    metadata = {
        "version": "2.0.0",
        "model_type": "GradientBoostingRegressor",
        "training_date": date.today().isoformat(),
        "training_source": "1-min ground station data (19 CONUS stations)",
        "reference_year": 2020,
        "n_samples": len(df),
        "n_stations": n_sites,
        "feature_list": FEATURE_COLS,
        "target_variable": "subhourly_loss_pct_clamped",
        "target_description": (
            "Clamped subhourly clipping loss percentage (>= 0, loss-only). "
            "Positive = hourly overestimates production due to missing "
            "subhourly irradiance variability."
        ),
        "clamping": "loss_only_gte_0",
        "hyperparameters": GB_PARAMS,
        "cv_metrics": {
            "r2_global": float(global_r2),
            "r2_mean_fold": float(mean_fold_r2),
            "rmse": float(global_rmse),
            "mae": float(global_mae),
            "bias": float(global_bias),
        },
        "correction_range": {
            "min": float(np.min(y)),
            "max": float(np.max(y)),
            "mean": float(np.mean(y)),
            "std": float(np.std(y)),
        },
        "feature_importances": {
            row["feature"]: round(float(row["importance"]), 4)
            for _, row in imp_df.iterrows()
        },
        "feature_sources": {
            "dcac_ratio": "from input CSV",
            "gcr": "from input CSV",
            "racking": "from input CSV (binary: 1=tracker, 0=fixed)",
            "latitude": "from input CSV or site metadata",
            "longitude": "from input CSV or site metadata",
            "cf_60min": "from hourly PySAM simulation capacity factor",
            "annual_ghi": "computed from hourly weather file",
            "mean_kt": "computed from hourly weather file",
            "std_kt": "computed from hourly weather file",
            "ghi_cv": "computed from hourly weather file",
            "mean_dni": "computed from hourly weather file",
            "pct_clear_hours": "computed from hourly weather file",
            "climate_cloudy": "one-hot from climate classification (pct_clear_hours < 30)",
            "climate_variable": "one-hot from climate classification (30 <= pct_clear_hours < 45)",
        },
        "comparison_to_v1": {
            "v1_mean_correction": 0.3,
            "v2_mean_correction": round(float(y.mean()), 4),
            "v1_training_data": "NSRDB 5-min satellite (45 sites, 3240 samples)",
            "v2_training_data": "Ground station 1-min (19 sites, 760 samples)",
            "improvement_note": (
                "v2 trained on real 1-min ground-truth irradiance captures higher "
                "clipping losses than v1's satellite-smoothed 5-min data. Mean "
                f"correction increased from ~0.3% to {y.mean():.2f}%."
            ),
        },
        "notes": (
            "Trained on 1-min ground station vs 60-min paired PySAM simulations. "
            "Target is clamped >= 0 (loss-only) at training time. Negative raw "
            "deltas (29% of pairs) occur at low DC/AC ratios and clear-sky sites "
            "where subhourly variability slightly increases net production."
        ),
    }

    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)
    meta_size_kb = METADATA_PATH.stat().st_size / 1024
    print(f"  Saved metadata: {METADATA_PATH.name} ({meta_size_kb:.1f} KB)")

    # --- Quick validation ---
    print(f"\n{'='*70}")
    print("VALIDATION: PREDICT ON REFERENCE CONFIGS")
    print(f"{'='*70}")

    loaded_model = joblib.load(MODEL_PATH)

    # Bondville IL, DC/AC=1.4, GCR=0.4, tracker
    ref_configs = [
        {"label": "Bondville IL, DC/AC=1.4, GCR=0.4, tracker",
         "site": "Bondville_IL", "dcac": 1.4, "gcr": 0.4, "racking_text": "tracker"},
        {"label": "Desert Rock NV, DC/AC=1.4, GCR=0.4, tracker",
         "site": "DesertRock_NV", "dcac": 1.4, "gcr": 0.4, "racking_text": "tracker"},
    ]

    for cfg in ref_configs:
        mask = (
            (df["site_name"] == cfg["site"])
            & (df["dcac_ratio"] == cfg["dcac"])
            & (df["gcr"] == cfg["gcr"])
            & (df["racking"] == cfg["racking_text"])
        )
        if mask.sum() == 0:
            print(f"  {cfg['label']}: NO MATCHING ROW")
            continue

        row = df[mask].iloc[[0]]
        X_row = build_features(row)
        pred = loaded_model.predict(X_row)[0]
        actual_clamped = row["subhourly_loss_pct_clamped"].values[0]
        actual_raw = row.get("subhourly_loss_pct", pd.Series([float("nan")])).values[0]

        print(f"  {cfg['label']}:")
        print(f"    predicted (v2):    {pred:+.4f}%")
        print(f"    actual (clamped):  {actual_clamped:+.4f}%")
        print(f"    actual (raw):      {actual_raw:+.4f}%")
        print(f"    error:             {pred - actual_clamped:+.4f}%")

    print(f"\nDone. Model v2 artifacts saved to {ARTIFACT_DIR}/")


if __name__ == "__main__":
    main()
