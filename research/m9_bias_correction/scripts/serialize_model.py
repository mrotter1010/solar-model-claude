"""Train and serialize final NSRDB bias correction models.

GHI: GradientBoostingRegressor — trained on all 1,350 station-months.
DNI: RandomForestRegressor — trained on outlier-filtered data.

Models are saved as joblib files with a metadata JSON sidecar.
"""

import json
import logging
from datetime import date
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "outputs"
DATASET_PATH = OUTPUT_DIR / "bias_merged_dataset_v2.csv"


def main() -> None:
    """Train, serialize, and sanity-check the bias correction models."""

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    if not DATASET_PATH.exists():
        raise RuntimeError(f"STOP: {DATASET_PATH} not found")

    df = pd.read_csv(DATASET_PATH)
    logger.info("Loaded %d rows, %d stations", len(df), df["station_id"].nunique())

    # ------------------------------------------------------------------
    # Step 1: DNI outlier filtering
    # ------------------------------------------------------------------
    logger.info("\n--- STEP 1: DNI Outlier Filtering ---")
    outlier_mask = (df["correction_factor_dni"] > 2.0) | (df["correction_factor_dni"] < 0.5)
    outlier_rows = df[outlier_mask]
    n_outliers = len(outlier_rows)

    logger.info("DNI outlier rows removed: %d of %d", n_outliers, len(df))
    print(f"\nDNI outlier rows removed: {n_outliers} of {len(df)}")

    if n_outliers > 0:
        station_counts = outlier_rows.groupby("station_id").size().sort_values(ascending=False)
        for sid, cnt in station_counts.items():
            logger.info("  %s: %d rows", sid, cnt)
            print(f"  {sid}: {cnt} rows")

    df_dni = df[~outlier_mask].copy()
    logger.info("DNI training rows after filtering: %d", len(df_dni))

    # ------------------------------------------------------------------
    # Step 2: Train final models
    # ------------------------------------------------------------------
    logger.info("\n--- STEP 2: Train Final Models ---")

    # GHI features and target
    ghi_features = np.column_stack([
        df["latitude"].values,
        df["longitude"].values,
        df["elevation_m"].values,
        df["month"].values,
        df["ghi_kwh_m2_day_nsrdb"].values,
    ])
    ghi_target = df["correction_factor_ghi"].values

    # DNI features and target (outlier-filtered)
    dni_features = np.column_stack([
        df_dni["latitude"].values,
        df_dni["longitude"].values,
        df_dni["elevation_m"].values,
        df_dni["month"].values,
        df_dni["dni_kwh_m2_day_nsrdb"].values,
    ])
    dni_target = df_dni["correction_factor_dni"].values

    # GHI: Gradient Boosting
    logger.info("Training GHI model (GradientBoosting) on %d rows...", len(ghi_target))
    ghi_model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.1,
        min_samples_leaf=5,
        random_state=42,
    )
    ghi_model.fit(ghi_features, ghi_target)
    logger.info("  GHI model trained. Train RMSE=%.4f",
                np.sqrt(((ghi_model.predict(ghi_features) - ghi_target) ** 2).mean()))

    # DNI: Random Forest
    logger.info("Training DNI model (RandomForest) on %d rows...", len(dni_target))
    dni_model = RandomForestRegressor(
        n_estimators=200,
        max_depth=10,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1,
    )
    dni_model.fit(dni_features, dni_target)
    logger.info("  DNI model trained. Train RMSE=%.4f",
                np.sqrt(((dni_model.predict(dni_features) - dni_target) ** 2).mean()))

    # ------------------------------------------------------------------
    # Step 3: Serialize
    # ------------------------------------------------------------------
    logger.info("\n--- STEP 3: Serialize ---")

    ghi_path = OUTPUT_DIR / "nsrdb_bias_correction_model_ghi.joblib"
    dni_path = OUTPUT_DIR / "nsrdb_bias_correction_model_dni.joblib"
    meta_path = OUTPUT_DIR / "nsrdb_bias_correction_metadata.json"

    joblib.dump(ghi_model, ghi_path)
    joblib.dump(dni_model, dni_path)
    logger.info("Saved GHI model: %s (%.1f KB)", ghi_path.name, ghi_path.stat().st_size / 1024)
    logger.info("Saved DNI model: %s (%.1f KB)", dni_path.name, dni_path.stat().st_size / 1024)

    training_stations = sorted(df["station_id"].unique().tolist())

    metadata = {
        "model_version": "1.0.0",
        "ghi_model_type": "GradientBoostingRegressor",
        "dni_model_type": "RandomForestRegressor",
        "features": ["latitude", "longitude", "elevation_m", "month", "nsrdb_ghi_or_dni"],
        "n_training_stations": 20,
        "n_training_rows_ghi": len(df),
        "n_training_rows_dni": len(df_dni),
        "training_stations": training_stations,
        "training_networks": ["SURFRAD", "SOLRAD", "MIDC"],
        "training_years": "2018-2023",
        "cv_rmse_ghi": 0.0960,
        "cv_rmse_dni": 0.0979,
        "cv_r2_ghi": 0.1724,
        "cv_r2_dni": 0.0214,
        "pct_improvement_ghi": 39.4,
        "pct_improvement_dni": 36.6,
        "date_trained": date.today().isoformat(),
        "min_correction_threshold": 0.03,
        "notes": (
            "NSRDB GOES Aggregated v4.0.0 bias correction. "
            "GHI uses Gradient Boosting, DNI uses Random Forest. "
            "Trained on SURFRAD + SOLRAD + MIDC ground stations. "
            "Conditional application recommended: skip correction when "
            "predicted factor is between 0.97 and 1.03."
        ),
    }

    meta_path.write_text(json.dumps(metadata, indent=2) + "\n")
    logger.info("Saved metadata: %s", meta_path.name)

    # ------------------------------------------------------------------
    # Step 4: Sanity check
    # ------------------------------------------------------------------
    logger.info("\n--- STEP 4: Sanity Check ---")

    # Reload from disk
    ghi_loaded = joblib.load(ghi_path)
    dni_loaded = joblib.load(dni_path)

    test_cases = [
        {
            "name": "Phoenix, AZ",
            "lat": 33.45, "lon": -112.07, "elev": 340,
            "month": 7, "nsrdb_ghi": 8.5, "nsrdb_dni": 10.0,
        },
        {
            "name": "Seattle, WA",
            "lat": 47.61, "lon": -122.33, "elev": 50,
            "month": 1, "nsrdb_ghi": 1.2, "nsrdb_dni": 1.5,
        },
        {
            "name": "Bondville, IL",
            "lat": 40.05, "lon": -88.37, "elev": 213,
            "month": 6, "nsrdb_ghi": 6.0, "nsrdb_dni": 6.5,
        },
    ]

    print("\nSanity Check Results:")
    print(f"  {'Location':<18} {'Month':>5} {'GHI CF':>8} {'DNI CF':>8} {'Flags'}")
    print(f"  {'-' * 55}")

    any_warning = False
    for tc in test_cases:
        ghi_features_tc = np.array([[tc["lat"], tc["lon"], tc["elev"], tc["month"], tc["nsrdb_ghi"]]])
        dni_features_tc = np.array([[tc["lat"], tc["lon"], tc["elev"], tc["month"], tc["nsrdb_dni"]]])

        ghi_cf = float(ghi_loaded.predict(ghi_features_tc)[0])
        dni_cf = float(dni_loaded.predict(dni_features_tc)[0])

        flags = []
        for label, cf in [("GHI", ghi_cf), ("DNI", dni_cf)]:
            if cf < 0.5 or cf > 1.5:
                flags.append(f"WARNING: {label} CF={cf:.4f} outside [0.5, 1.5]")
                any_warning = True

        flag_str = "; ".join(flags) if flags else "OK"
        print(f"  {tc['name']:<18} {tc['month']:>5} {ghi_cf:>8.4f} {dni_cf:>8.4f} {flag_str}")
        logger.info(
            "  %s (month=%d): GHI CF=%.4f, DNI CF=%.4f %s",
            tc["name"], tc["month"], ghi_cf, dni_cf,
            "WARNING" if flags else "OK",
        )

    if any_warning:
        print("\n  WARNING: Some correction factors are outside [0.5, 1.5].")
    else:
        print("\n  All correction factors in expected range.")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'=' * 60}")
    print("SERIALIZATION COMPLETE")
    print(f"{'=' * 60}")
    print(f"  GHI model: {ghi_path.name} ({ghi_path.stat().st_size / 1024:.1f} KB)")
    print(f"  DNI model: {dni_path.name} ({dni_path.stat().st_size / 1024:.1f} KB)")
    print(f"  Metadata:  {meta_path.name}")
    print(f"  GHI training rows: {len(df)}")
    print(f"  DNI training rows: {len(df_dni)} ({n_outliers} outliers removed)")
    print(f"  Training stations: {len(training_stations)}")
    print()


if __name__ == "__main__":
    main()
