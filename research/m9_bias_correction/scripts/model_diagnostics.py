"""Detailed model diagnostics from cv_predictions_v2.csv.

Computes R², per-station breakdowns, DNI outlier analysis, stability
analysis, and a final decision matrix — all from existing cross-validation
predictions (no retraining).
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import r2_score

# Add project root for station metadata
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

from stations import STATIONS  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = _project_root / "outputs"
CV_PREDICTIONS = OUTPUT_DIR / "cv_predictions_v2.csv"

GENERALIZABLE_MODELS = [
    "monthly_climatology",
    "linear_regression",
    "random_forest",
    "gradient_boosting",
]

STATION_LOOKUP = {s["station_id"]: s for s in STATIONS}


def load_predictions() -> pd.DataFrame:
    """Load and validate cv_predictions_v2.csv."""
    if not CV_PREDICTIONS.exists():
        raise RuntimeError(f"STOP: {CV_PREDICTIONS} not found")

    df = pd.read_csv(CV_PREDICTIONS)

    required = {
        "station_id", "year", "month", "model_name",
        "predicted_cf_ghi", "actual_cf_ghi",
        "predicted_cf_dni", "actual_cf_dni",
        "corrected_ghi_error", "uncorrected_ghi_error",
        "corrected_dni_error", "uncorrected_dni_error",
    }
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"STOP: Missing columns: {missing}")

    logger.info("Loaded %d rows, %d models, %d stations",
                len(df), df["model_name"].nunique(), df["station_id"].nunique())
    return df


# -----------------------------------------------------------------------
# 1. R² for each model
# -----------------------------------------------------------------------
def compute_r2(df: pd.DataFrame) -> list[str]:
    """Compute R² (predicted CF vs actual CF) for each model and variable."""
    lines = [
        "",
        "1. R² FOR EACH MODEL (predicted vs actual correction factor)",
        "=" * 70,
        "",
        f"  {'Model':<30} {'GHI R²':>10} {'DNI R²':>10}",
        f"  {'-' * 55}",
    ]

    r2_results: dict[str, dict[str, float]] = {}

    for model in GENERALIZABLE_MODELS:
        m = df[df["model_name"] == model]
        r2_ghi = r2_score(m["actual_cf_ghi"], m["predicted_cf_ghi"])
        r2_dni = r2_score(m["actual_cf_dni"], m["predicted_cf_dni"])
        lines.append(f"  {model:<30} {r2_ghi:>10.4f} {r2_dni:>10.4f}")
        r2_results[model] = {"ghi": r2_ghi, "dni": r2_dni}

    lines.append("")
    return lines, r2_results


# -----------------------------------------------------------------------
# 2. Per-station breakdown
# -----------------------------------------------------------------------
def per_station_breakdown(df: pd.DataFrame) -> list[str]:
    """Per-station error and improvement for each model."""
    lines = [
        "2. PER-STATION BREAKDOWN (GHI) — sorted by uncorrected error (worst first)",
        "=" * 70,
        "",
    ]

    station_results: dict[str, dict] = {}

    # For each model, compute per-station metrics
    for model in GENERALIZABLE_MODELS:
        m = df[df["model_name"] == model]
        per_st = m.groupby("station_id").agg(
            mean_uncorr=("uncorrected_ghi_error", "mean"),
            mean_corr=("corrected_ghi_error", "mean"),
        )
        per_st["pct_improv"] = (
            (per_st["mean_uncorr"] - per_st["mean_corr"])
            / per_st["mean_uncorr"] * 100
        )

        for sid, row in per_st.iterrows():
            if sid not in station_results:
                station_results[sid] = {"uncorr": row["mean_uncorr"]}
            station_results[sid][model] = {
                "corr": row["mean_corr"],
                "pct": row["pct_improv"],
            }

    # Sort by uncorrected error descending
    sorted_stations = sorted(
        station_results.keys(),
        key=lambda s: station_results[s]["uncorr"],
        reverse=True,
    )

    # Header
    hdr = f"  {'Station':<10} {'Name':<22} {'Uncorr':>8}"
    for model in GENERALIZABLE_MODELS:
        short = model.replace("monthly_climatology", "MthClim") \
                     .replace("linear_regression", "LinReg") \
                     .replace("random_forest", "RF") \
                     .replace("gradient_boosting", "GB")
        hdr += f"  {short:>12}"
    lines.append(hdr)

    hdr2 = f"  {'':10} {'':22} {'kWh/m²/d':>8}"
    for _ in GENERALIZABLE_MODELS:
        hdr2 += f"  {'err / %imp':>12}"
    lines.append(hdr2)
    lines.append(f"  {'-' * (10 + 22 + 8 + 14 * len(GENERALIZABLE_MODELS))}")

    helped_count = {m: 0 for m in GENERALIZABLE_MODELS}
    hurt_count = {m: 0 for m in GENERALIZABLE_MODELS}

    for sid in sorted_stations:
        info = STATION_LOOKUP.get(sid, {})
        name = info.get("name", sid)[:20]
        uncorr = station_results[sid]["uncorr"]
        row_str = f"  {sid:<10} {name:<22} {uncorr:>8.3f}"

        for model in GENERALIZABLE_MODELS:
            d = station_results[sid][model]
            pct = d["pct"]
            row_str += f"  {d['corr']:>5.3f}/{pct:>+5.1f}%"
            if pct > 0:
                helped_count[model] += 1
            else:
                hurt_count[model] += 1

        lines.append(row_str)

    lines.append("")
    lines.append("  Stations helped / hurt:")
    for model in GENERALIZABLE_MODELS:
        short = model.replace("monthly_climatology", "MthClim") \
                     .replace("linear_regression", "LinReg") \
                     .replace("random_forest", "RF") \
                     .replace("gradient_boosting", "GB")
        lines.append(
            f"    {short:<12}  helped={helped_count[model]:>2}, "
            f"hurt={hurt_count[model]:>2}"
        )
    lines.append("")

    return lines, helped_count, hurt_count


# -----------------------------------------------------------------------
# 3. DNI outlier analysis
# -----------------------------------------------------------------------
def dni_outlier_analysis(df: pd.DataFrame) -> list[str]:
    """Identify DNI correction factor outliers and recompute metrics without them."""
    lines = [
        "3. DNI OUTLIER ANALYSIS (actual CF > 2.0 or < 0.5)",
        "=" * 70,
        "",
    ]

    # Use any model's rows — actual_cf_dni is the same across models
    ref = df[df["model_name"] == GENERALIZABLE_MODELS[0]].copy()
    outlier_mask = (ref["actual_cf_dni"] > 2.0) | (ref["actual_cf_dni"] < 0.5)
    outliers = ref[outlier_mask].sort_values("actual_cf_dni", ascending=False)

    lines.append(f"  Outlier station-months: {len(outliers)} of {len(ref)}")
    lines.append("")

    if len(outliers) > 0:
        lines.append(
            f"  {'Station':<10} {'Year':>5} {'Month':>6} "
            f"{'Actual CF':>10} {'Uncorr Err':>11}"
        )
        lines.append(f"  {'-' * 50}")

        station_counts: dict[str, int] = {}
        for _, row in outliers.iterrows():
            sid = row["station_id"]
            station_counts[sid] = station_counts.get(sid, 0) + 1
            if len(outliers) <= 30:
                lines.append(
                    f"  {sid:<10} {int(row['year']):>5} {int(row['month']):>6} "
                    f"{row['actual_cf_dni']:>10.4f} {row['uncorrected_dni_error']:>11.3f}"
                )

        if len(outliers) > 30:
            lines.append(f"  (showing station counts only — {len(outliers)} total outlier rows)")

        lines.append("")
        lines.append("  Outliers by station:")
        for sid, cnt in sorted(station_counts.items(), key=lambda x: -x[1]):
            name = STATION_LOOKUP.get(sid, {}).get("name", sid)
            lines.append(f"    {sid:<10} {name:<22} {cnt:>3} outlier months")

    # Compute DNI metrics with and without outliers
    lines.append("")
    lines.append("  DNI Model Metrics — with vs without outliers:")
    lines.append(
        f"  {'Model':<30} {'RMSE all':>10} {'RMSE excl':>10} "
        f"{'R² all':>8} {'R² excl':>8} {'%Imp all':>9} {'%Imp excl':>9}"
    )
    lines.append(f"  {'-' * 90}")

    # Identify outlier (station_id, year, month) tuples
    outlier_keys = set(
        zip(outliers["station_id"], outliers["year"], outliers["month"])
    )

    r2_excl_results: dict[str, float] = {}
    rmse_excl_results: dict[str, float] = {}

    for model in GENERALIZABLE_MODELS:
        m = df[df["model_name"] == model]

        # All rows
        rmse_all = np.sqrt(((m["actual_cf_dni"] - m["predicted_cf_dni"]) ** 2).mean())
        r2_all = r2_score(m["actual_cf_dni"], m["predicted_cf_dni"])
        uncorr_all = m["uncorrected_dni_error"].abs().mean()
        corr_all = m["corrected_dni_error"].abs().mean()
        pct_all = (uncorr_all - corr_all) / uncorr_all * 100 if uncorr_all > 0 else 0

        # Excluding outliers
        excl_mask = ~m.apply(
            lambda r: (r["station_id"], r["year"], r["month"]) in outlier_keys,
            axis=1,
        )
        m_excl = m[excl_mask]
        rmse_excl = np.sqrt(((m_excl["actual_cf_dni"] - m_excl["predicted_cf_dni"]) ** 2).mean())
        r2_excl = r2_score(m_excl["actual_cf_dni"], m_excl["predicted_cf_dni"])
        uncorr_excl = m_excl["uncorrected_dni_error"].abs().mean()
        corr_excl = m_excl["corrected_dni_error"].abs().mean()
        pct_excl = (uncorr_excl - corr_excl) / uncorr_excl * 100 if uncorr_excl > 0 else 0

        lines.append(
            f"  {model:<30} {rmse_all:>10.4f} {rmse_excl:>10.4f} "
            f"{r2_all:>8.4f} {r2_excl:>8.4f} {pct_all:>+8.1f}% {pct_excl:>+8.1f}%"
        )

        r2_excl_results[model] = r2_excl
        rmse_excl_results[model] = rmse_excl

    lines.append("")
    return lines, r2_excl_results, rmse_excl_results


# -----------------------------------------------------------------------
# 4. Stability analysis
# -----------------------------------------------------------------------
def stability_analysis(df: pd.DataFrame) -> list[str]:
    """Compare per-station improvement variability across models."""
    lines = [
        "4. STABILITY ANALYSIS (std dev of per-station % improvement)",
        "=" * 70,
        "",
    ]

    lines.append(
        f"  {'Model':<30} {'Var':<5} {'Mean %Imp':>10} "
        f"{'Std %Imp':>10} {'Min %Imp':>10} {'Max %Imp':>10}"
    )
    lines.append(f"  {'-' * 80}")

    for var_label, corr_col, uncorr_col in [
        ("GHI", "corrected_ghi_error", "uncorrected_ghi_error"),
        ("DNI", "corrected_dni_error", "uncorrected_dni_error"),
    ]:
        for model in GENERALIZABLE_MODELS:
            m = df[df["model_name"] == model]
            per_st = m.groupby("station_id").agg(
                mean_uncorr=(uncorr_col, "mean"),
                mean_corr=(corr_col, "mean"),
            )
            per_st["pct_improv"] = (
                (per_st["mean_uncorr"] - per_st["mean_corr"])
                / per_st["mean_uncorr"] * 100
            )
            mean_imp = per_st["pct_improv"].mean()
            std_imp = per_st["pct_improv"].std()
            min_imp = per_st["pct_improv"].min()
            max_imp = per_st["pct_improv"].max()

            lines.append(
                f"  {model:<30} {var_label:<5} {mean_imp:>+9.1f}% "
                f"{std_imp:>9.1f}% {min_imp:>+9.1f}% {max_imp:>+9.1f}%"
            )
        lines.append("")

    # Direct GB vs monthly climatology comparison
    lines.append("  GB vs Monthly Climatology stability (GHI):")
    for model in ["monthly_climatology", "gradient_boosting"]:
        m = df[df["model_name"] == model]
        per_st = m.groupby("station_id").agg(
            mean_uncorr=("uncorrected_ghi_error", "mean"),
            mean_corr=("corrected_ghi_error", "mean"),
        )
        per_st["pct_improv"] = (
            (per_st["mean_uncorr"] - per_st["mean_corr"])
            / per_st["mean_uncorr"] * 100
        )
        short = "MthClim" if "monthly" in model else "GB"
        lines.append(
            f"    {short:<10} mean={per_st['pct_improv'].mean():>+6.1f}%, "
            f"std={per_st['pct_improv'].std():>5.1f}%, "
            f"range=[{per_st['pct_improv'].min():>+6.1f}%, "
            f"{per_st['pct_improv'].max():>+6.1f}%]"
        )

    lines.append("")
    return lines


# -----------------------------------------------------------------------
# 5. Decision matrix
# -----------------------------------------------------------------------
def decision_matrix(
    df: pd.DataFrame,
    r2_results: dict,
    helped_count: dict,
    hurt_count: dict,
    r2_excl_results: dict,
    rmse_excl_results: dict,
) -> list[str]:
    """Final summary decision matrix."""
    lines = [
        "5. DECISION MATRIX",
        "=" * 70,
        "",
        f"  {'Model':<25} {'GHI R²':>8} {'GHI RMSE':>10} "
        f"{'Help/Hurt':>10} {'DNI R²*':>8} {'DNI RMSE*':>10}",
        f"  {'-' * 75}",
    ]

    for model in GENERALIZABLE_MODELS:
        m = df[df["model_name"] == model]

        # GHI RMSE
        ghi_rmse = np.sqrt(
            ((m["actual_cf_ghi"] - m["predicted_cf_ghi"]) ** 2).mean()
        )
        ghi_r2 = r2_results[model]["ghi"]
        hh = f"{helped_count[model]}/{hurt_count[model]}"
        dni_r2 = r2_excl_results[model]
        dni_rmse = rmse_excl_results[model]

        lines.append(
            f"  {model:<25} {ghi_r2:>8.4f} {ghi_rmse:>10.4f} "
            f"{hh:>10} {dni_r2:>8.4f} {dni_rmse:>10.4f}"
        )

    lines.append("")
    lines.append("  * DNI metrics exclude outlier station-months (actual CF > 2.0 or < 0.5)")
    lines.append("")

    return lines


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------
def main() -> None:
    """Run all diagnostics and save output."""
    df = load_predictions()

    all_lines: list[str] = [
        "MODEL DIAGNOSTICS (v2 — 20 stations)",
        "=" * 70,
        f"Rows: {len(df)}, Models: {df['model_name'].nunique()}, "
        f"Stations: {df['station_id'].nunique()}",
    ]

    # 1. R²
    r2_lines, r2_results = compute_r2(df)
    all_lines.extend(r2_lines)

    # 2. Per-station breakdown
    ps_lines, helped, hurt = per_station_breakdown(df)
    all_lines.extend(ps_lines)

    # 3. DNI outlier analysis
    dni_lines, r2_excl, rmse_excl = dni_outlier_analysis(df)
    all_lines.extend(dni_lines)

    # 4. Stability analysis
    stab_lines = stability_analysis(df)
    all_lines.extend(stab_lines)

    # 5. Decision matrix
    dm_lines = decision_matrix(df, r2_results, helped, hurt, r2_excl, rmse_excl)
    all_lines.extend(dm_lines)

    # Save and print
    output_text = "\n".join(all_lines) + "\n"
    output_path = OUTPUT_DIR / "model_diagnostics_v2.txt"
    output_path.write_text(output_text)
    logger.info("Saved %s", output_path)
    print("\n" + output_text)


if __name__ == "__main__":
    main()
