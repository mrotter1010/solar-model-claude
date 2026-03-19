"""Compare PVDAQ site 9068 against PVWatts V8 and PVGIS estimates.

Calls PVWatts V8 at three GCR values (0.30, 0.34, 0.38) with matched
system parameters (dc_ac_ratio, losses excluding PVWatts-internal losses),
plus PVGIS as a screening reference.

Prints a comparison table against measured production and our pipeline results.
"""

import csv
import json
import logging
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from src.climate.nsrdb_client import NSRDBClient

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# --- Site parameters ---
LAT = 40.3864
LON = -104.5512
DC_KW = 4738  # DC capacity in kW
AC_KW = 3820  # AC capacity in kW
DC_AC_RATIO = DC_KW / AC_KW  # 1.2403...
MEASURED_MWH = 8239  # 5-year average (2018-2022)

# Losses for PVWatts — only losses PVWatts does NOT model internally.
# PVWatts handles: inverter efficiency, tracker self-shading (via GCR),
# inverter clipping (via dc_ac_ratio), temperature effects.
# We supply: soiling, DC wiring, AC wiring, mismatch, snow.
SOILING = 0.0238
DC_WIRING = 0.02
AC_WIRING = 0.01
MISMATCH = 0.01
SNOW = 0.0215
# Multiplicative: total_loss = 1 - (1-s)(1-dw)(1-aw)(1-mm)(1-sn)
RETENTION = (1 - SOILING) * (1 - DC_WIRING) * (1 - AC_WIRING) * (1 - MISMATCH) * (1 - SNOW)
PVWATTS_LOSSES_PCT = round((1 - RETENTION) * 100, 2)

GCRS = [0.30, 0.34, 0.38]

# Our pipeline results (from prior benchmark run)
OUR_RESULTS = {
    "Our model GCR=0.30": 8648.4,
    "Our model GCR=0.34": 8540.8,
    "Our model GCR=0.38": 8424.9,
}

API_KEY = NSRDBClient().api_key
OUTPUT_DIR = Path(__file__).parent / "output"


def call_pvwatts(gcr: float) -> dict | None:
    """Call PVWatts V8 API with explicit GCR and DC/AC ratio."""
    params = {
        "api_key": API_KEY,
        "system_capacity": DC_KW,
        "module_type": 2,           # Thin film (CdTe)
        "losses": PVWATTS_LOSSES_PCT,
        "array_type": 2,            # Single-axis tracker
        "tilt": 0,
        "azimuth": 180,
        "lat": LAT,
        "lon": LON,
        "dataset": "nsrdb",
        "timeframe": "monthly",
        "gcr": gcr,
        "dc_ac_ratio": round(DC_AC_RATIO, 2),
    }
    url = "https://developer.nrel.gov/api/pvwatts/v8.json?" + urllib.parse.urlencode(params)
    logger.info(f"PVWatts request URL (GCR={gcr}): {url}")

    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        return data
    except Exception as e:
        logger.error(f"PVWatts API call failed (GCR={gcr}): {e}")
        return None


def call_pvgis() -> dict | None:
    """Call PVGIS API and return the full JSON response."""
    params = {
        "lat": LAT,
        "lon": LON,
        "peakpower": DC_KW,  # PVGIS takes kWp; 4738 kW = 4738 kWp
        "loss": 14,  # Flat 14% — PVGIS doesn't model GCR/clipping/inverter separately
        "pvtechchoice": "CdTe",
        "mountingplace": "free",
        "inclined_axis": 1,         # Single-axis tracking (horizontal N-S axis)
        "inclinedaxisangle": 0,     # Horizontal axis
        "outputformat": "json",
    }
    url = "https://re.jrc.ec.europa.eu/api/v5_3/PVcalc?" + urllib.parse.urlencode(params)
    logger.info(f"PVGIS request URL: {url}")

    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        return data
    except Exception as e:
        logger.error(f"PVGIS API call failed: {e}")
        return None


def extract_pvwatts_results(data: dict) -> tuple[float, float, list[float]]:
    """Extract annual kWh, capacity factor, and monthly kWh from PVWatts response."""
    outputs = data["outputs"]
    annual_kwh = outputs["ac_annual"]
    capacity_factor = outputs["capacity_factor"]
    monthly_kwh = outputs["ac_monthly"]
    return annual_kwh, capacity_factor, monthly_kwh


def extract_pvgis_results(data: dict) -> tuple[float, list[dict]]:
    """Extract annual kWh and monthly data from PVGIS response."""
    totals = data["outputs"]["totals"]
    if "inclined_axis" in totals:
        annual_kwh = totals["inclined_axis"]["E_y"]
    elif "fixed" in totals:
        annual_kwh = totals["fixed"]["E_y"]
    else:
        for key, val in totals.items():
            if isinstance(val, dict) and "E_y" in val:
                annual_kwh = val["E_y"]
                break
        else:
            raise KeyError(f"Could not find E_y in PVGIS totals: {list(totals.keys())}")

    monthly_data = data["outputs"].get("monthly", {})
    return annual_kwh, monthly_data


def error_pct(modeled: float, measured: float) -> str:
    """Format error percentage vs measured."""
    pct = ((modeled - measured) / measured) * 100
    return f"{pct:+.1f}%"


def main() -> None:
    """Run PVWatts (3 GCRs) and PVGIS API calls, print comparison table."""
    logger.info(f"PVWatts loss calculation:")
    logger.info(f"  Soiling:    {SOILING*100:.2f}%")
    logger.info(f"  DC Wiring:  {DC_WIRING*100:.1f}%")
    logger.info(f"  AC Wiring:  {AC_WIRING*100:.1f}%")
    logger.info(f"  Mismatch:   {MISMATCH*100:.1f}%")
    logger.info(f"  Snow:       {SNOW*100:.2f}%")
    logger.info(f"  Retention:  {RETENTION*100:.2f}%")
    logger.info(f"  Total loss: {PVWATTS_LOSSES_PCT:.2f}%")
    logger.info(f"  DC/AC ratio: {DC_AC_RATIO:.2f}")

    pvwatts_results = {}  # gcr -> mwh

    # --- PVWatts at each GCR ---
    for gcr in GCRS:
        logger.info("\n" + "=" * 70)
        logger.info(f"CALLING PVWatts V8 API — GCR={gcr}")
        logger.info("=" * 70)
        data = call_pvwatts(gcr)

        if data is not None:
            logger.info(f"\nFull PVWatts response (GCR={gcr}):")
            logger.info(json.dumps(data, indent=2))

            if "errors" in data and data["errors"]:
                logger.error(f"PVWatts returned errors: {data['errors']}")
            elif "outputs" in data:
                annual_kwh, cf, monthly = extract_pvwatts_results(data)
                mwh = annual_kwh / 1000
                pvwatts_results[gcr] = mwh

                logger.info(f"\nPVWatts V8 Results (GCR={gcr}):")
                logger.info(f"  Annual AC:        {annual_kwh:,.0f} kWh = {mwh:,.1f} MWh")
                logger.info(f"  Capacity Factor:  {cf:.1f}%")
                logger.info(f"  Monthly AC (kWh): {[round(m, 1) for m in monthly]}")
        else:
            logger.error(f"PVWatts call returned None (GCR={gcr})")

        # Brief pause between API calls
        time.sleep(1)

    # --- PVGIS ---
    logger.info("\n" + "=" * 70)
    logger.info("CALLING PVGIS API")
    logger.info("=" * 70)
    pvgis_data = call_pvgis()

    pvgis_mwh = None
    if pvgis_data is not None:
        logger.info("\nFull PVGIS response:")
        logger.info(json.dumps(pvgis_data, indent=2))

        try:
            annual_kwh, monthly_data = extract_pvgis_results(pvgis_data)
            pvgis_mwh = annual_kwh / 1000
            logger.info(f"\nPVGIS Results:")
            logger.info(f"  Annual AC:        {annual_kwh:,.0f} kWh = {pvgis_mwh:,.1f} MWh")
        except Exception as e:
            logger.error(f"Failed to extract PVGIS results: {e}")
    else:
        logger.error("PVGIS call returned None")

    # --- Comparison table ---
    logger.info("\n" + "=" * 80)
    logger.info("COMPARISON TABLE — PVDAQ Site 9068 (Kersey, CO)")
    logger.info(f"PVWatts params: losses={PVWATTS_LOSSES_PCT}%, dc_ac_ratio={DC_AC_RATIO:.2f}, module=CdTe, tracker")
    logger.info("=" * 80)
    logger.info(f"{'Source':<25} | {'Annual AC (MWh)':>16} | {'Error vs Measured':>18}")
    logger.info("-" * 80)

    rows = []

    # Measured
    logger.info(f"{'Measured (5yr avg)':<25} | {MEASURED_MWH:>13,.1f}    | {'---':>18}")
    rows.append(("Measured (5yr avg)", f"{MEASURED_MWH:.1f}", "---"))

    # Interleave our model and PVWatts at each GCR for easy comparison
    for gcr in GCRS:
        gcr_str = f"{gcr:.2f}"

        # Our model
        our_key = f"Our model GCR={gcr_str}"
        our_mwh = OUR_RESULTS.get(our_key)
        if our_mwh is not None:
            err = error_pct(our_mwh, MEASURED_MWH)
            logger.info(f"{our_key:<25} | {our_mwh:>13,.1f}    | {err:>18}")
            rows.append((our_key, f"{our_mwh:.1f}", err))

        # PVWatts
        pw_key = f"PVWatts GCR={gcr_str}"
        pw_mwh = pvwatts_results.get(gcr)
        if pw_mwh is not None:
            err = error_pct(pw_mwh, MEASURED_MWH)
            logger.info(f"{pw_key:<25} | {pw_mwh:>13,.1f}    | {err:>18}")
            rows.append((pw_key, f"{pw_mwh:.1f}", err))
        else:
            logger.info(f"{pw_key:<25} | {'FAILED':>16} | {'N/A':>18}")
            rows.append((pw_key, "FAILED", "N/A"))

    # PVGIS
    if pvgis_mwh is not None:
        err = error_pct(pvgis_mwh, MEASURED_MWH)
        logger.info(f"{'PVGIS (incl. axis)':<25} | {pvgis_mwh:>13,.1f}    | {err:>18}")
        rows.append(("PVGIS (incl. axis)", f"{pvgis_mwh:.1f}", err))
    else:
        logger.info(f"{'PVGIS (incl. axis)':<25} | {'FAILED':>16} | {'N/A':>18}")
        rows.append(("PVGIS (incl. axis)", "FAILED", "N/A"))

    # --- Save CSV ---
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUTPUT_DIR / "9068_comparison_results.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Source", "Annual AC (MWh)", "Error vs Measured"])
        writer.writerows(rows)
    logger.info(f"\nResults saved to: {csv_path}")


if __name__ == "__main__":
    main()
