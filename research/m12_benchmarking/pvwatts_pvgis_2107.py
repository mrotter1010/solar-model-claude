"""PVWatts V8 comparison for PVDAQ 2107 (Farm Solar Array, Arbuckle CA).

Calls PVWatts V8 API with real system parameters (not proxy inverter)
and compares against measured production and our model results.

System: 893 kW DC, fixed-tilt 25 deg, azimuth 180, Arbuckle CA
Measured 4-year average (2019-2022): 1,372 MWh AC

Usage:
    python research/m12_benchmarking/pvwatts_pvgis_2107.py
"""

from pathlib import Path

import pandas as pd
import requests

NREL_API_KEY = "y1zAp5Hghami0SWXdi0xhc6kcvfWZhpliZoApVzB"
PVWATTS_URL = "https://developer.nrel.gov/api/pvwatts/v8.json"

# Real system parameters (NOT proxy)
SYSTEM_PARAMS = {
    "lat": 38.996306,
    "lon": -122.134111,
    "system_capacity": 893,       # DC kW
    "dc_ac_ratio": 1.35,          # Real: 24 x 27.6 kW = 662.4 kW AC
    "array_type": 0,              # Fixed open rack
    "tilt": 25,
    "azimuth": 180,
    "module_type": 0,             # Standard (mono-Si)
    "losses": 7.6,                # Multiplicative: soiling 2.28% + DC wiring 2%
                                  # + AC wiring 1% + mismatch 1% + LID 1.5%
}

MEASURED_MWH = 1372  # 4-year average (2019-2022)

# Our model results (proxy inverter: Delta M24U_120, DC/AC=1.41)
OUR_MODEL_RESULTS = {
    0.50: 1469.4,
    0.55: 1464.9,
    0.60: 1458.2,
}

GCR_VALUES = [0.50, 0.55, 0.60]

OUTPUT_DIR = Path("research/m12_benchmarking/output")


def fetch_pvwatts(gcr: float) -> dict:
    """Fetch PVWatts V8 results for a given GCR.

    Args:
        gcr: Ground coverage ratio.

    Returns:
        Dict with annual_ac_kwh and monthly values.

    Raises:
        RuntimeError: On API errors.
    """
    params = {
        "api_key": NREL_API_KEY,
        "gcr": gcr,
        **SYSTEM_PARAMS,
    }

    print(f"  Fetching PVWatts GCR={gcr}...")
    response = requests.get(PVWATTS_URL, params=params, timeout=60)

    if response.status_code != 200:
        raise RuntimeError(
            f"PVWatts API error {response.status_code}: {response.text[:500]}"
        )

    data = response.json()

    if "errors" in data and data["errors"]:
        raise RuntimeError(f"PVWatts API returned errors: {data['errors']}")

    outputs = data["outputs"]
    station = data.get("station_info", {})

    return {
        "annual_ac_kwh": outputs["ac_annual"],
        "monthly_ac_kwh": outputs["ac_monthly"],
        "solrad_annual": outputs["solrad_annual"],
        "capacity_factor": outputs["capacity_factor"],
        "station_city": station.get("city", ""),
        "station_state": station.get("state", ""),
        "station_distance_km": station.get("distance", 0) / 1000,
    }


def main() -> None:
    """Run PVWatts comparison and print results."""
    print("=" * 75)
    print("PVDAQ 2107 — PVWatts V8 Comparison")
    print(f"System: {SYSTEM_PARAMS['system_capacity']} kW DC, "
          f"DC/AC={SYSTEM_PARAMS['dc_ac_ratio']}, "
          f"fixed tilt={SYSTEM_PARAMS['tilt']} deg, "
          f"losses={SYSTEM_PARAMS['losses']}%")
    print("=" * 75)

    # Fetch PVWatts for each GCR
    pvwatts_results = {}
    for gcr in GCR_VALUES:
        result = fetch_pvwatts(gcr)
        pvwatts_results[gcr] = result
        print(f"    AC annual: {result['annual_ac_kwh']:,.0f} kWh "
              f"({result['annual_ac_kwh']/1000:,.1f} MWh), "
              f"CF={result['capacity_factor']:.1f}%, "
              f"solrad={result['solrad_annual']:.2f} kWh/m2/day")

    # Print station info (same for all GCR values)
    first = pvwatts_results[GCR_VALUES[0]]
    print(f"\nPVWatts station: {first['station_city']}, {first['station_state']} "
          f"({first['station_distance_km']:.1f} km from site)")

    # Build comparison table
    print(f"\n{'='*75}")
    print(f"{'Source':<30} | {'Annual AC (MWh)':>15} | {'Error vs Measured':>18}")
    print(f"{'-'*30}-+-{'-'*15}-+-{'-'*18}")
    print(f"{'Measured (4yr avg 2019-2022)':<30} | {MEASURED_MWH:>15,} | {'—':>18}")

    rows = [
        {"source": "Measured (4yr avg)", "annual_mwh": MEASURED_MWH, "error_pct": 0.0},
    ]

    # Our model results (proxy inverter, DC/AC=1.41)
    for gcr in GCR_VALUES:
        mwh = OUR_MODEL_RESULTS[gcr]
        err = (mwh - MEASURED_MWH) / MEASURED_MWH * 100
        label = f"Our model GCR={gcr:.2f}"
        print(f"{label:<30} | {mwh:>15,.1f} | {err:>+17.1f}%")
        rows.append({"source": label, "annual_mwh": mwh, "error_pct": round(err, 2)})

    # PVWatts results (real inverter, DC/AC=1.35)
    for gcr in GCR_VALUES:
        mwh = pvwatts_results[gcr]["annual_ac_kwh"] / 1000
        err = (mwh - MEASURED_MWH) / MEASURED_MWH * 100
        label = f"PVWatts GCR={gcr:.2f}"
        print(f"{label:<30} | {mwh:>15,.1f} | {err:>+17.1f}%")
        rows.append({"source": label, "annual_mwh": round(mwh, 1), "error_pct": round(err, 2)})

    print(f"{'-'*30}-+-{'-'*15}-+-{'-'*18}")

    # Note on DC/AC ratio difference
    print(f"\nNOTE: Our model uses proxy inverter (Delta M24U_120, DC/AC=1.41)")
    print(f"      PVWatts uses real system DC/AC=1.35")
    print(f"      Higher DC/AC ratio causes more clipping, reducing our model's output")

    # Save CSV
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "2107_comparison_results.csv"
    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False)
    print(f"\nSaved to: {output_path}")


if __name__ == "__main__":
    main()
