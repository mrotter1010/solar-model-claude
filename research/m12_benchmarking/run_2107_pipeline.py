"""Run PVDAQ site 2107 (Farm Solar Array, Arbuckle CA) through the full production pipeline.

This script invokes the complete pipeline:
  CSV input -> NSRDB TMY -> bias correction -> ERA5 snow/precip ->
  PySAM (snow loss + dynamic soiling) -> subhourly correction -> outputs

System: 893 kW DC (2880 x Hyundai HiS-M310TI), fixed tilt 25 deg, azimuth 180
Actual inverter: 24 x ABB TRIO-27.6-TL-OUTD-S1B = 662.4 kW AC (not in CEC DB)
Proxy inverter: 24 x Delta Electronics M24U_120 [480V] = 633.7 kW AC
Proxy DC/AC = 1.41 vs real DC/AC = 1.35

Measured production: 4-year average (2019-2022) = 1,372 MWh AC
"""

import json
import logging
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from src.pipeline import SolarModelingPipeline
from src.utils.logger import setup_logger

MEASURED_MWH = 1372  # 4-year average (2019-2022)

INPUT_CSV = Path(__file__).parent / "pvdaq_2107_input.csv"
OUTPUT_DIR = Path(__file__).parent / "pipeline_output_2107"


def main() -> None:
    """Run the full pipeline and report results."""
    logger = setup_logger("src", console_level=logging.INFO)
    logger = setup_logger(__name__, console_level=logging.INFO)

    logger.info(f"Input CSV: {INPUT_CSV}")
    logger.info(f"Output dir: {OUTPUT_DIR}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pipeline = SolarModelingPipeline(output_dir=OUTPUT_DIR)
    results = pipeline.run(csv_path=INPUT_CSV)

    logger.info(
        f"Pipeline complete: {results['successful']}/{results['total_sites']} succeeded, "
        f"{results['failed']} failed"
    )

    # Print detailed results
    print("\n" + "=" * 80)
    print("PVDAQ 2107 BENCHMARK RESULTS")
    print(f"Measured 4-year average (2019-2022): {MEASURED_MWH:,.0f} MWh AC")
    print("NOTE: Proxy inverter Delta M24U_120 (26.4 kW) vs actual ABB TRIO-27.6")
    print("      Proxy AC = 633.7 kW vs real AC = 662.4 kW (DC/AC 1.41 vs 1.35)")
    print("=" * 80)

    for i, summary in enumerate(results.get("summaries", [])):
        run_name = summary.get("run_name", f"Run {i+1}")
        annual_mwh = summary.get("annual_energy_mwh", 0)
        cf = summary.get("capacity_factor_ac", 0)
        error_pct = ((annual_mwh - MEASURED_MWH) / MEASURED_MWH) * 100

        print(f"\n--- {run_name} ---")
        print(f"  Annual AC Energy:    {annual_mwh:,.1f} MWh")
        print(f"  AC Capacity Factor:  {cf:.1f}%")
        print(f"  Error vs Measured:   {error_pct:+.1f}%")

        # Loss details
        for key in sorted(summary.keys()):
            if key not in ("run_name", "annual_energy_mwh", "capacity_factor_ac"):
                print(f"  {key}: {summary[key]}")

    # Dump full summaries to JSON for inspection
    summaries_path = OUTPUT_DIR / "benchmark_summaries_2107.json"
    with open(summaries_path, "w") as f:
        json.dump(
            results.get("summaries", []),
            f,
            indent=2,
            default=str,
        )
    print(f"\nFull summaries written to: {summaries_path}")


if __name__ == "__main__":
    main()
