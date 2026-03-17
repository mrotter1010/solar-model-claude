"""M11 Subhourly: Parallelized PySAM batch runner for clipping loss training data.

Runs 1,600 configs from config_matrix_m11.csv (800 configs × 2 resolutions),
extracting annual generation and capacity factor for clipping loss analysis.

Follows M8's batch_runner.py pattern:
- ProcessPoolExecutor with per-worker CEC database loading
- Production ModelConfigurator for PySAM Pvsamv1 setup
- Three post-configuration patches (snow model, bifacial, self-shading)
- Graceful per-run error handling (failures logged, batch continues)

Key difference from M8: each config matrix row already specifies a single
resolution and weather file path, so no task duplication is needed.

Usage:
    # Smoke test (2 stations, 160 runs)
    python batch_runner_m11.py --stations "Bondville_IL,DesertRock_NV"

    # Full run (20 stations, 1600 runs)
    python batch_runner_m11.py

    # Custom workers and output
    python batch_runner_m11.py --workers 8 --output results.csv
"""

import argparse
import logging
import multiprocessing
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd

# Add project root to sys.path so worker processes can import src.*
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.loader import COLUMN_MAP  # noqa: E402
from src.config.schema import SiteConfig  # noqa: E402
from src.pysam_integration.cec_database import CECDatabase  # noqa: E402
from src.pysam_integration.model_configurator import ModelConfigurator  # noqa: E402

# --- Paths ---
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = SCRIPT_DIR / "config_matrix_m11.csv"
DEFAULT_OUTPUT = SCRIPT_DIR / "batch_results.csv"

# --- Per-process globals (set by worker_init) ---
_configurator: ModelConfigurator | None = None


def worker_init() -> None:
    """Initialize per-worker PySAM configurator with CEC database.

    Each worker process loads its own CECDatabase and ModelConfigurator
    since PySAM model objects cannot be shared across processes.
    Suppresses INFO-level logging to avoid flooding output from 1,600 runs.
    """
    global _configurator
    logging.disable(logging.INFO)
    cec_db = CECDatabase()
    _configurator = ModelConfigurator(cec_database=cec_db)


def run_single(task: tuple[dict, str]) -> dict[str, Any]:
    """Run a single PySAM simulation and return summary metrics.

    Args:
        task: (row_dict with SiteConfig field names, run_name)

    Returns:
        Dict with run metrics. Includes 'error' key on failure.
    """
    row_dict, run_name = task
    resolution = run_name.rsplit("_", 1)[-1]  # "1min" or "60min"

    t0 = time.time()
    try:
        config_dict = dict(row_dict)
        config_dict["weather_file_path"] = Path(config_dict.pop("resource_file_path"))

        site_config = SiteConfig(**config_dict)

        # Configure PySAM model (reuses full production configurator)
        model_config = _configurator.configure_model(site_config)
        model = model_config.model

        # --- Post-configuration patches (same as M8) ---
        # 1. Snow model: weather files lack Snow Depth column
        model.Losses.en_snow_model = 0

        # 2. Bifacial ground clearance: must be positive when bifacial=1
        cec = model.CECPerformanceModelWithModuleDatabase
        if cec.cec_is_bifacial == 1 and cec.cec_bifacial_ground_clearance_height <= 0:
            cec.cec_bifacial_ground_clearance_height = (
                site_config.ground_clearance_height_m
            )

        # 3. Self-shading layout: set nmodx = nstrings for correct total
        sd = model.SystemDesign
        model.Layout.subarray1_nmodx = sd.subarray1_nstrings

        # Execute simulation
        model.execute()

        outputs = model.Outputs
        elapsed = time.time() - t0
        return {
            "run_name": run_name,
            "site_name": site_config.site_name,
            "latitude": site_config.latitude,
            "longitude": site_config.longitude,
            "dcac_ratio": round(
                site_config.dc_size_mw / site_config.ac_installed_mw, 2
            ),
            "gcr": site_config.gcr,
            "racking": site_config.racking,
            "resolution": resolution,
            "annual_ac_kwh": float(outputs.annual_energy),
            "annual_dc_kwh": float(sum(outputs.dc_net)),
            "capacity_factor_pct": float(outputs.capacity_factor),
            "runtime_s": round(elapsed, 1),
            "status": "success",
            "error_message": "",
        }
    except Exception as exc:
        elapsed = time.time() - t0
        return {
            "run_name": run_name,
            "site_name": row_dict.get("site_name", "unknown"),
            "latitude": row_dict.get("latitude"),
            "longitude": row_dict.get("longitude"),
            "dcac_ratio": None,
            "gcr": None,
            "racking": None,
            "resolution": resolution,
            "annual_ac_kwh": None,
            "annual_dc_kwh": None,
            "capacity_factor_pct": None,
            "runtime_s": round(elapsed, 1),
            "status": "error",
            "error_message": str(exc),
        }


def load_config_matrix(
    csv_path: Path, stations_filter: list[str] | None = None,
) -> pd.DataFrame:
    """Load config matrix CSV and rename columns to SiteConfig field names.

    Args:
        csv_path: Path to config_matrix_m11.csv.
        stations_filter: Optional list of site names to include.

    Returns:
        DataFrame with columns renamed to SiteConfig field names.
    """
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    df = df.rename(columns=COLUMN_MAP)

    if stations_filter:
        df = df[df["site_name"].isin(stations_filter)].reset_index(drop=True)
        print(f"Filtered to {len(df)} rows for stations: {stations_filter}")

    return df


def build_tasks(df: pd.DataFrame) -> list[tuple[dict, str]]:
    """Build (row_dict, run_name) task tuples from config matrix.

    Each row in the M11 matrix is already a single task (one resolution).

    Args:
        df: Config DataFrame with SiteConfig field names.

    Returns:
        List of (row_dict, run_name) tuples.
    """
    tasks: list[tuple[dict, str]] = []
    missing_files = 0

    for _, row in df.iterrows():
        row_dict = {
            k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()
        }
        run_name = row_dict["run_name"]

        # Validate weather file exists
        weather_path = row_dict.get("resource_file_path", "")
        if not weather_path or not Path(weather_path).exists():
            print(f"WARNING: Weather file missing for {run_name}: {weather_path}")
            missing_files += 1
            continue

        tasks.append((row_dict, run_name))

    if missing_files:
        print(f"WARNING: {missing_files} tasks skipped due to missing weather files")

    return tasks


def main() -> None:
    """Entry point: load configs, build tasks, run parallel PySAM batch."""
    parser = argparse.ArgumentParser(
        description="M11 Subhourly PySAM Batch Runner",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=str(DEFAULT_CONFIG),
        help=f"Path to config matrix CSV (default: {DEFAULT_CONFIG.name})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of worker processes (default: cpu_count - 2, min 1)",
    )
    parser.add_argument(
        "--stations",
        type=str,
        default=None,
        help='Comma-separated station filter (e.g., "Bondville_IL,DesertRock_NV")',
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT),
        help=f"Output results CSV path (default: {DEFAULT_OUTPUT.name})",
    )
    args = parser.parse_args()

    stations_filter = args.stations.split(",") if args.stations else None
    num_workers = args.workers or max(
        1, (multiprocessing.cpu_count() or 4) - 2,
    )
    output_path = Path(args.output)

    # Load config matrix
    config_path = Path(args.config)
    print(f"Loading config matrix from {config_path.name}")
    df = load_config_matrix(config_path, stations_filter=stations_filter)
    print(f"Loaded {len(df)} rows")

    # Build task list
    tasks = build_tasks(df)
    total = len(tasks)
    print(f"Built {total} tasks")
    print(f"Using {num_workers} worker processes\n")

    # Execute in parallel
    results: list[dict[str, Any]] = []
    failures = 0
    t_start = time.time()

    with ProcessPoolExecutor(
        max_workers=num_workers, initializer=worker_init,
    ) as pool:
        futures = {pool.submit(run_single, task): task for task in tasks}

        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
            except Exception as exc:
                task = futures[future]
                result = {
                    "run_name": task[1],
                    "site_name": task[0].get("site_name", "unknown"),
                    "latitude": None,
                    "longitude": None,
                    "dcac_ratio": None,
                    "gcr": None,
                    "racking": None,
                    "resolution": task[1].rsplit("_", 1)[-1],
                    "annual_ac_kwh": None,
                    "annual_dc_kwh": None,
                    "capacity_factor_pct": None,
                    "runtime_s": None,
                    "status": "error",
                    "error_message": f"Worker crash: {exc}",
                }

            results.append(result)
            if result.get("status") == "error":
                failures += 1
                print(
                    f"  FAILED [{i}/{total}]: "
                    f"{result['run_name']} -- {result['error_message']}"
                )

            # Progress logging every 10 runs
            if i % 10 == 0 or i == total:
                elapsed = time.time() - t_start
                rate = i / elapsed if elapsed > 0 else 0
                last_name = result["run_name"]
                last_time = result.get("runtime_s", "?")
                if rate > 0 and i >= 20:
                    eta = (total - i) / rate
                    print(
                        f"  [{i}/{total}] {i - failures} success, "
                        f"{failures} fail -- "
                        f"{rate:.1f} runs/s -- ETA: {eta:.0f}s -- "
                        f"last: {last_name} ({last_time}s)"
                    )
                else:
                    print(
                        f"  [{i}/{total}] {i - failures} success, "
                        f"{failures} fail -- "
                        f"last: {last_name} ({last_time}s)"
                    )

    elapsed_total = time.time() - t_start

    # Save results
    results_df = pd.DataFrame(results)
    output_cols = [
        "run_name", "site_name", "latitude", "longitude",
        "dcac_ratio", "gcr", "racking", "resolution",
        "annual_ac_kwh", "annual_dc_kwh", "capacity_factor_pct",
        "runtime_s", "status", "error_message",
    ]
    results_df = results_df[[c for c in output_cols if c in results_df.columns]]
    results_df.to_csv(output_path, index=False)

    # Summary
    successful = total - failures
    print(f"\n{'=' * 70}")
    print(f"Batch complete: {successful}/{total} succeeded, {failures} failed")
    print(
        f"Total time: {elapsed_total:.1f}s "
        f"({elapsed_total / max(total, 1):.2f}s avg per run)"
    )
    print(f"Results saved to: {output_path}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
