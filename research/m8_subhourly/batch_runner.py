"""M8 Subhourly: Parallelized PySAM batch runner for clipping loss training data.

Runs every config from config_matrix.csv at both 5-min and 60-min resolution,
extracting annual generation and capacity factor for clipping loss analysis.

Reuses production pipeline components:
- src.config.loader.COLUMN_MAP for CSV column name -> SiteConfig field mapping
- src.config.schema.SiteConfig for Pydantic-validated site configuration
- src.pysam_integration.cec_database.CECDatabase for CEC module/inverter lookup
- src.pysam_integration.model_configurator.ModelConfigurator for PySAM Pvsamv1 setup

Simplified from production pipeline:
- CSV loading: Replicates column mapping from load_config() but skips cross-row
  validation (unique run names, coordinate consistency) since we modify run_names
  with resolution suffixes that would conflict with the uniqueness check.
- Weather file: Sets weather_file_path directly on SiteConfig instead of using
  resource_file_path (which goes through the Solcast parser in the climate
  orchestrator). Our cached weather files are already in PySAM format with the
  standard 2-row NSRDB header, so no conversion is needed.
- Simulation: Calls ModelConfigurator.configure_model() + model.execute() directly,
  extracting only annual_energy and capacity_factor (skips full timeseries
  extraction from PySAMSimulator._extract_timeseries).

Usage:
    # Validation run (2 sites, 288 PySAM runs)
    python batch_runner.py --sites Phoenix_AZ,Seattle_WA

    # Full run (45 sites, 6480 PySAM runs)
    python batch_runner.py

    # Custom worker count
    python batch_runner.py --workers 4
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

# Add project root to sys.path for production pipeline imports.
# This runs at module level so worker processes (spawned via ProcessPoolExecutor)
# also get the path before attempting src.* imports.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.loader import COLUMN_MAP  # noqa: E402
from src.config.schema import SiteConfig  # noqa: E402
from src.pysam_integration.cec_database import CECDatabase  # noqa: E402
from src.pysam_integration.model_configurator import ModelConfigurator  # noqa: E402

# --- Paths ---
RESEARCH_DIR = Path(__file__).resolve().parent
CONFIG_MATRIX = RESEARCH_DIR / "config_matrix.csv"
CACHE_5MIN = RESEARCH_DIR / "cache" / "5min"
CACHE_60MIN = RESEARCH_DIR / "cache" / "60min"
OUTPUT_CSV = RESEARCH_DIR / "raw_run_results.csv"

# --- Per-process globals (set by worker_init) ---
_configurator: ModelConfigurator | None = None


def worker_init() -> None:
    """Initialize per-worker PySAM configurator with CEC database.

    Each worker process loads its own CECDatabase and ModelConfigurator
    since PySAM model objects cannot be shared across processes.
    Suppresses INFO-level logging to avoid flooding output from 6,480 runs.
    """
    global _configurator
    logging.disable(logging.INFO)
    cec_db = CECDatabase()
    _configurator = ModelConfigurator(cec_database=cec_db)


def run_single(task: tuple[dict, str, str, str]) -> dict[str, Any]:
    """Run a single PySAM simulation and return summary metrics.

    Reuses ModelConfigurator.configure_model() from the production pipeline
    for all PySAM Pvsamv1 setup (module, inverter, array, losses, weather).
    Calls model.execute() directly and extracts only annual_energy and
    capacity_factor.

    Args:
        task: (row_dict, weather_file_path, resolution, run_name_suffixed)

    Returns:
        Dict with run metrics. Includes 'error' key on failure.
    """
    row_dict, weather_file_str, resolution, run_name_suffixed = task

    try:
        # Build SiteConfig with modified run_name and weather_file_path
        config_dict = dict(row_dict)
        config_dict["run_name"] = run_name_suffixed
        config_dict["weather_file_path"] = Path(weather_file_str)

        site_config = SiteConfig(**config_dict)

        # Configure PySAM model (reuses full production configurator)
        model_config = _configurator.configure_model(site_config)
        model = model_config.model

        # --- Post-configuration patches for batch research context ---
        # These fix three mismatches between the production configurator
        # (designed for pipeline runs with formatted weather files) and our
        # batch use case (raw NSRDB cached files, mixed racking types).
        # Production code is NOT modified.

        # 1. Snow model: Production pipeline adds Snow Depth via
        #    WeatherFormatter + ERA5 data. Our cached NSRDB files lack this
        #    column. Disable the snow model to avoid exec fail.
        model.Losses.en_snow_model = 0

        # 2. Bifacial ground clearance: Production configurator sets
        #    cec_bifacial_ground_clearance_height = 0.0 for fixed racking
        #    (model_configurator.py:239-241), but PySAM requires positive
        #    when cec_is_bifacial=1. Use the site's ground clearance height.
        cec = model.CECPerformanceModelWithModuleDatabase
        if cec.cec_is_bifacial == 1 and cec.cec_bifacial_ground_clearance_height <= 0:
            cec.cec_bifacial_ground_clearance_height = (
                site_config.ground_clearance_height_m
            )

        # 3. Self-shading layout: Production sets nmodx=number_of_modules
        #    (1-2, table width) and nmody=modules_per_string. PySAM requires
        #    nmodx * nmody == nstrings * modules_per_string (total modules).
        #    For utility-scale (nstrings >> nmodx), this fails. Fix by
        #    setting nmodx = nstrings so the product matches.
        sd = model.SystemDesign
        model.Layout.subarray1_nmodx = sd.subarray1_nstrings

        # Execute simulation
        model.execute()

        outputs = model_config.model.Outputs
        return {
            "run_name": run_name_suffixed,
            "site_name": site_config.site_name,
            "dcac_ratio": round(
                site_config.dc_size_mw / site_config.ac_installed_mw, 4
            ),
            "gcr": site_config.gcr,
            "racking": site_config.racking,
            "resolution": resolution,
            "total_gen_kwh": float(outputs.annual_energy),
            "capacity_factor": float(outputs.capacity_factor),
        }
    except Exception as exc:
        return {
            "run_name": run_name_suffixed,
            "site_name": row_dict.get("site_name", "unknown"),
            "dcac_ratio": None,
            "gcr": None,
            "racking": None,
            "resolution": resolution,
            "total_gen_kwh": None,
            "capacity_factor": None,
            "error": str(exc),
        }


def load_config_matrix(
    csv_path: Path, sites_filter: list[str] | None = None
) -> pd.DataFrame:
    """Load config matrix CSV and rename columns to SiteConfig field names.

    Replicates the column-mapping logic from src.config.loader.load_config
    but skips cross-row validation (unique run names, coordinate consistency)
    since we modify run_names with resolution suffixes post-load.

    Args:
        csv_path: Path to config_matrix.csv.
        sites_filter: Optional list of site names to include.

    Returns:
        DataFrame with columns renamed to SiteConfig field names.
    """
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    df = df.rename(columns=COLUMN_MAP)

    if sites_filter:
        df = df[df["site_name"].isin(sites_filter)].reset_index(drop=True)
        print(f"Filtered to {len(df)} configs for sites: {sites_filter}")

    return df


def build_tasks(df: pd.DataFrame) -> list[tuple[dict, str, str, str]]:
    """Build (row_dict, weather_file, resolution, run_name) task tuples.

    For each config row, creates exactly two tasks -- one per resolution.

    Critical invariant: the run_name suffix MUST match the weather file
    resolution directory. _5min suffix always pairs with cache/5min/ files,
    _60min suffix always pairs with cache/60min/ files. A mismatch here
    silently corrupts the entire training dataset.
    """
    tasks: list[tuple[dict, str, str, str]] = []
    missing_files = 0

    for _, row in df.iterrows():
        row_dict = {
            k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()
        }
        base_run_name = row_dict["run_name"]
        site_name = row_dict["site_name"]

        for resolution, cache_dir, suffix in [
            ("5min", CACHE_5MIN, "_5min"),
            ("60min", CACHE_60MIN, "_60min"),
        ]:
            weather_file = cache_dir / f"{site_name}_2022_{resolution}.csv"
            if not weather_file.exists():
                print(f"WARNING: Weather file missing: {weather_file}")
                missing_files += 1
                continue

            run_name_suffixed = f"{base_run_name}{suffix}"
            tasks.append(
                (row_dict, str(weather_file), resolution, run_name_suffixed)
            )

    if missing_files:
        print(
            f"WARNING: {missing_files} tasks skipped due to missing weather files"
        )

    return tasks


def main() -> None:
    """Entry point: load configs, build tasks, run parallel PySAM batch."""
    parser = argparse.ArgumentParser(
        description="M8 Subhourly PySAM Batch Runner"
    )
    parser.add_argument(
        "--sites",
        type=str,
        default=None,
        help="Comma-separated site names to filter (e.g., Phoenix_AZ,Seattle_WA)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of worker processes (default: cpu_count - 2, min 1)",
    )
    args = parser.parse_args()

    sites_filter = args.sites.split(",") if args.sites else None
    num_workers = args.workers or max(
        1, (multiprocessing.cpu_count() or 4) - 2
    )

    # Load config matrix
    print(f"Loading config matrix from {CONFIG_MATRIX}")
    df = load_config_matrix(CONFIG_MATRIX, sites_filter=sites_filter)
    print(f"Loaded {len(df)} configurations")

    # Build task list
    tasks = build_tasks(df)
    total = len(tasks)
    print(f"Built {total} tasks ({len(df)} configs x 2 resolutions)")
    print(f"Using {num_workers} worker processes\n")

    # Execute in parallel
    results: list[dict[str, Any]] = []
    failures = 0
    t_start = time.time()

    with ProcessPoolExecutor(
        max_workers=num_workers, initializer=worker_init
    ) as pool:
        futures = {pool.submit(run_single, task): task for task in tasks}

        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
            except Exception as exc:
                # Worker process crash (e.g., PySAM segfault)
                task = futures[future]
                result = {
                    "run_name": task[3],
                    "site_name": task[0].get("site_name", "unknown"),
                    "dcac_ratio": None,
                    "gcr": None,
                    "racking": None,
                    "resolution": task[2],
                    "total_gen_kwh": None,
                    "capacity_factor": None,
                    "error": f"Worker crash: {exc}",
                }

            results.append(result)
            if "error" in result:
                failures += 1
                print(
                    f"  FAILED [{i}/{total}]: "
                    f"{result['run_name']} -- {result['error']}"
                )

            # Progress logging every 100 runs, with ETA after first 50
            if i % 100 == 0 or i == total:
                elapsed = time.time() - t_start
                rate = i / elapsed if elapsed > 0 else 0
                if i >= 50 and rate > 0:
                    eta = (total - i) / rate
                    print(
                        f"  Progress: {i}/{total} ({i / total * 100:.1f}%) -- "
                        f"{rate:.1f} runs/s -- ETA: {eta:.0f}s"
                    )
                else:
                    print(f"  Progress: {i}/{total} ({i / total * 100:.1f}%)")

    elapsed_total = time.time() - t_start

    # Save results
    results_df = pd.DataFrame(results)
    cols = [
        "run_name",
        "site_name",
        "dcac_ratio",
        "gcr",
        "racking",
        "resolution",
        "total_gen_kwh",
        "capacity_factor",
    ]
    if "error" in results_df.columns:
        cols.append("error")
    results_df = results_df[[c for c in cols if c in results_df.columns]]
    results_df.to_csv(OUTPUT_CSV, index=False)

    # Summary
    successful = total - failures
    print(f"\n{'=' * 60}")
    print(f"Batch complete: {successful}/{total} succeeded, {failures} failed")
    print(
        f"Total time: {elapsed_total:.1f}s "
        f"({elapsed_total / max(total, 1):.2f}s per run)"
    )
    print(f"Results saved to {OUTPUT_CSV}")

    if successful > 0:
        success_df = results_df[results_df["total_gen_kwh"].notna()]
        for res in ["5min", "60min"]:
            res_df = success_df[success_df["resolution"] == res]
            if not res_df.empty:
                print(
                    f"  {res}: {len(res_df)} runs, "
                    f"avg CF={res_df['capacity_factor'].mean():.2f}%, "
                    f"avg gen={res_df['total_gen_kwh'].mean():,.0f} kWh"
                )

        # Estimated full-run time
        if sites_filter and total > 0:
            full_total = 6480
            est_time = (elapsed_total / total) * full_total
            print(
                f"\n  Estimated time for full {full_total} runs "
                f"at {num_workers} workers: {est_time:.0f}s "
                f"({est_time / 60:.1f} min)"
            )

        print(f"\nSample results (first 5 rows):")
        print(success_df.head().to_string(index=False))


if __name__ == "__main__":
    main()
