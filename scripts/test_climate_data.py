"""Manual testing script for the climate data pipeline.

Usage:
    python scripts/test_climate_data.py --csv "Energy Analytics Inputs Single Row Test - Sheet1.csv"
    python scripts/test_climate_data.py --csv input.csv --year 2023
"""

import argparse
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline import run_climate_data_pipeline
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def main() -> None:
    """Run the climate data pipeline from the command line."""
    parser = argparse.ArgumentParser(
        description="Fetch climate data for solar sites defined in a CSV file."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        required=True,
        help="Path to CSV file with site configurations.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Weather data year to retrieve (default: 2024).",
    )
    args = parser.parse_args()

    if not args.csv.exists():
        logger.error(f"CSV file not found: {args.csv}")
        sys.exit(1)

    sites = run_climate_data_pipeline(args.csv, year=args.year)

    # Print results table
    logger.info(f"\n{'Site Name':<30} {'Location':<25} {'Weather File'}")
    logger.info("-" * 90)
    for site in sites:
        loc = f"({site.latitude}, {site.longitude})"
        weather = str(site.weather_file_path) if site.weather_file_path else "N/A"
        logger.info(f"{site.site_name:<30} {loc:<25} {weather}")


if __name__ == "__main__":
    main()
