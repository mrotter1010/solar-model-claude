"""CLI entry point for the solar modeling pipeline."""

import argparse
import logging
import sys
from pathlib import Path

from src.pipeline import SolarModelingPipeline
from src.utils.logger import setup_logger


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Argument list (defaults to sys.argv[1:]).

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Solar production modeling pipeline using PySAM.",
    )
    parser.add_argument(
        "csv_path",
        type=Path,
        help="Path to input CSV with site configurations.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs"),
        help="Root directory for output files (default: outputs/).",
    )
    parser.add_argument(
        "--skip-climate",
        action="store_true",
        help="Skip climate data fetch (sites must have weather files assigned).",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Console logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Run the solar modeling pipeline from CLI arguments.

    Args:
        argv: Argument list (defaults to sys.argv[1:]).
    """
    args = parse_args(argv)

    log_level = getattr(logging, args.log_level)
    logger = setup_logger("src", console_level=log_level)

    logger.info(f"Starting solar modeling pipeline: {args.csv_path}")

    pipeline = SolarModelingPipeline(output_dir=args.output_dir)
    results = pipeline.run(
        csv_path=args.csv_path,
        skip_climate=args.skip_climate,
    )

    logger.info(
        f"Done — {results['successful']}/{results['total_sites']} sites succeeded"
    )

    if results["failed"] > 0:
        logger.warning(f"{results['failed']} sites failed — see error JSONs")
        sys.exit(1)


if __name__ == "__main__":
    main()
