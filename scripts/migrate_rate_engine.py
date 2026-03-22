"""Apply rate engine database migration.

Adds bill calculation columns to run_inputs and bill_savings to run_results.
Idempotent: safe to run multiple times.

Usage:
    python scripts/migrate_rate_engine.py
    DATABASE_URL=postgresql://user:pass@host/db python scripts/migrate_rate_engine.py
"""

import sys
from pathlib import Path

from sqlalchemy import text

# Add project root to path so we can import src modules
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import get_engine
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

MIGRATIONS: list[tuple[str, str, str]] = [
    # (table, column, ALTER statement)
    ("run_inputs", "bill_calculation", "ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bill_calculation BOOLEAN"),
    ("run_inputs", "rate_file_path", "ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS rate_file_path VARCHAR"),
    ("run_inputs", "utility_name", "ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS utility_name VARCHAR"),
    ("run_inputs", "tariff_name", "ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS tariff_name VARCHAR"),
    ("run_inputs", "load_profile_path", "ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS load_profile_path VARCHAR"),
    ("run_inputs", "load_type", "ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS load_type VARCHAR"),
    ("run_inputs", "annual_consumption_kwh", "ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS annual_consumption_kwh DOUBLE PRECISION"),
    ("run_inputs", "peak_demand_kw", "ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS peak_demand_kw DOUBLE PRECISION"),
    ("run_results", "bill_savings", "ALTER TABLE run_results ADD COLUMN IF NOT EXISTS bill_savings JSONB"),
]


def run_migration() -> None:
    """Connect to the database and apply all migration statements."""
    engine = get_engine()
    logger.info("Connecting to: %s", engine.url)

    with engine.connect() as conn:
        for table, column, sql in MIGRATIONS:
            try:
                conn.execute(text(sql))
                conn.commit()
                logger.info("  %s.%s — added", table, column)
            except Exception as exc:
                conn.rollback()
                logger.error("  %s.%s — failed: %s", table, column, exc)

    logger.info("Migration complete")


if __name__ == "__main__":
    run_migration()
