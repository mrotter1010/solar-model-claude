"""Generate synthetic Solcast TMY test fixtures from existing NSRDB PySAM data.

Reads the NSRDB-derived PySAM CSV and transforms it into Solcast SAM CSV
format, renaming columns to match Solcast conventions and adding placeholder
columns for fields Solcast provides but NSRDB does not.

Produces two fixtures:
  - synthetic_solcast_tmy_phoenix.csv: Full column set
  - synthetic_solcast_tmy_phoenix_minimal.csv: Minimal required columns only
"""

from pathlib import Path

import pandas as pd


def main() -> None:
    """Read NSRDB PySAM data and write synthetic Solcast fixtures."""
    project_root = Path(__file__).resolve().parent.parent
    source_path = (
        project_root / "data" / "climate" / "nsrdb_33.483_-112.073_20260226.pysam.csv"
    )
    fixtures_dir = project_root / "tests" / "fixtures"
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    # Read data rows (skip 2-row metadata header, row 3 is the column header)
    df = pd.read_csv(source_path, skiprows=2)

    # Rename columns: NSRDB → Solcast conventions
    df = df.rename(
        columns={
            "Temperature": "Tdry",
            "Wind Speed": "Wspd",
            "Surface Albedo": "Albedo",
        }
    )

    # Strip leap day (Feb 29) so TMY output is exactly 8760 rows
    df = df[~((df["Month"] == 2) & (df["Day"] == 29))].reset_index(drop=True)

    # Add Solcast-specific placeholder columns
    df["RH"] = 30
    df["Pres"] = 1013
    df["Wdir"] = 180

    # --- Full fixture ---
    full_path = fixtures_dir / "synthetic_solcast_tmy_phoenix.csv"
    full_columns = [
        "Year", "Month", "Day", "Hour", "Minute",
        "GHI", "DNI", "DHI", "Tdry", "RH", "Pres",
        "Wspd", "Wdir", "Albedo", "Snow Depth",
    ]
    _write_solcast_csv(full_path, df[full_columns])
    print(f"Full fixture:    {full_path} — {len(df)} data rows")

    # --- Minimal fixture ---
    minimal_path = fixtures_dir / "synthetic_solcast_tmy_phoenix_minimal.csv"
    minimal_columns = [
        "Year", "Month", "Day", "Hour", "Minute",
        "GHI", "DNI", "DHI", "Tdry", "Wspd",
    ]
    _write_solcast_csv(minimal_path, df[minimal_columns])
    print(f"Minimal fixture: {minimal_path} — {len(df)} data rows")


def _write_solcast_csv(path: Path, df: pd.DataFrame) -> None:
    """Write a DataFrame as a Solcast SAM CSV with 2-row metadata header.

    Args:
        path: Output file path.
        df: DataFrame containing the hourly data rows.
    """
    header_row1 = "Latitude,Longitude,Time Zone,Elevation,Source"
    header_row2 = "33.483,-112.073,-7,343,Solcast"

    with open(path, "w") as f:
        f.write(header_row1 + "\n")
        f.write(header_row2 + "\n")
        df.to_csv(f, index=False, lineterminator="\n")


if __name__ == "__main__":
    main()
