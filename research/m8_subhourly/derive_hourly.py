"""Derive 60-minute weather files from cached 5-minute NSRDB data.

For each 5-min file, computes hourly means of each 12-row block for all
weather columns. Preserves the SAM CSV 3-row header. Output Minute column
is 0 for all rows (start-of-hour convention).

Input:  cache/5min/{site_name}_2022_5min.csv   (105,120 data rows)
Output: cache/60min/{site_name}_2022_60min.csv  (8,760 data rows)
"""

import csv
import time
from io import StringIO
from pathlib import Path

RESEARCH_DIR = Path(__file__).parent
CACHE_5MIN = RESEARCH_DIR / "cache" / "5min"
CACHE_60MIN = RESEARCH_DIR / "cache" / "60min"

EXPECTED_5MIN_ROWS = 105_120
EXPECTED_60MIN_ROWS = 8_760
ROWS_PER_HOUR = 12

# Columns to average (everything after the 5 time columns)
WEATHER_COLS = ["GHI", "DNI", "DHI", "Temperature", "Wind Speed",
                "Pressure", "Dew Point", "Surface Albedo"]


def is_cached(path: Path, expected_rows: int) -> bool:
    """Check if output file exists with the expected data row count."""
    if not path.exists():
        return False
    count = 0
    with open(path) as f:
        for _ in f:
            count += 1
    data_rows = count - 3  # 3 header rows
    if data_rows == expected_rows:
        return True
    print(f"    Output exists but has {data_rows} data rows (expected {expected_rows}), re-deriving")
    return False


def derive_hourly(src: Path, dst: Path) -> int:
    """Derive a 60-min file from a 5-min file. Returns output data row count."""
    with open(src) as f:
        lines = f.read().strip().split("\n")

    # 3-row header
    header_lines = lines[:3]
    data_lines = lines[3:]

    if len(data_lines) != EXPECTED_5MIN_ROWS:
        raise ValueError(
            f"{src.name}: expected {EXPECTED_5MIN_ROWS} data rows, got {len(data_lines)}"
        )

    # Parse column names from row 2
    col_reader = csv.reader(StringIO(header_lines[2]))
    columns = next(col_reader)

    # Find indices for weather columns
    weather_indices = []
    for wc in WEATHER_COLS:
        idx = columns.index(wc)
        weather_indices.append(idx)

    # Find indices for time columns
    year_idx = columns.index("Year")
    month_idx = columns.index("Month")
    day_idx = columns.index("Day")
    hour_idx = columns.index("Hour")
    minute_idx = columns.index("Minute")

    # Parse all data rows
    reader = csv.reader(StringIO("\n".join(data_lines)))
    all_rows = list(reader)

    # Aggregate in 12-row blocks
    hourly_rows = []
    for block_start in range(0, len(all_rows), ROWS_PER_HOUR):
        block = all_rows[block_start : block_start + ROWS_PER_HOUR]

        # Time from first row of block, Minute = 0
        first = block[0]
        year = first[year_idx]
        month = first[month_idx]
        day = first[day_idx]
        hour = first[hour_idx]

        # Mean of weather columns
        means = []
        for wi in weather_indices:
            vals = [float(row[wi]) for row in block]
            means.append(sum(vals) / len(vals))

        # Build output row in same column order
        out_row = [""] * len(columns)
        out_row[year_idx] = year
        out_row[month_idx] = month
        out_row[day_idx] = day
        out_row[hour_idx] = hour
        out_row[minute_idx] = "0"
        for wi, mean_val in zip(weather_indices, means):
            out_row[wi] = f"{mean_val:.6g}"

        hourly_rows.append(out_row)

    # Write output
    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(dst, "w", newline="") as f:
        # Preserve original header lines exactly
        for hl in header_lines:
            f.write(hl + "\n")
        writer = csv.writer(f)
        writer.writerows(hourly_rows)

    return len(hourly_rows)


def derive_all() -> None:
    """Process all cached 5-min files."""
    CACHE_60MIN.mkdir(parents=True, exist_ok=True)

    src_files = sorted(CACHE_5MIN.glob("*_2022_5min.csv"))
    total = len(src_files)

    if total == 0:
        print(f"No 5-min files found in {CACHE_5MIN}")
        return

    print(f"Deriving 60-min files from {total} sites")
    print(f"Input:  {CACHE_5MIN}")
    print(f"Output: {CACHE_60MIN}")
    print()

    successes = []
    failures = []
    skipped = []
    t0 = time.time()

    for i, src in enumerate(src_files, 1):
        # Extract site name: {site_name}_2022_5min.csv -> {site_name}
        site_name = src.stem.replace("_2022_5min", "")
        dst = CACHE_60MIN / f"{site_name}_2022_60min.csv"

        if is_cached(dst, EXPECTED_60MIN_ROWS):
            print(f"[{i:2d}/{total}] {site_name} already derived, skipping")
            skipped.append(site_name)
            continue

        print(f"[{i:2d}/{total}] Deriving 60-min for {site_name}...", end=" ", flush=True)

        try:
            row_count = derive_hourly(src, dst)
            if row_count != EXPECTED_60MIN_ROWS:
                raise ValueError(f"Output has {row_count} rows, expected {EXPECTED_60MIN_ROWS}")
            print(f"done ({row_count} rows)")
            successes.append(site_name)
        except Exception as e:
            print(f"FAILED ({e})")
            failures.append((site_name, str(e)))

    elapsed = time.time() - t0

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total sites:     {total}")
    print(f"Derived:         {len(successes)}")
    print(f"Skipped (cached):{len(skipped)}")
    print(f"Failed:          {len(failures)}")
    print(f"Total time:      {elapsed:.1f}s")

    if failures:
        print(f"\nFailed sites:")
        for name, msg in failures:
            print(f"  {name}: {msg}")


if __name__ == "__main__":
    derive_all()
