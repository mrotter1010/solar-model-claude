"""Download and locally cache 5-minute NSRDB data for all 45 research sites.

Endpoint: NSRDB GOES CONUS v4.0.0
Resolution: 5-minute (105,120 rows per non-leap year)
Year: 2022
Cache: research/m8_subhourly/cache/5min/{site_name}_2022_5min.csv
"""

import csv
import os
import time
from io import StringIO
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Credentials — env vars first, fall back to defaults from nsrdb_client.py
API_KEY = os.environ.get("NSRDB_API_KEY", "y1zAp5Hghami0SWXdi0xhc6kcvfWZhpliZoApVzB")
EMAIL = os.environ.get("NSRDB_API_EMAIL", "rotter.mich@gmail.com")

ENDPOINT_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/nsrdb-GOES-conus-v4-0-0-download.csv"
YEAR = 2022
INTERVAL = 5
EXPECTED_DATA_ROWS = 105_120

ATTRIBUTES = [
    "ghi",
    "dni",
    "dhi",
    "air_temperature",
    "wind_speed",
    "surface_pressure",
    "dew_point",
    "surface_albedo",
]

REQUIRED_COLUMNS = {
    "GHI",
    "DNI",
    "DHI",
    "Temperature",
    "Wind Speed",
    "Pressure",
    "Dew Point",
    "Surface Albedo",
}

# Rate limit: 1.1s between requests (conservative margin on 1/sec limit)
MIN_REQUEST_INTERVAL_S = 1.1

RESEARCH_DIR = Path(__file__).parent
SITE_CSV = RESEARCH_DIR / "research_sites.csv"
CACHE_DIR = RESEARCH_DIR / "cache" / "5min"

REQUEST_TIMEOUT_S = 300


def load_sites(path: Path) -> list[dict]:
    """Load sites from research_sites.csv."""
    sites = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sites.append({
                "site_name": row["site_name"],
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
            })
    return sites


def cache_path(site_name: str) -> Path:
    """Return the cache file path for a site."""
    return CACHE_DIR / f"{site_name}_{YEAR}_5min.csv"


def is_cached(path: Path) -> bool:
    """Check if a cached file exists with the expected row count.

    NSRDB SAM CSV has 3 header rows (meta labels, meta values, column names)
    followed by data rows. Total lines = 3 + 105,120 = 105,123.
    """
    if not path.exists():
        return False

    # Count lines without loading entire file into memory
    count = 0
    with open(path, "r") as f:
        for _ in f:
            count += 1

    data_rows = count - 3  # 3 header rows
    if data_rows == EXPECTED_DATA_ROWS:
        return True

    print(f"    Cache file exists but has {data_rows} data rows (expected {EXPECTED_DATA_ROWS}), re-downloading")
    return False


def validate_csv(text: str) -> tuple[bool, str]:
    """Validate a downloaded CSV response.

    Returns:
        (is_valid, message) tuple.
    """
    # Check for JSON error wrapped in 200
    if text.strip().startswith("{"):
        return False, f"Got JSON error instead of CSV: {text[:300]}"

    lines = text.strip().split("\n")
    total_lines = len(lines)

    if total_lines < 4:
        return False, f"Too few lines: {total_lines}"

    # 3-row header: meta labels, meta values, column names
    column_row = lines[2]
    reader = csv.reader(StringIO(column_row))
    columns = [c.strip() for c in next(reader)]

    data_rows = total_lines - 3

    # Check row count
    if data_rows != EXPECTED_DATA_ROWS:
        return False, f"Row count {data_rows} != expected {EXPECTED_DATA_ROWS}"

    # Check required columns
    missing = REQUIRED_COLUMNS - set(columns)
    if missing:
        return False, f"Missing columns: {missing}"

    return True, f"{data_rows} rows, all columns present"


def fetch_site(site: dict) -> tuple[bool, str, float]:
    """Fetch 5-min data for a single site.

    Returns:
        (success, message, elapsed_seconds) tuple.
    """
    wkt = f"POINT({site['longitude']} {site['latitude']})"
    params = {
        "api_key": API_KEY,
        "email": EMAIL,
        "wkt": wkt,
        "names": str(YEAR),
        "attributes": ",".join(ATTRIBUTES),
        "interval": str(INTERVAL),
        "leap_day": "false",
        "utc": "true",
    }

    t0 = time.time()

    try:
        response = requests.get(ENDPOINT_URL, params=params, timeout=REQUEST_TIMEOUT_S)
    except requests.exceptions.Timeout:
        return False, "Request timed out", time.time() - t0
    except requests.exceptions.RequestException as e:
        return False, f"Request error: {e}", time.time() - t0

    elapsed = time.time() - t0

    if response.status_code != 200:
        return False, f"HTTP {response.status_code}: {response.text[:300]}", elapsed

    # Validate
    is_valid, msg = validate_csv(response.text)
    if not is_valid:
        return False, msg, elapsed

    # Save to cache
    out = cache_path(site["site_name"])
    out.write_text(response.text)

    return True, msg, elapsed


def fetch_all_sites() -> None:
    """Download 5-min data for all research sites with caching and rate limiting."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    sites = load_sites(SITE_CSV)
    total = len(sites)

    print(f"NSRDB 5-Min Fetcher — {total} sites, year {YEAR}")
    print(f"Cache dir: {CACHE_DIR}")
    print(f"Endpoint: {ENDPOINT_URL}")
    print()

    successes = []
    failures = []
    skipped = []
    last_request_time = 0.0
    batch_start = time.time()

    for i, site in enumerate(sites, 1):
        name = site["site_name"]
        cp = cache_path(name)

        # Check cache
        if is_cached(cp):
            print(f"[{i:2d}/{total}] {name} already cached, skipping")
            skipped.append(name)
            continue

        # Rate limiting
        since_last = time.time() - last_request_time
        if since_last < MIN_REQUEST_INTERVAL_S:
            wait = MIN_REQUEST_INTERVAL_S - since_last
            time.sleep(wait)

        print(f"[{i:2d}/{total}] Fetching {name}...", end=" ", flush=True)

        success, msg, elapsed = fetch_site(site)
        last_request_time = time.time()

        if success:
            print(f"done ({msg}, {elapsed:.1f}s)")
            successes.append(name)
        else:
            print(f"FAILED ({msg})")
            failures.append((name, msg))

    batch_elapsed = time.time() - batch_start

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total sites:     {total}")
    print(f"Downloaded:      {len(successes)}")
    print(f"Cached (skipped):{len(skipped)}")
    print(f"Failed:          {len(failures)}")
    print(f"Total time:      {batch_elapsed:.0f}s ({batch_elapsed/60:.1f}min)")

    if failures:
        print(f"\nFailed sites:")
        for name, msg in failures:
            print(f"  {name}: {msg}")

    if successes or skipped:
        print(f"\nAll cached files: {CACHE_DIR}/")


if __name__ == "__main__":
    fetch_all_sites()
