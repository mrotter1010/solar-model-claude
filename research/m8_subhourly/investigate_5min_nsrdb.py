"""Investigate NSRDB 5-minute resolution data availability.

Milestone 8 research: Can we fetch 5-min subhourly irradiance data from NSRDB
for clipping loss analysis?

Endpoint: NSRDB GOES CONUS v4.0.0 (supports interval=5,15,30,60)
Test site: Phoenix, AZ (33.483, -112.073), year 2022.
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

# Credentials — env vars first, then fall back to defaults from nsrdb_client.py
API_KEY = os.environ.get("NSRDB_API_KEY", "y1zAp5Hghami0SWXdi0xhc6kcvfWZhpliZoApVzB")
EMAIL = os.environ.get("NSRDB_API_EMAIL", "rotter.mich@gmail.com")

# Test site
LAT = 33.483
LON = -112.073
YEAR = 2022

# GOES CONUS v4.0.0 endpoint (supports 5-min resolution)
ENDPOINT_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/nsrdb-GOES-conus-v4-0-0-download.csv"

# Attributes available on the GOES CONUS endpoint
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

# Hourly aggregated endpoint (for comparison)
HOURLY_ENDPOINT_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/nsrdb-GOES-aggregated-v4-0-0-download.csv"
HOURLY_ATTRIBUTES = [
    "ghi",
    "dni",
    "dhi",
    "air_temperature",
    "wind_speed",
    "surface_albedo",
]

OUTPUT_DIR = Path(__file__).parent
RESULTS_FILE = OUTPUT_DIR / "investigation_results.txt"


def fetch_5min_data() -> dict:
    """Fetch 5-min NSRDB data from GOES CONUS v4.0.0 endpoint."""
    wkt = f"POINT({LON} {LAT})"
    params = {
        "api_key": API_KEY,
        "email": EMAIL,
        "wkt": wkt,
        "names": str(YEAR),
        "attributes": ",".join(ATTRIBUTES),
        "interval": "5",
        "leap_day": "false",
        "utc": "true",
    }

    # Build display URL with masked key
    param_str = "&".join(f"{k}={v}" for k, v in params.items())
    safe_url = f"{ENDPOINT_URL}?{param_str}".replace(API_KEY, "***API_KEY***")

    print(f"\n{'='*70}")
    print("  GOES CONUS v4.0.0 — 5-minute resolution")
    print(f"{'='*70}")
    print(f"URL: {safe_url}")
    print("Requesting... (timeout=300s)")

    result = {
        "label": "GOES CONUS v4.0.0 (interval=5)",
        "url": ENDPOINT_URL,
        "safe_url": safe_url,
        "params": {k: v for k, v in params.items() if k != "api_key"},
        "success": False,
        "status_code": None,
        "error": None,
        "details": None,
    }

    try:
        t0 = time.time()
        response = requests.get(ENDPOINT_URL, params=params, timeout=300)
        elapsed = time.time() - t0
        result["status_code"] = response.status_code
        result["elapsed_s"] = round(elapsed, 1)

        print(f"Status: {response.status_code} ({elapsed:.1f}s)")

        if response.status_code != 200:
            snippet = response.text[:2000]
            result["error"] = f"HTTP {response.status_code}: {snippet}"
            print(f"Error response:\n{snippet}")
            return result

        # Check for JSON error wrapped in 200
        text = response.text
        if text.strip().startswith("{"):
            result["error"] = f"JSON error in 200 response: {text[:1000]}"
            print(f"Got JSON instead of CSV:\n{text[:1000]}")
            return result

        result["success"] = True
        result["details"] = analyze_csv(text, elapsed)
        return result

    except requests.exceptions.Timeout:
        result["error"] = "Request timed out (300s)"
        print("TIMEOUT")
        return result
    except requests.exceptions.RequestException as e:
        result["error"] = str(e)
        print(f"Request error: {e}")
        return result


def analyze_csv(text: str, elapsed: float) -> dict:
    """Analyze a successful CSV response.

    NSRDB SAM-format CSV has 3 header rows:
      Row 0: metadata field labels (Source, Location ID, City, ...)
      Row 1: metadata field values (NSRDB, 518063, -, -, ...)
      Row 2: data column names    (Year, Month, Day, Hour, Minute, GHI, ...)
      Row 3+: data
    """
    lines = text.strip().split("\n")
    total_lines = len(lines)

    print(f"\nTotal lines in response: {total_lines}")

    if total_lines < 4:
        print("ERROR: Too few lines to be a valid dataset")
        return {"error": "Too few lines", "raw_head": text[:500]}

    meta_labels = lines[0]
    meta_values = lines[1]
    column_row = lines[2]

    # Parse column names from row 2
    reader = csv.reader(StringIO(column_row))
    columns = next(reader)

    data_rows = total_lines - 3  # subtract 3 header rows

    print(f"Header row 0 (meta labels):  {meta_labels[:300]}")
    print(f"Header row 1 (meta values):  {meta_values[:300]}")
    print(f"Header row 2 (column names): {column_row}")
    print(f"Data rows: {data_rows}")
    print(f"Expected for 5-min non-leap year: 105,120")
    print(f"Match: {'YES' if data_rows == 105120 else 'NO'}")
    print(f"Columns ({len(columns)}): {columns}")

    # Check for PySAM-required columns
    pysam_needed = {
        "GHI": False,
        "DNI": False,
        "DHI": False,
        "Temperature": False,
        "Wind Speed": False,
        "Pressure": False,
        "Dew Point": False,
        "Albedo": False,
    }
    for col in columns:
        cl = col.strip().lower()
        if cl == "ghi":
            pysam_needed["GHI"] = True
        elif cl == "dni":
            pysam_needed["DNI"] = True
        elif cl == "dhi":
            pysam_needed["DHI"] = True
        elif "temperature" in cl:
            pysam_needed["Temperature"] = True
        elif "wind speed" in cl:
            pysam_needed["Wind Speed"] = True
        elif "pressure" in cl:
            pysam_needed["Pressure"] = True
        elif "dew point" in cl or "dew_point" in cl:
            pysam_needed["Dew Point"] = True
        elif "albedo" in cl:
            pysam_needed["Albedo"] = True

    print("\nPySAM column check:")
    all_present = True
    for field, found in pysam_needed.items():
        status = "FOUND" if found else "MISSING"
        if not found:
            all_present = False
        print(f"  {field}: {status}")

    # Print first 5 data rows (data starts at line 3)
    print("\nFirst 5 data rows:")
    for i, line in enumerate(lines[3:8]):
        print(f"  [{i}] {line}")

    # Print last 5 data rows
    print("\nLast 5 data rows:")
    for i, line in enumerate(lines[-5:]):
        print(f"  [{i}] {line}")

    # Verify 5-min resolution via Minute column
    minute_col_idx = None
    for i, c in enumerate(columns):
        if c.strip().lower() == "minute":
            minute_col_idx = i
            break

    minute_values = set()
    if minute_col_idx is not None:
        sample_reader = csv.reader(StringIO("\n".join(lines[3:151])))
        for row in sample_reader:
            if len(row) > minute_col_idx:
                minute_values.add(row[minute_col_idx].strip())
        sorted_mins = sorted(minute_values, key=lambda x: int(x))
        print(f"\nMinute values in first ~148 rows: {sorted_mins}")
        if set(sorted_mins) == {"0", "5", "10", "15", "20", "25", "30", "35", "40", "45", "50", "55"}:
            print("=> CONFIRMED: 5-minute resolution (all 12 intervals per hour)")
        elif "5" in minute_values or "10" in minute_values:
            print("=> Sub-hourly resolution detected")
        else:
            print(f"=> Resolution unclear from minute values: {sorted_mins}")

    return {
        "data_rows": data_rows,
        "columns": columns,
        "pysam_columns_present": pysam_needed,
        "all_pysam_cols_found": all_present,
        "minute_values_sample": sorted(minute_values),
        "elapsed_s": round(elapsed, 1),
        "first_5_rows": lines[3:8],
        "last_5_rows": lines[-5:],
        "metadata_labels": meta_labels,
        "metadata_values": meta_values,
        "column_header_row": column_row,
    }


def write_results(result_5min: dict, hourly_columns: list[str] | None) -> None:
    """Write investigation results to text file."""
    with open(RESULTS_FILE, "w") as f:
        f.write("NSRDB 5-Minute Resolution Investigation Results\n")
        f.write(f"{'='*70}\n")
        f.write(f"Test site: Phoenix, AZ ({LAT}, {LON})\n")
        f.write(f"Year: {YEAR}\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        r = result_5min
        f.write(f"Endpoint: {r['url']}\n")
        f.write(f"Parameters: {r['params']}\n")
        f.write(f"URL (masked): {r['safe_url']}\n")
        f.write(f"HTTP Status: {r['status_code']}\n")
        f.write(f"Success: {r['success']}\n")

        if r.get("elapsed_s"):
            f.write(f"Elapsed: {r['elapsed_s']}s\n")

        if r["error"]:
            f.write(f"\nERROR: {r['error']}\n")

        if r["success"] and r["details"]:
            d = r["details"]
            f.write(f"\n{'='*70}\n")
            f.write("RESPONSE DETAILS\n")
            f.write(f"{'='*70}\n")
            f.write(f"Data rows: {d['data_rows']}\n")
            f.write(f"Expected (5-min, non-leap): 105,120\n")
            f.write(f"Row count match: {'YES' if d['data_rows'] == 105120 else 'NO'}\n")
            f.write(f"\nMetadata labels:  {d['metadata_labels']}\n")
            f.write(f"Metadata values:  {d['metadata_values']}\n")
            f.write(f"Column header row: {d['column_header_row']}\n")
            f.write(f"Column count: {len(d['columns'])}\n")
            f.write(f"Columns: {d['columns']}\n")

            f.write(f"\n{'='*70}\n")
            f.write("PySAM COLUMN AVAILABILITY\n")
            f.write(f"{'='*70}\n")
            for field, found in d["pysam_columns_present"].items():
                f.write(f"  {field}: {'FOUND' if found else 'MISSING'}\n")
            f.write(f"All PySAM columns found: {d['all_pysam_cols_found']}\n")

            if d.get("minute_values_sample"):
                f.write(f"\nMinute values (sample): {d['minute_values_sample']}\n")

            f.write(f"\n{'='*70}\n")
            f.write("FIRST 5 DATA ROWS\n")
            f.write(f"{'='*70}\n")
            for row in d["first_5_rows"]:
                f.write(f"  {row}\n")

            f.write(f"\n{'='*70}\n")
            f.write("LAST 5 DATA ROWS\n")
            f.write(f"{'='*70}\n")
            for row in d["last_5_rows"]:
                f.write(f"  {row}\n")

            # Comparison with hourly endpoint
            f.write(f"\n{'='*70}\n")
            f.write("DIFFERENCES FROM HOURLY AGGREGATED ENDPOINT\n")
            f.write(f"{'='*70}\n")
            f.write(f"Hourly endpoint: {HOURLY_ENDPOINT_URL}\n")
            f.write(f"5-min endpoint:  {ENDPOINT_URL}\n\n")

            if hourly_columns is not None:
                conus_cols = set(c.strip() for c in d["columns"])
                hourly_cols = set(c.strip() for c in hourly_columns)
                in_5min_only = conus_cols - hourly_cols
                in_hourly_only = hourly_cols - conus_cols
                in_both = conus_cols & hourly_cols

                f.write(f"Columns in both: {sorted(in_both)}\n")
                f.write(f"Columns in 5-min only: {sorted(in_5min_only)}\n")
                f.write(f"Columns in hourly only: {sorted(in_hourly_only)}\n")
                f.write(f"\nHourly columns ({len(hourly_columns)}): {hourly_columns}\n")
                f.write(f"5-min columns ({len(d['columns'])}): {d['columns']}\n")
            else:
                f.write("Could not fetch hourly data for comparison.\n")

            f.write(f"\nKey differences:\n")
            f.write(f"  - Resolution: 5-min ({d['data_rows']} rows) vs hourly (8,760 rows)\n")
            f.write(f"  - Endpoint URL path differs (GOES-conus-v4-0-0 vs GOES-aggregated-v4-0-0)\n")
            f.write(f"  - 5-min endpoint supports interval param: 5, 15, 30, 60\n")
            f.write(f"  - Aggregated endpoint supports interval param: 30, 60\n")

        # Summary
        f.write(f"\n{'='*70}\n")
        f.write("SUMMARY\n")
        f.write(f"{'='*70}\n")
        if r["success"]:
            f.write("SUCCESS: NSRDB GOES CONUS v4.0.0 returns 5-minute data.\n")
            f.write(f"Working endpoint: {r['url']}\n")
            f.write(f"interval=5, utc=true, leap_day=false\n")
            if r["details"]:
                f.write(f"Row count: {r['details']['data_rows']}\n")
                f.write(f"All PySAM columns present: {r['details']['all_pysam_cols_found']}\n")
                f.write(f"Download time: {r['details']['elapsed_s']}s\n")
        else:
            f.write(f"FAILED: {r['error']}\n")

    print(f"\nResults written to {RESULTS_FILE}")


def fetch_hourly_columns() -> list[str] | None:
    """Fetch hourly data column names for comparison."""
    wkt = f"POINT({LON} {LAT})"
    params = {
        "api_key": API_KEY,
        "email": EMAIL,
        "wkt": wkt,
        "names": str(YEAR),
        "attributes": ",".join(HOURLY_ATTRIBUTES),
        "interval": "60",
        "leap_day": "false",
        "utc": "true",
    }

    print(f"\n{'='*70}")
    print("  Fetching hourly data for column comparison...")
    print(f"{'='*70}")

    try:
        response = requests.get(HOURLY_ENDPOINT_URL, params=params, timeout=120)
        if response.status_code == 200 and not response.text.strip().startswith("{"):
            lines = response.text.strip().split("\n")
            if len(lines) >= 3:
                # 3-row header: meta labels, meta values, column names
                reader = csv.reader(StringIO(lines[2]))
                columns = next(reader)
                print(f"Hourly columns ({len(columns)}): {columns}")
                print(f"Hourly data rows: {len(lines) - 3}")
                return columns
        print(f"Hourly fetch failed: HTTP {response.status_code}")
        return None
    except Exception as e:
        print(f"Hourly fetch error: {e}")
        return None


def main() -> None:
    """Run the investigation."""
    print("NSRDB 5-Minute Resolution Investigation")
    print(f"Site: Phoenix, AZ ({LAT}, {LON})")
    print(f"Year: {YEAR}")
    print(f"API Key: {API_KEY[:8]}...{API_KEY[-4:]}")
    print(f"Email: {EMAIL}")

    # Fetch 5-min data
    result_5min = fetch_5min_data()

    if not result_5min["success"]:
        print(f"\nFAILED — stopping. Error: {result_5min['error']}")
        write_results(result_5min, hourly_columns=None)
        return

    print(f"\n*** SUCCESS ***")

    # Also fetch hourly columns for comparison
    time.sleep(1)  # respect rate limit
    hourly_columns = fetch_hourly_columns()

    write_results(result_5min, hourly_columns)
    print("\nDone.")


if __name__ == "__main__":
    main()
