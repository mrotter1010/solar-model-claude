"""Diagnostic: probe NSRDB TMY endpoints.

Attempt 1: GOES Aggregated v4.0.0 (same endpoint as nsrdb_client.py) — FAILED,
    returns 400; requires single year 1998-2024, no TMY support.

Attempt 2: PSM3 TMY endpoint (nsrdb-GOES-tmy-v4-0-0).

Usage:
    python research/m12_benchmarking/nsrdb_tmy_probe.py
"""

import requests

ENDPOINTS = {
    "GOES_v4_TMY": (
        "https://developer.nrel.gov/api/nsrdb/v2/solar/"
        "nsrdb-GOES-tmy-v4-0-0-download.csv"
    ),
    "PSM3_TMY": (
        "https://developer.nrel.gov/api/nsrdb/v2/solar/"
        "psm3-tmy-download.csv"
    ),
}

API_KEY = "y1zAp5Hghami0SWXdi0xhc6kcvfWZhpliZoApVzB"
EMAIL = "rotter.mich@gmail.com"

# Phoenix, AZ
LAT, LON = 33.45, -111.95


def probe_endpoint(name: str, url: str) -> bool:
    wkt = f"POINT({LON} {LAT})"

    params = {
        "api_key": API_KEY,
        "email": EMAIL,
        "wkt": wkt,
        "names": "tmy",
        "attributes": "ghi,dni,dhi,air_temperature,wind_speed",
        "interval": "60",
        "leap_day": "true",
        "utc": "false",
    }

    print(f"=== Probing {name} ===")
    print(f"URL: {url}")
    print(f"names={params['names']}")
    print()

    resp = requests.get(url, params=params, timeout=120)

    print(f"HTTP Status: {resp.status_code}")
    print(f"Response size: {len(resp.text)} chars")
    print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
    print()

    lines = resp.text.splitlines()
    preview_count = min(10, len(lines))
    print(f"First {preview_count} lines of response:")
    print("-" * 60)
    for line in lines[:preview_count]:
        print(line[:200])
    print("-" * 60)

    if resp.status_code != 200:
        print(f"\nERROR: Non-200 status. Full response (first 500 chars):")
        print(resp.text[:500])

    print()
    return resp.status_code == 200


def probe_tmy() -> None:
    for name, url in ENDPOINTS.items():
        success = probe_endpoint(name, url)
        if success:
            print(f"SUCCESS: {name} returned data.\n")
        else:
            print(f"FAILED: {name} did not return data.\n")


if __name__ == "__main__":
    probe_tmy()
