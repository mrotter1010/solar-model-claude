"""Verify no two research sites fall within the same NSRDB 2km grid cell.

Uses a conservative 4km minimum-distance threshold (2x the NSRDB GOES CONUS
grid spacing) to guarantee no two sites map to the same grid cell.

Distance computed via the Haversine formula.
"""

import csv
import math
import time
from itertools import combinations
from pathlib import Path

SITE_CSV = Path(__file__).parent / "research_sites.csv"
RESULTS_FILE = Path(__file__).parent / "site_verification_results.txt"
MIN_DISTANCE_KM = 4.0
EARTH_RADIUS_KM = 6371.0


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two points."""
    lat1, lon1, lat2, lon2 = (math.radians(v) for v in (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def load_sites(path: Path) -> list[dict]:
    """Load sites from CSV."""
    sites = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sites.append({
                "site_id": int(row["site_id"]),
                "site_name": row["site_name"],
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
            })
    return sites


def check_distances(sites: list[dict], threshold_km: float) -> list[dict]:
    """Return list of site pairs closer than threshold_km."""
    too_close = []
    for a, b in combinations(sites, 2):
        dist = haversine(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
        if dist < threshold_km:
            too_close.append({
                "site_a": a,
                "site_b": b,
                "distance_km": round(dist, 3),
            })
    return too_close


def main() -> None:
    """Run verification."""
    sites = load_sites(SITE_CSV)
    total = len(sites)
    total_pairs = total * (total - 1) // 2

    print(f"Loaded {total} sites from {SITE_CSV.name}")
    print(f"Checking {total_pairs} pairs against {MIN_DISTANCE_KM}km threshold")

    too_close = check_distances(sites, MIN_DISTANCE_KM)

    # Find the closest pair overall for reference
    min_dist = float("inf")
    closest_pair = None
    for a, b in combinations(sites, 2):
        dist = haversine(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
        if dist < min_dist:
            min_dist = dist
            closest_pair = (a, b)

    passed = len(too_close) == 0

    # Print summary
    print(f"\n{'='*60}")
    print("RESULTS")
    print(f"{'='*60}")
    print(f"Total sites: {total}")
    print(f"Total pairs checked: {total_pairs}")
    print(f"Threshold: {MIN_DISTANCE_KM} km")
    print(f"Pairs too close: {len(too_close)}")

    if closest_pair:
        a, b = closest_pair
        print(f"Closest pair: {a['site_name']} <-> {b['site_name']} = {min_dist:.1f} km")

    if too_close:
        print("\nFAILING PAIRS:")
        for pair in too_close:
            a, b = pair["site_a"], pair["site_b"]
            print(f"  [{a['site_id']}] {a['site_name']} ({a['latitude']}, {a['longitude']})")
            print(f"  [{b['site_id']}] {b['site_name']} ({b['latitude']}, {b['longitude']})")
            print(f"  Distance: {pair['distance_km']} km")
            print()

    result = "PASS" if passed else "FAIL"
    print(f"\nResult: {result}")

    # Write results file
    with open(RESULTS_FILE, "w") as f:
        f.write("NSRDB Grid Cell Overlap Verification\n")
        f.write(f"{'='*60}\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Site file: {SITE_CSV.name}\n")
        f.write(f"Total sites: {total}\n")
        f.write(f"Total pairs checked: {total_pairs}\n")
        f.write(f"Minimum distance threshold: {MIN_DISTANCE_KM} km\n")
        f.write(f"(NSRDB GOES CONUS grid = 2km; 4km = 2x safety margin)\n\n")

        if closest_pair:
            a, b = closest_pair
            f.write(f"Closest pair: {a['site_name']} <-> {b['site_name']} = {min_dist:.1f} km\n\n")

        if too_close:
            f.write(f"FAILING PAIRS ({len(too_close)}):\n")
            f.write(f"{'-'*60}\n")
            for pair in too_close:
                a, b = pair["site_a"], pair["site_b"]
                f.write(f"  [{a['site_id']}] {a['site_name']} ({a['latitude']}, {a['longitude']})\n")
                f.write(f"  [{b['site_id']}] {b['site_name']} ({b['latitude']}, {b['longitude']})\n")
                f.write(f"  Distance: {pair['distance_km']} km\n\n")
        else:
            f.write("No pairs within threshold. All sites map to unique grid cells.\n\n")

        f.write(f"Result: {result}\n")

    print(f"\nResults written to {RESULTS_FILE.name}")


if __name__ == "__main__":
    main()
