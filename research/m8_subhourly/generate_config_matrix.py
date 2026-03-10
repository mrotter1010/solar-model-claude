"""Generate the full configuration matrix CSV for M8 subhourly training data.

Crosses 45 sites x 9 DC/AC ratios x 4 GCRs x 2 racking types = 3,240 rows.
Output format matches the pipeline's 30-column input CSV.
"""

import csv
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SITE_CSV = Path(__file__).parent / "research_sites.csv"
OUTPUT_CSV = Path(__file__).parent / "config_matrix.csv"

# ---------------------------------------------------------------------------
# Matrix dimensions
# ---------------------------------------------------------------------------
DC_AC_RATIOS = [1.20, 1.25, 1.30, 1.35, 1.40, 1.45, 1.50, 1.55, 1.60]
GCRS = [0.30, 0.40, 0.50, 0.60]
RACKING_TYPES = ["tracker", "fixed"]

# ---------------------------------------------------------------------------
# Fixed parameters
# ---------------------------------------------------------------------------
AC_SIZE_MW = 20
CUSTOMER = "Subhourly_Research"
AZIMUTH = 180
MODULE_ORIENTATION = "portrait"
NUMBER_OF_MODULES = 2  # modules along row width (subarray1_nmodx), portrait
GROUND_CLEARANCE_M = 1.8
PANEL_MODEL = "SunPower SPR-310-WHT-U"
BIFACIAL = "TRUE"
INVERTER_MODEL = "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]"
SHADING_PCT = 1
DC_WIRING_LOSS_PCT = 1.5
AC_WIRING_LOSS_PCT = 1.5
TRANSFORMER_LOSS_PCT = 0
DEGRADATION_PCT = 0.3
AVAILABILITY_PCT = 3
MODULE_MISMATCH_PCT = 1.5
LID_PCT = 1

# Tilt by racking type
TILT_MAP = {"tracker": 60, "fixed": 25}

# ---------------------------------------------------------------------------
# CSV column headers — must match pipeline input format exactly
# (including trailing spaces on certain columns)
# ---------------------------------------------------------------------------
HEADERS = [
    "Run Name",
    "Site Name",
    "Customer",
    "Latitude",
    "Longitude",
    "BESS Dispatch Required",
    "BESS Optimization Required",
    "DC Size (MW)",
    "AC Installed (MW)",
    "AC POI (MW) ",
    "Racking",
    "Tilt",
    "Azimuth",
    "Module Orientation",
    "Number of Modules",
    "Ground Clearance Height (m)",
    "Panel Model",
    "Bifacial",
    "Inverter Model",
    "GCR",
    "Shading (%)",
    "DC Wiring Loss (%) ",
    "AC Wiring Loss (%) ",
    "Transformer Losses (%)",
    "Degradation (%)",
    "Availability (%)",
    "Module Mismatch (%)",
    "LID(%)",
    "Report",
    "Resource File Path",
]


def load_sites(path: Path) -> list[dict]:
    """Load sites from CSV."""
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


def generate_matrix(sites: list[dict]) -> list[dict]:
    """Generate all configurations."""
    rows = []
    for site in sites:
        for dcac in DC_AC_RATIOS:
            for gcr in GCRS:
                for racking in RACKING_TYPES:
                    dc_size_mw = round(AC_SIZE_MW * dcac, 2)
                    tilt = TILT_MAP[racking]
                    run_name = (
                        f"{site['site_name']}_{dcac:.2f}_{gcr:.2f}_{racking}"
                    )

                    rows.append({
                        "Run Name": run_name,
                        "Site Name": site["site_name"],
                        "Customer": CUSTOMER,
                        "Latitude": site["latitude"],
                        "Longitude": site["longitude"],
                        "BESS Dispatch Required": "",
                        "BESS Optimization Required": "",
                        "DC Size (MW)": dc_size_mw,
                        "AC Installed (MW)": AC_SIZE_MW,
                        "AC POI (MW) ": AC_SIZE_MW,
                        "Racking": racking,
                        "Tilt": tilt,
                        "Azimuth": AZIMUTH,
                        "Module Orientation": MODULE_ORIENTATION,
                        "Number of Modules": NUMBER_OF_MODULES,
                        "Ground Clearance Height (m)": GROUND_CLEARANCE_M,
                        "Panel Model": PANEL_MODEL,
                        "Bifacial": BIFACIAL,
                        "Inverter Model": INVERTER_MODEL,
                        "GCR": gcr,
                        "Shading (%)": SHADING_PCT,
                        "DC Wiring Loss (%) ": DC_WIRING_LOSS_PCT,
                        "AC Wiring Loss (%) ": AC_WIRING_LOSS_PCT,
                        "Transformer Losses (%)": TRANSFORMER_LOSS_PCT,
                        "Degradation (%)": DEGRADATION_PCT,
                        "Availability (%)": AVAILABILITY_PCT,
                        "Module Mismatch (%)": MODULE_MISMATCH_PCT,
                        "LID(%)": LID_PCT,
                        "Report": "",
                        "Resource File Path": "",
                    })
    return rows


def main() -> None:
    """Generate and save configuration matrix."""
    sites = load_sites(SITE_CSV)
    print(f"Loaded {len(sites)} sites")

    rows = generate_matrix(sites)
    total = len(rows)
    expected = len(sites) * len(DC_AC_RATIOS) * len(GCRS) * len(RACKING_TYPES)

    print(f"Generated {total} configurations (expected {expected})")
    assert total == expected, f"Row count mismatch: {total} != {expected}"

    # Write CSV
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved to {OUTPUT_CSV}")

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total rows: {total}")
    print(f"Column count: {len(HEADERS)}")
    print(f"Sites: {len(sites)}")
    print(f"DC/AC ratios: {DC_AC_RATIOS}")
    print(f"GCRs: {GCRS}")
    print(f"Racking: {RACKING_TYPES}")
    print(f"AC size: {AC_SIZE_MW} MW (all rows)")
    print(f"DC sizes: {sorted(set(round(AC_SIZE_MW * r, 2) for r in DC_AC_RATIOS))} MW")

    # Sample rows
    print(f"\nFirst 3 rows:")
    for row in rows[:3]:
        print(f"  {row['Run Name']} | DC={row['DC Size (MW)']}MW | "
              f"GCR={row['GCR']} | {row['Racking']} | tilt={row['Tilt']}")

    print(f"\nLast 3 rows:")
    for row in rows[-3:]:
        print(f"  {row['Run Name']} | DC={row['DC Size (MW)']}MW | "
              f"GCR={row['GCR']} | {row['Racking']} | tilt={row['Tilt']}")

    # Verify uniqueness of run names
    run_names = [r["Run Name"] for r in rows]
    unique = len(set(run_names))
    print(f"\nUnique run names: {unique}/{total} — {'OK' if unique == total else 'DUPLICATES!'}")


if __name__ == "__main__":
    main()
