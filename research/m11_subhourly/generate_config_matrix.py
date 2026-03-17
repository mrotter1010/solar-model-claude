"""Generate M11 config matrix: 1,600 PySAM runs across 20 stations.

Cross-product: 20 stations × 5 DC/AC × 4 GCR × 2 racking × 2 resolutions = 1,600 rows.
Fixed equipment parameters pulled from M8 config matrix.
"""

import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
M8_DIR = SCRIPT_DIR.parent / "m8_subhourly"
M9_DIR = SCRIPT_DIR.parent / "m9_bias_correction"
WEATHER_DIR = SCRIPT_DIR / "weather_files"

sys.path.insert(0, str(M9_DIR))
from stations import STATIONS  # noqa: E402

# ---------------------------------------------------------------------------
# M11 sweep parameters
# ---------------------------------------------------------------------------
DC_AC_RATIOS = [1.2, 1.3, 1.4, 1.5, 1.6]
GCR_VALUES = [0.3, 0.4, 0.5, 0.6]
RACKING_TYPES = ["tracker", "fixed"]
RESOLUTIONS = ["1min", "60min"]

# ---------------------------------------------------------------------------
# Step 1: Read M8 fixed parameters
# ---------------------------------------------------------------------------
print("Step 1: Reading M8 config matrix...")
m8_df = pd.read_csv(M8_DIR / "config_matrix.csv")
m8_df.columns = m8_df.columns.str.strip()
print(f"  M8 matrix: {len(m8_df)} rows × {len(m8_df.columns)} columns")
print(f"  M8 columns: {list(m8_df.columns)}")

# Extract fixed parameters from M8 (constant across all rows)
FIXED = {
    "AC Installed (MW)": m8_df["AC Installed (MW)"].iloc[0],
    "AC POI (MW)": m8_df["AC POI (MW)"].iloc[0],
    "Azimuth": m8_df["Azimuth"].iloc[0],
    "Module Orientation": m8_df["Module Orientation"].iloc[0],
    "Number of Modules": m8_df["Number of Modules"].iloc[0],
    "Ground Clearance Height (m)": m8_df["Ground Clearance Height (m)"].iloc[0],
    "Panel Model": m8_df["Panel Model"].iloc[0],
    "Bifacial": m8_df["Bifacial"].iloc[0],
    "Inverter Model": m8_df["Inverter Model"].iloc[0],
    "Shading (%)": m8_df["Shading (%)"].iloc[0],
    "DC Wiring Loss (%)": m8_df["DC Wiring Loss (%)"].iloc[0],
    "AC Wiring Loss (%)": m8_df["AC Wiring Loss (%)"].iloc[0],
    "Transformer Losses (%)": m8_df["Transformer Losses (%)"].iloc[0],
    "Degradation (%)": m8_df["Degradation (%)"].iloc[0],
    "Availability (%)": m8_df["Availability (%)"].iloc[0],
    "Module Mismatch (%)": m8_df["Module Mismatch (%)"].iloc[0],
    "LID(%)": m8_df["LID(%)"].iloc[0],
}

# Tilt per racking type (from M8)
m8_tracker = m8_df[m8_df["Racking"] == "tracker"]["Tilt"].iloc[0]
m8_fixed = m8_df[m8_df["Racking"] == "fixed"]["Tilt"].iloc[0]
TILT_MAP = {"tracker": m8_tracker, "fixed": m8_fixed}

print(f"\n  Fixed parameters from M8:")
for k, v in FIXED.items():
    print(f"    {k}: {v}")
print(f"    Tilt (tracker): {TILT_MAP['tracker']}")
print(f"    Tilt (fixed): {TILT_MAP['fixed']}")

# ---------------------------------------------------------------------------
# Step 2: Generate M11 rows
# ---------------------------------------------------------------------------
print(f"\nStep 2: Generating M11 config matrix...")
print(f"  Stations: {len(STATIONS)}")
print(f"  DC/AC ratios: {DC_AC_RATIOS}")
print(f"  GCR values: {GCR_VALUES}")
print(f"  Racking: {RACKING_TYPES}")
print(f"  Resolutions: {RESOLUTIONS}")
print(f"  Expected rows: {len(STATIONS)} × {len(DC_AC_RATIOS)} × {len(GCR_VALUES)} × {len(RACKING_TYPES)} × {len(RESOLUTIONS)} = {len(STATIONS) * len(DC_AC_RATIOS) * len(GCR_VALUES) * len(RACKING_TYPES) * len(RESOLUTIONS)}")

ac_size = float(FIXED["AC Installed (MW)"])
rows: list[dict] = []

for station in STATIONS:
    name = station["name"]
    state = station["state"]
    lat = station["latitude"]
    lon = station["longitude"]
    prefix = name.replace(" ", "") + "_" + state  # e.g., "Bondville_IL"

    for dcac in DC_AC_RATIOS:
        dc_size = round(ac_size * dcac, 2)
        for gcr in GCR_VALUES:
            for racking in RACKING_TYPES:
                tilt = TILT_MAP[racking]
                for resolution in RESOLUTIONS:
                    run_name = f"{prefix}_{dcac:.2f}_{gcr:.2f}_{racking}_{resolution}"
                    weather_file = WEATHER_DIR / f"{prefix}_2020_{resolution}.csv"

                    row = {
                        "Run Name": run_name,
                        "Site Name": prefix,
                        "Customer": "Subhourly_Research_Refinement",
                        "Latitude": lat,
                        "Longitude": lon,
                        "BESS Dispatch Required": "",
                        "BESS Optimization Required": "",
                        "DC Size (MW)": dc_size,
                        "AC Installed (MW)": FIXED["AC Installed (MW)"],
                        "AC POI (MW)": FIXED["AC POI (MW)"],
                        "Racking": racking,
                        "Tilt": tilt,
                        "Azimuth": FIXED["Azimuth"],
                        "Module Orientation": FIXED["Module Orientation"],
                        "Number of Modules": FIXED["Number of Modules"],
                        "Ground Clearance Height (m)": FIXED["Ground Clearance Height (m)"],
                        "Panel Model": FIXED["Panel Model"],
                        "Bifacial": FIXED["Bifacial"],
                        "Inverter Model": FIXED["Inverter Model"],
                        "GCR": gcr,
                        "Shading (%)": FIXED["Shading (%)"],
                        "DC Wiring Loss (%)": FIXED["DC Wiring Loss (%)"],
                        "AC Wiring Loss (%)": FIXED["AC Wiring Loss (%)"],
                        "Transformer Losses (%)": FIXED["Transformer Losses (%)"],
                        "Degradation (%)": FIXED["Degradation (%)"],
                        "Availability (%)": FIXED["Availability (%)"],
                        "Module Mismatch (%)": FIXED["Module Mismatch (%)"],
                        "LID(%)": FIXED["LID(%)"],
                        "Report": "",
                        "Resource File Path": str(weather_file),
                        "Ground Truth Data File": "",
                    }
                    rows.append(row)

df = pd.DataFrame(rows)
print(f"  Generated: {len(df)} rows × {len(df.columns)} columns")

# ---------------------------------------------------------------------------
# Step 3: Validate
# ---------------------------------------------------------------------------
print(f"\nStep 3: Validation...")
errors = []

# Check row count
if len(df) != 1600:
    errors.append(f"Row count: {len(df)} != 1600")
else:
    print(f"  [PASS] Row count: {len(df)}")

# Check unique run names
n_unique = df["Run Name"].nunique()
if n_unique != len(df):
    dups = df[df["Run Name"].duplicated(keep=False)]["Run Name"].unique()
    errors.append(f"Duplicate run names: {len(df) - n_unique} duplicates. Examples: {dups[:3]}")
else:
    print(f"  [PASS] Run names: all {n_unique} unique")

# Check all weather files exist
missing_files = []
for path_str in df["Resource File Path"].unique():
    if not Path(path_str).exists():
        missing_files.append(path_str)
if missing_files:
    errors.append(f"Missing weather files: {len(missing_files)}")
    for f in missing_files:
        print(f"    MISSING: {f}")
else:
    print(f"  [PASS] All {df['Resource File Path'].nunique()} unique weather files exist")

# Check DC/AC ratios
df["_dcac_check"] = (df["DC Size (MW)"] / df["AC Installed (MW)"]).round(2)
expected_ratios = set(DC_AC_RATIOS)
actual_ratios = set(df["_dcac_check"].unique())
if actual_ratios != expected_ratios:
    errors.append(f"DC/AC ratios mismatch: expected {expected_ratios}, got {actual_ratios}")
else:
    print(f"  [PASS] DC/AC ratios: {sorted(actual_ratios)}")
df = df.drop(columns=["_dcac_check"])

# Check all 20 stations appear
stations_in_matrix = df["Site Name"].nunique()
if stations_in_matrix != 20:
    errors.append(f"Station count: {stations_in_matrix} != 20")
else:
    print(f"  [PASS] Stations: {stations_in_matrix} unique")

# Check balance: 80 rows per station (5 dcac × 4 gcr × 2 racking × 2 resolution)
station_counts = df["Site Name"].value_counts()
unbalanced = station_counts[station_counts != 80]
if len(unbalanced) > 0:
    errors.append(f"Unbalanced stations: {dict(unbalanced)}")
else:
    print(f"  [PASS] Balance: all stations have {station_counts.iloc[0]} rows")

# Check station × resolution counts
resolution_counts = df.groupby("Site Name")["Run Name"].apply(
    lambda x: x.str.contains("_1min").sum()
).rename("1min_count")
res60_counts = df.groupby("Site Name")["Run Name"].apply(
    lambda x: x.str.contains("_60min").sum()
).rename("60min_count")
if (resolution_counts != 40).any() or (res60_counts != 40).any():
    errors.append("Resolution imbalance detected")
else:
    print(f"  [PASS] Resolution balance: 40 × 1min + 40 × 60min per station")

if errors:
    print(f"\n  ERRORS ({len(errors)}):")
    for e in errors:
        print(f"    {e}")
    sys.exit(1)
else:
    print(f"\n  All validations passed!")

# ---------------------------------------------------------------------------
# Step 4: Save
# ---------------------------------------------------------------------------
output_path = SCRIPT_DIR / "config_matrix_m11.csv"
df.to_csv(output_path, index=False)
print(f"\nStep 4: Saved to {output_path}")
print(f"  {len(df)} rows × {len(df.columns)} columns")

# Print sample rows
print(f"\n--- First 2 rows ---")
print(df.head(2).to_string(index=False))
print(f"\n--- Last 2 rows ---")
print(df.tail(2).to_string(index=False))

# Summary stats
print(f"\n--- Summary ---")
print(f"  Unique stations: {df['Site Name'].nunique()}")
print(f"  Unique configs (excl resolution): {len(df) // 2}")
print(f"  DC/AC ratios: {sorted(df['DC Size (MW)'].unique() / ac_size)}")
print(f"  GCR values: {sorted(df['GCR'].unique())}")
print(f"  Racking types: {sorted(df['Racking'].unique())}")
print(f"  Weather files referenced: {df['Resource File Path'].nunique()}")
