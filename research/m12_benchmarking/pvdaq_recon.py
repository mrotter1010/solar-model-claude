"""PVDAQ S3 metadata reconnaissance for M12 benchmarking.

Parses downloaded system_metadata.json files from both Solar Data Prize
sites and regular PVDAQ sites to produce a benchmarking candidate summary.

Metadata JSONs should be pre-downloaded to research/m12_benchmarking/pvdaq_meta/.

Usage:
    PYTHONPATH=. python research/m12_benchmarking/pvdaq_recon.py
"""

import json
from pathlib import Path

import pandas as pd

META_DIR = Path("research/m12_benchmarking/pvdaq_meta")
SYSTEMS_CSV = Path("/tmp/pvdaq_systems.csv")

# Solar Data Prize system IDs
PRIZE_IDS = {2105, 2107, 9068, 7333, 9069}

# Regular PVDAQ systems >= 1 MW that we also downloaded metadata for
REGULAR_BIG_IDS = {14201, 14698, 1332, 1431, 14200}


def extract_system_info(meta: dict) -> dict:
    """Extract key fields from a PVDAQ system_metadata.json."""
    sys_info = meta.get("System", {})
    site_info = meta.get("Site", {})
    mount_info = meta.get("Mount", {})
    inverters = meta.get("Inverters", {})
    modules = meta.get("Modules", {})
    metrics = meta.get("Metrics", {})

    # System basics
    system_id = sys_info.get("system_id", "?")
    name = sys_info.get("public_name", "?")
    dc_kw = sys_info.get("power", "?")

    # Site location
    lat = site_info.get("latitude", "?")
    lon = site_info.get("longitude", "?")
    location = site_info.get("location", "?")
    elevation = site_info.get("elevation", "?")

    # Mount/racking — may have multiple mounts
    mount_entries = []
    for key, m in mount_info.items():
        tracking = m.get("tracking", "?")
        mount_type = m.get("type", "?")
        tilt = m.get("tilt", "?")
        azimuth = m.get("azimuth", "?")
        gcr = m.get("gcr", m.get("ground_coverage_ratio", "?"))
        mount_entries.append({
            "tracking": tracking,
            "type": mount_type,
            "tilt": tilt,
            "azimuth": azimuth,
            "gcr": gcr,
        })

    # Inverters
    inv_entries = []
    for key, inv in inverters.items():
        inv_entries.append({
            "manufacturer": inv.get("manufacturer", "?"),
            "model": inv.get("model", "?"),
            "quantity": inv.get("quantity", "?"),
            "paco": inv.get("paco", inv.get("ac_capacity", "?")),
        })

    # Modules
    mod_entries = []
    for key, mod in modules.items():
        mod_entries.append({
            "manufacturer": mod.get("manufacturer", "?"),
            "model": mod.get("model", "?"),
            "quantity": mod.get("quantity", "?"),
            "type": mod.get("type", "?"),
        })

    # Metrics/sensors — count and identify irradiance/temp sensors
    sensor_names = []
    has_irradiance = False
    has_temperature = False
    has_ac_power = False
    for key, m in metrics.items():
        common = m.get("common_name", "")
        sensor = m.get("sensor_name", "")
        sensor_names.append(common or sensor)
        lower = (common + " " + sensor).lower()
        if any(x in lower for x in ["ghi", "irradiance", "poa", "solar"]):
            has_irradiance = True
        if any(x in lower for x in ["temperature", "temp", "ambient"]):
            has_temperature = True
        if any(x in lower for x in ["ac_power", "ac power", "active_power"]):
            has_ac_power = True

    return {
        "system_id": system_id,
        "name": name,
        "dc_kw": dc_kw,
        "latitude": lat,
        "longitude": lon,
        "location": location,
        "elevation": elevation,
        "n_mounts": len(mount_entries),
        "mounts": mount_entries,
        "n_inverters": len(inv_entries),
        "inverters": inv_entries,
        "n_modules": len(mod_entries),
        "modules": mod_entries,
        "n_sensors": len(sensor_names),
        "has_irradiance": has_irradiance,
        "has_temperature": has_temperature,
        "has_ac_power": has_ac_power,
    }


def print_site_detail(info: dict, systems_row: pd.Series | None = None) -> None:
    """Print detailed info for a single site."""
    sid = info["system_id"]
    tag = " [PRIZE]" if sid in PRIZE_IDS else ""
    print(f"\n{'='*70}")
    print(f"System {sid}: {info['name']}{tag}")
    print(f"{'='*70}")

    print(f"  DC Capacity:     {info['dc_kw']} kW")
    print(f"  Location:        {info['location']}")
    print(f"  Lat/Lon:         {info['latitude']}, {info['longitude']}")
    print(f"  Elevation:       {info['elevation']} m")

    if systems_row is not None:
        print(f"  Date Range:      {systems_row.get('first_timestamp', '?')} → {systems_row.get('last_timestamp', '?')}")
        print(f"  Years of Data:   {systems_row.get('years', '?')}")
        print(f"  Records:         {systems_row.get('number_records', '?'):,.0f}" if pd.notna(systems_row.get('number_records')) else "  Records:         ?")
        print(f"  Dataset Size:    {systems_row.get('dataset_size_mb', '?'):,.1f} MB" if pd.notna(systems_row.get('dataset_size_mb')) else "  Dataset Size:    ?")
        print(f"  QA Status:       {systems_row.get('qa_status', '?')}")

    # Mount details
    print(f"  Mounts:          {info['n_mounts']}")
    for i, m in enumerate(info["mounts"][:5]):  # limit to first 5
        tracking_str = "tracker" if m["tracking"] in ("t", True, "True", "tracking") else "fixed" if m["tracking"] in ("f", False, "False", "fixed") else m["tracking"]
        print(f"    [{i}] tracking={tracking_str}, type={m['type']}, tilt={m['tilt']}, azimuth={m['azimuth']}, gcr={m['gcr']}")
    if info["n_mounts"] > 5:
        print(f"    ... and {info['n_mounts'] - 5} more mounts")

    # Module details
    print(f"  Modules:         {info['n_modules']} type(s)")
    for i, mod in enumerate(info["modules"][:3]):
        print(f"    [{i}] {mod['manufacturer']} / {mod['model']}, qty={mod['quantity']}, type={mod['type']}")
    if info["n_modules"] > 3:
        print(f"    ... and {info['n_modules'] - 3} more module types")

    # Inverter details
    print(f"  Inverters:       {info['n_inverters']} type(s)")
    for i, inv in enumerate(info["inverters"][:3]):
        print(f"    [{i}] {inv['manufacturer']} / {inv['model']}, qty={inv['quantity']}")
    if info["n_inverters"] > 3:
        print(f"    ... and {info['n_inverters'] - 3} more inverter types")

    # Sensors
    print(f"  Sensor Channels: {info['n_sensors']}")
    print(f"  Has Irradiance:  {info['has_irradiance']}")
    print(f"  Has Temperature: {info['has_temperature']}")
    print(f"  Has AC Power:    {info['has_ac_power']}")


def main() -> None:
    # Load systems CSV for date ranges and QA status
    systems_df = pd.read_csv(SYSTEMS_CSV)

    # Process all downloaded metadata files
    all_info = []
    for json_path in sorted(META_DIR.glob("*_system_metadata.json")):
        sid_str = json_path.stem.split("_")[0]
        try:
            sid = int(sid_str)
        except ValueError:
            continue

        with open(json_path) as f:
            meta = json.load(f)

        info = extract_system_info(meta)
        info["system_id"] = sid  # ensure integer

        # Find matching row in systems CSV
        row_match = systems_df[systems_df["system_id"] == sid]
        sys_row = row_match.iloc[0] if len(row_match) > 0 else None

        all_info.append((info, sys_row))

    # Print Prize sites first
    print("=" * 70)
    print("SOLAR DATA PRIZE SITES (curated for modeling validation)")
    print("=" * 70)
    for info, sys_row in all_info:
        if info["system_id"] in PRIZE_IDS:
            print_site_detail(info, sys_row)

    # Print regular >1 MW sites
    print("\n\n" + "=" * 70)
    print("REGULAR PVDAQ SITES >= 1 MW DC")
    print("=" * 70)
    for info, sys_row in all_info:
        if info["system_id"] in REGULAR_BIG_IDS:
            print_site_detail(info, sys_row)

    # Summary table
    print("\n\n" + "=" * 70)
    print("SUMMARY TABLE — ALL CANDIDATE SITES")
    print("=" * 70)
    print(f"{'ID':>6}  {'Name':<35} {'DC_kW':>8}  {'Track':>7}  {'Tilt':>5}  {'Az':>5}  {'GCR':>5}  "
          f"{'Irrad':>5}  {'Temp':>5}  {'AC':>5}  {'QA':>4}  {'Years':>5}  {'Prize':>5}")
    print("-" * 140)
    for info, sys_row in all_info:
        sid = info["system_id"]
        name = info["name"][:35]
        dc = info["dc_kw"]
        mount0 = info["mounts"][0] if info["mounts"] else {}
        tracking_raw = mount0.get("tracking", "?")
        tracking = "track" if tracking_raw in ("t", True, "True", "tracking") else "fixed" if tracking_raw in ("f", False, "False", "fixed") else str(tracking_raw)[:7]
        tilt = mount0.get("tilt", "?")
        az = mount0.get("azimuth", "?")
        gcr = mount0.get("gcr", "?")
        irrad = "Y" if info["has_irradiance"] else "N"
        temp = "Y" if info["has_temperature"] else "N"
        ac = "Y" if info["has_ac_power"] else "N"
        qa = sys_row.get("qa_status", "?") if sys_row is not None else "?"
        years = f"{sys_row.get('years', 0):.1f}" if sys_row is not None and pd.notna(sys_row.get("years")) else "?"
        prize = "YES" if sid in PRIZE_IDS else ""

        # Truncate/format fields
        dc_str = str(dc)[:8] if dc != "?" else "?"
        tilt_str = str(tilt)[:5] if tilt != "?" else "?"
        az_str = str(az)[:5] if az != "?" else "?"
        gcr_str = str(gcr)[:5] if gcr != "?" else "?"

        print(f"{sid:>6}  {name:<35} {dc_str:>8}  {tracking:>7}  {tilt_str:>5}  {az_str:>5}  {gcr_str:>5}  "
              f"{irrad:>5}  {temp:>5}  {ac:>5}  {qa:>4}  {years:>5}  {prize:>5}")

    # Benchmarking recommendations
    print("\n\n" + "=" * 70)
    print("BENCHMARKING RECOMMENDATIONS")
    print("=" * 70)
    print("""
Ranked candidates (by suitability for PySAM model validation):

1. 9068 - SR_CO (4.7 MW, tracker, CO) — PRIZE
   + Single-axis tracker, 7.7 years, QA=pass, 438 sensor channels
   + Has on-site irradiance + temperature
   + Utility-scale, good metadata completeness
   - Need to verify module/inverter match to CEC database

2. 2107 - Farm Solar Array (893 kW, fixed, CA) — PRIZE
   + Fixed ground-mount, 7.9 years, QA=pass, 125 channels
   + Has irradiance + temperature sensors
   + Tilt=25, azimuth=180 — standard config
   - Sub-MW but substantial size

3. 9069 - Simon Solar Farm (33 MW, fixed, GA) — PRIZE
   + Large utility-scale, 7.8 years, QA=pass, 5119 channels
   + Fixed ground-mount, tilt=20, azimuth=180
   + Southeast US climate diversity
   - Extremely large dataset (~40 GB) — need selective download
   - Complex multi-mount system

4. 7333 - Shine On Solar Facility (205 MW, tracker, CA) — PRIZE
   + Largest system, single-axis tracker, 7 years
   + Central Valley CA climate
   - QA=fail — data quality concerns
   - Massive dataset (~930 GB at 10-sec resolution)
   - 5-min version available (7333_5_min_OEDI)

5. 14201 - SAS_SF2 (1.3 MW, tracker, NC)
   + Utility-scale tracker, QA=pass
   + Eastern US (Cary, NC)
   - Only 2.5 years, 19 channels — may lack irradiance sensors

6. 2105 - Maui Ocean Center (110 kW, rooftop, HI) — PRIZE
   + 71 sensor channels, 4.9 years, QA=pass
   + Unique tropical island climate
   - Small rooftop system, tracking/tilt unknown from CSV
   - Not representative of utility-scale

7. 1332 - NREL Parking Garage (1.15 MW, fixed, CO)
   + Long record (11 years), QA=pass, 26 channels
   + NREL site — likely well-maintained data
   - Rooftop parking garage — unusual configuration

Sites NOT recommended:
- 14698: No location data in systems CSV
- 1431: QA=fail
- 14200: Only 2.5 years, limited channels
""")


if __name__ == "__main__":
    main()
