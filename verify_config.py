"""Quick script to verify CSV field mapping with real test files."""

from pathlib import Path
from src.config import load_config

ROOT = Path(__file__).parent

for csv_name in [
    "/Users/michaelrotter/solar-model-claude/Energy Analytics Inputs Multi Row Test - Sheet1.csv",
    "/Users/michaelrotter/solar-model-claude/Energy Analytics Inputs Single Row Test - Sheet1.csv",
]:
    csv_path = ROOT / csv_name
    if not csv_path.exists():
        print(f"\nSKIPPED: {csv_name} not found in repo root")
        continue

    print(f"\n{'='*60}")
    print(f"FILE: {csv_name}")
    print(f"{'='*60}")

    configs = load_config(csv_path)

    for i, site in enumerate(configs):
        print(f"\n--- Site {i+1}: {site.site_name} ---")
        for field, value in site.model_dump().items():
            print(f"  {field}: {value}")
        print(f"  [property] system_capacity_kw: {site.system_capacity_kw}")
        print(f"  [property] tracking_mode: {site.tracking_mode}")
        print(f"  [property] rotation_limit: {site.rotation_limit}")
        print(f"  [property] location: {site.location}")
