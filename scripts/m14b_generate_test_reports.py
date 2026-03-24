"""Generate M14b test PDF reports for visual review.

Creates 3 PDF reports with synthetic data to verify BESS report section
rendering for M14b features (NEM export, ITC solar-only, baseline).

Usage:
    python -m scripts.m14b_generate_test_reports
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.reporting.report_generator import generate_report  # noqa: E402

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "test_results" / "m14b_report_review"


# ---------------------------------------------------------------------------
# Synthetic data builders
# ---------------------------------------------------------------------------

def _make_site_config() -> dict:
    """Build a minimal site config dict matching extract_site_summary needs."""
    return {
        "site_name": "Desert Sun Solar",
        "customer": "Southwest Power Co.",
        "latitude": 33.45,
        "longitude": -111.95,
        "state": "AZ",
        "dc_size_mw": 10.0,
        "ac_installed_mw": 8.0,
        "racking": "Tracker",
        "tilt": 52.0,
        "azimuth": 180.0,
        "gcr": 0.35,
        "panel_model": "CSI Solar CS7N-665TB-AG",
        "number_of_modules": 14925,
        "bifacial": True,
        "inverter_model": "Sungrow SG250HX-US",
        "data_source": "nsrdb",
        "resource_file_path": None,
    }


def _make_loss_data() -> dict:
    """Build synthetic loss_data matching PySAM SimulationResult structure."""
    annual_dc_nominal = 22_000_000  # 22 GWh nominal
    annual_dc_gross = 20_500_000
    annual_dc_net = 19_800_000
    annual_ac_gross = 18_500_000
    annual_energy = 17_800_000  # ~17.8 GWh net

    monthly_energy = [
        1_100_000, 1_250_000, 1_500_000, 1_650_000, 1_750_000, 1_800_000,
        1_850_000, 1_750_000, 1_600_000, 1_400_000, 1_150_000, 1_000_000,
    ]

    return {
        "annual_dc_nominal": annual_dc_nominal,
        "annual_dc_gross": annual_dc_gross,
        "annual_dc_net": annual_dc_net,
        "annual_ac_gross": annual_ac_gross,
        "annual_energy": annual_energy,
        "monthly_energy": monthly_energy,
        "capacity_factor": 25.4,
        "kwh_per_kw": 1780.0,
        "performance_ratio": 0.82,
        "annual_ghi_kwh_m2": 2150.0,
        "avg_daytime_ghi_wm2": 580.0,
        "bifacial": True,
        "annual_dc_snow_loss_percent": 0.0,
        "annual_poa_shading_loss_percent": 2.1,
        "annual_poa_soiling_loss_percent": 1.5,
        "annual_poa_cover_loss_percent": 2.8,
        "annual_dc_module_loss_percent": 1.2,
        "annual_poa_rear_gain_percent": 3.5,
        "annual_dc_mismatch_loss_percent": 1.0,
        "annual_bifacial_electrical_mismatch_percent": 0.5,
        "annual_dc_diodes_loss_percent": 0.5,
        "annual_dc_wiring_loss_percent": 2.0,
        "annual_dc_tracking_loss_percent": 0.0,
        "annual_dc_nameplate_loss_percent": 0.5,
        "annual_ac_inv_clip_loss_percent": 3.5,
        "annual_ac_inv_eff_loss_percent": 3.0,
        "annual_ac_wiring_loss_percent": 1.0,
        "annual_xfmr_loss_percent": 1.0,
        "annual_ac_perf_adj_loss_percent": 1.5,
    }


def _make_solar_profile() -> list[float]:
    """Build a realistic 8760-hour solar production profile (kW).

    Peak ~8000 kW midday in summer, zero at night.
    """
    profile = []
    for hour_of_year in range(8760):
        day_of_year = hour_of_year // 24
        hour_of_day = hour_of_year % 24

        # Seasonal factor: peaks in June (day ~172)
        seasonal = 0.85 + 0.15 * math.sin(
            2 * math.pi * (day_of_year - 80) / 365
        )

        # Solar curve: bell curve centered at hour 12
        if 6 <= hour_of_day <= 18:
            solar_frac = math.cos(
                math.pi * (hour_of_day - 12) / 13
            ) ** 2
        else:
            solar_frac = 0.0

        # Peak kW output
        kw = 8000 * seasonal * solar_frac
        profile.append(max(0.0, kw))

    return profile


def _make_load_profile() -> list[float]:
    """Build a synthetic 8760-hour load profile (kW).

    Commercial-style: 500 kW baseload, peaks to 1200 kW in afternoon.
    """
    profile = []
    for hour_of_year in range(8760):
        hour_of_day = hour_of_year % 24

        if 8 <= hour_of_day <= 18:
            # Business hours: ramp up to afternoon peak
            load = 500 + 700 * math.sin(
                math.pi * (hour_of_day - 8) / 10
            )
        elif 18 < hour_of_day <= 22:
            load = 600  # evening
        else:
            load = 400  # overnight baseload

        profile.append(load)

    return profile


def _make_heatmap_data(
    solar: list[float],
    load: list[float],
    has_export: bool = False,
    solar_only_charge: bool = False,
) -> list[list[float]]:
    """Build a 12x24 heatmap matrix from synthetic dispatch logic.

    Positive = discharge, negative = charge.
    """
    days_per_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    heatmap = [[0.0] * 24 for _ in range(12)]
    hour_idx = 0

    for month in range(12):
        for day in range(days_per_month[month]):
            for h in range(24):
                s = solar[hour_idx]
                l = load[hour_idx]  # noqa: E741
                excess = s - l

                if excess > 0:
                    # Solar excess: charge or export
                    charge = min(excess, 1600)  # max charge rate
                    if has_export:
                        # Partial charge, rest exports
                        charge = min(charge, 800)
                    heatmap[month][h] += -charge / days_per_month[month]
                elif excess < 0 and not solar_only_charge:
                    # Deficit: discharge
                    discharge = min(-excess, 1600)
                    heatmap[month][h] += discharge / days_per_month[month]
                elif excess < 0 and solar_only_charge:
                    # Solar-only: discharge stored solar
                    if h >= 16:
                        discharge = min(-excess, 1200)
                        heatmap[month][h] += discharge / days_per_month[month]

                hour_idx += 1

    return heatmap


def _make_dispatch_profile_24h(
    solar: list[float],
    load: list[float],
    month_idx: int,
    has_export: bool = False,
    solar_only_charge: bool = False,
) -> tuple[list[float], list[float], list[float], list[float]]:
    """Compute 24-hour average dispatch profile for a given month.

    Returns (avg_load, avg_solar, avg_battery, avg_export) each 24 elements.
    """
    days_per_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    start_hour = sum(d * 24 for d in days_per_month[:month_idx])
    n_days = days_per_month[month_idx]

    avg_load = [0.0] * 24
    avg_solar = [0.0] * 24
    avg_battery = [0.0] * 24
    avg_export = [0.0] * 24

    for day in range(n_days):
        for h in range(24):
            idx = start_hour + day * 24 + h
            s = solar[idx]
            l = load[idx]  # noqa: E741
            avg_load[h] += l / n_days
            avg_solar[h] += s / n_days

            excess = s - l
            if excess > 0:
                charge = min(excess, 1600)
                if has_export:
                    charge = min(charge, 800)
                    export = excess - charge
                    avg_export[h] += export / n_days
                avg_battery[h] += -charge / n_days  # negative = charge
            elif excess < 0 and not solar_only_charge:
                discharge = min(-excess, 1600)
                avg_battery[h] += discharge / n_days
            elif excess < 0 and solar_only_charge:
                if h >= 16:
                    discharge = min(-excess, 1200)
                    avg_battery[h] += discharge / n_days

    return avg_load, avg_solar, avg_battery, avg_export


def _make_bess_dispatch_dict(
    solar: list[float],
    load: list[float],
    scenario: str,
) -> dict:
    """Build the full bess_dispatch dict matching pipeline.py output.

    Args:
        solar: 8760-hour solar production (kW).
        load: 8760-hour load profile (kW).
        scenario: One of "nem_export", "itc_solar_only", "baseline".
    """
    has_export = scenario == "nem_export"
    solar_only = scenario == "itc_solar_only"

    # Charging source
    if solar_only:
        charging_source = "solar_only"
    else:
        charging_source = "any"

    # Metrics (simplified synthetic values)
    if scenario == "nem_export":
        annual_cycles = 185.0
        annual_throughput = 296_000.0
        total_curtailed = 0.0
        total_export_kwh = 850_000.0
        total_export_hours = 2800
        solar_only_annual_bill = 180_000.0
        solar_plus_bess_annual_bill = 152_000.0
        bess_incremental_savings = 28_000.0
        bess_demand_savings = 8_000.0
        bess_energy_savings = 20_000.0
        solar_only_export_kwh = 780_000.0
        solar_only_export_credits = 39_000.0
        solar_plus_bess_export_kwh = 850_000.0
        solar_plus_bess_export_credits = 42_500.0
    elif scenario == "itc_solar_only":
        annual_cycles = 240.0
        annual_throughput = 384_000.0
        total_curtailed = 320_000.0
        total_export_kwh = 0.0
        total_export_hours = 0
        solar_only_annual_bill = 220_000.0
        solar_plus_bess_annual_bill = 185_000.0
        bess_incremental_savings = 35_000.0
        bess_demand_savings = 0.0
        bess_energy_savings = 35_000.0
        solar_only_export_kwh = 0.0
        solar_only_export_credits = 0.0
        solar_plus_bess_export_kwh = 0.0
        solar_plus_bess_export_credits = 0.0
    else:  # baseline
        annual_cycles = 300.0
        annual_throughput = 480_000.0
        total_curtailed = 400_000.0
        total_export_kwh = 0.0
        total_export_hours = 0
        solar_only_annual_bill = 230_000.0
        solar_plus_bess_annual_bill = 190_000.0
        bess_incremental_savings = 40_000.0
        bess_demand_savings = 12_000.0
        bess_energy_savings = 28_000.0
        solar_only_export_kwh = 0.0
        solar_only_export_credits = 0.0
        solar_plus_bess_export_kwh = 0.0
        solar_plus_bess_export_credits = 0.0

    heatmap = _make_heatmap_data(solar, load, has_export, solar_only)

    # Use July (month 6) for dispatch profile — peak solar activity
    peak_month = 6
    avg_load, avg_solar, avg_battery, avg_export = _make_dispatch_profile_24h(
        solar, load, peak_month, has_export, solar_only,
    )

    return {
        "bess_power_mw": 1.6,
        "bess_duration_hr": 4.0,
        "bess_capacity_kwh": 6400.0,
        "bess_strategy": "global",
        "solar_only_annual_bill": solar_only_annual_bill,
        "solar_plus_bess_annual_bill": solar_plus_bess_annual_bill,
        "bess_incremental_savings": bess_incremental_savings,
        "bess_demand_savings": bess_demand_savings,
        "bess_energy_savings": bess_energy_savings,
        "annual_cycles": annual_cycles,
        "annual_throughput_kwh": annual_throughput,
        "average_daily_cycles": round(annual_cycles / 365, 2),
        "capacity_utilization_pct": round(annual_cycles / 365 * 100, 1),
        "total_curtailed_kwh": total_curtailed,
        "estimated_annual_degradation_pct": round(annual_cycles / 5000 * 100, 2),
        "total_export_kwh": total_export_kwh,
        "total_export_hours": total_export_hours,
        "charging_source": charging_source,
        "solar_only_export_kwh": solar_only_export_kwh,
        "solar_only_export_credits": solar_only_export_credits,
        "solar_plus_bess_export_kwh": solar_plus_bess_export_kwh,
        "solar_plus_bess_export_credits": solar_plus_bess_export_credits,
        "monthly_solver_status": ["Optimal"] * 12,
        "heatmap_data": heatmap,
        "dispatch_profile_month": "July",
        "dispatch_profile_load": avg_load,
        "dispatch_profile_solar": avg_solar,
        "dispatch_profile_battery": avg_battery,
        "dispatch_profile_export": avg_export,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Generate 3 scenario PDF reports for M14b visual review."""
    solar = _make_solar_profile()
    load = _make_load_profile()
    site_config = _make_site_config()
    loss_data = _make_loss_data()

    scenarios = [
        ("scenario_a_nem_export", "nem_export"),
        ("scenario_b_itc_solar_only", "itc_solar_only"),
        ("scenario_c_baseline", "baseline"),
    ]

    for filename, scenario_key in scenarios:
        print(f"\n{'='*60}")
        print(f"Generating: {filename}")
        print(f"{'='*60}")

        bess_dispatch = _make_bess_dispatch_dict(solar, load, scenario_key)

        # Customize site name per scenario for distinct PDF filenames
        cfg = dict(site_config)
        cfg["site_name"] = f"Desert Sun Solar - {scenario_key.replace('_', ' ').title()}"

        summary = {"bess_dispatch": bess_dispatch}

        output_dir = OUTPUT_DIR / filename
        output_dir.mkdir(parents=True, exist_ok=True)

        pdf_path = generate_report(
            site_config=cfg,
            loss_data=loss_data,
            output_dir=output_dir,
            summary=summary,
        )

        if pdf_path is not None:
            size_kb = pdf_path.stat().st_size / 1024
            print(f"  -> PDF generated: {pdf_path}")
            print(f"  -> Size: {size_kb:.1f} KB")
        else:
            print(f"  -> FAILED to generate PDF for {filename}")
            sys.exit(1)

    print(f"\n{'='*60}")
    print(f"All PDFs saved to: {OUTPUT_DIR}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
