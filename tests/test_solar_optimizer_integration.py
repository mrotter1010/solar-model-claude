"""Real-PySAM integration validation for optimize_solar() at Phoenix TMY.

Three integration tests exercise the full optimizer end-to-end against actual
NREL PySAM simulations using Phoenix TMY weather data, one per optimizer mode:

- test_optimize_solar_phoenix_production_mode: no economic inputs; returns
  max_production and max_yield winners.
- test_optimize_solar_phoenix_lcoe_mode: full economic inputs; adds lcoe
  winner (min $/MWh).
- test_optimize_solar_phoenix_npv_mode: economic + revenue inputs ($50/MWh
  flat PPA, 2% escalator); adds npv winner.

Each runs a 70-point tracker sweep (~35-60s). Gated behind
SOLAR_INTEGRATION=1 environment variable so default `pytest` invocations
skip them. To run:

    SOLAR_INTEGRATION=1 pytest tests/test_solar_optimizer_integration.py -v -s
"""

import json
import os
import time
from pathlib import Path

import pytest

from src.climate.weather_formatter import WeatherFormatter
from src.config.schema import SiteConfig
from src.lmp.models import LMPData
from src.optimization.bess_optimizer import optimize_solar_bess
from src.optimization.capacity import capacity_from_acreage
from src.optimization.defaults import (
    DEFAULT_MODULE_AREA_M2,
    DEFAULT_MODULE_NAME,
    DEFAULT_MODULE_POWER_W,
    DEFAULT_UTILIZATION_FACTOR,
)
from src.optimization.solar_optimizer import _run_single_pysam, optimize_solar
from src.pysam_integration.cec_database import CECDatabase
from src.pysam_integration.model_configurator import ModelConfigurator
from src.pysam_integration.simulator import PySAMSimulator
from src.rates.models import LoadProfile, RateSchedule, RateTier

# ---------------------------------------------------------------------------
# Opt-in gate: skip unless SOLAR_INTEGRATION=1
# ---------------------------------------------------------------------------

RUN_INTEGRATION = os.environ.get("SOLAR_INTEGRATION") == "1"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not RUN_INTEGRATION,
        reason=(
            "Real-PySAM integration test. "
            "Set SOLAR_INTEGRATION=1 to run (slow, ~3-8 minutes)."
        ),
    ),
]

# ---------------------------------------------------------------------------
# Phoenix TMY constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Raw NSRDB TMY file (GOES Aggregated v4.0.0) matching the lat/lon used in
# tests/fixtures/single_row_test.csv.
PHOENIX_LAT = 33.483
PHOENIX_LON = -112.073
RAW_TMY_CSV = (
    PROJECT_ROOT / "data" / "climate" / f"nsrdb_{PHOENIX_LAT}_{PHOENIX_LON}_tmy_20260320.csv"
)

# Cache the PySAM-formatted weather file under outputs/test_results/ so
# subsequent test runs don't have to reformat (cheap, but keeps artifacts
# inspectable).
CACHE_DIR = PROJECT_ROOT / "outputs" / "test_results" / "integration_cache"
PHOENIX_PYSAM_WEATHER = CACHE_DIR / "phoenix_tmy_pysam.csv"

# Test module/inverter: use the optimizer's default module (LONGi 551W) so
# that num_modules from capacity_from_acreage() matches the actual simulation.
# Use the SG250HX-US [800V] inverter since that's the only SG250HX-US variant
# in the CEC CSV (DEFAULT_INVERTER_NAME refers to a [600V] variant that
# isn't in the database).
TEST_MODULE = DEFAULT_MODULE_NAME
TEST_INVERTER = "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]"

# Economic inputs used across LCOE and NPV integration tests.
PHOENIX_ECONOMIC_KWARGS = {
    "solar_cost_per_kw_dc": 1200.0,
    "solar_opex_per_kw_dc_year": 20.0,
    "discount_rate_pct": 7.0,
    "project_lifetime_years": 25,
    "degradation_pct": 0.5,
    "itc_pct": 30.0,
}

# Revenue inputs for NPV mode.
PHOENIX_REVENUE_KWARGS = {
    "energy_price_per_kwh": 0.05,  # $50/MWh flat PPA
    "energy_cost_escalator_pct": 2.0,
}


def _prepare_phoenix_weather_file() -> Path:
    """Build a PySAM-formatted Phoenix TMY weather file (cached).

    Reads the raw NSRDB TMY CSV once, runs it through WeatherFormatter,
    and writes a PySAM-ready CSV under outputs/test_results/ for reuse.

    Returns:
        Path to the PySAM-formatted weather CSV.
    """
    if PHOENIX_PYSAM_WEATHER.exists():
        return PHOENIX_PYSAM_WEATHER

    assert RAW_TMY_CSV.exists(), (
        f"Raw NSRDB TMY file not found: {RAW_TMY_CSV}. "
        "Fetch Phoenix TMY before running this integration test."
    )

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    formatter = WeatherFormatter()
    raw_csv = RAW_TMY_CSV.read_text()
    df, metadata = formatter.format_for_pysam(
        nsrdb_csv=raw_csv, lat=PHOENIX_LAT, lon=PHOENIX_LON
    )
    formatter.save_to_csv(
        df,
        PHOENIX_PYSAM_WEATHER,
        lat=PHOENIX_LAT,
        lon=PHOENIX_LON,
        metadata=metadata,
    )
    return PHOENIX_PYSAM_WEATHER


def _make_phoenix_site_config(weather_file: Path) -> SiteConfig:
    """Build a Phoenix SiteConfig for the optimizer's sweep.

    gcr, dc_size_mw, ac_installed_mw, and racking are overridden per
    sweep point by optimize_solar(), so their values here are placeholders
    that must satisfy the Pydantic validators.
    """
    return SiteConfig(
        run_name="OptimizerIntegration",
        site_name="Phoenix_TMY_Optimizer",
        customer="IntegrationTest",
        latitude=PHOENIX_LAT,
        longitude=PHOENIX_LON,
        weather_file_path=weather_file,
        # Placeholder system size — overridden per sweep point.
        dc_size_mw=100.0,
        ac_installed_mw=75.0,
        ac_poi_mw=75.0,
        # System design
        racking="tracker",
        tilt=60.0,  # Tracker rotation limit (single_row_test.csv uses 0 = unrealistic)
        azimuth=180.0,
        module_orientation="portrait",
        number_of_modules=2,
        ground_clearance_height_m=1.5,
        # Equipment (matches optimizer defaults)
        panel_model=TEST_MODULE,
        bifacial=True,
        inverter_model=TEST_INVERTER,
        # Placeholder GCR — overridden per sweep point.
        gcr=0.35,
        # Losses (reasonable Phoenix-realistic values from fixture)
        shading_percent=2.0,
        dc_wiring_loss_percent=1.5,
        ac_wiring_loss_percent=0.5,
        transformer_losses_percent=1.0,
        degradation_percent=0.5,
        availability_percent=2.0,  # 2% unavailability => 98% uptime for PySAM
        module_mismatch_percent=2.0,
        lid_percent=1.5,
    )


def _print_point_row(index: int, total: int, point: dict) -> None:
    """Print a single per-progress-step summary row."""
    lcoe = point.get("lcoe_per_mwh")
    npv = point.get("npv")
    cf = point.get("capacity_factor_ac")
    failed = point.get("failed")
    marker = "FAIL" if failed else "OK  "
    lcoe_str = "--" if lcoe is None else f"${lcoe:.2f}/MWh"
    npv_str = "--" if npv is None else f"NPV=${npv:,.0f}"
    cf_str = "--" if cf is None else f"{cf:.2f}%"
    print(
        f"  [{index:2d}/{total}] {marker} gcr={point['gcr']:.2f} "
        f"dcac={point['dcac_ratio']:.2f} "
        f"lcoe={lcoe_str} {npv_str} cf_ac={cf_str}",
        flush=True,
    )


# ---------------------------------------------------------------------------
# Integration test — LCOE mode (original test, preserved with economic inputs)
# ---------------------------------------------------------------------------


def test_optimize_solar_phoenix_lcoe_mode(test_results_dir: Path) -> None:
    """Run optimize_solar() in LCOE mode with real PySAM at Phoenix TMY.

    Validates that the full 70-point tracker sweep (7 GCRs × 10 DCACs)
    produces results consistent with Phoenix benchmark capacity factors
    (~25-35%) and industry LCOE ranges ($30-$80/MWh).
    """
    weather_file = _prepare_phoenix_weather_file()
    base_site = _make_phoenix_site_config(weather_file)

    buildable_acres = 100.0
    racking = "tracker"

    def _progress(index, total, point):
        _print_point_row(index, total, point)

    print(
        f"\n[LCOE MODE] Starting optimize_solar() sweep: "
        f"{buildable_acres} acres, racking={racking}"
    )
    t0 = time.perf_counter()
    result = optimize_solar(
        base_site_config=base_site,
        buildable_acres=buildable_acres,
        racking=racking,
        progress_callback=_progress,
        **PHOENIX_ECONOMIC_KWARGS,
    )
    elapsed = time.perf_counter() - t0
    print(f"\n[LCOE MODE] Sweep complete in {elapsed:.1f}s ({elapsed/60:.2f} min)")

    winners = result["winners"]
    sweep_results = result["sweep_results"]
    sweep_metadata = result["sweep_metadata"]

    # Mode check
    assert result["mode"] == "lcoe"
    assert set(winners.keys()) == {"max_production", "max_yield", "lcoe"}
    winner = winners["lcoe"]

    # --- Assertion 1: sweep completes with no failures ---
    assert sweep_metadata["total_combinations"] == 70, (
        f"Expected 70 sweep points (7 GCRs × 10 DCACs), "
        f"got {sweep_metadata['total_combinations']}"
    )
    assert sweep_metadata["failed_combinations"] == 0, (
        f"Expected 0 failed sweep points, got "
        f"{sweep_metadata['failed_combinations']}: "
        f"{[p for p in sweep_results if p['failed']]}"
    )

    # --- Assertion 2: winner is a valid dict with all fields populated ---
    required_keys = [
        "gcr",
        "dcac_ratio",
        "mw_dc",
        "mw_ac",
        "num_modules",
        "annual_energy_kwh",
        "annual_energy_mwh",
        "capacity_factor_ac",
        "clipping_correction_pct",
        "capex_total",
        "opex_per_year",
        "net_capex",
        "lcoe_per_mwh",
        "lcoe_per_kwh",
    ]
    for key in required_keys:
        assert key in winner, f"Winner missing key: {key}"
        assert winner[key] is not None, f"Winner has None for key: {key}"
    assert winner["failed"] is False
    assert winner["error"] is None

    # --- Assertion 3: winner GCR/DCAC within sweep bounds ---
    assert 0.30 <= winner["gcr"] <= 0.60, (
        f"Winner gcr={winner['gcr']} outside tracker range [0.30, 0.60]"
    )
    assert 1.15 <= winner["dcac_ratio"] <= 1.60, (
        f"Winner dcac={winner['dcac_ratio']} outside [1.15, 1.60]"
    )

    # --- Assertion 4: winner LCOE in sanity range for Phoenix tracker ---
    assert 30.0 <= winner["lcoe_per_mwh"] <= 80.0, (
        f"Winner LCOE ${winner['lcoe_per_mwh']:.2f}/MWh outside sanity "
        f"range $30-$80/MWh. Check capex/opex defaults and Phoenix CF."
    )

    # --- Assertion 5: winner AC capacity factor in Phoenix tracker range ---
    # PySAM's outputs.capacity_factor_ac is already in percent (e.g. 32.12
    # for 32.12%). The optimizer passes this value through unchanged.
    assert 25.0 <= winner["capacity_factor_ac"] <= 35.0, (
        f"Winner AC capacity factor {winner['capacity_factor_ac']:.4f}% "
        f"outside Phoenix tracker range [25.0%, 35.0%]"
    )

    # --- Assertion 6: clipping correction is non-negative and reasonable ---
    assert 0.0 <= winner["clipping_correction_pct"] <= 5.0, (
        f"Winner clipping_correction_pct={winner['clipping_correction_pct']} "
        f"outside [0.0, 5.0]"
    )

    # --- Assertion 7: winner mw_dc matches capacity_from_acreage ---
    cap = capacity_from_acreage(
        buildable_acres=buildable_acres,
        gcr=winner["gcr"],
        module_area_m2=DEFAULT_MODULE_AREA_M2,
        module_power_w=DEFAULT_MODULE_POWER_W,
        utilization_factor=DEFAULT_UTILIZATION_FACTOR,
    )
    assert winner["mw_dc"] == pytest.approx(cap["mw_dc"], rel=1e-6), (
        f"Winner mw_dc={winner['mw_dc']} does not match "
        f"capacity_from_acreage() mw_dc={cap['mw_dc']}"
    )
    assert winner["num_modules"] == cap["num_modules"], (
        f"Winner num_modules={winner['num_modules']} does not match "
        f"capacity_from_acreage() num_modules={cap['num_modules']}"
    )

    # --- Assertion 8: mw_ac == mw_dc / dcac_ratio (within rounding) ---
    expected_mw_ac = winner["mw_dc"] / winner["dcac_ratio"]
    assert winner["mw_ac"] == pytest.approx(expected_mw_ac, abs=0.001), (
        f"Winner mw_ac={winner['mw_ac']} does not match "
        f"mw_dc/dcac={expected_mw_ac:.6f}"
    )

    # ---- Cross-validation: rerun PySAM at the winner's (gcr, dcac) ----
    print(
        f"\nCross-validating: standalone PySAM rerun at "
        f"gcr={winner['gcr']}, dcac={winner['dcac_ratio']}..."
    )
    cross_site = base_site.model_copy(
        update={
            "gcr": winner["gcr"],
            "dc_size_mw": winner["mw_dc"],
            "ac_installed_mw": winner["mw_ac"],
            "racking": racking,
        }
    )
    cross_configurator = ModelConfigurator(cec_database=CECDatabase())
    cross_simulator = PySAMSimulator()
    cross_out = _run_single_pysam(
        site_config=cross_site,
        configurator=cross_configurator,
        simulator=cross_simulator,
        apply_clipping_correction=True,
    )
    cross_energy = cross_out["annual_energy_kwh"]
    delta_pct = abs(cross_energy - winner["annual_energy_kwh"]) / winner[
        "annual_energy_kwh"
    ] * 100
    print(
        f"  Winner annual_energy_kwh:   {winner['annual_energy_kwh']:,.0f}"
    )
    print(f"  Standalone annual_energy_kwh: {cross_energy:,.0f}")
    print(f"  Delta: {delta_pct:.4f}%  (tolerance: 0.10%)")
    assert delta_pct < 0.1, (
        f"Standalone PySAM rerun energy {cross_energy} differs from "
        f"winner energy {winner['annual_energy_kwh']} by {delta_pct:.4f}% "
        f"(> 0.1% tolerance)"
    )

    # ---- Industry sanity prints (do NOT assert) ----
    print("\n=== Industry sanity check (manual review) ===")
    acres_per_mw = cap["acres_per_mw"]
    print(
        f"Winner acres_per_mw: {acres_per_mw:.2f}  "
        f"(expected 3-6 for modern tracker)"
    )

    # LCOE surface slice at 3 corners of the sweep
    def _find(gcr: float, dcac: float) -> dict | None:
        for p in sweep_results:
            if abs(p["gcr"] - gcr) < 1e-6 and abs(p["dcac_ratio"] - dcac) < 1e-6:
                return p
        return None

    slice_points = [(0.30, 1.15), (0.45, 1.35), (0.60, 1.60)]
    print("\nLCOE surface slice (expected to be smooth, no cliffs):")
    for g, d in slice_points:
        p = _find(g, d)
        if p is not None:
            print(
                f"  gcr={g:.2f} dcac={d:.2f} -> "
                f"LCOE=${p['lcoe_per_mwh']:.2f}/MWh  "
                f"CF_AC={p['capacity_factor_ac']:.2f}%  "
                f"clipping={p['clipping_correction_pct']:.3f}%"
            )
        else:
            print(f"  gcr={g:.2f} dcac={d:.2f} -> NOT FOUND")

    # Clipping correction monotonicity (at a mid GCR, low vs high DCAC)
    mid_gcr = 0.45
    low = _find(mid_gcr, 1.15)
    high = _find(mid_gcr, 1.60)
    print(
        f"\nClipping correction at gcr={mid_gcr}:  "
        f"DC/AC=1.15 -> {low['clipping_correction_pct']:.3f}%, "
        f"DC/AC=1.60 -> {high['clipping_correction_pct']:.3f}%  "
        f"(expected monotonically non-decreasing with DC/AC)"
    )
    if high["clipping_correction_pct"] < low["clipping_correction_pct"]:
        print(
            "  NOTE: high-DC/AC clipping correction is LOWER than low-DC/AC. "
            "Flag for manual review — this is unusual but not a test failure."
        )

    # ---- Print all three winners side by side ----
    print("\n=== LCOE-mode winners ===")
    for name, w in winners.items():
        print(
            f"  {name:16s}: gcr={w['gcr']:.2f} dcac={w['dcac_ratio']:.2f} "
            f"mwh={w['annual_energy_mwh']:,.0f} "
            f"cf={w['capacity_factor_ac']:.2f}% "
            f"lcoe=${w['lcoe_per_mwh']:.2f}/MWh"
        )

    # ---- Write artifact for manual inspection ----
    artifact = {
        "mode": result["mode"],
        "sweep_metadata": sweep_metadata,
        "winners": winners,
        "cross_validation": {
            "winner_annual_energy_kwh": winner["annual_energy_kwh"],
            "standalone_annual_energy_kwh": cross_energy,
            "delta_pct": delta_pct,
        },
        "industry_checks": {
            "acres_per_mw": acres_per_mw,
            "lcoe_surface_slice": [
                {
                    "gcr": g,
                    "dcac_ratio": d,
                    "lcoe_per_mwh": _find(g, d)["lcoe_per_mwh"] if _find(g, d) else None,
                    "capacity_factor_ac": _find(g, d)["capacity_factor_ac"]
                    if _find(g, d)
                    else None,
                    "clipping_correction_pct": _find(g, d)["clipping_correction_pct"]
                    if _find(g, d)
                    else None,
                }
                for g, d in slice_points
            ],
            "clipping_low_dcac_pct": low["clipping_correction_pct"],
            "clipping_high_dcac_pct": high["clipping_correction_pct"],
        },
        "runtime_seconds": elapsed,
    }
    out_path = test_results_dir / "test_optimize_solar_phoenix.json"
    out_path.write_text(json.dumps(artifact, indent=2, default=str))
    print(f"\nArtifact written: {out_path}")


# ---------------------------------------------------------------------------
# Integration test — production mode (no economic inputs)
# ---------------------------------------------------------------------------


def test_optimize_solar_phoenix_production_mode(test_results_dir: Path) -> None:
    """Run optimize_solar() in production mode at Phoenix TMY.

    No economic inputs. Validates max_production and max_yield winners
    are selected correctly from the 70-point sweep.
    """
    weather_file = _prepare_phoenix_weather_file()
    base_site = _make_phoenix_site_config(weather_file)

    buildable_acres = 100.0
    racking = "tracker"

    def _progress(index, total, point):
        _print_point_row(index, total, point)

    print(
        f"\n[PRODUCTION MODE] Starting optimize_solar() sweep: "
        f"{buildable_acres} acres, racking={racking}"
    )
    t0 = time.perf_counter()
    result = optimize_solar(
        base_site_config=base_site,
        buildable_acres=buildable_acres,
        racking=racking,
        progress_callback=_progress,
    )
    elapsed = time.perf_counter() - t0
    print(
        f"\n[PRODUCTION MODE] Sweep complete in "
        f"{elapsed:.1f}s ({elapsed/60:.2f} min)"
    )

    # Mode + winner keys
    assert result["mode"] == "production"
    assert set(result["winners"].keys()) == {"max_production", "max_yield"}

    winners = result["winners"]
    sweep_results = result["sweep_results"]
    sweep_metadata = result["sweep_metadata"]

    # Sweep completed fully
    assert sweep_metadata["total_combinations"] == 70
    assert sweep_metadata["failed_combinations"] == 0

    # Verify max_production: scan sweep_results for max annual_energy_kwh
    valid = [p for p in sweep_results if not p["failed"]]
    max_energy = max(p["annual_energy_kwh"] for p in valid)
    assert winners["max_production"]["annual_energy_kwh"] == max_energy, (
        f"max_production winner energy {winners['max_production']['annual_energy_kwh']} "
        f"does not match scan maximum {max_energy}"
    )

    # Verify max_yield: scan sweep_results for max capacity_factor_ac
    max_cf = max(p["capacity_factor_ac"] for p in valid)
    assert winners["max_yield"]["capacity_factor_ac"] == max_cf, (
        f"max_yield winner cf {winners['max_yield']['capacity_factor_ac']} "
        f"does not match scan maximum {max_cf}"
    )

    # Production-mode points have no LCOE/NPV fields populated
    w_prod = winners["max_production"]
    assert w_prod["lcoe_per_mwh"] is None
    assert w_prod["npv"] is None
    assert w_prod["capex_total"] is None

    # Metadata: production mode → economic/revenue metadata all None
    assert sweep_metadata["mode"] == "production"
    assert sweep_metadata["itc_pct"] is None
    assert sweep_metadata["solar_cost_per_kw_dc"] is None
    assert sweep_metadata["energy_price_per_kwh"] is None

    # --- Print side by side for manual inspection ---
    print("\n=== Production-mode winners ===")
    for name, w in winners.items():
        print(
            f"  {name:16s}: gcr={w['gcr']:.2f} dcac={w['dcac_ratio']:.2f} "
            f"mw_dc={w['mw_dc']:.2f} mw_ac={w['mw_ac']:.2f} "
            f"mwh={w['annual_energy_mwh']:,.0f} "
            f"cf={w['capacity_factor_ac']:.2f}%"
        )

    # --- Write artifact ---
    artifact = {
        "mode": result["mode"],
        "sweep_metadata": sweep_metadata,
        "winners": winners,
        "runtime_seconds": elapsed,
    }
    out_path = test_results_dir / "test_optimize_solar_phoenix_production.json"
    out_path.write_text(json.dumps(artifact, indent=2, default=str))
    print(f"\nArtifact written: {out_path}")


# ---------------------------------------------------------------------------
# Integration test — NPV mode (economic + revenue inputs)
# ---------------------------------------------------------------------------


def test_optimize_solar_phoenix_npv_mode(test_results_dir: Path) -> None:
    """Run optimize_solar() in NPV mode at Phoenix TMY with $50/MWh PPA.

    Full economic + revenue inputs. Validates that all four winners are
    selected and the npv winner has positive NPV.
    """
    weather_file = _prepare_phoenix_weather_file()
    base_site = _make_phoenix_site_config(weather_file)

    buildable_acres = 100.0
    racking = "tracker"

    def _progress(index, total, point):
        _print_point_row(index, total, point)

    print(
        f"\n[NPV MODE] Starting optimize_solar() sweep: "
        f"{buildable_acres} acres, racking={racking}, "
        f"PPA=${PHOENIX_REVENUE_KWARGS['energy_price_per_kwh'] * 1000:.0f}/MWh"
    )
    t0 = time.perf_counter()
    result = optimize_solar(
        base_site_config=base_site,
        buildable_acres=buildable_acres,
        racking=racking,
        progress_callback=_progress,
        **PHOENIX_ECONOMIC_KWARGS,
        **PHOENIX_REVENUE_KWARGS,
    )
    elapsed = time.perf_counter() - t0
    print(f"\n[NPV MODE] Sweep complete in {elapsed:.1f}s ({elapsed/60:.2f} min)")

    # Mode + winner keys
    assert result["mode"] == "npv"
    assert set(result["winners"].keys()) == {
        "max_production", "max_yield", "lcoe", "npv",
    }

    winners = result["winners"]
    sweep_results = result["sweep_results"]
    sweep_metadata = result["sweep_metadata"]

    # Sweep completed fully
    assert sweep_metadata["total_combinations"] == 70
    assert sweep_metadata["failed_combinations"] == 0

    # NPV winner: highest NPV, expected positive given $50/MWh flat PPA >
    # LCOE floor (~$40-50/MWh at Phoenix + 30% ITC)
    w_npv = winners["npv"]
    assert w_npv["npv"] is not None
    assert w_npv["npv"] > 0, (
        f"NPV winner has non-positive NPV={w_npv['npv']}. "
        f"Either the PPA price is too low or LCOE is unusually high. "
        f"Check lcoe={w_npv['lcoe_per_mwh']}/MWh."
    )

    # Verify NPV is max across valid points
    valid = [p for p in sweep_results if not p["failed"]]
    max_npv = max(p["npv"] for p in valid)
    assert w_npv["npv"] == max_npv

    # Metadata: full economic + revenue populated
    assert sweep_metadata["mode"] == "npv"
    assert sweep_metadata["energy_price_per_kwh"] == 0.05
    assert sweep_metadata["energy_cost_escalator_pct"] == 2.0
    assert sweep_metadata["itc_pct"] == 30.0

    # --- Print side by side for manual inspection ---
    print("\n=== NPV-mode winners (all four) ===")
    for name, w in winners.items():
        npv_str = "--" if w["npv"] is None else f"${w['npv']:,.0f}"
        lcoe_str = (
            "--" if w["lcoe_per_mwh"] is None else f"${w['lcoe_per_mwh']:.2f}/MWh"
        )
        print(
            f"  {name:16s}: gcr={w['gcr']:.2f} dcac={w['dcac_ratio']:.2f} "
            f"mw_dc={w['mw_dc']:.2f} mwh={w['annual_energy_mwh']:,.0f} "
            f"cf={w['capacity_factor_ac']:.2f}% "
            f"lcoe={lcoe_str} npv={npv_str}"
        )

    # --- Write artifact ---
    artifact = {
        "mode": result["mode"],
        "sweep_metadata": sweep_metadata,
        "winners": winners,
        "runtime_seconds": elapsed,
    }
    out_path = test_results_dir / "test_optimize_solar_phoenix_npv.json"
    out_path.write_text(json.dumps(artifact, indent=2, default=str))
    print(f"\nArtifact written: {out_path}")


# ---------------------------------------------------------------------------
# Helpers for solar+BESS integration tests
# ---------------------------------------------------------------------------


def _make_flat_rate_schedule(
    energy_rate: float = 0.10,
    flat_demand_rate: float = 0.0,
) -> RateSchedule:
    """Build a flat RateSchedule with a single energy tier and no TOU periods.

    Mirrors the _make_flat_rate_schedule helper in
    tests/test_bess_dispatch_runner.py. flat_demand_rate defaults to 0 so
    the BTM bill exercises only energy charges.
    """
    flat_schedule = [[0] * 24 for _ in range(12)]
    return RateSchedule(
        utility_name="Test Utility",
        tariff_name="Flat Rate",
        energyratestructure=[[RateTier(rate=energy_rate)]],
        energyweekdayschedule=flat_schedule,
        energyweekendschedule=flat_schedule,
        demandratestructure=None,
        demandweekdayschedule=None,
        demandweekendschedule=None,
        flatdemandstructure=[[RateTier(rate=flat_demand_rate)]],
        flatdemandmonths=[0] * 12,
    )


def _make_synthetic_lmp_prices() -> list[float]:
    """Build an 8760-hour synthetic LMP price series with a realistic daily shape.

    Overnight low (hours 0-5, ~$20-25/MWh), morning ramp (6-9, ~$30-45/MWh),
    midday plateau (10-15, ~$45-55/MWh), evening peak (16-20, up to
    ~$110/MWh), then decline (21-23, ~$30-55/MWh). The same pattern repeats
    for all 365 days — enough arbitrage signal to give the BESS something
    to chase without imposing a seasonal model.
    """
    daily = (
        # Hours 0-5: overnight low
        [25.0, 22.0, 20.0, 20.0, 22.0, 25.0]
        # Hours 6-9: morning ramp
        + [30.0, 35.0, 40.0, 45.0]
        # Hours 10-15: midday plateau
        + [50.0, 55.0, 55.0, 55.0, 50.0, 45.0]
        # Hours 16-20: evening peak (peaks at hour 19 = $110)
        + [55.0, 75.0, 100.0, 110.0, 90.0]
        # Hours 21-23: wind down
        + [55.0, 40.0, 30.0]
    )
    assert len(daily) == 24, "daily LMP pattern must have 24 entries"
    prices = daily * 365
    assert len(prices) == 8760
    return prices


# ---------------------------------------------------------------------------
# Integration test — solar + BESS, BTM dispatch
# ---------------------------------------------------------------------------


def test_optimize_solar_bess_phoenix_btm(test_results_dir: Path) -> None:
    """Run optimize_solar_bess() in BTM mode at Phoenix TMY.

    Reduced sweep (4 GCR × 3 DCAC = 12 solar configs × 10 BESS combos =
    120 BESS dispatches) keeps runtime manageable. Flat $0.10/kWh rate and
    constant 500 kW load exercise the full BTM path.
    """
    weather_file = _prepare_phoenix_weather_file()
    base_site = _make_phoenix_site_config(weather_file)

    rate_schedule = _make_flat_rate_schedule(
        energy_rate=0.10, flat_demand_rate=0.0
    )
    load_profile = LoadProfile(hourly_kwh=[500.0] * 8760, source="customer")

    buildable_acres = 100.0
    racking = "tracker"

    def _progress(index, total, stage, result):
        if stage == "solar_sweep":
            _print_point_row(index, total, result)
        elif stage == "bess_sweep" and index % 20 == 0:
            failed = result.get("failed")
            marker = "FAIL" if failed else "OK  "
            npv = result.get("combined_npv")
            npv_str = "--" if npv is None else f"${npv:,.0f}"
            print(
                f"  bess[{index:3d}/{total}] {marker} "
                f"gcr={result['solar_gcr']:.2f} "
                f"dcac={result['solar_dcac']:.2f} "
                f"bess={result['bess_power_mw']:.2f}MW/"
                f"{result['bess_duration_hr']:.1f}hr "
                f"combined_npv={npv_str}",
                flush=True,
            )

    print(
        f"\n[SOLAR+BESS BTM] Starting optimize_solar_bess() sweep: "
        f"{buildable_acres} acres, racking={racking}"
    )
    t0 = time.perf_counter()
    result = optimize_solar_bess(
        base_site_config=base_site,
        buildable_acres=buildable_acres,
        dispatch_mode="btm",
        racking=racking,
        # Reduced sweep: 4 GCRs × 3 DCACs = 12 solar configs
        gcr_range=(0.35, 0.50),
        gcr_step=0.05,
        dcac_range=(1.20, 1.40),
        dcac_step=0.10,
        charging_mode="solar_and_grid",
        # Economic inputs
        solar_cost_per_kw_dc=1200.0,
        solar_opex_per_kw_dc_year=20.0,
        discount_rate_pct=7.0,
        project_lifetime_years=25,
        degradation_pct=0.5,
        itc_pct=30.0,
        energy_price_per_kwh=0.10,
        energy_cost_escalator_pct=2.0,
        rate_schedule=rate_schedule,
        load_profile=load_profile,
        progress_callback=_progress,
    )
    elapsed = time.perf_counter() - t0
    print(
        f"\n[SOLAR+BESS BTM] Sweep complete in "
        f"{elapsed:.1f}s ({elapsed/60:.2f} min)"
    )

    winners = result["winners"]
    bess_winner_detail = result["bess_winner_detail"]
    sweep_metadata = result["sweep_metadata"]

    # --- Assertion 1: sweep completes (mode + structure) ---
    assert result["mode"] == "solar_bess"

    # --- Assertion 2: all four winners present ---
    assert set(winners.keys()) == {
        "max_production",
        "max_yield",
        "solar_only_npv",
        "solar_bess_npv",
    }
    assert winners["solar_bess_npv"] is not None, (
        "solar_bess_npv winner missing — no BESS combos succeeded"
    )

    # --- Assertion 3: bess_winner_detail has all sections ---
    assert bess_winner_detail is not None
    assert "solar" in bess_winner_detail
    assert "bess" in bess_winner_detail
    assert "combined" in bess_winner_detail

    # --- Assertion 4: bess_npv / combined_npv are real numbers ---
    assert isinstance(bess_winner_detail["bess"]["npv"], (int, float))
    assert bess_winner_detail["bess"]["npv"] != 0.0, (
        "bess_npv == 0.0 suggests the dispatch did not actually run"
    )
    assert isinstance(bess_winner_detail["combined"]["npv"], (int, float))

    # --- Assertion 5: sweep metadata reflects the reduced sweep ---
    # 4 GCR × 3 DCAC = 12 solar combos × 10 BESS combos = 120
    assert sweep_metadata["total_solar_combinations"] == 12
    assert sweep_metadata["total_bess_combinations"] == 120, (
        f"Expected 120 BESS combos, got "
        f"{sweep_metadata['total_bess_combinations']}"
    )
    assert sweep_metadata["failed_bess_combinations"] == 0, (
        f"Expected 0 failed BESS combos, got "
        f"{sweep_metadata['failed_bess_combinations']}"
    )

    # --- Print winners side by side ---
    print("\n=== Solar+BESS BTM winners ===")
    for name, w in winners.items():
        if w is None:
            print(f"  {name:18s}: (None)")
            continue
        gcr = w.get("gcr") or w.get("solar_gcr")
        dcac = w.get("dcac_ratio") or w.get("solar_dcac")
        npv = w.get("npv") or w.get("combined_npv")
        mw_dc = w.get("mw_dc")
        mwh = w.get("annual_energy_mwh")
        npv_str = "--" if npv is None else f"${npv:,.0f}"
        mw_dc_str = "--" if mw_dc is None else f"{mw_dc:.2f}MW"
        mwh_str = "--" if mwh is None else f"{mwh:,.0f}"
        print(
            f"  {name:18s}: gcr={gcr:.2f} dcac={dcac:.2f} "
            f"mw_dc={mw_dc_str} mwh={mwh_str} npv={npv_str}"
        )

    # --- Print bess_winner_detail ---
    print("\n=== BESS winner detail ===")
    print(
        f"  Solar: gcr={bess_winner_detail['solar']['gcr']:.2f} "
        f"dcac={bess_winner_detail['solar']['dcac_ratio']:.2f} "
        f"mw_dc={bess_winner_detail['solar']['mw_dc']:.2f} "
        f"npv=${bess_winner_detail['solar']['npv']:,.0f}"
    )
    bess = bess_winner_detail["bess"]
    print(
        f"  BESS:  {bess['power_mw']:.2f}MW / {bess['duration_hr']:.1f}hr "
        f"({bess['capacity_mwh']:.2f}MWh) "
        f"capex=${bess['capex']:,.0f} "
        f"opex/yr=${bess['opex_per_year']:,.0f} "
        f"annual_rev=${bess['annual_revenue']:,.0f} "
        f"npv=${bess['npv']:,.0f}"
    )
    combined = bess_winner_detail["combined"]
    print(
        f"  Combined: solar_npv=${combined['solar_npv']:,.0f} + "
        f"bess_npv=${combined['bess_npv']:,.0f} = "
        f"${combined['npv']:,.0f}  "
        f"(total_capex=${combined['total_capex']:,.0f}, "
        f"total_opex/yr=${combined['total_opex']:,.0f})"
    )
    print(f"  bess_adds_value = {result['bess_adds_value']}")

    # --- Write artifact for manual inspection ---
    artifact = {
        "mode": result["mode"],
        "sweep_metadata": sweep_metadata,
        "winners": winners,
        "bess_winner_detail": bess_winner_detail,
        "bess_adds_value": result["bess_adds_value"],
        "runtime_seconds": elapsed,
    }
    out_path = test_results_dir / "test_optimize_solar_bess_phoenix_btm.json"
    out_path.write_text(json.dumps(artifact, indent=2, default=str))
    print(f"\nArtifact written: {out_path}")


# ---------------------------------------------------------------------------
# Integration test — solar + BESS, FTM dispatch
# ---------------------------------------------------------------------------


def test_optimize_solar_bess_phoenix_ftm(test_results_dir: Path) -> None:
    """Run optimize_solar_bess() in FTM mode at Phoenix TMY.

    Same reduced sweep as the BTM test. Synthetic LMP prices with a
    realistic daily pattern (overnight low, midday plateau, evening peak)
    give the BESS arbitrage opportunity across all 365 days.
    """
    weather_file = _prepare_phoenix_weather_file()
    base_site = _make_phoenix_site_config(weather_file)

    lmp_data = LMPData(
        iso="test_iso",
        zone="test_zone",
        market="DAY_AHEAD_HOURLY",
        year=2024,
        prices=_make_synthetic_lmp_prices(),
    )

    buildable_acres = 100.0
    racking = "tracker"

    def _progress(index, total, stage, result):
        if stage == "solar_sweep":
            _print_point_row(index, total, result)
        elif stage == "bess_sweep" and index % 20 == 0:
            failed = result.get("failed")
            marker = "FAIL" if failed else "OK  "
            npv = result.get("combined_npv")
            npv_str = "--" if npv is None else f"${npv:,.0f}"
            print(
                f"  bess[{index:3d}/{total}] {marker} "
                f"gcr={result['solar_gcr']:.2f} "
                f"dcac={result['solar_dcac']:.2f} "
                f"bess={result['bess_power_mw']:.2f}MW/"
                f"{result['bess_duration_hr']:.1f}hr "
                f"combined_npv={npv_str}",
                flush=True,
            )

    print(
        f"\n[SOLAR+BESS FTM] Starting optimize_solar_bess() sweep: "
        f"{buildable_acres} acres, racking={racking}"
    )
    t0 = time.perf_counter()
    result = optimize_solar_bess(
        base_site_config=base_site,
        buildable_acres=buildable_acres,
        dispatch_mode="ftm",
        racking=racking,
        # Reduced sweep: 4 GCRs × 3 DCACs = 12 solar configs
        gcr_range=(0.35, 0.50),
        gcr_step=0.05,
        dcac_range=(1.20, 1.40),
        dcac_step=0.10,
        charging_mode="solar_and_grid",
        # Economic inputs — wholesale-ish PPA average
        solar_cost_per_kw_dc=1200.0,
        solar_opex_per_kw_dc_year=20.0,
        discount_rate_pct=7.0,
        project_lifetime_years=25,
        degradation_pct=0.5,
        itc_pct=30.0,
        energy_price_per_kwh=0.05,
        energy_cost_escalator_pct=2.0,
        lmp_data=lmp_data,
        progress_callback=_progress,
    )
    elapsed = time.perf_counter() - t0
    print(
        f"\n[SOLAR+BESS FTM] Sweep complete in "
        f"{elapsed:.1f}s ({elapsed/60:.2f} min)"
    )

    winners = result["winners"]
    bess_winner_detail = result["bess_winner_detail"]
    sweep_metadata = result["sweep_metadata"]

    # --- Assertion 1: sweep completes (mode + structure) ---
    assert result["mode"] == "solar_bess"

    # --- Assertion 2: all four winners present ---
    assert set(winners.keys()) == {
        "max_production",
        "max_yield",
        "solar_only_npv",
        "solar_bess_npv",
    }
    assert winners["solar_bess_npv"] is not None, (
        "solar_bess_npv winner missing — no BESS combos succeeded"
    )

    # --- Assertion 3: bess_winner_detail has all sections ---
    assert bess_winner_detail is not None
    assert "solar" in bess_winner_detail
    assert "bess" in bess_winner_detail
    assert "combined" in bess_winner_detail

    # --- Assertion 4: bess_npv / combined_npv are real numbers ---
    assert isinstance(bess_winner_detail["bess"]["npv"], (int, float))
    assert bess_winner_detail["bess"]["npv"] != 0.0, (
        "bess_npv == 0.0 suggests the dispatch did not actually run"
    )
    assert isinstance(bess_winner_detail["combined"]["npv"], (int, float))

    # --- Assertion 5: sweep metadata reflects the reduced sweep ---
    assert sweep_metadata["total_solar_combinations"] == 12
    assert sweep_metadata["total_bess_combinations"] == 120, (
        f"Expected 120 BESS combos, got "
        f"{sweep_metadata['total_bess_combinations']}"
    )
    assert sweep_metadata["failed_bess_combinations"] == 0, (
        f"Expected 0 failed BESS combos, got "
        f"{sweep_metadata['failed_bess_combinations']}"
    )

    # --- Print winners side by side ---
    print("\n=== Solar+BESS FTM winners ===")
    for name, w in winners.items():
        if w is None:
            print(f"  {name:18s}: (None)")
            continue
        gcr = w.get("gcr") or w.get("solar_gcr")
        dcac = w.get("dcac_ratio") or w.get("solar_dcac")
        npv = w.get("npv") or w.get("combined_npv")
        mw_dc = w.get("mw_dc")
        mwh = w.get("annual_energy_mwh")
        npv_str = "--" if npv is None else f"${npv:,.0f}"
        mw_dc_str = "--" if mw_dc is None else f"{mw_dc:.2f}MW"
        mwh_str = "--" if mwh is None else f"{mwh:,.0f}"
        print(
            f"  {name:18s}: gcr={gcr:.2f} dcac={dcac:.2f} "
            f"mw_dc={mw_dc_str} mwh={mwh_str} npv={npv_str}"
        )

    # --- Print bess_winner_detail ---
    print("\n=== BESS winner detail ===")
    print(
        f"  Solar: gcr={bess_winner_detail['solar']['gcr']:.2f} "
        f"dcac={bess_winner_detail['solar']['dcac_ratio']:.2f} "
        f"mw_dc={bess_winner_detail['solar']['mw_dc']:.2f} "
        f"npv=${bess_winner_detail['solar']['npv']:,.0f}"
    )
    bess = bess_winner_detail["bess"]
    print(
        f"  BESS:  {bess['power_mw']:.2f}MW / {bess['duration_hr']:.1f}hr "
        f"({bess['capacity_mwh']:.2f}MWh) "
        f"capex=${bess['capex']:,.0f} "
        f"opex/yr=${bess['opex_per_year']:,.0f} "
        f"annual_rev=${bess['annual_revenue']:,.0f} "
        f"npv=${bess['npv']:,.0f}"
    )
    combined = bess_winner_detail["combined"]
    print(
        f"  Combined: solar_npv=${combined['solar_npv']:,.0f} + "
        f"bess_npv=${combined['bess_npv']:,.0f} = "
        f"${combined['npv']:,.0f}  "
        f"(total_capex=${combined['total_capex']:,.0f}, "
        f"total_opex/yr=${combined['total_opex']:,.0f})"
    )
    print(f"  bess_adds_value = {result['bess_adds_value']}")

    # --- Write artifact for manual inspection ---
    artifact = {
        "mode": result["mode"],
        "sweep_metadata": sweep_metadata,
        "winners": winners,
        "bess_winner_detail": bess_winner_detail,
        "bess_adds_value": result["bess_adds_value"],
        "runtime_seconds": elapsed,
    }
    out_path = test_results_dir / "test_optimize_solar_bess_phoenix_ftm.json"
    out_path.write_text(json.dumps(artifact, indent=2, default=str))
    print(f"\nArtifact written: {out_path}")
