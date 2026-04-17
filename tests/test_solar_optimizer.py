"""Tests for optimize_solar() — the GCR × DC/AC sweep orchestrator.

All tests mock _run_single_pysam, ModelConfigurator, PySAMSimulator, and
CECDatabase to keep the suite hermetic (no real PySAM execution, no CSV
loads). Economics and capacity math run for real.

Covers all three optimizer modes:
- production (no economic inputs)
- lcoe (full economic inputs)
- npv (full economic + revenue inputs)

Plus input-validation errors for partial economic or revenue inputs.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.config.loader import load_config
from src.optimization.solar_optimizer import optimize_solar


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SINGLE_ROW_CSV = FIXTURES_DIR / "single_row_test.csv"

TRACKER_GRID_SIZE = 70  # 7 GCRs × 10 DCACs
TRACKER_GCRS = [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60]
TRACKER_DCACS = [1.15, 1.20, 1.25, 1.30, 1.35, 1.40, 1.45, 1.50, 1.55, 1.60]

# Standard economic inputs used across LCOE/NPV tests.
ECONOMIC_KWARGS = {
    "solar_cost_per_kw_dc": 1200.0,
    "solar_opex_per_kw_dc_year": 20.0,
    "discount_rate_pct": 7.0,
    "project_lifetime_years": 25,
    "degradation_pct": 0.5,
    "itc_pct": 30.0,
}

# Standard revenue inputs used across NPV tests.
REVENUE_KWARGS = {
    "energy_price_per_kwh": 0.05,
    "energy_cost_escalator_pct": 2.0,
}


def _base_site():
    """Load the single-row test fixture and return the SiteConfig."""
    return load_config(SINGLE_ROW_CSV)[0]


def _make_pysam_return(
    annual_energy_kwh: float, capacity_factor_ac: float = 32.34
) -> dict:
    """Build a stub _run_single_pysam return dict."""
    return {
        "annual_energy_kwh": annual_energy_kwh,
        "capacity_factor_ac": capacity_factor_ac,
        "clipping_correction_pct": 0.4,
        "ac_hourly_kw": [annual_energy_kwh / 8760] * 8760,
    }


def _constant_pysam_side_effect(annual_energy_kwh: float = 30_000_000.0):
    """Return a side_effect function producing constant PySAM outputs."""
    def _side_effect(site_config, configurator, simulator, apply_clipping_correction=True):
        return _make_pysam_return(annual_energy_kwh)
    return _side_effect


def _varying_pysam_side_effect(target_gcr: float, target_dcac: float):
    """Return a side_effect producing the MAX energy at (target_gcr, target_dcac).

    Energy scales proportionally with the system's DC capacity (2000 kWh/kW-DC
    yield) so that — all else equal — LCOE is constant across the grid. The
    target point gets a +20% yield bonus so its LCOE is unambiguously the
    lowest.
    """
    def _side_effect(site_config, configurator, simulator, apply_clipping_correction=True):
        # Match within 1e-6 to avoid float-equality pitfalls
        is_target = (
            abs(site_config.gcr - target_gcr) < 1e-6
            and abs(site_config.dc_size_mw / site_config.ac_installed_mw - target_dcac) < 1e-2
        )
        base_yield_kwh_per_kw = 2000.0
        yield_kwh_per_kw = base_yield_kwh_per_kw * (1.20 if is_target else 1.0)
        energy_kwh = site_config.dc_size_mw * 1000 * yield_kwh_per_kw
        return _make_pysam_return(energy_kwh)
    return _side_effect


def _per_point_pysam_side_effect(
    point_energies: dict[tuple[float, float], float],
    default_energy: float = 30_000_000.0,
    point_cfs: dict[tuple[float, float], float] | None = None,
    default_cf: float = 32.34,
):
    """Return a side_effect mapping (gcr, dcac) to specific (energy, cf).

    Unmatched points fall back to (default_energy, default_cf). Keys are
    matched within 1e-6 on GCR and 1e-2 on DC/AC.
    """
    cfs = point_cfs or {}

    def _side_effect(site_config, configurator, simulator, apply_clipping_correction=True):
        dcac = site_config.dc_size_mw / site_config.ac_installed_mw
        energy = default_energy
        cf = default_cf
        for (tgt_gcr, tgt_dcac), tgt_energy in point_energies.items():
            if (
                abs(site_config.gcr - tgt_gcr) < 1e-6
                and abs(dcac - tgt_dcac) < 1e-2
            ):
                energy = tgt_energy
                if (tgt_gcr, tgt_dcac) in cfs:
                    cf = cfs[(tgt_gcr, tgt_dcac)]
                break
        return _make_pysam_return(energy, capacity_factor_ac=cf)
    return _side_effect


def _failing_pysam_side_effect(fail_indices: set[int]):
    """Raise RuntimeError on the N-th call for each index in fail_indices.

    Indices are 0-based per call order.
    """
    call_counter = {"n": 0}

    def _side_effect(site_config, configurator, simulator, apply_clipping_correction=True):
        idx = call_counter["n"]
        call_counter["n"] += 1
        if idx in fail_indices:
            raise RuntimeError(f"simulated failure at call #{idx}")
        return _make_pysam_return(30_000_000.0)
    return _side_effect


def _patch_heavy_deps():
    """Patch the non-PySAM heavy constructors used inside optimize_solar."""
    return (
        patch("src.optimization.solar_optimizer.CECDatabase", return_value=MagicMock()),
        patch(
            "src.optimization.solar_optimizer.ModelConfigurator",
            return_value=MagicMock(),
        ),
        patch(
            "src.optimization.solar_optimizer.PySAMSimulator",
            return_value=MagicMock(),
        ),
    )


# ---------------------------------------------------------------------------
# a. Happy path — LCOE mode
# ---------------------------------------------------------------------------


class TestHappyPathLCOE:
    """End-to-end sweep with mocked PySAM returning constant energy (LCOE mode)."""

    def test_full_tracker_sweep_shape_and_metadata(self, tmp_path) -> None:
        """70-point tracker sweep: correct shape, no failures, metadata set."""
        site = _base_site()
        progress_calls: list[tuple[int, int, dict]] = []

        def on_progress(i, total, point):
            progress_calls.append((i, total, point))

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_constant_pysam_side_effect(30_000_000.0),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                progress_callback=on_progress,
                **ECONOMIC_KWARGS,
            )

        # Mode and shape
        assert out["mode"] == "lcoe"
        assert len(out["sweep_results"]) == TRACKER_GRID_SIZE
        assert out["sweep_metadata"]["total_combinations"] == TRACKER_GRID_SIZE
        assert out["sweep_metadata"]["failed_combinations"] == 0

        # Winners dict: LCOE mode → max_production, max_yield, lcoe
        assert set(out["winners"].keys()) == {"max_production", "max_yield", "lcoe"}

        # LCOE winner is a valid (non-failed) point with all numeric fields set
        w = out["winners"]["lcoe"]
        assert w["failed"] is False
        assert w["error"] is None
        assert w["lcoe_per_mwh"] is not None
        assert w["lcoe_per_kwh"] is not None
        assert w["annual_energy_kwh"] == 30_000_000.0
        assert w["annual_energy_mwh"] == 30_000.0
        assert w["capex_total"] is not None
        assert w["net_capex"] == pytest.approx(w["capex_total"] * 0.70)
        # NPV fields are None in LCOE mode
        assert w["npv"] is None
        assert w["annual_revenue_year1"] is None
        assert w["simple_payback_years"] is None
        assert w["irr"] is None

        # Metadata reflects the resolved tracker range
        meta = out["sweep_metadata"]
        assert meta["mode"] == "lcoe"
        assert meta["racking"] == "tracker"
        assert meta["gcr_range"] == (0.30, 0.60)
        assert meta["dcac_range"] == (1.15, 1.60)
        assert meta["buildable_acres"] == 100.0
        # Economic metadata populated in lcoe mode
        assert meta["itc_pct"] == 30.0
        assert meta["discount_rate_pct"] == 7.0
        assert meta["solar_cost_per_kw_dc"] == 1200.0
        # Revenue metadata None in lcoe mode
        assert meta["energy_price_per_kwh"] is None
        assert meta["energy_cost_escalator_pct"] is None

        # Progress callback was invoked exactly 70 times in order
        assert len(progress_calls) == TRACKER_GRID_SIZE
        indices = [c[0] for c in progress_calls]
        assert indices == list(range(1, TRACKER_GRID_SIZE + 1))
        # Total argument is always the grid size
        assert all(c[1] == TRACKER_GRID_SIZE for c in progress_calls)

    def test_sweep_grid_order_preserved(self) -> None:
        """sweep_results preserves build_sweep_grid order (gcr outer, dcac inner)."""
        site = _base_site()

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_constant_pysam_side_effect(30_000_000.0),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                **ECONOMIC_KWARGS,
            )

        # First 5 entries should all be at GCR 0.30 with increasing DCAC
        first_five = out["sweep_results"][:5]
        assert [p["gcr"] for p in first_five] == [0.30] * 5
        assert [p["dcac_ratio"] for p in first_five] == [
            1.15, 1.20, 1.25, 1.30, 1.35,
        ]


# ---------------------------------------------------------------------------
# b. capacity_from_acreage called correctly per point
# ---------------------------------------------------------------------------


class TestCapacityCalls:
    """Capacity sizing uses each point's GCR."""

    def test_capacity_invoked_once_per_point_with_point_gcr(self) -> None:
        """capacity_from_acreage is called once per sweep point with that GCR."""
        site = _base_site()

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_constant_pysam_side_effect(30_000_000.0),
        ), patch(
            "src.optimization.solar_optimizer.capacity_from_acreage",
            wraps=__import__(
                "src.optimization.capacity", fromlist=["capacity_from_acreage"]
            ).capacity_from_acreage,
        ) as mock_cap:
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                **ECONOMIC_KWARGS,
            )

        # One call per sweep point
        assert mock_cap.call_count == TRACKER_GRID_SIZE

        # Each call's gcr argument matches the sweep-results gcr at that index
        for (call, result_point) in zip(mock_cap.call_args_list, out["sweep_results"]):
            assert call.kwargs["gcr"] == result_point["gcr"]
            assert call.kwargs["buildable_acres"] == 100.0

        # Across the full sweep, each of the 7 tracker GCRs appears 10 times
        gcrs_used = [call.kwargs["gcr"] for call in mock_cap.call_args_list]
        for expected_gcr in TRACKER_GCRS:
            assert gcrs_used.count(expected_gcr) == 10


# ---------------------------------------------------------------------------
# c. Winner selection — target is min LCOE (LCOE mode)
# ---------------------------------------------------------------------------


class TestWinnerSelectionLCOE:
    """A crafted energy profile forces a known LCOE winner."""

    def test_specific_point_wins_when_it_has_max_energy(self) -> None:
        """Target (0.50, 1.35) getting +20% energy yields a lower LCOE."""
        site = _base_site()
        target_gcr, target_dcac = 0.50, 1.35

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_varying_pysam_side_effect(target_gcr, target_dcac),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                **ECONOMIC_KWARGS,
            )

        w = out["winners"]["lcoe"]
        assert w["gcr"] == target_gcr
        assert w["dcac_ratio"] == target_dcac

        # Confirm winner's LCOE is strictly less than every other valid point
        other_lcoes = [
            p["lcoe_per_mwh"]
            for p in out["sweep_results"]
            if not p["failed"]
            and (p["gcr"], p["dcac_ratio"]) != (target_gcr, target_dcac)
        ]
        assert all(w["lcoe_per_mwh"] < other for other in other_lcoes)


# ---------------------------------------------------------------------------
# d. Partial failures
# ---------------------------------------------------------------------------


class TestPartialFailures:
    """Ten scattered failures: sweep completes, winner from the rest."""

    def test_partial_failures_recorded_but_sweep_completes(self) -> None:
        """10 of 70 points fail → 60 valid points, failed=True on the 10."""
        site = _base_site()
        fail_indices = {2, 7, 13, 21, 30, 38, 45, 51, 60, 68}

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_failing_pysam_side_effect(fail_indices),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                **ECONOMIC_KWARGS,
            )

        # Sweep still has all 70 entries
        assert len(out["sweep_results"]) == TRACKER_GRID_SIZE
        assert out["sweep_metadata"]["failed_combinations"] == 10

        # Exactly the flagged indices are marked failed
        failed_points = [
            i for i, p in enumerate(out["sweep_results"]) if p["failed"]
        ]
        assert set(failed_points) == fail_indices

        # Failed points have numeric fields nulled and a non-empty error
        for i in fail_indices:
            p = out["sweep_results"][i]
            assert p["failed"] is True
            assert p["error"] is not None
            assert isinstance(p["error"], str) and len(p["error"]) > 0
            assert p["lcoe_per_mwh"] is None
            assert p["lcoe_per_kwh"] is None
            assert p["annual_energy_kwh"] is None
            assert p["capex_total"] is None

        # LCOE winner is from the 60 remaining valid points
        w = out["winners"]["lcoe"]
        assert w["failed"] is False
        winner_index = next(
            i
            for i, p in enumerate(out["sweep_results"])
            if (p["gcr"], p["dcac_ratio"]) == (w["gcr"], w["dcac_ratio"])
            and not p["failed"]
        )
        assert winner_index not in fail_indices


# ---------------------------------------------------------------------------
# e. All failures → RuntimeError
# ---------------------------------------------------------------------------


class TestAllFailures:
    """If every point fails, optimize_solar raises RuntimeError."""

    def test_all_failures_raises_runtime_error(self) -> None:
        """Every call raises → RuntimeError('All sweep points failed')."""
        site = _base_site()

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_failing_pysam_side_effect(set(range(TRACKER_GRID_SIZE))),
        ):
            with pytest.raises(RuntimeError, match="All sweep points failed"):
                optimize_solar(
                    base_site_config=site,
                    buildable_acres=100.0,
                    **ECONOMIC_KWARGS,
                )


# ---------------------------------------------------------------------------
# f. Progress callback exception does not abort the sweep
# ---------------------------------------------------------------------------


class TestProgressCallbackExceptions:
    """A raising progress_callback is swallowed and logged."""

    def test_callback_exception_does_not_abort_sweep(self) -> None:
        """Callback raises on the 5th call — sweep still produces 70 results."""
        site = _base_site()
        call_count = {"n": 0}

        def bad_callback(i, total, point):
            call_count["n"] += 1
            if i == 5:
                raise ValueError("boom in callback")

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_constant_pysam_side_effect(30_000_000.0),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                progress_callback=bad_callback,
                **ECONOMIC_KWARGS,
            )

        # Sweep completes end-to-end despite the single raise
        assert len(out["sweep_results"]) == TRACKER_GRID_SIZE
        assert out["sweep_metadata"]["failed_combinations"] == 0
        # Callback was called every iteration (including after the raise)
        assert call_count["n"] == TRACKER_GRID_SIZE


# ---------------------------------------------------------------------------
# g. Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    """Same inputs → byte-identical outputs across runs."""

    def test_two_runs_produce_identical_output(self) -> None:
        """Run optimize_solar twice with identical mocks — outputs equal."""
        site = _base_site()

        def _run_once():
            cec_p, cfg_p, sim_p = _patch_heavy_deps()
            with cec_p, cfg_p, sim_p, patch(
                "src.optimization.solar_optimizer._run_single_pysam",
                side_effect=_varying_pysam_side_effect(0.45, 1.30),
            ):
                return optimize_solar(
                    base_site_config=site,
                    buildable_acres=100.0,
                    **ECONOMIC_KWARGS,
                )

        out_a = _run_once()
        out_b = _run_once()

        # Winners dict matches exactly
        assert out_a["winners"] == out_b["winners"]

        # Full sweep_results match element-by-element
        assert len(out_a["sweep_results"]) == len(out_b["sweep_results"])
        for pa, pb in zip(out_a["sweep_results"], out_b["sweep_results"]):
            assert pa == pb

        # Metadata matches
        assert out_a["sweep_metadata"] == out_b["sweep_metadata"]

        # Write an artifact for manual inspection
        artifact_dir = Path("outputs/test_results")
        artifact_dir.mkdir(parents=True, exist_ok=True)
        with open(artifact_dir / "test_optimize_solar_determinism.json", "w") as f:
            # dicts with tuple keys/values need conversion for JSON
            meta = dict(out_a["sweep_metadata"])
            meta["gcr_range"] = list(meta["gcr_range"])
            meta["dcac_range"] = list(meta["dcac_range"])
            json.dump(
                {
                    "mode": out_a["mode"],
                    "winners": out_a["winners"],
                    "num_sweep_points": len(out_a["sweep_results"]),
                    "sample_first_point": out_a["sweep_results"][0],
                    "metadata": meta,
                },
                f,
                indent=2,
            )


# ---------------------------------------------------------------------------
# h. Custom ranges respected
# ---------------------------------------------------------------------------


class TestCustomRanges:
    """Narrow gcr_range produces a smaller sweep."""

    def test_narrow_gcr_range(self) -> None:
        """gcr_range=(0.35, 0.45) yields 3 GCRs × 10 DCACs = 30 points."""
        site = _base_site()

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_constant_pysam_side_effect(30_000_000.0),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                gcr_range=(0.35, 0.45),
                **ECONOMIC_KWARGS,
            )

        # 3 GCRs × 10 DCACs = 30
        assert len(out["sweep_results"]) == 30
        assert out["sweep_metadata"]["total_combinations"] == 30

        # All GCRs are within the requested window
        gcrs_seen = sorted({p["gcr"] for p in out["sweep_results"]})
        assert gcrs_seen == [0.35, 0.40, 0.45]

        # Metadata reports the custom range (not the tracker default)
        assert out["sweep_metadata"]["gcr_range"] == (0.35, 0.45)


# ---------------------------------------------------------------------------
# i. Production mode (no economic inputs)
# ---------------------------------------------------------------------------


class TestProductionMode:
    """Production mode: no economics, returns max_production + max_yield."""

    def test_production_mode_winners_keys(self) -> None:
        """No economic inputs → mode=production, winners has exactly 2 keys."""
        site = _base_site()

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_constant_pysam_side_effect(30_000_000.0),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
            )

        assert out["mode"] == "production"
        assert set(out["winners"].keys()) == {"max_production", "max_yield"}

        # LCOE/NPV fields are all None on every point in production mode
        for p in out["sweep_results"]:
            if p["failed"]:
                continue
            assert p["lcoe_per_mwh"] is None
            assert p["lcoe_per_kwh"] is None
            assert p["capex_total"] is None
            assert p["opex_per_year"] is None
            assert p["net_capex"] is None
            assert p["npv"] is None
            assert p["annual_revenue_year1"] is None
            assert p["simple_payback_years"] is None
            assert p["irr"] is None
            # Production fields ARE populated
            assert p["annual_energy_kwh"] == 30_000_000.0
            assert p["capacity_factor_ac"] == 32.34

        # Metadata: mode=production, economic+revenue metadata all None
        meta = out["sweep_metadata"]
        assert meta["mode"] == "production"
        assert meta["itc_pct"] is None
        assert meta["discount_rate_pct"] is None
        assert meta["project_lifetime_years"] is None
        assert meta["degradation_pct"] is None
        assert meta["solar_cost_per_kw_dc"] is None
        assert meta["solar_opex_per_kw_dc_year"] is None
        assert meta["energy_price_per_kwh"] is None
        assert meta["energy_cost_escalator_pct"] is None

    def test_production_mode_max_production_winner(self) -> None:
        """max_production winner has the highest annual_energy_kwh."""
        site = _base_site()

        # Boost energy at one specific point so it dominates production.
        target = (0.55, 1.45)
        point_energies = {target: 99_999_999.0}

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_per_point_pysam_side_effect(
                point_energies, default_energy=30_000_000.0
            ),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
            )

        w = out["winners"]["max_production"]
        assert (w["gcr"], w["dcac_ratio"]) == target
        assert w["annual_energy_kwh"] == 99_999_999.0

        # Verify against scanning sweep_results
        valid = [p for p in out["sweep_results"] if not p["failed"]]
        max_energy = max(p["annual_energy_kwh"] for p in valid)
        assert w["annual_energy_kwh"] == max_energy

    def test_production_mode_max_yield_winner(self) -> None:
        """max_yield winner has the highest capacity_factor_ac."""
        site = _base_site()

        target = (0.40, 1.25)
        # Bump CF at target; leave energy at default so production winner != yield winner.
        point_cfs = {target: 45.0}

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_per_point_pysam_side_effect(
                {target: 30_000_000.0},  # same energy as default
                default_energy=30_000_000.0,
                point_cfs=point_cfs,
                default_cf=32.34,
            ),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
            )

        w = out["winners"]["max_yield"]
        assert (w["gcr"], w["dcac_ratio"]) == target
        assert w["capacity_factor_ac"] == 45.0

        # Verify against scanning sweep_results
        valid = [p for p in out["sweep_results"] if not p["failed"]]
        max_cf = max(p["capacity_factor_ac"] for p in valid)
        assert w["capacity_factor_ac"] == max_cf


# ---------------------------------------------------------------------------
# j. LCOE mode — all three winners present
# ---------------------------------------------------------------------------


class TestLCOEModeWinners:
    """LCOE mode: winners has max_production, max_yield, AND lcoe."""

    def test_lcoe_mode_has_three_winner_keys(self) -> None:
        """Full economic inputs → winners has exactly 3 keys."""
        site = _base_site()

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_constant_pysam_side_effect(30_000_000.0),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                **ECONOMIC_KWARGS,
            )

        assert out["mode"] == "lcoe"
        assert set(out["winners"].keys()) == {
            "max_production", "max_yield", "lcoe",
        }

        # NPV fields still None on every point
        for p in out["sweep_results"]:
            if p["failed"]:
                continue
            assert p["npv"] is None
            assert p["annual_revenue_year1"] is None
            assert p["simple_payback_years"] is None
            assert p["irr"] is None
            # But LCOE fields are populated
            assert p["lcoe_per_mwh"] is not None
            assert p["capex_total"] is not None


# ---------------------------------------------------------------------------
# k. NPV mode — all four winners present, npv selected correctly
# ---------------------------------------------------------------------------


class TestNPVMode:
    """NPV mode: winners has all four keys, npv winner has highest NPV."""

    def test_npv_mode_has_four_winner_keys(self) -> None:
        """Economics + revenue → winners has exactly 4 keys."""
        site = _base_site()

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_constant_pysam_side_effect(30_000_000.0),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                **ECONOMIC_KWARGS,
                **REVENUE_KWARGS,
            )

        assert out["mode"] == "npv"
        assert set(out["winners"].keys()) == {
            "max_production", "max_yield", "lcoe", "npv",
        }

        # All fields populated on non-failed points
        for p in out["sweep_results"]:
            if p["failed"]:
                continue
            assert p["lcoe_per_mwh"] is not None
            assert p["npv"] is not None
            assert p["annual_revenue_year1"] is not None
            # IRR may be None if no sign change, but usually populated.

        # Metadata: both economic and revenue fields populated
        meta = out["sweep_metadata"]
        assert meta["mode"] == "npv"
        assert meta["energy_price_per_kwh"] == 0.05
        assert meta["energy_cost_escalator_pct"] == 2.0
        assert meta["itc_pct"] == 30.0
        assert meta["solar_cost_per_kw_dc"] == 1200.0

    def test_npv_winner_has_max_npv(self) -> None:
        """npv winner has the highest NPV across all valid points."""
        site = _base_site()

        # Boost energy at a specific point → higher revenue → higher NPV
        target = (0.35, 1.40)
        point_energies = {target: 200_000_000.0}

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_per_point_pysam_side_effect(
                point_energies, default_energy=30_000_000.0
            ),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                **ECONOMIC_KWARGS,
                **REVENUE_KWARGS,
            )

        w = out["winners"]["npv"]
        assert (w["gcr"], w["dcac_ratio"]) == target

        # Winner NPV is the max across valid points
        valid = [p for p in out["sweep_results"] if not p["failed"]]
        max_npv = max(p["npv"] for p in valid)
        assert w["npv"] == max_npv


# ---------------------------------------------------------------------------
# k2. retain_hourly flag — opt-in inclusion of the 8760 AC array
# ---------------------------------------------------------------------------


class TestRetainHourly:
    """retain_hourly=True carries ac_hourly_kw on each point; default omits."""

    def test_retain_hourly_true_includes_8760_array(self) -> None:
        """Every sweep_results entry has an 8760-element ac_hourly_kw list."""
        site = _base_site()

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_constant_pysam_side_effect(30_000_000.0),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                gcr_range=(0.35, 0.40),  # small sweep: 2 GCRs × 10 DCACs = 20
                retain_hourly=True,
                **ECONOMIC_KWARGS,
            )

        # Metadata flag
        assert out["sweep_metadata"]["retain_hourly"] is True

        # Every (non-failed) point carries an 8760-element list
        assert len(out["sweep_results"]) == 20
        for p in out["sweep_results"]:
            assert "ac_hourly_kw" in p
            if not p["failed"]:
                assert isinstance(p["ac_hourly_kw"], list)
                assert len(p["ac_hourly_kw"]) == 8760
                # The stub fills with annual_energy_kwh / 8760
                expected_hour_kw = 30_000_000.0 / 8760
                assert p["ac_hourly_kw"][0] == pytest.approx(expected_hour_kw)

    def test_retain_hourly_false_omits_key(self) -> None:
        """Default omits ac_hourly_kw from sweep_results; metadata flag False."""
        site = _base_site()

        cec_p, cfg_p, sim_p = _patch_heavy_deps()
        with cec_p, cfg_p, sim_p, patch(
            "src.optimization.solar_optimizer._run_single_pysam",
            side_effect=_constant_pysam_side_effect(30_000_000.0),
        ):
            out = optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                gcr_range=(0.35, 0.40),
                **ECONOMIC_KWARGS,
                # retain_hourly omitted → defaults to False
            )

        # Metadata flag False
        assert out["sweep_metadata"]["retain_hourly"] is False

        # No sweep_results entry has ac_hourly_kw
        assert len(out["sweep_results"]) == 20
        for p in out["sweep_results"]:
            assert "ac_hourly_kw" not in p


# ---------------------------------------------------------------------------
# l. Input validation — partial economic / revenue inputs raise ValueError
# ---------------------------------------------------------------------------


class TestInputValidation:
    """Partial economic or revenue inputs should raise ValueError eagerly."""

    def test_partial_economic_inputs_raises(self) -> None:
        """Providing only some economic inputs raises ValueError."""
        site = _base_site()

        with pytest.raises(ValueError, match="Partial economic inputs"):
            optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                solar_cost_per_kw_dc=1200.0,  # only 1 of 6
            )

    def test_partial_economic_inputs_missing_one_raises(self) -> None:
        """Providing 5 of 6 economic inputs still raises."""
        site = _base_site()
        partial = dict(ECONOMIC_KWARGS)
        del partial["itc_pct"]  # drop one

        with pytest.raises(ValueError, match="Partial economic inputs"):
            optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                **partial,
            )

    def test_partial_revenue_inputs_raises_price_only(self) -> None:
        """Providing energy_price without escalator raises."""
        site = _base_site()

        with pytest.raises(ValueError, match="Partial revenue inputs"):
            optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                **ECONOMIC_KWARGS,
                energy_price_per_kwh=0.05,
                # missing escalator
            )

    def test_partial_revenue_inputs_raises_escalator_only(self) -> None:
        """Providing escalator without price raises."""
        site = _base_site()

        with pytest.raises(ValueError, match="Partial revenue inputs"):
            optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                **ECONOMIC_KWARGS,
                energy_cost_escalator_pct=2.0,
                # missing price
            )

    def test_revenue_without_economics_raises(self) -> None:
        """Full revenue inputs but no economics raises."""
        site = _base_site()

        with pytest.raises(ValueError, match="Revenue inputs require full economic"):
            optimize_solar(
                base_site_config=site,
                buildable_acres=100.0,
                **REVENUE_KWARGS,
                # no economics
            )
