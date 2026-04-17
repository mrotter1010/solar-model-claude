"""Tests for optimize_solar_bess() — joint solar+BESS layout optimization.

Most tests mock optimize_solar() (to supply canned solar sweeps) and either
_evaluate_bess_config() (for high-level winner/structure checks) or the
underlying dispatch functions — run_bess_dispatch, run_ftm_dispatch — for
the dispatch-wiring checks. No real PySAM or LP solves.

The module-scoped autouse fixture `_mock_calculate_bill` replaces
calculate_bill() globally so that the BTM solar_only_bill precomputation is
a no-op (returns a stand-in object) for tests that pass a dummy rate_schedule
/ load_profile. Tests that care about calculate_bill being invoked a specific
number of times can introspect the mock directly.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.config.loader import load_config
from src.optimization.bess_optimizer import (
    DEFAULT_BESS_DURATIONS_HR,
    DEFAULT_BESS_POWER_FRACTIONS,
    _evaluate_bess_config,
    build_bess_sweep_grid,
    optimize_solar_bess,
)


# --- Global autouse fixture: stub out calculate_bill ---------------------
# optimize_solar_bess() now precomputes a solar-only bill per valid solar
# design by calling calculate_bill(). The unit tests in this module pass
# object() as rate_schedule / load_profile, which would blow up inside the
# real calculate_bill(). We intercept it at import site and return a sentinel
# MagicMock so the precomputation is harmless. Tests that need to assert on
# calculate_bill invocation counts use the returned mock via the fixture.


@pytest.fixture(autouse=True)
def _mock_calculate_bill():
    """Module-wide patch of calculate_bill to a MagicMock."""
    with patch(
        "src.optimization.bess_optimizer.calculate_bill"
    ) as mock_bill:
        mock_bill.return_value = MagicMock(name="fake_solar_only_bill")
        yield mock_bill


FIXTURES_DIR = Path(__file__).parent / "fixtures"
SINGLE_ROW_CSV = FIXTURES_DIR / "single_row_test.csv"


# Standard NPV-mode inputs used across solar+BESS tests.
NPV_KWARGS = {
    "solar_cost_per_kw_dc": 1200.0,
    "solar_opex_per_kw_dc_year": 20.0,
    "discount_rate_pct": 7.0,
    "project_lifetime_years": 25,
    "degradation_pct": 0.5,
    "itc_pct": 30.0,
    "energy_price_per_kwh": 0.05,
    "energy_cost_escalator_pct": 2.0,
}


def _base_site():
    """Load the single-row test SiteConfig fixture."""
    return load_config(SINGLE_ROW_CSV)[0]


# --- Canned solar sweep helpers --------------------------------------------


def _make_solar_point(
    gcr: float,
    dcac: float,
    mw_dc: float,
    mw_ac: float,
    npv: float,
    annual_energy_kwh: float = 30_000_000.0,
    capacity_factor_ac: float = 32.0,
    capex_total: float = 300_000_000.0,
    opex_per_year: float = 5_000_000.0,
    failed: bool = False,
    error: str | None = None,
) -> dict:
    """Build a sweep-point dict matching optimize_solar's _empty_point shape.

    Includes an 8760-element "ac_hourly_kw" list to mirror what real
    optimize_solar returns when called with retain_hourly=True (which
    optimize_solar_bess now does internally).
    """
    return {
        "gcr": gcr,
        "dcac_ratio": dcac,
        "mw_dc": mw_dc,
        "mw_ac": mw_ac,
        "num_modules": 10_000,
        "annual_energy_kwh": annual_energy_kwh,
        "annual_energy_mwh": annual_energy_kwh / 1000,
        "capacity_factor_ac": capacity_factor_ac,
        "clipping_correction_pct": 0.4,
        "capex_total": capex_total,
        "opex_per_year": opex_per_year,
        "net_capex": capex_total * 0.70,
        "lcoe_per_mwh": 40.0,
        "lcoe_per_kwh": 0.04,
        "annual_revenue_year1": 1_500_000.0,
        "npv": npv,
        "simple_payback_years": 10.0,
        "irr": 0.09,
        "failed": failed,
        "error": error,
        "ac_hourly_kw": (
            None if failed else [annual_energy_kwh / 8760] * 8760
        ),
    }


def _make_solar_result(
    points: list[dict],
    mode: str = "npv",
    racking: str = "tracker",
    buildable_acres: float = 100.0,
) -> dict:
    """Build a fake optimize_solar() return for the given sweep points."""
    valid = [p for p in points if not p["failed"]]
    winners = {
        "max_production": max(valid, key=lambda p: p["annual_energy_kwh"]),
        "max_yield": max(valid, key=lambda p: p["capacity_factor_ac"]),
    }
    if mode in ("lcoe", "npv"):
        winners["lcoe"] = min(valid, key=lambda p: p["lcoe_per_mwh"])
    if mode == "npv":
        winners["npv"] = max(valid, key=lambda p: p["npv"])

    return {
        "mode": mode,
        "winners": winners,
        "sweep_results": points,
        "sweep_metadata": {
            "mode": mode,
            "racking": racking,
            "buildable_acres": buildable_acres,
            "utilization_factor": 0.75,
            "module_power_w": 551.0,
            "module_area_m2": 2.51,
            "gcr_range": (0.30, 0.60),
            "dcac_range": (1.15, 1.60),
            "total_combinations": len(points),
            "failed_combinations": sum(1 for p in points if p["failed"]),
            "itc_pct": 30.0,
            "discount_rate_pct": 7.0,
            "project_lifetime_years": 25,
            "degradation_pct": 0.5,
            "solar_cost_per_kw_dc": 1200.0,
            "solar_opex_per_kw_dc_year": 20.0,
            "energy_price_per_kwh": 0.05,
            "energy_cost_escalator_pct": 2.0,
            "retain_hourly": True,
        },
    }


def _canned_two_point_solar_result() -> dict:
    """Two-point sweep: distinct GCR/DCAC/MW_AC/NPV for winner selection."""
    return _make_solar_result(
        [
            _make_solar_point(
                gcr=0.30, dcac=1.20, mw_dc=100.0, mw_ac=83.333,
                npv=10_000_000.0,
            ),
            _make_solar_point(
                gcr=0.50, dcac=1.40, mw_dc=150.0, mw_ac=107.143,
                npv=15_000_000.0,
            ),
        ]
    )


def _make_bess_eval(
    power_mw: float,
    duration_hr: float,
    bess_npv: float,
    bess_cost_per_kwh: float = 340.0,
    bess_opex_per_kw_year: float = 25.0,
    dispatch_mode: str = "btm",
    charging_mode: str = "solar_and_grid",
    failed: bool = False,
    error: str | None = None,
) -> dict:
    """Build a canned _evaluate_bess_config return dict."""
    capacity_mwh = round(power_mw * duration_hr, 3)
    return {
        "bess_power_mw": power_mw,
        "bess_duration_hr": duration_hr,
        "bess_capacity_mwh": capacity_mwh,
        "bess_capex": round(capacity_mwh * 1000 * bess_cost_per_kwh, 2),
        "bess_opex_per_year": round(
            power_mw * 1000 * bess_opex_per_kw_year, 2
        ),
        "bess_annual_revenue": 0.0,
        "bess_npv": bess_npv,
        "dispatch_mode": dispatch_mode,
        "charging_mode": charging_mode,
        "failed": failed,
        "error": error,
    }


# ---------------------------------------------------------------------------
# a. build_bess_sweep_grid defaults
# ---------------------------------------------------------------------------


class TestBuildBessSweepGridDefaults:
    """Default grid: 5 power × 2 duration = 10 combos, scaled to solar_mw_ac."""

    def test_default_grid_has_10_combos(self) -> None:
        """Defaults produce the expected 5×2 = 10 combos."""
        grid = build_bess_sweep_grid(solar_mw_ac=100.0)
        assert len(grid) == 10

    def test_power_scales_with_solar_mw_ac(self) -> None:
        """Each combo's power_mw = fraction × solar_mw_ac, rounded to 3 dp."""
        solar_mw_ac = 87.5
        grid = build_bess_sweep_grid(solar_mw_ac=solar_mw_ac)

        expected_powers = [
            round(f * solar_mw_ac, 3) for f in DEFAULT_BESS_POWER_FRACTIONS
        ]
        observed_powers = sorted({c["power_mw"] for c in grid})
        assert observed_powers == sorted(expected_powers)

    def test_durations_are_defaults(self) -> None:
        """Every combo's duration is one of the default durations."""
        grid = build_bess_sweep_grid(solar_mw_ac=50.0)
        observed_durations = sorted({c["duration_hr"] for c in grid})
        assert observed_durations == sorted(DEFAULT_BESS_DURATIONS_HR)

    def test_capacity_matches_power_times_duration(self) -> None:
        """capacity_mwh = round(power_mw × duration_hr, 3) for every combo."""
        grid = build_bess_sweep_grid(solar_mw_ac=100.0)
        for combo in grid:
            assert combo["capacity_mwh"] == pytest.approx(
                combo["power_mw"] * combo["duration_hr"], rel=1e-6
            )

    def test_full_cartesian_product(self) -> None:
        """Every (fraction × solar_mw_ac, duration) pair appears exactly once."""
        solar_mw_ac = 100.0
        grid = build_bess_sweep_grid(solar_mw_ac=solar_mw_ac)
        pairs = {(c["power_mw"], c["duration_hr"]) for c in grid}
        expected = {
            (round(f * solar_mw_ac, 3), d)
            for f in DEFAULT_BESS_POWER_FRACTIONS
            for d in DEFAULT_BESS_DURATIONS_HR
        }
        assert pairs == expected


# ---------------------------------------------------------------------------
# b. build_bess_sweep_grid custom
# ---------------------------------------------------------------------------


class TestBuildBessSweepGridCustom:
    """Custom fractions and durations produce the expected combos."""

    def test_custom_fractions_and_durations(self) -> None:
        """3 fractions × 4 durations → 12 combos with custom values."""
        grid = build_bess_sweep_grid(
            solar_mw_ac=200.0,
            power_fractions=[0.10, 0.30, 0.50],
            durations_hr=[1.0, 2.0, 4.0, 6.0],
        )
        assert len(grid) == 12
        assert sorted({c["power_mw"] for c in grid}) == [20.0, 60.0, 100.0]
        assert sorted({c["duration_hr"] for c in grid}) == [1.0, 2.0, 4.0, 6.0]

    def test_rejects_zero_mw_ac(self) -> None:
        """solar_mw_ac must be strictly positive."""
        with pytest.raises(ValueError, match="solar_mw_ac must be > 0"):
            build_bess_sweep_grid(solar_mw_ac=0.0)

    def test_rejects_empty_power_fractions(self) -> None:
        """Empty power_fractions is not allowed."""
        with pytest.raises(ValueError, match="power_fractions"):
            build_bess_sweep_grid(solar_mw_ac=100.0, power_fractions=[])

    def test_rejects_empty_durations(self) -> None:
        """Empty durations_hr is not allowed."""
        with pytest.raises(ValueError, match="durations_hr"):
            build_bess_sweep_grid(solar_mw_ac=100.0, durations_hr=[])


# ---------------------------------------------------------------------------
# c. optimize_solar_bess — happy path with mocks
# ---------------------------------------------------------------------------


class TestOptimizeSolarBessHappyPath:
    """Solar mocked, BESS stub mocked: verify wiring and winner selection."""

    def test_solar_sweep_called_first_with_npv_inputs(self) -> None:
        """optimize_solar is called once, in NPV mode, with retain_hourly=True."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ) as mock_solar, patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=lambda **kw: _make_bess_eval(
                power_mw=kw["bess_power_mw"],
                duration_hr=kw["bess_duration_hr"],
                bess_npv=0.0,
            ),
        ):
            optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

        # optimize_solar called exactly once with NPV-mode revenue kwargs
        assert mock_solar.call_count == 1
        kwargs = mock_solar.call_args.kwargs
        assert kwargs["energy_price_per_kwh"] == 0.05
        assert kwargs["energy_cost_escalator_pct"] == 2.0
        assert kwargs["solar_cost_per_kw_dc"] == 1200.0
        # retain_hourly must be True so BESS dispatch can see the 8760 series
        assert kwargs["retain_hourly"] is True

    def test_hourly_production_forwarded_to_bess_eval(self) -> None:
        """Each _evaluate_bess_config call receives the solar point's 8760 list."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()
        captured_hourly: list[list[float]] = []

        def _capture_eval(**kwargs):
            captured_hourly.append(kwargs["solar_production_kwh"])
            return _make_bess_eval(
                power_mw=kwargs["bess_power_mw"],
                duration_hr=kwargs["bess_duration_hr"],
                bess_npv=0.0,
            )

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=_capture_eval,
        ):
            optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

        # 20 BESS combos captured, every one received an 8760-element list
        assert len(captured_hourly) == 20
        for hourly in captured_hourly:
            assert isinstance(hourly, list)
            assert len(hourly) == 8760

    def test_bess_sweep_iterates_over_valid_solar_x_bess(self) -> None:
        """2 valid solar × 10 default BESS = 20 BESS evaluations."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()
        call_count = {"n": 0}

        def _fake_eval(**kwargs):
            call_count["n"] += 1
            return _make_bess_eval(
                power_mw=kwargs["bess_power_mw"],
                duration_hr=kwargs["bess_duration_hr"],
                bess_npv=1_000_000.0,
            )

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=_fake_eval,
        ):
            out = optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

        assert call_count["n"] == 20  # 2 solar × (5 × 2) BESS
        assert len(out["bess_sweep_results"]) == 20
        assert out["sweep_metadata"]["total_bess_combinations"] == 20
        assert out["sweep_metadata"]["failed_bess_combinations"] == 0

    def test_combined_npv_winner_selected(self) -> None:
        """Winner maximizes solar_npv + bess_npv across all combos."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()
        # Solar NPVs: 10M (gcr=0.30, mw_ac=83.333) and 15M (gcr=0.50, mw_ac=107.143).
        # Craft BESS to make (gcr=0.30, 0.70-fraction, 4hr) beat all:
        #   combined = 10M + 10M = 20M
        # vs solar-only best (gcr=0.50) + typical BESS 1M = 16M.
        target_power = round(0.70 * 83.333, 3)

        def _fake_eval(**kw):
            power = kw["bess_power_mw"]
            dur = kw["bess_duration_hr"]
            if power == target_power and dur == 4.0:
                return _make_bess_eval(power, dur, bess_npv=10_000_000.0)
            return _make_bess_eval(power, dur, bess_npv=1_000_000.0)

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=_fake_eval,
        ):
            out = optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

        winner = out["winners"]["solar_bess_npv"]
        assert winner["solar_gcr"] == 0.30
        assert winner["solar_dcac"] == 1.20
        assert winner["bess_duration_hr"] == 4.0
        assert winner["bess_power_mw"] == target_power
        assert winner["combined_npv"] == pytest.approx(20_000_000.0)

        # bess_adds_value: combined 20M > solar-only 15M
        assert out["bess_adds_value"] is True

        # bess_winner_detail structure
        detail = out["bess_winner_detail"]
        assert detail["solar"]["gcr"] == 0.30
        assert detail["bess"]["duration_hr"] == 4.0
        assert detail["combined"]["npv"] == pytest.approx(20_000_000.0)
        assert detail["combined"]["solar_npv"] == 10_000_000.0
        assert detail["combined"]["bess_npv"] == 10_000_000.0
        assert detail["combined"]["total_capex"] == pytest.approx(
            detail["solar"]["capex_total"] + detail["bess"]["capex"]
        )

    def test_return_structure_and_metadata(self) -> None:
        """Return dict has all documented keys with expected values."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=lambda **kw: _make_bess_eval(
                power_mw=kw["bess_power_mw"],
                duration_hr=kw["bess_duration_hr"],
                bess_npv=500_000.0,
            ),
        ):
            out = optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

        # Top-level shape
        assert out["mode"] == "solar_bess"
        assert set(out.keys()) >= {
            "mode",
            "winners",
            "bess_adds_value",
            "bess_winner_detail",
            "solar_sweep_results",
            "bess_sweep_results",
            "sweep_metadata",
        }

        # Winners dict
        assert set(out["winners"].keys()) == {
            "max_production",
            "max_yield",
            "solar_only_npv",
            "solar_bess_npv",
        }

        # solar_only_npv is the NPV winner from the solar sweep (15M here)
        assert out["winners"]["solar_only_npv"]["npv"] == 15_000_000.0

        # solar_sweep_results is forwarded from optimize_solar
        assert len(out["solar_sweep_results"]) == 2

        # Metadata shape
        meta = out["sweep_metadata"]
        assert meta["mode"] == "solar_bess"
        assert meta["dispatch_mode"] == "btm"
        assert meta["charging_mode"] == "solar_and_grid"
        assert meta["total_solar_combinations"] == 2
        assert meta["total_bess_combinations"] == 20
        assert meta["failed_bess_combinations"] == 0
        assert meta["bess_power_fractions"] == DEFAULT_BESS_POWER_FRACTIONS
        assert meta["bess_durations_hr"] == DEFAULT_BESS_DURATIONS_HR
        assert meta["bess_cost_per_kwh"] == 340.0
        assert meta["bess_opex_per_kw_year"] == 25.0
        # Solar metadata is carried forward
        assert meta["racking"] == "tracker"
        assert meta["buildable_acres"] == 100.0


# ---------------------------------------------------------------------------
# d. BESS adds no value → bess_adds_value=False
# ---------------------------------------------------------------------------


class TestBessAddsNoValue:
    """When BESS NPV is zero/negative, combined NPV doesn't beat solar-only."""

    def test_zero_bess_npv(self) -> None:
        """bess_npv=0 → combined_npv == solar_npv; doesn't exceed solar-only."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=lambda **kw: _make_bess_eval(
                power_mw=kw["bess_power_mw"],
                duration_hr=kw["bess_duration_hr"],
                bess_npv=0.0,
            ),
        ):
            out = optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

        # Best combined NPV = max solar NPV + 0 = 15M, equal to solar-only.
        assert out["winners"]["solar_bess_npv"]["combined_npv"] == 15_000_000.0
        assert out["bess_adds_value"] is False

    def test_negative_bess_npv(self) -> None:
        """Every bess_npv negative → combined < solar-only NPV."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=lambda **kw: _make_bess_eval(
                power_mw=kw["bess_power_mw"],
                duration_hr=kw["bess_duration_hr"],
                bess_npv=-1_000_000.0,
            ),
        ):
            out = optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

        assert out["bess_adds_value"] is False


# ---------------------------------------------------------------------------
# e. Input validation
# ---------------------------------------------------------------------------


class TestInputValidation:
    """Required inputs per mode, exclusive BTM vs FTM, valid enum values."""

    def test_btm_without_rate_schedule_raises(self) -> None:
        """BTM mode requires rate_schedule."""
        site = _base_site()
        with pytest.raises(ValueError, match="BTM dispatch requires"):
            optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=None,
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

    def test_btm_without_load_profile_raises(self) -> None:
        """BTM mode requires load_profile."""
        site = _base_site()
        with pytest.raises(ValueError, match="BTM dispatch requires"):
            optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=None,
                **NPV_KWARGS,
            )

    def test_ftm_without_lmp_data_raises(self) -> None:
        """FTM mode requires lmp_data."""
        site = _base_site()
        with pytest.raises(ValueError, match="FTM dispatch requires"):
            optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="ftm",
                lmp_data=None,
                **NPV_KWARGS,
            )

    def test_both_btm_and_ftm_inputs_raises(self) -> None:
        """Providing both BTM and FTM inputs is an error."""
        site = _base_site()
        with pytest.raises(
            ValueError, match="Cannot provide both BTM .* and FTM"
        ):
            optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                lmp_data=object(),
                **NPV_KWARGS,
            )

    def test_invalid_dispatch_mode_raises(self) -> None:
        """dispatch_mode must be 'btm' or 'ftm'."""
        site = _base_site()
        with pytest.raises(ValueError, match="dispatch_mode must be"):
            optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="hybrid",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

    def test_invalid_charging_mode_raises(self) -> None:
        """charging_mode must be 'solar_and_grid' or 'solar_only'."""
        site = _base_site()
        with pytest.raises(ValueError, match="charging_mode must be"):
            optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                charging_mode="grid_only",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )


# ---------------------------------------------------------------------------
# f. Progress callback
# ---------------------------------------------------------------------------


class TestProgressCallback:
    """Callback is invoked with 'bess_sweep' stage during the BESS loop."""

    def test_callback_receives_bess_sweep_events(self) -> None:
        """Every BESS combo triggers a 'bess_sweep' callback with monotonic idx."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()
        events: list[tuple[int, int, str]] = []

        def on_progress(i, total, stage, result):
            events.append((i, total, stage))

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=lambda **kw: _make_bess_eval(
                power_mw=kw["bess_power_mw"],
                duration_hr=kw["bess_duration_hr"],
                bess_npv=100_000.0,
            ),
        ):
            optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                progress_callback=on_progress,
                **NPV_KWARGS,
            )

        bess_events = [e for e in events if e[2] == "bess_sweep"]
        assert len(bess_events) == 20
        indices = [e[0] for e in bess_events]
        assert indices == list(range(1, 21))
        assert all(e[1] == 20 for e in bess_events)

    def test_callback_exception_does_not_abort_bess_sweep(self) -> None:
        """A raising callback is swallowed; the sweep completes."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()
        call_count = {"n": 0}

        def bad_callback(i, total, stage, result):
            call_count["n"] += 1
            if stage == "bess_sweep" and i == 3:
                raise ValueError("boom")

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=lambda **kw: _make_bess_eval(
                power_mw=kw["bess_power_mw"],
                duration_hr=kw["bess_duration_hr"],
                bess_npv=100_000.0,
            ),
        ):
            out = optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                progress_callback=bad_callback,
                **NPV_KWARGS,
            )

        assert len(out["bess_sweep_results"]) == 20
        # Callback ran at least once per BESS combo despite the raise
        assert call_count["n"] >= 20


# ---------------------------------------------------------------------------
# g. Partial BESS failures
# ---------------------------------------------------------------------------


class TestPartialBessFailures:
    """Some BESS evaluations fail; sweep completes with winner from valid ones."""

    def test_partial_bess_failures_recorded(self) -> None:
        """A subset of eval calls raise; failed flags recorded, winner valid."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()
        call_counter = {"n": 0}

        def _fake_eval(**kwargs):
            call_counter["n"] += 1
            # Fail every 3rd call
            if call_counter["n"] % 3 == 0:
                raise RuntimeError("dispatch LP infeasible")
            return _make_bess_eval(
                power_mw=kwargs["bess_power_mw"],
                duration_hr=kwargs["bess_duration_hr"],
                bess_npv=500_000.0,
            )

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=_fake_eval,
        ):
            out = optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

        # All 20 attempted combos appear in results; some failed
        assert len(out["bess_sweep_results"]) == 20
        failed = [r for r in out["bess_sweep_results"] if r["failed"]]
        valid = [r for r in out["bess_sweep_results"] if not r["failed"]]
        assert len(failed) > 0
        assert len(valid) > 0
        assert (
            out["sweep_metadata"]["failed_bess_combinations"]
            == len(failed)
        )

        # Failed entries carry error strings and null NPV fields
        for r in failed:
            assert r["error"]
            assert r["combined_npv"] is None
            assert r["bess_npv"] is None

        # Winner is from the valid pool
        winner = out["winners"]["solar_bess_npv"]
        assert winner["failed"] is False
        assert winner["combined_npv"] is not None


# ---------------------------------------------------------------------------
# h. All BESS evaluations fail → fall back to solar-only
# ---------------------------------------------------------------------------


class TestAllBessFailures:
    """When every BESS eval fails, return solar-only winners with flag off."""

    def test_all_failures_return_solar_only(self) -> None:
        """All BESS evals raise → bess_adds_value=False, no bess winner."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=RuntimeError("all LPs infeasible"),
        ):
            out = optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

        # Solar winners still present
        assert out["winners"]["max_production"] is not None
        assert out["winners"]["max_yield"] is not None
        assert out["winners"]["solar_only_npv"]["npv"] == 15_000_000.0

        # No solar+BESS winner, no detail, flag is False
        assert out["winners"]["solar_bess_npv"] is None
        assert out["bess_winner_detail"] is None
        assert out["bess_adds_value"] is False
        assert out["sweep_metadata"]["failed_bess_combinations"] == 20


# ---------------------------------------------------------------------------
# i. Direct-call behavior of _evaluate_bess_config (costs + happy paths)
# ---------------------------------------------------------------------------


def _fake_btm_dispatch_return(
    bess_incremental_savings: float = 100_000.0,
    annual_degradation_pct: float = 1.0,
):
    """Build a (DispatchResult, BESSBillComparison) pair with a MagicMock shape."""
    fake_dispatch = MagicMock(name="fake_dispatch_result")
    fake_dispatch.metrics.estimated_annual_degradation_pct = annual_degradation_pct
    fake_bill = MagicMock(name="fake_bill_comparison")
    fake_bill.bess_incremental_savings = bess_incremental_savings
    return fake_dispatch, fake_bill


def _fake_ftm_dispatch_return(
    ftm_revenue: float = 200_000.0,
    annual_degradation_pct: float = 1.5,
):
    """Build a DispatchResult-shaped MagicMock for FTM dispatch."""
    fake_dispatch = MagicMock(name="fake_ftm_dispatch_result")
    fake_dispatch.ftm_revenue = ftm_revenue
    fake_dispatch.metrics.estimated_annual_degradation_pct = annual_degradation_pct
    return fake_dispatch


class TestEvaluateBessConfigDirectCall:
    """Direct-call wiring: capex/opex always populated; dispatch drives NPV."""

    def test_btm_capex_and_opex_independent_of_dispatch(self) -> None:
        """Capex = capacity × cost_per_kwh; opex = power × opex_rate."""
        site = _base_site()

        with patch(
            "src.optimization.bess_optimizer.run_bess_dispatch",
            return_value=_fake_btm_dispatch_return(
                bess_incremental_savings=0.0, annual_degradation_pct=1.0
            ),
        ):
            out = _evaluate_bess_config(
                site_config=site,
                solar_production_kwh=[0.0] * 8760,
                bess_power_mw=2.0,
                bess_duration_hr=4.0,
                dispatch_mode="btm",
                charging_mode="solar_and_grid",
                rate_schedule=object(),
                load_profile=MagicMock(),
                solar_only_bill=object(),
                bess_cost_per_kwh=340.0,
                bess_opex_per_kw_year=25.0,
            )

        # capacity = 2.0 × 4.0 = 8.0 MWh; capex_gross = 8 × 1000 × 340
        assert out["bess_capacity_mwh"] == 8.0
        assert out["bess_capex"] == 2_720_000.0
        # opex = 2.0 × 1000 × 25 = $50,000/yr
        assert out["bess_opex_per_year"] == 50_000.0
        assert out["failed"] is False
        assert out["error"] is None
        assert out["dispatch_mode"] == "btm"
        assert out["charging_mode"] == "solar_and_grid"

    def test_btm_missing_solar_only_bill_fails_gracefully(self) -> None:
        """BTM without a precomputed solar_only_bill returns failed=True."""
        site = _base_site()

        out = _evaluate_bess_config(
            site_config=site,
            solar_production_kwh=[0.0] * 8760,
            bess_power_mw=2.0,
            bess_duration_hr=4.0,
            dispatch_mode="btm",
            charging_mode="solar_and_grid",
            rate_schedule=object(),
            load_profile=MagicMock(),
            solar_only_bill=None,
            bess_cost_per_kwh=340.0,
            bess_opex_per_kw_year=25.0,
        )

        assert out["failed"] is True
        assert "solar_only_bill" in (out["error"] or "")


# ---------------------------------------------------------------------------
# j. BTM dispatch wiring — run_bess_dispatch gets correct args; NPV flows
# ---------------------------------------------------------------------------


class TestBtmDispatchWiring:
    """_evaluate_bess_config (BTM) calls run_bess_dispatch and wires NPV."""

    def test_run_bess_dispatch_called_with_temp_site_and_bill(self) -> None:
        """Mock run_bess_dispatch; verify positional args and temp config fields."""
        site = _base_site()
        hourly = [10.0] * 8760
        fake_rate = object()
        fake_load = object()
        fake_bill = MagicMock(name="solar_only_bill")

        with patch(
            "src.optimization.bess_optimizer.run_bess_dispatch",
            return_value=_fake_btm_dispatch_return(),
        ) as mock_dispatch:
            _evaluate_bess_config(
                site_config=site,
                solar_production_kwh=hourly,
                bess_power_mw=5.0,
                bess_duration_hr=4.0,
                dispatch_mode="btm",
                charging_mode="solar_only",
                rate_schedule=fake_rate,
                load_profile=fake_load,
                solar_only_bill=fake_bill,
                bess_cost_per_kwh=340.0,
                bess_opex_per_kw_year=25.0,
                itc_pct=30.0,
                discount_rate_pct=7.0,
                project_lifetime_years=25,
                rate_escalation_pct=2.0,
            )

        # Confirm run_bess_dispatch was called exactly once with the right args
        assert mock_dispatch.call_count == 1
        call_args = mock_dispatch.call_args.args
        assert len(call_args) == 5
        temp_site_cfg, prod, rate, load, bill = call_args
        assert prod is hourly
        assert rate is fake_rate
        assert load is fake_load
        assert bill is fake_bill

        # Temp SiteConfig has BESS fields set per the combo
        assert temp_site_cfg.bess_power_mw == 5.0
        assert temp_site_cfg.bess_duration_hr == 4.0
        assert temp_site_cfg.bess_installed_cost_per_kwh == 340.0
        # charging_mode=solar_only → bess_solar_only_charging=True
        assert temp_site_cfg.bess_solar_only_charging is True
        assert temp_site_cfg.bess_grid_only_charging is False

    def test_solar_and_grid_charging_mode_sets_flag_false(self) -> None:
        """charging_mode=solar_and_grid → bess_solar_only_charging=False."""
        site = _base_site()

        with patch(
            "src.optimization.bess_optimizer.run_bess_dispatch",
            return_value=_fake_btm_dispatch_return(),
        ) as mock_dispatch:
            _evaluate_bess_config(
                site_config=site,
                solar_production_kwh=[1.0] * 8760,
                bess_power_mw=1.0,
                bess_duration_hr=2.0,
                dispatch_mode="btm",
                charging_mode="solar_and_grid",
                rate_schedule=object(),
                load_profile=MagicMock(),
                solar_only_bill=object(),
                bess_cost_per_kwh=340.0,
                bess_opex_per_kw_year=25.0,
            )

        temp_site_cfg = mock_dispatch.call_args.args[0]
        assert temp_site_cfg.bess_solar_only_charging is False
        assert temp_site_cfg.bess_grid_only_charging is False

    def test_bess_incremental_savings_flows_to_revenue_and_npv(self) -> None:
        """Incremental savings + ITC-adjusted cost flow into compute_bess_npv."""
        site = _base_site()
        incremental_savings = 250_000.0
        degradation_pct = 1.5

        with patch(
            "src.optimization.bess_optimizer.run_bess_dispatch",
            return_value=_fake_btm_dispatch_return(
                bess_incremental_savings=incremental_savings,
                annual_degradation_pct=degradation_pct,
            ),
        ), patch(
            "src.optimization.bess_optimizer.compute_bess_npv",
            return_value=9_999_999.0,
        ) as mock_npv:
            out = _evaluate_bess_config(
                site_config=site,
                solar_production_kwh=[1.0] * 8760,
                bess_power_mw=4.0,
                bess_duration_hr=2.0,
                dispatch_mode="btm",
                charging_mode="solar_and_grid",
                rate_schedule=object(),
                load_profile=MagicMock(),
                solar_only_bill=object(),
                bess_cost_per_kwh=340.0,
                bess_opex_per_kw_year=25.0,
                itc_pct=30.0,
                discount_rate_pct=7.0,
                project_lifetime_years=25,
                rate_escalation_pct=2.0,
            )

        # bess_annual_revenue was pulled from bill_comparison
        assert out["bess_annual_revenue"] == round(incremental_savings, 2)
        # bess_npv is whatever compute_bess_npv returned
        assert out["bess_npv"] == 9_999_999.0

        # Verify compute_bess_npv got the right parameters
        call_kwargs = mock_npv.call_args.kwargs
        assert call_kwargs["bess_incremental_savings"] == incremental_savings
        # Gross capex = 4.0 × 2.0 × 1000 × 340 = $2,720,000;
        # net (ITC 30%) = 2,720,000 × 0.70 = $1,904,000
        assert call_kwargs["bess_installed_cost"] == pytest.approx(1_904_000.0)
        assert call_kwargs["bess_annual_degradation_pct"] == degradation_pct
        assert call_kwargs["rate_escalation_pct"] == 2.0
        assert call_kwargs["bess_opex_per_kw_year"] == 25.0
        assert call_kwargs["bess_power_kw"] == 4_000.0
        assert call_kwargs["discount_rate_pct"] == 7.0
        assert call_kwargs["project_lifetime_years"] == 25


# ---------------------------------------------------------------------------
# k. FTM dispatch wiring — run_ftm_dispatch gets correct args; NPV flows
# ---------------------------------------------------------------------------


class TestFtmDispatchWiring:
    """_evaluate_bess_config (FTM) calls run_ftm_dispatch and wires NPV."""

    def test_run_ftm_dispatch_called_with_temp_site_and_lmp(self) -> None:
        """Mock run_ftm_dispatch; verify positional args and temp config."""
        site = _base_site()
        hourly = [12.0] * 8760
        fake_lmp = MagicMock(name="lmp_data")
        fake_lmp.prices = [50.0] * 8760

        with patch(
            "src.optimization.bess_optimizer.run_ftm_dispatch",
            return_value=_fake_ftm_dispatch_return(),
        ) as mock_ftm, patch(
            "src.optimization.bess_optimizer.compute_ftm_solar_revenue",
            return_value=30_000.0,
        ):
            _evaluate_bess_config(
                site_config=site,
                solar_production_kwh=hourly,
                bess_power_mw=3.0,
                bess_duration_hr=4.0,
                dispatch_mode="ftm",
                charging_mode="solar_only",
                lmp_data=fake_lmp,
                bess_cost_per_kwh=340.0,
                bess_opex_per_kw_year=25.0,
            )

        assert mock_ftm.call_count == 1
        call_args = mock_ftm.call_args.args
        assert len(call_args) == 3
        temp_site_cfg, prod, lmp = call_args
        assert prod is hourly
        assert lmp is fake_lmp
        # Temp SiteConfig reflects the BESS combo
        assert temp_site_cfg.bess_power_mw == 3.0
        assert temp_site_cfg.bess_duration_hr == 4.0
        assert temp_site_cfg.bess_solar_only_charging is True
        assert temp_site_cfg.bess_grid_only_charging is False

    def test_arbitrage_revenue_flows_to_npv(self) -> None:
        """bess_arbitrage = ftm_revenue - solar_revenue → compute_ftm_bess_npv."""
        site = _base_site()
        ftm_revenue = 500_000.0
        solar_revenue = 300_000.0
        expected_arbitrage = ftm_revenue - solar_revenue
        degradation_pct = 1.75

        fake_lmp = MagicMock(name="lmp_data")
        fake_lmp.prices = [20.0] * 8760

        with patch(
            "src.optimization.bess_optimizer.run_ftm_dispatch",
            return_value=_fake_ftm_dispatch_return(
                ftm_revenue=ftm_revenue,
                annual_degradation_pct=degradation_pct,
            ),
        ), patch(
            "src.optimization.bess_optimizer.compute_ftm_solar_revenue",
            return_value=solar_revenue,
        ), patch(
            "src.optimization.bess_optimizer.compute_ftm_bess_npv",
            return_value=4_444_444.0,
        ) as mock_ftm_npv:
            out = _evaluate_bess_config(
                site_config=site,
                solar_production_kwh=[2.0] * 8760,
                bess_power_mw=2.0,
                bess_duration_hr=4.0,
                dispatch_mode="ftm",
                charging_mode="solar_and_grid",
                lmp_data=fake_lmp,
                bess_cost_per_kwh=340.0,
                bess_opex_per_kw_year=25.0,
                itc_pct=30.0,
                discount_rate_pct=7.0,
                project_lifetime_years=25,
                rate_escalation_pct=2.0,
            )

        assert out["bess_annual_revenue"] == round(expected_arbitrage, 2)
        assert out["bess_npv"] == 4_444_444.0

        # Verify compute_ftm_bess_npv params
        call_kwargs = mock_ftm_npv.call_args.kwargs
        assert call_kwargs["bess_arbitrage_revenue"] == expected_arbitrage
        # Gross capex = 2 × 4 × 1000 × 340 = 2,720,000 → net = 1,904,000
        assert call_kwargs["bess_installed_cost"] == pytest.approx(1_904_000.0)
        assert call_kwargs["bess_annual_degradation_pct"] == degradation_pct
        assert call_kwargs["revenue_escalation_pct"] == 2.0
        assert call_kwargs["bess_power_kw"] == 2_000.0
        # ancillary comes from the base site_config (default 0.0 for this fixture)
        assert call_kwargs["ancillary_revenue_per_kw_year"] == (
            site.ancillary_revenue_per_kw_year
        )
        assert call_kwargs["discount_rate_pct"] == 7.0
        assert call_kwargs["project_lifetime_years"] == 25


# ---------------------------------------------------------------------------
# l. solar_only_bill precomputation — once per solar config (BTM only)
# ---------------------------------------------------------------------------


class TestSolarOnlyBillPrecomputation:
    """calculate_bill runs once per solar config in BTM mode, not per BESS combo."""

    def test_bill_computed_once_per_solar_config_btm(
        self, _mock_calculate_bill
    ) -> None:
        """2 valid solar configs × 10 BESS combos → 2 calculate_bill calls."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=lambda **kw: _make_bess_eval(
                power_mw=kw["bess_power_mw"],
                duration_hr=kw["bess_duration_hr"],
                bess_npv=1_000.0,
            ),
        ):
            optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

        # 2 valid solar configs → exactly 2 calculate_bill calls (not 20)
        assert _mock_calculate_bill.call_count == 2

    def test_bill_forwarded_to_each_bess_eval(
        self, _mock_calculate_bill
    ) -> None:
        """Every _evaluate_bess_config call for a solar config gets that config's bill."""
        site = _base_site()
        # Build a two-point solar result with *distinct* annual_energy_kwh so
        # the per-config production array differs (and thus the precomputed
        # bill can be sentinel-tagged per config).
        solar_canned = _make_solar_result(
            [
                _make_solar_point(
                    gcr=0.30, dcac=1.20, mw_dc=100.0, mw_ac=83.333,
                    npv=10_000_000.0, annual_energy_kwh=30_000_000.0,
                ),
                _make_solar_point(
                    gcr=0.50, dcac=1.40, mw_dc=150.0, mw_ac=107.143,
                    npv=15_000_000.0, annual_energy_kwh=45_000_000.0,
                ),
            ]
        )

        # Make calculate_bill return a distinct sentinel per solar config,
        # driven by the production_kwh sequence (which differs per config).
        sentinel_by_first_hour: dict[float, object] = {}

        def _side_effect(*, load_kwh, production_kwh, rate):
            key = production_kwh[0] if production_kwh else None
            if key not in sentinel_by_first_hour:
                sentinel_by_first_hour[key] = MagicMock(
                    name=f"bill_for_first_hour_{key}"
                )
            return sentinel_by_first_hour[key]

        _mock_calculate_bill.side_effect = _side_effect

        captured_bills_by_gcr: dict[float, list[object]] = {}

        def _capture_eval(**kwargs):
            captured_bills_by_gcr.setdefault(
                # Use solar_production_kwh[0] as a per-config discriminator
                kwargs["solar_production_kwh"][0], []
            ).append(kwargs["solar_only_bill"])
            return _make_bess_eval(
                power_mw=kwargs["bess_power_mw"],
                duration_hr=kwargs["bess_duration_hr"],
                bess_npv=1_000.0,
            )

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=_capture_eval,
        ):
            optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

        # For each solar config (keyed by first-hour production), every BESS
        # combo received the SAME precomputed bill instance.
        assert len(captured_bills_by_gcr) == 2  # 2 solar configs
        for bills in captured_bills_by_gcr.values():
            assert len(bills) == 10  # 10 BESS combos per solar
            first_bill = bills[0]
            assert all(b is first_bill for b in bills)
        # And the two solar configs got different bill sentinels
        sentinels_used = {bills[0] for bills in captured_bills_by_gcr.values()}
        assert len(sentinels_used) == 2

    def test_ftm_skips_bill_precomputation(
        self, _mock_calculate_bill
    ) -> None:
        """FTM mode does not call calculate_bill at all."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer._evaluate_bess_config",
            side_effect=lambda **kw: _make_bess_eval(
                power_mw=kw["bess_power_mw"],
                duration_hr=kw["bess_duration_hr"],
                bess_npv=1_000.0,
                dispatch_mode="ftm",
            ),
        ):
            optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="ftm",
                lmp_data=object(),
                **NPV_KWARGS,
            )

        assert _mock_calculate_bill.call_count == 0


# ---------------------------------------------------------------------------
# m. Dispatch failure → failed=True, sweep continues
# ---------------------------------------------------------------------------


class TestDispatchFailureIsCaught:
    """Exceptions raised inside run_bess_dispatch / run_ftm_dispatch mark the
    combo as failed without aborting the outer sweep."""

    def test_btm_dispatch_exception_returns_failed(self) -> None:
        """run_bess_dispatch raises → failed=True, error populated."""
        site = _base_site()

        with patch(
            "src.optimization.bess_optimizer.run_bess_dispatch",
            side_effect=RuntimeError("LP infeasible"),
        ):
            out = _evaluate_bess_config(
                site_config=site,
                solar_production_kwh=[0.0] * 8760,
                bess_power_mw=1.0,
                bess_duration_hr=2.0,
                dispatch_mode="btm",
                charging_mode="solar_and_grid",
                rate_schedule=object(),
                load_profile=MagicMock(),
                solar_only_bill=object(),
                bess_cost_per_kwh=340.0,
                bess_opex_per_kw_year=25.0,
            )

        assert out["failed"] is True
        assert "LP infeasible" in (out["error"] or "")
        # Capex/opex still populated even on dispatch failure
        assert out["bess_capex"] == 1.0 * 2.0 * 1000 * 340.0
        # NPV stays at the base-result placeholder
        assert out["bess_npv"] == 0.0

    def test_ftm_dispatch_exception_returns_failed(self) -> None:
        """run_ftm_dispatch raises → failed=True, error populated."""
        site = _base_site()
        fake_lmp = MagicMock()
        fake_lmp.prices = [0.0] * 8760

        with patch(
            "src.optimization.bess_optimizer.run_ftm_dispatch",
            side_effect=ValueError("bad LMP data"),
        ):
            out = _evaluate_bess_config(
                site_config=site,
                solar_production_kwh=[0.0] * 8760,
                bess_power_mw=1.0,
                bess_duration_hr=4.0,
                dispatch_mode="ftm",
                charging_mode="solar_only",
                lmp_data=fake_lmp,
                bess_cost_per_kwh=340.0,
                bess_opex_per_kw_year=25.0,
            )

        assert out["failed"] is True
        assert "bad LMP data" in (out["error"] or "")
        assert out["bess_npv"] == 0.0

    def test_sweep_continues_on_partial_dispatch_failure(self) -> None:
        """optimize_solar_bess keeps running when some BTM dispatches fail."""
        site = _base_site()
        solar_canned = _canned_two_point_solar_result()

        # Every other run_bess_dispatch call raises; the rest succeed.
        dispatch_call_count = {"n": 0}

        def _dispatch_side_effect(*args, **kwargs):
            dispatch_call_count["n"] += 1
            if dispatch_call_count["n"] % 2 == 0:
                raise RuntimeError("alternating failure")
            return _fake_btm_dispatch_return(
                bess_incremental_savings=50_000.0,
                annual_degradation_pct=1.0,
            )

        with patch(
            "src.optimization.bess_optimizer.optimize_solar",
            return_value=solar_canned,
        ), patch(
            "src.optimization.bess_optimizer.run_bess_dispatch",
            side_effect=_dispatch_side_effect,
        ), patch(
            "src.optimization.bess_optimizer.compute_bess_npv",
            return_value=123_456.0,
        ):
            out = optimize_solar_bess(
                base_site_config=site,
                buildable_acres=100.0,
                dispatch_mode="btm",
                rate_schedule=object(),
                load_profile=MagicMock(),
                **NPV_KWARGS,
            )

        # Sweep completes with 20 attempts; roughly half fail, half succeed
        assert len(out["bess_sweep_results"]) == 20
        failed = [r for r in out["bess_sweep_results"] if r["failed"]]
        valid = [r for r in out["bess_sweep_results"] if not r["failed"]]
        assert len(failed) > 0
        assert len(valid) > 0
        assert (
            out["sweep_metadata"]["failed_bess_combinations"] == len(failed)
        )
        # Winner comes from the valid pool
        winner = out["winners"]["solar_bess_npv"]
        assert winner["failed"] is False
