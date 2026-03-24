"""Tests for NEM bill calculation: export rates, export tracking, banking, and demand protection."""

from pathlib import Path

from src.rates.bill_calculator import (
    apply_nem_banking,
    calculate_bill,
    get_export_rate_for_hour,
)
from src.rates.models import (
    FixedCharges,
    MonthlyBillDetail,
    NetMeteringConfig,
    RateSchedule,
    RateTier,
)

OUTPUT_DIR = (
    Path(__file__).resolve().parents[2] / "outputs" / "test_results" / "m14b_prompt3c"
)

# Hours per month in a non-leap TMY year
_HOURS_PER_MONTH = [744, 672, 744, 720, 744, 720, 744, 744, 720, 744, 720, 744]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sched_12x24(value: int = 0) -> list[list[int]]:
    """Build a 12x24 schedule matrix filled with a single value."""
    return [[value] * 24 for _ in range(12)]


def _tou_weekday_sched() -> list[list[int]]:
    """Period 0 off-peak, period 1 for hours 12-19 (weekdays)."""
    return [[0] * 12 + [1] * 8 + [0] * 4 for _ in range(12)]


def _make_rate(
    nem_mode: str = "none",
    export_rate: float | None = None,
    true_up_rate: float = 0.0,
    tou: bool = False,
    with_demand: bool = False,
    fixed_charge: float = 0.0,
    export_schedule: list[list[int]] | None = None,
    export_weekend_schedule: list[list[int]] | None = None,
    export_rate_structure: list[list[RateTier]] | None = None,
) -> RateSchedule:
    """Build a minimal RateSchedule with configurable NEM settings.

    Args:
        nem_mode: NEM mode string.
        export_rate: Flat export rate (flat_rate mode).
        true_up_rate: Year-end bank cashout rate.
        tou: If True, use 2 energy periods ($0.05 off-peak, $0.15 peak 12-19 weekday).
        with_demand: If True, add demand charges at $5/kW, single period.
        fixed_charge: Monthly fixed charge amount.
        export_schedule: 12x24 weekday export schedule (detailed mode).
        export_weekend_schedule: 12x24 weekend export schedule (detailed mode).
        export_rate_structure: Export rate tiers (detailed mode).
    """
    if tou:
        energy_struct = [[RateTier(rate=0.05)], [RateTier(rate=0.15)]]
        wd_sched = _tou_weekday_sched()
        we_sched = _sched_12x24(0)
    else:
        energy_struct = [[RateTier(rate=0.10)]]
        wd_sched = _sched_12x24(0)
        we_sched = _sched_12x24(0)

    nem_kwargs: dict = {"mode": nem_mode, "true_up_rate": true_up_rate}
    if export_rate is not None:
        nem_kwargs["export_rate"] = export_rate
    if export_schedule is not None:
        nem_kwargs["export_schedule"] = export_schedule
    if export_weekend_schedule is not None:
        nem_kwargs["export_weekend_schedule"] = export_weekend_schedule
    if export_rate_structure is not None:
        nem_kwargs["export_rate_structure"] = export_rate_structure

    kwargs: dict = dict(
        utility_name="Test Utility",
        tariff_name="Test Tariff",
        energyratestructure=energy_struct,
        energyweekdayschedule=wd_sched,
        energyweekendschedule=we_sched,
        net_metering=NetMeteringConfig(**nem_kwargs),
    )

    if fixed_charge > 0:
        kwargs["fixed_charges"] = FixedCharges(
            fixed_charge_first_meter=fixed_charge
        )

    if with_demand:
        kwargs["demandratestructure"] = [[RateTier(rate=5.0)]]
        kwargs["demandweekdayschedule"] = _sched_12x24(0)
        kwargs["demandweekendschedule"] = _sched_12x24(0)

    return RateSchedule(**kwargs)


def _make_monthly_detail(
    month: int,
    energy_charges: float = 0.0,
    export_kwh: float = 0.0,
    export_credits: float = 0.0,
    demand_charges: float = 0.0,
    fixed_charges: float = 0.0,
) -> MonthlyBillDetail:
    """Build a MonthlyBillDetail with specified charges and export data."""
    return MonthlyBillDetail(
        month=month,
        energy_charges=energy_charges,
        demand_charges=demand_charges,
        flat_demand_charges=0.0,
        fixed_charges=fixed_charges,
        production_kwh=0.0,
        consumption_kwh=0.0,
        peak_demand_kw=0.0,
        export_kwh=export_kwh,
        export_credits=export_credits,
    )


def _make_12_months(**overrides: float) -> list[MonthlyBillDetail]:
    """Create 12 identical MonthlyBillDetail records with optional overrides."""
    return [_make_monthly_detail(month=m + 1, **overrides) for m in range(12)]


# ====================================================================
# 1. get_export_rate_for_hour() tests
# ====================================================================


class TestGetExportRateForHour:
    """Tests for per-hour export rate lookup across all 4 NEM modes."""

    def test_mode_none_returns_zero(self) -> None:
        """mode='none' returns 0.0 for any hour."""
        rate = _make_rate(nem_mode="none")
        assert get_export_rate_for_hour(0, rate) == 0.0
        assert get_export_rate_for_hour(4000, rate) == 0.0
        assert get_export_rate_for_hour(8759, rate) == 0.0

    def test_flat_rate_returns_configured_rate(self) -> None:
        """mode='flat_rate' returns the fixed export_rate for any hour."""
        rate = _make_rate(nem_mode="flat_rate", export_rate=0.08)
        assert get_export_rate_for_hour(0, rate) == 0.08
        assert get_export_rate_for_hour(4380, rate) == 0.08
        assert get_export_rate_for_hour(8759, rate) == 0.08

    def test_match_import_weekday_peak(self) -> None:
        """match_import on weekday peak hour returns peak energy rate.

        Hour 12 on Jan 1 (Monday, day_of_week=0) → weekday schedule
        period 1 → $0.15/kWh.
        """
        rate = _make_rate(nem_mode="match_import", tou=True)
        assert get_export_rate_for_hour(12, rate) == 0.15

    def test_match_import_weekday_offpeak(self) -> None:
        """match_import on weekday off-peak hour returns off-peak energy rate.

        Hour 0 on Jan 1 (Monday) → weekday schedule period 0 → $0.05/kWh.
        """
        rate = _make_rate(nem_mode="match_import", tou=True)
        assert get_export_rate_for_hour(0, rate) == 0.05

    def test_match_import_weekend(self) -> None:
        """match_import on weekend uses weekend schedule.

        Jan 6 (Saturday, day 5) hour 12 → hour_of_year=132.
        Weekend schedule is all period 0 → $0.05/kWh.
        """
        rate = _make_rate(nem_mode="match_import", tou=True)
        # day 5 = Saturday, hour 12 → hour_of_year = 5*24 + 12 = 132
        assert get_export_rate_for_hour(132, rate) == 0.05

    def test_detailed_weekday(self) -> None:
        """detailed mode: weekday hour 12 → export schedule period 1 → $0.07.

        Export weekday schedule: hours 10-16 = period 1, rest = period 0.
        Export rates: period 0=$0.03, period 1=$0.07.
        """
        export_wd = [[0] * 10 + [1] * 7 + [0] * 7 for _ in range(12)]
        export_we = _sched_12x24(0)
        export_struct = [[RateTier(rate=0.03)], [RateTier(rate=0.07)]]
        rate = _make_rate(
            nem_mode="detailed",
            export_schedule=export_wd,
            export_weekend_schedule=export_we,
            export_rate_structure=export_struct,
        )
        # Jan 1 (Monday) hour 12 → hour_of_year = 12
        assert get_export_rate_for_hour(12, rate) == 0.07

    def test_detailed_weekend(self) -> None:
        """detailed mode: weekend hour → export schedule period 0 → $0.03."""
        export_wd = [[0] * 10 + [1] * 7 + [0] * 7 for _ in range(12)]
        export_we = _sched_12x24(0)
        export_struct = [[RateTier(rate=0.03)], [RateTier(rate=0.07)]]
        rate = _make_rate(
            nem_mode="detailed",
            export_schedule=export_wd,
            export_weekend_schedule=export_we,
            export_rate_structure=export_struct,
        )
        # Jan 6 (Saturday) hour 12 → hour_of_year = 132
        assert get_export_rate_for_hour(132, rate) == 0.03


# ====================================================================
# 2. calculate_bill() export computation tests
# ====================================================================


class TestCalculateBillExportComputation:
    """Tests for per-month export_kwh and export_credits in calculate_bill."""

    def test_no_nem_export_fields_are_zero(self) -> None:
        """mode='none': export_kwh and export_credits are 0 for all months."""
        rate = _make_rate(nem_mode="none")
        bill = calculate_bill([100.0] * 8760, [150.0] * 8760, rate)
        for m in bill.monthly:
            assert m.export_kwh == 0.0
            assert m.export_credits == 0.0

    def test_flat_rate_constant_export(self) -> None:
        """Constant load=100, prod=150, flat_rate $0.08: 50 kWh export/hr.

        Each hour exports 50 kWh. Monthly export_kwh = 50 * hours_in_month.
        Export credits = export_kwh * 0.08. Energy charges = 0 (all export).
        """
        rate = _make_rate(nem_mode="flat_rate", export_rate=0.08)
        bill = calculate_bill([100.0] * 8760, [150.0] * 8760, rate)

        for i, m in enumerate(bill.monthly):
            expected_kwh = 50.0 * _HOURS_PER_MONTH[i]
            expected_credits = expected_kwh * 0.08
            assert m.export_kwh == expected_kwh
            assert m.export_credits == expected_credits
            assert m.energy_charges == 0.0

    def test_no_export_when_production_less_than_load(self) -> None:
        """NEM active but production < load every hour: no export."""
        rate = _make_rate(nem_mode="flat_rate", export_rate=0.08)
        bill = calculate_bill([150.0] * 8760, [100.0] * 8760, rate)
        for m in bill.monthly:
            assert m.export_kwh == 0.0
            assert m.export_credits == 0.0

    def test_match_import_tou_export_credits_vary(self) -> None:
        """match_import with TOU: export credits reflect varying TOU rates.

        Weekday peak hours ($0.15) and off-peak/weekend ($0.05) produce
        credits between the pure off-peak and pure peak bounds.
        """
        rate = _make_rate(nem_mode="match_import", tou=True)
        bill = calculate_bill([100.0] * 8760, [150.0] * 8760, rate)

        total_export = 50.0 * 8760
        assert bill.annual_export_kwh == total_export
        # Credits must be between all-offpeak and all-peak bounds
        assert bill.annual_export_credits > total_export * 0.05
        assert bill.annual_export_credits < total_export * 0.15

    def test_mixed_months_export(self) -> None:
        """First 6 months export (prod=150), last 6 don't (prod=50)."""
        rate = _make_rate(nem_mode="flat_rate", export_rate=0.08)
        load = [100.0] * 8760
        prod: list[float] = []
        for m_idx in range(12):
            val = 150.0 if m_idx < 6 else 50.0
            prod.extend([val] * _HOURS_PER_MONTH[m_idx])

        bill = calculate_bill(load, prod, rate)

        for i in range(6):
            assert bill.monthly[i].export_kwh == 50.0 * _HOURS_PER_MONTH[i]
        for i in range(6, 12):
            assert bill.monthly[i].export_kwh == 0.0


# ====================================================================
# 3. Demand charge protection tests
# ====================================================================


class TestDemandChargeProtection:
    """Verify export hours never reduce demand peaks or demand charges."""

    def test_peak_demand_not_negative_with_full_export(self) -> None:
        """When prod > load every hour, peak_demand_kw = 0 (not negative)."""
        rate = _make_rate(
            nem_mode="flat_rate", export_rate=0.08, with_demand=True
        )
        bill = calculate_bill([100.0] * 8760, [300.0] * 8760, rate)

        for m in bill.monthly:
            assert m.peak_demand_kw == 0.0
            assert m.demand_charges == 0.0

    def test_demand_charges_same_with_and_without_nem(self) -> None:
        """Demand charges identical with/without NEM for same load/prod.

        Alternating hours: even hours import (net=+150), odd hours export
        (net=-150). Peak demand should be 150 in both cases.
        """
        load: list[float] = []
        prod: list[float] = []
        for h in range(8760):
            if h % 2 == 0:
                load.append(200.0)
                prod.append(50.0)
            else:
                load.append(50.0)
                prod.append(200.0)

        rate_none = _make_rate(nem_mode="none", with_demand=True)
        rate_nem = _make_rate(
            nem_mode="flat_rate", export_rate=0.08, with_demand=True
        )

        bill_none = calculate_bill(load, prod, rate_none)
        bill_nem = calculate_bill(load, prod, rate_nem)

        for i in range(12):
            assert (
                bill_none.monthly[i].peak_demand_kw
                == bill_nem.monthly[i].peak_demand_kw
            )
            assert (
                bill_none.monthly[i].demand_charges
                == bill_nem.monthly[i].demand_charges
            )


# ====================================================================
# 4. apply_nem_banking() tests
# ====================================================================


class TestApplyNemBanking:
    """Tests for month-to-month credit banking and year-end true-up."""

    def test_credits_capped_at_energy_charges(self) -> None:
        """Credits applied cannot exceed energy charges (energy floor $0).

        Month 1: 100 kWh exported at $0.10 = $10 credits, energy=$8.
        kwh_to_apply = min(100, 8/0.10) = 80. Credits applied = $8.
        Bank remainder = 20 kWh.
        """
        details = _make_12_months()
        details[0] = _make_monthly_detail(
            month=1, energy_charges=8.0, export_kwh=100.0, export_credits=10.0
        )
        rate = _make_rate(
            nem_mode="flat_rate", export_rate=0.10, true_up_rate=0.04
        )
        updated, true_up = apply_nem_banking(details, rate)

        assert updated[0].nem_credits_applied == 8.0
        assert updated[0].nem_credits_banked_kwh == 20.0
        assert updated[0].total == 0.0
        assert true_up == 0.8  # 20 * 0.04

    def test_bank_carries_forward_across_months(self) -> None:
        """Banked kWh carry forward when subsequent month has no export.

        Month 1: 200 kWh at $0.10, energy=$5 → apply 50 kWh ($5), bank=150.
        Month 2: no export, energy=$10 → avg_rate=0, can't apply. Bank=150.
        """
        details = _make_12_months()
        details[0] = _make_monthly_detail(
            month=1, energy_charges=5.0, export_kwh=200.0, export_credits=20.0
        )
        details[1] = _make_monthly_detail(month=2, energy_charges=10.0)
        rate = _make_rate(nem_mode="flat_rate", export_rate=0.10)

        updated, _ = apply_nem_banking(details, rate)

        assert updated[0].nem_credits_applied == 5.0
        assert updated[0].nem_credits_banked_kwh == 150.0
        assert updated[1].nem_credits_applied == 0.0
        assert updated[1].nem_credits_banked_kwh == 150.0

    def test_bank_exceeds_energy_charges(self) -> None:
        """When bank exceeds what's needed, only enough kWh to zero energy is used.

        Month 1: 1000 kWh at $0.10 = $100, energy=$50.
        kwh_to_apply = min(1000, 50/0.10) = 500. Bank remainder = 500.
        """
        details = _make_12_months()
        details[0] = _make_monthly_detail(
            month=1, energy_charges=50.0, export_kwh=1000.0, export_credits=100.0
        )
        rate = _make_rate(nem_mode="flat_rate", export_rate=0.10)

        updated, _ = apply_nem_banking(details, rate)

        assert updated[0].nem_credits_applied == 50.0
        assert updated[0].nem_credits_banked_kwh == 500.0

    def test_true_up_cashes_out_remaining_bank(self) -> None:
        """Year-end true-up = remaining bank_kwh * true_up_rate.

        Month 1: 100 kWh exported, energy=$0 → bank=100, no credits applied.
        true_up = 100 * 0.04 = $4.00.
        """
        details = _make_12_months()
        details[0] = _make_monthly_detail(
            month=1, export_kwh=100.0, export_credits=10.0
        )
        rate = _make_rate(
            nem_mode="flat_rate", export_rate=0.10, true_up_rate=0.04
        )
        updated, true_up = apply_nem_banking(details, rate)

        assert updated[0].nem_credits_banked_kwh == 100.0
        assert true_up == 4.0

    def test_true_up_rate_zero_means_no_cashout(self) -> None:
        """true_up_rate=0 produces $0 true-up even with remaining bank."""
        details = _make_12_months()
        details[0] = _make_monthly_detail(
            month=1, export_kwh=100.0, export_credits=10.0
        )
        rate = _make_rate(
            nem_mode="flat_rate", export_rate=0.10, true_up_rate=0.0
        )
        _, true_up = apply_nem_banking(details, rate)

        assert true_up == 0.0

    def test_steady_export_cumulative_banking(self) -> None:
        """Full year: each month exports 100 kWh ($10), energy=$8.

        kwh_to_apply = min(bank, 8/0.10) = min(bank, 80). Bank always > 80
        after month 1, so each month applies 80 kWh ($8), bank grows by 20.
        Final bank = 240 kWh. true_up = 240 * 0.04 = $9.60.
        """
        details = _make_12_months(
            energy_charges=8.0, export_kwh=100.0, export_credits=10.0
        )
        rate = _make_rate(
            nem_mode="flat_rate", export_rate=0.10, true_up_rate=0.04
        )
        updated, true_up = apply_nem_banking(details, rate)

        # Month 1: bank=100, apply 80, bank=20
        assert updated[0].nem_credits_applied == 8.0
        assert updated[0].nem_credits_banked_kwh == 20.0
        # Month 2: bank=120, apply 80, bank=40
        assert updated[1].nem_credits_applied == 8.0
        assert updated[1].nem_credits_banked_kwh == 40.0
        # Month 12: bank=320, apply 80, bank=240
        assert updated[11].nem_credits_applied == 8.0
        assert updated[11].nem_credits_banked_kwh == 240.0
        # true_up = 240 * 0.04 = $9.60
        assert true_up == 9.6

    def test_returns_new_objects_not_mutated(self) -> None:
        """apply_nem_banking returns new objects; originals are unchanged."""
        details = _make_12_months(
            energy_charges=10.0, export_kwh=50.0, export_credits=5.0
        )
        original_totals = [d.total for d in details]

        updated, _ = apply_nem_banking(
            details, _make_rate(nem_mode="flat_rate", export_rate=0.10)
        )

        # Originals unchanged
        for i, d in enumerate(details):
            assert d.total == original_totals[i]
            assert d.nem_credits_applied == 0.0
        # Updated objects should have credits applied
        assert updated[0].nem_credits_applied > 0


# ====================================================================
# 5. BillResult annual totals tests
# ====================================================================


class TestBillResultAnnualTotals:
    """Verify BillResult computed annual totals for NEM fields."""

    def test_annual_export_kwh_sums_monthly(self) -> None:
        """annual_export_kwh = sum of monthly export_kwh."""
        rate = _make_rate(nem_mode="flat_rate", export_rate=0.08)
        bill = calculate_bill([100.0] * 8760, [150.0] * 8760, rate)

        expected = sum(m.export_kwh for m in bill.monthly)
        assert bill.annual_export_kwh == expected
        assert bill.annual_export_kwh == 50.0 * 8760

    def test_annual_export_credits_sums_monthly(self) -> None:
        """annual_export_credits = sum of monthly export_credits."""
        rate = _make_rate(nem_mode="flat_rate", export_rate=0.08)
        bill = calculate_bill([100.0] * 8760, [150.0] * 8760, rate)

        expected = sum(m.export_credits for m in bill.monthly)
        assert bill.annual_export_credits == expected

    def test_annual_total_includes_credits_and_true_up(self) -> None:
        """annual_total = sum(monthly.total) - nem_true_up_credit.

        All-export scenario: energy=$0, fixed=$10/month, no credits applied.
        Bank accumulates all 438,000 kWh. true_up = 438000 * 0.04 = $17,520.
        annual_total = 12*10 - 17520 = -$17,400.
        """
        rate = _make_rate(
            nem_mode="flat_rate",
            export_rate=0.10,
            true_up_rate=0.04,
            fixed_charge=10.0,
        )
        bill = calculate_bill([100.0] * 8760, [150.0] * 8760, rate)

        for m in bill.monthly:
            assert m.total == 10.0  # fixed only, no energy, no credits applied
        assert bill.nem_true_up_credit == 17520.0
        assert bill.annual_total == 120.0 - 17520.0


# ====================================================================
# 6. Test output file
# ====================================================================


class TestWriteOutputs:
    """Write test result artifacts for manual inspection."""

    def test_write_summary(self) -> None:
        """Write a summary text file with key NEM test scenario values."""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        rate = _make_rate(
            nem_mode="flat_rate", export_rate=0.08, true_up_rate=0.04
        )
        bill = calculate_bill([100.0] * 8760, [150.0] * 8760, rate)

        lines = [
            "NEM Bill Calculator Test Summary",
            "=" * 60,
            "",
            "Scenario: constant load=100 kWh/hr, production=150 kWh/hr",
            "NEM mode: flat_rate, export_rate=$0.08, true_up_rate=$0.04",
            "Energy rate: $0.10/kWh (flat, single period)",
            "",
            "Monthly Results:",
            f"{'Month':<7} {'ExportKWh':>11} {'ExportCr$':>11} {'Energy$':>9} "
            f"{'Credits$':>10} {'BankKWh':>10} {'Total$':>9}",
            "-" * 70,
        ]
        for m in bill.monthly:
            lines.append(
                f"{m.month:<7} {m.export_kwh:>11.2f} {m.export_credits:>11.2f} "
                f"{m.energy_charges:>9.2f} {m.nem_credits_applied:>10.2f} "
                f"{m.nem_credits_banked_kwh:>10.2f} {m.total:>9.2f}"
            )
        lines.extend([
            "",
            f"Annual export kWh:     {bill.annual_export_kwh:>12.2f}",
            f"Annual export credits: ${bill.annual_export_credits:>11.2f}",
            f"Annual energy charges: ${bill.annual_energy_charges:>11.2f}",
            f"NEM true-up credit:    ${bill.nem_true_up_credit:>11.2f}",
            f"Annual total:          ${bill.annual_total:>11.2f}",
        ])

        output_path = OUTPUT_DIR / "nem_test_summary.txt"
        output_path.write_text("\n".join(lines))
        assert output_path.exists()
