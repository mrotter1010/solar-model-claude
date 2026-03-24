"""Interactive CLI for building electricity rate schedule JSON files."""

from pathlib import Path

from src.rates.models import RateSchedule, RateTier
from src.rates.rate_builder import (
    _build_schedule_matrix,
    build_rate_schedule,
    save_rate_file,
    set_net_metering,
)
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def parse_int_list(text: str) -> list[int]:
    """Parse a comma-separated list of ints, supporting ranges.

    Accepts formats like "1,2,3", "1-5", or "1,2,5-8,12".

    Args:
        text: Comma-separated string of integers and/or ranges.

    Returns:
        Sorted list of unique integers.

    Raises:
        ValueError: If the text contains invalid tokens.
    """
    result: list[int] = []
    for token in text.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            parts = token.split("-", 1)
            start = int(parts[0].strip())
            end = int(parts[1].strip())
            result.extend(range(start, end + 1))
        else:
            result.append(int(token))
    return sorted(set(result))


def prompt_str(message: str, default: str | None = None) -> str:
    """Prompt for a string value with optional default.

    Args:
        message: Prompt message to display.
        default: Default value if user presses Enter.

    Returns:
        User input or default value.
    """
    if default is not None:
        display = f"{message} (default: {default}): "
    else:
        display = f"{message}: "

    while True:
        value = input(display).strip()
        if value:
            return value
        if default is not None:
            return default
        print("  Value required.")


def prompt_float(message: str, default: float | None = None) -> float:
    """Prompt for a float value with optional default.

    Args:
        message: Prompt message to display.
        default: Default value if user presses Enter.

    Returns:
        Parsed float value.
    """
    if default is not None:
        display = f"{message} (default: {default}): "
    else:
        display = f"{message}: "

    while True:
        value = input(display).strip()
        if not value and default is not None:
            return default
        try:
            return float(value)
        except ValueError:
            print(f"  Invalid number: '{value}'. Try again.")


def prompt_int(message: str, default: int | None = None) -> int:
    """Prompt for an integer value with optional default.

    Args:
        message: Prompt message to display.
        default: Default value if user presses Enter.

    Returns:
        Parsed integer value.
    """
    if default is not None:
        display = f"{message} (default: {default}): "
    else:
        display = f"{message}: "

    while True:
        value = input(display).strip()
        if not value and default is not None:
            return default
        try:
            return int(value)
        except ValueError:
            print(f"  Invalid integer: '{value}'. Try again.")


def prompt_yes_no(message: str, default: bool | None = None) -> bool:
    """Prompt for a yes/no answer.

    Args:
        message: Prompt message to display.
        default: Default value if user presses Enter.

    Returns:
        True for yes, False for no.
    """
    if default is True:
        hint = "[Y/n]"
    elif default is False:
        hint = "[y/N]"
    else:
        hint = "[y/n]"

    display = f"{message} {hint}: "

    while True:
        value = input(display).strip().lower()
        if not value and default is not None:
            return default
        if value in ("y", "yes"):
            return True
        if value in ("n", "no"):
            return False
        print("  Please enter 'y' or 'n'.")


def validate_nem_export_vs_import(rate: RateSchedule) -> list[str]:
    """Check whether any NEM export rate exceeds the import rate for the same cell.

    Compares export vs. import rates across the 12x24 weekday and weekend
    schedule matrices. Returns a list of human-readable warning strings
    (empty if no issues found).

    Args:
        rate: Fully built RateSchedule with net_metering applied.

    Returns:
        List of warning strings. Empty if no export > import found.
    """
    nem = rate.net_metering

    if nem.mode in ("none", "match_import"):
        return []

    warnings: list[str] = []

    def _import_rate(month: int, hour: int, weekend: bool) -> float:
        schedule = rate.energyweekendschedule if weekend else rate.energyweekdayschedule
        period_idx = schedule[month][hour]
        return rate.energyratestructure[period_idx][0].rate

    if nem.mode == "flat_rate":
        export = nem.export_rate
        for m in range(12):
            for h in range(24):
                # Weekday
                imp_wd = _import_rate(m, h, weekend=False)
                if export > imp_wd:
                    warnings.append(
                        f"Export rate ${export:.4f} exceeds import rate "
                        f"${imp_wd:.4f} for month {m + 1}, hour {h} (weekday)"
                    )
                # Weekend
                imp_we = _import_rate(m, h, weekend=True)
                if export > imp_we:
                    warnings.append(
                        f"Export rate ${export:.4f} exceeds import rate "
                        f"${imp_we:.4f} for month {m + 1}, hour {h} (weekend)"
                    )

    elif nem.mode == "detailed":
        for m in range(12):
            for h in range(24):
                # Weekday
                imp_wd = _import_rate(m, h, weekend=False)
                exp_idx_wd = nem.export_schedule[m][h]
                exp_wd = nem.export_rate_structure[exp_idx_wd][0].rate
                if exp_wd > imp_wd:
                    warnings.append(
                        f"Export rate ${exp_wd:.4f} exceeds import rate "
                        f"${imp_wd:.4f} for month {m + 1}, hour {h} (weekday)"
                    )
                # Weekend
                imp_we = _import_rate(m, h, weekend=True)
                exp_idx_we = nem.export_weekend_schedule[m][h]
                exp_we = nem.export_rate_structure[exp_idx_we][0].rate
                if exp_we > imp_we:
                    warnings.append(
                        f"Export rate ${exp_we:.4f} exceeds import rate "
                        f"${imp_we:.4f} for month {m + 1}, hour {h} (weekend)"
                    )

    return warnings


def _collect_seasons() -> dict[str, list[int]]:
    """Interactively collect season definitions.

    Returns:
        Dict mapping season names to lists of month numbers (1-12).
    """
    n_seasons = prompt_int("Number of seasons", default=2)
    seasons: dict[str, list[int]] = {}

    for i in range(1, n_seasons + 1):
        while True:
            name = prompt_str(f"Season {i} name (e.g., summer, winter)")
            months_str = prompt_str(
                f"Season {i} months (e.g., 6-9 or 1-5,10-12)"
            )
            try:
                months = parse_int_list(months_str)
                if not months:
                    print("  No months specified. Try again.")
                    continue
                invalid = [m for m in months if m < 1 or m > 12]
                if invalid:
                    print(f"  Invalid months: {invalid}. Must be 1-12.")
                    continue
                seasons[name] = months
                break
            except ValueError as exc:
                print(f"  Invalid input: {exc}. Try again.")

    # Validate coverage
    all_months = [m for ms in seasons.values() for m in ms]
    missing = set(range(1, 13)) - set(all_months)
    if missing:
        print(f"  Warning: months not covered: {sorted(missing)}")
        print("  All 12 months must be assigned. Please re-enter seasons.")
        return _collect_seasons()

    duplicated = [m for m in all_months if all_months.count(m) > 1]
    if duplicated:
        print(f"  Warning: duplicate months: {sorted(set(duplicated))}")
        print("  Each month must appear in exactly one season. Please re-enter.")
        return _collect_seasons()

    return seasons


def _collect_tou_periods() -> dict[str, dict]:
    """Interactively collect TOU period definitions.

    Returns:
        Dict mapping period names to dicts with weekday_hours and
        weekend_hours lists.
    """
    n_periods = prompt_int("Number of TOU periods", default=2)
    periods: dict[str, dict] = {}

    for i in range(1, n_periods + 1):
        while True:
            name = prompt_str(f"Period {i} name (e.g., peak, off_peak)")
            wd_str = prompt_str(
                f"Period {i} weekday hours (e.g., 16-20 or 0-15,21-23)"
            )
            we_str = prompt_str(
                f"Period {i} weekend hours (e.g., 0-23, 'none', or 'same')"
            )

            try:
                weekday_hours = parse_int_list(wd_str)
                if we_str.lower() == "none":
                    weekend_hours: list[int] = []
                elif we_str.lower() == "same":
                    weekend_hours = list(weekday_hours)
                else:
                    weekend_hours = parse_int_list(we_str)

                periods[name] = {
                    "weekday_hours": weekday_hours,
                    "weekend_hours": weekend_hours,
                }
                break
            except ValueError as exc:
                print(f"  Invalid input: {exc}. Try again.")

    # Validate weekday coverage
    all_wd = [h for p in periods.values() for h in p["weekday_hours"]]
    missing_wd = set(range(24)) - set(all_wd)
    if missing_wd:
        print(f"  Warning: weekday hours not covered: {sorted(missing_wd)}")
        print("  All 24 hours must be assigned. Please re-enter periods.")
        return _collect_tou_periods()

    # Validate weekend coverage
    all_we = [h for p in periods.values() for h in p["weekend_hours"]]
    missing_we = set(range(24)) - set(all_we)
    if missing_we:
        print(f"  Warning: weekend hours not covered: {sorted(missing_we)}")
        print("  All 24 hours must be assigned. Please re-enter periods.")
        return _collect_tou_periods()

    return periods


def _collect_energy_rates(
    seasons: dict[str, list[int]],
    tou_periods: dict[str, dict],
) -> dict[str, dict[str, float]]:
    """Collect energy rates for every season x period combination.

    Args:
        seasons: Season definitions.
        tou_periods: TOU period definitions.

    Returns:
        Dict mapping season name to {period_name: rate}.
    """
    energy_rates: dict[str, dict[str, float]] = {}
    for season_name in seasons:
        energy_rates[season_name] = {}
        for period_name in tou_periods:
            rate = prompt_float(
                f"Energy rate for {season_name}/{period_name} ($/kWh)"
            )
            energy_rates[season_name][period_name] = rate
    return energy_rates


def _collect_demand_rates(
    seasons: dict[str, list[int]],
    tou_periods: dict[str, dict],
) -> dict[str, dict[str, float]]:
    """Collect TOU demand rates for every season x period combination.

    Args:
        seasons: Season definitions.
        tou_periods: TOU period definitions.

    Returns:
        Dict mapping season name to {period_name: rate}.
    """
    demand_rates: dict[str, dict[str, float]] = {}
    for season_name in seasons:
        demand_rates[season_name] = {}
        for period_name in tou_periods:
            rate = prompt_float(
                f"Demand charge for {season_name}/{period_name} ($/kW)"
            )
            demand_rates[season_name][period_name] = rate
    return demand_rates


def _collect_flat_demand_rates(
    seasons: dict[str, list[int]],
) -> dict[str, float]:
    """Collect flat demand rates for each season.

    Args:
        seasons: Season definitions.

    Returns:
        Dict mapping season name to rate.
    """
    flat_rates: dict[str, float] = {}
    for season_name in seasons:
        rate = prompt_float(
            f"Flat demand charge for {season_name} ($/kW)"
        )
        flat_rates[season_name] = rate
    return flat_rates


def _collect_net_metering(
    seasons: dict[str, list[int]],
) -> dict | None:
    """Interactively collect net energy metering configuration.

    Args:
        seasons: Season definitions from the main rate configuration,
            reused for detailed export schedule construction.

    Returns:
        Dict of kwargs for set_net_metering(), or None if NEM is declined.
    """
    if not prompt_yes_no("Configure net energy metering (NEM)?", default=False):
        return None

    print("\nSelect NEM mode:")
    print("  1. Flat rate — fixed $/kWh export credit")
    print("  2. Match import — export credited at same TOU rate as import")
    print("  3. Detailed — separate export rate schedule")

    while True:
        choice = prompt_int("Choice")
        if choice in (1, 2, 3):
            break
        print("  Please enter 1, 2, or 3.")

    mode_map = {1: "flat_rate", 2: "match_import", 3: "detailed"}
    mode = mode_map[choice]
    kwargs: dict = {"mode": mode}

    if mode == "flat_rate":
        while True:
            export_rate = prompt_float("Export rate ($/kWh)")
            if export_rate > 0:
                break
            print("  Export rate must be > 0. Try again.")
        kwargs["export_rate"] = export_rate

    elif mode == "detailed":
        print("\n--- Export Schedule ---")
        print("Define export TOU periods (same format as import periods):")
        export_periods = _collect_tou_periods()

        print("\n--- Export Rates ---")
        export_period_map: dict[tuple[str, str], int] = {}
        export_rate_structure: list[list[RateTier]] = []
        idx = 0
        season_names = list(seasons.keys())
        period_names = list(export_periods.keys())
        for s in season_names:
            for p in period_names:
                rate_val = prompt_float(
                    f"Export rate for {s}/{p} ($/kWh)"
                )
                export_period_map[(s, p)] = idx
                export_rate_structure.append([RateTier(rate=rate_val)])
                idx += 1

        kwargs["export_schedule"] = _build_schedule_matrix(
            seasons, export_periods, export_period_map, "weekday_hours"
        )
        kwargs["export_weekend_schedule"] = _build_schedule_matrix(
            seasons, export_periods, export_period_map, "weekend_hours"
        )
        kwargs["export_rate_structure"] = export_rate_structure

    kwargs["true_up_rate"] = prompt_float(
        "Annual true-up rate for banked credits ($/kWh)", default=0.00
    )

    return kwargs


def run_cli() -> Path:
    """Run the interactive rate builder flow.

    Returns:
        Path to the saved rate schedule JSON file.
    """
    print("\n=== Rate Schedule Builder ===\n")

    # Header
    utility_name = prompt_str("Utility name")
    tariff_name = prompt_str("Tariff name")
    sector = prompt_str("Sector [commercial/residential/industrial]", default="commercial")

    # Rate type
    is_tou = prompt_yes_no("Is this a Time-of-Use (TOU) rate?")

    if not is_tou:
        # Flat rate path
        energy_rate = prompt_float("Energy rate ($/kWh)")
        include_demand = prompt_yes_no("Include demand charges?")
        demand_rate = None
        if include_demand:
            demand_rate = prompt_float("Demand charge ($/kW)")
        fixed_charge = prompt_float("Monthly fixed charge ($)", default=0.0)

        seasons = {"annual": list(range(1, 13))}
        tou_periods = {
            "all": {
                "weekday_hours": list(range(24)),
                "weekend_hours": list(range(24)),
            }
        }
        energy_rates = {"annual": {"all": energy_rate}}
        flat_demand_rates = {"annual": demand_rate} if demand_rate is not None else None

        rate = build_rate_schedule(
            utility_name=utility_name,
            tariff_name=tariff_name,
            seasons=seasons,
            tou_periods=tou_periods,
            energy_rates=energy_rates,
            flat_demand_rates=flat_demand_rates,
            fixed_charge_monthly=fixed_charge,
            sector=sector,
        )
    else:
        # TOU rate path
        print("\n--- Step 1: Seasons ---")
        seasons = _collect_seasons()

        print("\n--- Step 2: TOU Periods ---")
        tou_periods = _collect_tou_periods()

        print("\n--- Step 3: Energy Rates ---")
        energy_rates = _collect_energy_rates(seasons, tou_periods)

        print("\n--- Step 4: Demand Charges ---")
        demand_rates = None
        if prompt_yes_no("Include TOU demand charges?"):
            demand_rates = _collect_demand_rates(seasons, tou_periods)

        flat_demand_rates = None
        if prompt_yes_no("Include flat (non-coincident) demand charges?"):
            flat_demand_rates = _collect_flat_demand_rates(seasons)

        print("\n--- Step 5: Fixed Charges ---")
        fixed_charge = prompt_float("Monthly fixed charge ($)", default=0.0)

        rate = build_rate_schedule(
            utility_name=utility_name,
            tariff_name=tariff_name,
            seasons=seasons,
            tou_periods=tou_periods,
            energy_rates=energy_rates,
            demand_rates=demand_rates,
            flat_demand_rates=flat_demand_rates,
            fixed_charge_monthly=fixed_charge,
            sector=sector,
        )

    # Output path
    output_path_str = prompt_str("Output file path", default="rate_schedule.json")
    output_path = Path(output_path_str)

    # --- Net Energy Metering (optional) ---
    print("\n--- Net Energy Metering ---")
    try:
        while True:
            nem_result = _collect_net_metering(seasons)
            if nem_result is None:
                break
            candidate = set_net_metering(rate, **nem_result)
            rate_warnings = validate_nem_export_vs_import(candidate)
            if rate_warnings:
                print("\nWARNING: Export rates exceed import rates:")
                for w in rate_warnings:
                    print(f"  {w}")
                proceed = prompt_yes_no(
                    "\nExport rates exceed import rates for some hours. "
                    "This may cause unrealistic results. Continue anyway?",
                    default=False,
                )
                if proceed:
                    rate = candidate
                    break
                print("\nRe-entering NEM configuration...")
            else:
                rate = candidate
                break
    except (StopIteration, EOFError):
        pass  # Skip NEM when input stream is exhausted

    save_rate_file(rate, output_path)

    # Summary
    n_energy = len(rate.energyratestructure)
    has_demand = rate.demandratestructure is not None
    has_flat_demand = rate.flatdemandstructure is not None

    print(f"\n=== Rate Schedule Created ===")
    print(f"  Utility:        {rate.utility_name}")
    print(f"  Tariff:         {rate.tariff_name}")
    print(f"  Sector:         {rate.sector}")
    print(f"  Energy periods: {n_energy}")
    print(f"  TOU demand:     {'yes' if has_demand else 'no'}")
    print(f"  Flat demand:    {'yes' if has_flat_demand else 'no'}")
    print(f"  Fixed charge:   ${rate.fixed_charges.fixed_charge_first_meter:.2f}/month")
    print(f"  NEM mode:       {rate.net_metering.mode}")
    print(f"  Saved to:       {output_path}")

    return output_path


if __name__ == "__main__":
    run_cli()
