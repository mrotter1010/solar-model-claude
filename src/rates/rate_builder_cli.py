"""Interactive CLI for building electricity rate schedule JSON files."""

from pathlib import Path

from src.rates.rate_builder import build_rate_schedule, save_rate_file
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

    # Output
    output_path_str = prompt_str("Output file path", default="rate_schedule.json")
    output_path = Path(output_path_str)
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
    print(f"  Saved to:       {output_path}")

    return output_path


if __name__ == "__main__":
    run_cli()
