"""Rate schedule builder: construct rate JSONs from human-friendly inputs."""

import json
from pathlib import Path

from src.rates.models import FixedCharges, RateSchedule, RateTier
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class RateBuilderError(Exception):
    """Raised when rate builder inputs are invalid."""


def _validate_seasons(seasons: dict[str, list[int]]) -> None:
    """Validate that seasons cover all 12 months exactly once.

    Args:
        seasons: Mapping of season name to list of month numbers (1-12).

    Raises:
        RateBuilderError: If months are missing, duplicated, or out of range.
    """
    all_months: list[int] = []
    for season_name, months in seasons.items():
        for m in months:
            if m < 1 or m > 12:
                raise RateBuilderError(
                    f"Season '{season_name}' has invalid month {m} "
                    f"(must be 1-12)"
                )
            all_months.append(m)

    if sorted(all_months) != list(range(1, 13)):
        missing = set(range(1, 13)) - set(all_months)
        duplicated = [m for m in all_months if all_months.count(m) > 1]
        parts = []
        if missing:
            parts.append(f"missing months: {sorted(missing)}")
        if duplicated:
            parts.append(f"duplicate months: {sorted(set(duplicated))}")
        raise RateBuilderError(
            f"Seasons must cover months 1-12 exactly once. {'; '.join(parts)}"
        )


def _validate_tou_periods(tou_periods: dict[str, dict]) -> None:
    """Validate that TOU periods cover all 24 hours for weekdays and weekends.

    Args:
        tou_periods: Mapping of period name to dict with weekday_hours
            and weekend_hours lists.

    Raises:
        RateBuilderError: If hours are missing, duplicated, or out of range.
    """
    for day_type in ("weekday_hours", "weekend_hours"):
        all_hours: list[int] = []
        for period_name, hours_dict in tou_periods.items():
            if day_type not in hours_dict:
                raise RateBuilderError(
                    f"TOU period '{period_name}' missing '{day_type}'"
                )
            for h in hours_dict[day_type]:
                if h < 0 or h > 23:
                    raise RateBuilderError(
                        f"Period '{period_name}' has invalid {day_type} "
                        f"hour {h} (must be 0-23)"
                    )
                all_hours.append(h)

        if sorted(all_hours) != list(range(24)):
            missing = set(range(24)) - set(all_hours)
            duplicated = [h for h in all_hours if all_hours.count(h) > 1]
            parts = []
            if missing:
                parts.append(f"missing hours: {sorted(missing)}")
            if duplicated:
                parts.append(f"duplicate hours: {sorted(set(duplicated))}")
            raise RateBuilderError(
                f"TOU periods must cover {day_type} 0-23 exactly once. "
                f"{'; '.join(parts)}"
            )


def _validate_rate_coverage(
    rates: dict[str, dict[str, float]],
    seasons: dict[str, list[int]],
    tou_periods: dict[str, dict],
    label: str,
) -> None:
    """Validate that rates are provided for every season x period combination.

    Args:
        rates: Mapping of season name to {period_name: rate}.
        seasons: Season definitions (for valid season names).
        tou_periods: TOU period definitions (for valid period names).
        label: Human-readable label for error messages.

    Raises:
        RateBuilderError: If any season x period combination is missing.
    """
    for season_name in seasons:
        if season_name not in rates:
            raise RateBuilderError(
                f"{label} missing season '{season_name}'"
            )
        for period_name in tou_periods:
            if period_name not in rates[season_name]:
                raise RateBuilderError(
                    f"{label} missing rate for "
                    f"'{season_name}'/'{period_name}'"
                )


def _build_schedule_matrix(
    seasons: dict[str, list[int]],
    tou_periods: dict[str, dict],
    period_map: dict[tuple[str, str], int],
    day_type: str,
) -> list[list[int]]:
    """Build a 12x24 schedule matrix mapping month-hour to period index.

    Args:
        seasons: Season definitions mapping name to month list.
        tou_periods: TOU period definitions with hour lists.
        period_map: Mapping of (season, period) to integer index.
        day_type: Either "weekday_hours" or "weekend_hours".

    Returns:
        12x24 matrix of period indices.
    """
    # Build month → season lookup
    month_to_season: dict[int, str] = {}
    for season_name, months in seasons.items():
        for m in months:
            month_to_season[m] = season_name

    # Build hour → period lookup for the given day type
    hour_to_period: dict[int, str] = {}
    for period_name, hours_dict in tou_periods.items():
        for h in hours_dict[day_type]:
            hour_to_period[h] = period_name

    matrix: list[list[int]] = []
    for month in range(1, 13):
        season = month_to_season[month]
        row = [
            period_map[(season, hour_to_period[hour])]
            for hour in range(24)
        ]
        matrix.append(row)
    return matrix


def build_rate_schedule(
    utility_name: str,
    tariff_name: str,
    seasons: dict[str, list[int]],
    tou_periods: dict[str, dict],
    energy_rates: dict[str, dict[str, float]],
    demand_rates: dict[str, dict[str, float]] | None = None,
    flat_demand_rates: dict[str, float] | None = None,
    fixed_charge_monthly: float = 0.0,
    sector: str = "commercial",
) -> RateSchedule:
    """Build a RateSchedule from human-friendly inputs.

    Constructs the 12x24 schedule matrices and rate structure arrays
    from season definitions, TOU period definitions, and rate tables.

    Args:
        utility_name: Name of the electric utility.
        tariff_name: Tariff schedule identifier.
        seasons: Mapping of season name to list of months (1-12).
            Every month must appear exactly once across all seasons.
        tou_periods: Mapping of period name to dict with "weekday_hours"
            and "weekend_hours" lists (hours 0-23). Every hour must appear
            exactly once per day type across all periods.
        energy_rates: Mapping of season name to {period_name: $/kWh}.
            Every season x period combination must be present.
        demand_rates: Optional TOU demand rates in $/kW, same structure
            as energy_rates.
        flat_demand_rates: Optional non-coincident demand rates as
            {season_name: $/kW}.
        fixed_charge_monthly: Fixed monthly charge in $.
        sector: Customer sector (commercial, residential, industrial).

    Returns:
        Validated RateSchedule model.

    Raises:
        RateBuilderError: If inputs are incomplete or inconsistent.
    """
    # Validate inputs
    _validate_seasons(seasons)
    _validate_tou_periods(tou_periods)
    _validate_rate_coverage(energy_rates, seasons, tou_periods, "energy_rates")
    if demand_rates is not None:
        _validate_rate_coverage(
            demand_rates, seasons, tou_periods, "demand_rates"
        )

    season_names = list(seasons.keys())
    period_names = list(tou_periods.keys())

    # Assign energy period indices: iterate seasons then periods
    energy_period_map: dict[tuple[str, str], int] = {}
    energyratestructure: list[list[RateTier]] = []
    idx = 0
    for s in season_names:
        for p in period_names:
            energy_period_map[(s, p)] = idx
            energyratestructure.append([RateTier(rate=energy_rates[s][p])])
            idx += 1

    # Build energy schedule matrices
    energyweekdayschedule = _build_schedule_matrix(
        seasons, tou_periods, energy_period_map, "weekday_hours"
    )
    energyweekendschedule = _build_schedule_matrix(
        seasons, tou_periods, energy_period_map, "weekend_hours"
    )

    # Build demand structures if provided
    demandratestructure = None
    demandweekdayschedule = None
    demandweekendschedule = None

    if demand_rates is not None:
        demand_period_map: dict[tuple[str, str], int] = {}
        demandratestructure = []
        idx = 0
        for s in season_names:
            for p in period_names:
                demand_period_map[(s, p)] = idx
                demandratestructure.append(
                    [RateTier(rate=demand_rates[s][p])]
                )
                idx += 1

        demandweekdayschedule = _build_schedule_matrix(
            seasons, tou_periods, demand_period_map, "weekday_hours"
        )
        demandweekendschedule = _build_schedule_matrix(
            seasons, tou_periods, demand_period_map, "weekend_hours"
        )

    # Build flat demand structures if provided
    flatdemandstructure = None
    flatdemandmonths = None

    if flat_demand_rates is not None:
        # One period per season
        flat_season_map: dict[str, int] = {}
        flatdemandstructure = []
        for i, s in enumerate(season_names):
            if s not in flat_demand_rates:
                raise RateBuilderError(
                    f"flat_demand_rates missing season '{s}'"
                )
            flat_season_map[s] = i
            flatdemandstructure.append(
                [RateTier(rate=flat_demand_rates[s])]
            )

        # Map each month to its season's flat demand period
        month_to_season: dict[int, str] = {}
        for season_name, months in seasons.items():
            for m in months:
                month_to_season[m] = season_name

        flatdemandmonths = [
            flat_season_map[month_to_season[m]] for m in range(1, 13)
        ]

    # Build fixed charges
    fixed_charges = FixedCharges(
        fixed_charge_first_meter=fixed_charge_monthly,
        fixed_charge_units="$/month",
    )

    rate = RateSchedule(
        utility_name=utility_name,
        tariff_name=tariff_name,
        sector=sector,
        source="manual",
        fixed_charges=fixed_charges,
        energyratestructure=energyratestructure,
        energyweekdayschedule=energyweekdayschedule,
        energyweekendschedule=energyweekendschedule,
        demandratestructure=demandratestructure,
        demandweekdayschedule=demandweekdayschedule,
        demandweekendschedule=demandweekendschedule,
        flatdemandstructure=flatdemandstructure,
        flatdemandmonths=flatdemandmonths,
    )

    logger.info(
        "Built rate schedule: %s / %s (%d energy periods, %d seasons)",
        utility_name,
        tariff_name,
        len(energyratestructure),
        len(seasons),
    )
    return rate


def save_rate_file(rate: RateSchedule, path: Path) -> Path:
    """Serialize a RateSchedule to a JSON file.

    Args:
        rate: Validated rate schedule to save.
        path: Destination file path.

    Returns:
        The path written to.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    data = rate.model_dump()
    path.write_text(json.dumps(data, indent=2, default=str))
    logger.info("Saved rate file: %s", path)
    return path


def build_and_save(
    path: Path,
    utility_name: str,
    tariff_name: str,
    seasons: dict[str, list[int]],
    tou_periods: dict[str, dict],
    energy_rates: dict[str, dict[str, float]],
    demand_rates: dict[str, dict[str, float]] | None = None,
    flat_demand_rates: dict[str, float] | None = None,
    fixed_charge_monthly: float = 0.0,
) -> Path:
    """Build a rate schedule and save it to a JSON file.

    Convenience function combining build_rate_schedule() and save_rate_file().

    Args:
        path: Destination file path.
        utility_name: Name of the electric utility.
        tariff_name: Tariff schedule identifier.
        seasons: Mapping of season name to list of months (1-12).
        tou_periods: TOU period definitions with hour lists.
        energy_rates: Energy rates per season per TOU period.
        demand_rates: Optional TOU demand rates.
        flat_demand_rates: Optional flat demand rates per season.
        fixed_charge_monthly: Fixed monthly charge in $.

    Returns:
        The path written to.
    """
    rate = build_rate_schedule(
        utility_name=utility_name,
        tariff_name=tariff_name,
        seasons=seasons,
        tou_periods=tou_periods,
        energy_rates=energy_rates,
        demand_rates=demand_rates,
        flat_demand_rates=flat_demand_rates,
        fixed_charge_monthly=fixed_charge_monthly,
    )
    return save_rate_file(rate, path)
