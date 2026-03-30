"""Bill calculation integration: connects rate engine to the pipeline."""

from pathlib import Path
from typing import NamedTuple

from src.config.schema import SiteConfig
from src.rates.load_profile import load_customer_profile, load_typical_profile
from src.rates.models import BillSavings, LoadProfile, RateSchedule
from src.rates.openei_client import fetch_and_translate
from src.rates.rate_parser import load_rate_file
from src.rates.savings_analyzer import analyze_savings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class BillCalculationResult(NamedTuple):
    """Result of a bill calculation run, bundling savings with inputs.

    Args:
        bill_savings: Computed savings comparing with/without solar.
        rate_schedule: The resolved rate schedule used for calculation.
        load_profile: The resolved load profile used for calculation.
    """

    bill_savings: BillSavings
    rate_schedule: RateSchedule
    load_profile: LoadProfile


def run_bill_calculation(
    site_config: SiteConfig,
    hourly_production_kwh: list[float],
    rate_schedule: RateSchedule | None = None,
) -> BillCalculationResult | None:
    """Run bill calculation for a site if enabled.

    Loads the rate schedule and load profile based on site configuration,
    then computes savings from solar production. Returns None if bill
    calculation is disabled or if an error occurs (non-fatal to pipeline).

    Args:
        site_config: Site configuration with bill calculation fields.
        hourly_production_kwh: 8760 hourly solar production values in kWh
            (post-shading, net AC output).
        rate_schedule: Pre-built RateSchedule to use directly. When
            provided, skips file-path and URDB loading. Useful for
            API callers that supply an inline rate.

    Returns:
        BillCalculationResult with savings, rate schedule, and load profile,
        or None if disabled or on error.
    """
    if not site_config.bill_calculation:
        return None

    try:
        # Load rate schedule
        if rate_schedule is not None:
            rate = rate_schedule
            logger.info(
                "Using provided rate schedule: %s / %s",
                rate.utility_name,
                rate.tariff_name,
            )
        elif site_config.rate_file_path:
            rate = load_rate_file(Path(site_config.rate_file_path))
            logger.info(
                "Loaded rate from file: %s / %s",
                rate.utility_name,
                rate.tariff_name,
            )
        elif site_config.utility_name and site_config.tariff_name:
            rate = fetch_and_translate(
                utility_name=site_config.utility_name,
                tariff_name=site_config.tariff_name,
            )
            logger.info(
                "Fetched rate from OpenEI: %s / %s",
                rate.utility_name,
                rate.tariff_name,
            )
        else:
            logger.error(
                "Bill calculation enabled but no rate source for %s",
                site_config.site_name,
            )
            return None

        # Load profile
        if site_config.load_profile_path:
            load = load_customer_profile(Path(site_config.load_profile_path))
            logger.info(
                "Loaded customer profile: %.0f kWh annual, %.1f kW peak",
                load.annual_consumption_kwh,
                load.peak_demand_kw,
            )
        elif site_config.load_type and site_config.annual_consumption_kwh:
            load = load_typical_profile(
                building_type=site_config.load_type,
                latitude=site_config.latitude,
                longitude=site_config.longitude,
                annual_consumption_kwh=site_config.annual_consumption_kwh,
            )
            logger.info(
                "Loaded typical profile: %s, scale=%.4f",
                site_config.load_type,
                load.scale_factor,
            )
        else:
            logger.error(
                "Bill calculation enabled but no load source for %s",
                site_config.site_name,
            )
            return None

        # Run savings analysis
        savings = analyze_savings(
            load_kwh=load.hourly_kwh,
            production_kwh=hourly_production_kwh,
            rate=rate,
        )

        logger.info(
            "Bill calculation complete for %s: "
            "savings=$%.0f/yr (%.1f%%)",
            site_config.site_name,
            savings.annual_savings,
            savings.savings_percent,
        )

        return BillCalculationResult(
            bill_savings=savings,
            rate_schedule=rate,
            load_profile=load,
        )

    except Exception as exc:
        logger.error(
            "Bill calculation failed for %s: %s",
            site_config.site_name,
            exc,
        )
        return None
