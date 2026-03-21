"""OpenEI URDB API client for fetching and translating electricity rate structures."""

import os
from datetime import datetime, timezone

import requests

from src.rates.models import FixedCharges, RateSchedule, RateTier
from src.utils.exceptions import SolarModelError
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

_BASE_URL = "https://api.openei.org/utility_rates"
_REQUEST_TIMEOUT_S = 30


class OpenEIError(SolarModelError):
    """Raised when OpenEI API calls or response translation fails.

    Example context:
        - api: "OpenEI URDB"
        - label: URDB rate label
        - status_code: HTTP status code
        - error: Underlying error message
    """


def _get_api_key(api_key: str | None = None) -> str:
    """Resolve the OpenEI API key from parameter or environment.

    Args:
        api_key: Explicit API key, or None to read from OPENEI_API_KEY env var.

    Returns:
        The API key string.

    Raises:
        OpenEIError: If no API key is available.
    """
    key = api_key or os.environ.get("OPENEI_API_KEY")
    if not key:
        raise OpenEIError(
            "No OpenEI API key provided. Set OPENEI_API_KEY environment variable "
            "or pass api_key parameter.",
            context={"api": "OpenEI URDB"},
        )
    return key


def _make_request(params: dict, api_key: str | None = None) -> dict:
    """Make a GET request to the OpenEI URDB API.

    Args:
        params: Query parameters (version and format are added automatically).
        api_key: Explicit API key, or None to use env var.

    Returns:
        Parsed JSON response dict.

    Raises:
        OpenEIError: On network errors, HTTP errors, or invalid JSON.
    """
    key = _get_api_key(api_key)
    params = {**params, "version": 8, "format": "json", "api_key": key}

    try:
        response = requests.get(_BASE_URL, params=params, timeout=_REQUEST_TIMEOUT_S)
        response.raise_for_status()
    except requests.exceptions.Timeout as e:
        logger.error("OpenEI request timed out")
        raise OpenEIError(
            "OpenEI API request timed out",
            context={"api": "OpenEI URDB"},
        ) from e
    except requests.exceptions.HTTPError as e:
        logger.error("OpenEI HTTP error %s", response.status_code)
        raise OpenEIError(
            f"OpenEI API returned HTTP {response.status_code}",
            context={
                "api": "OpenEI URDB",
                "status_code": response.status_code,
                "response": response.text[:500],
            },
        ) from e
    except requests.exceptions.RequestException as e:
        logger.error("OpenEI request failed: %s", e)
        raise OpenEIError(
            "OpenEI API request failed",
            context={"api": "OpenEI URDB", "error": str(e)},
        ) from e

    try:
        return response.json()
    except ValueError as e:
        raise OpenEIError(
            "OpenEI API returned invalid JSON",
            context={"api": "OpenEI URDB", "error": str(e)},
        ) from e


def search_utilities(name: str, api_key: str | None = None) -> list[dict]:
    """Search for utilities matching a name string.

    Args:
        name: Utility name search string.
        api_key: Explicit API key, or None to use env var.

    Returns:
        List of dicts with utility_name, utility_label, and rate_count.

    Raises:
        OpenEIError: On API errors or missing API key.
    """
    data = _make_request({"utility": name}, api_key=api_key)
    items = data.get("items", [])

    # Deduplicate utilities and count rates per utility
    utilities: dict[str, dict] = {}
    for item in items:
        uname = item.get("utility", "Unknown")
        if uname not in utilities:
            utilities[uname] = {
                "utility_name": uname,
                "utility_label": item.get("label", ""),
                "rate_count": 0,
            }
        utilities[uname]["rate_count"] += 1

    result = list(utilities.values())
    logger.info("Found %d utilities matching '%s'", len(result), name)
    return result


def search_rates(
    utility_name: str | None = None,
    tariff_name: str | None = None,
    api_key: str | None = None,
) -> list[dict]:
    """Search for rate schedules, optionally filtering by tariff name.

    Args:
        utility_name: Utility name to search rates for.
        tariff_name: Optional substring to filter rate names (case-insensitive).
        api_key: Explicit API key, or None to use env var.

    Returns:
        List of dicts with label, name, utility, sector, startdate, description.

    Raises:
        OpenEIError: On API errors or missing API key.
    """
    params: dict = {}
    if utility_name:
        params["ratesforutility"] = utility_name

    data = _make_request(params, api_key=api_key)
    items = data.get("items", [])

    rates = []
    for item in items:
        rates.append({
            "label": item.get("label", ""),
            "name": item.get("name", ""),
            "utility": item.get("utility", ""),
            "sector": item.get("sector", ""),
            "startdate": item.get("startdate", 0),
            "description": item.get("description", ""),
        })

    if tariff_name:
        needle = tariff_name.lower()
        rates = [r for r in rates if needle in r["name"].lower()]

    logger.info(
        "Found %d rates for utility='%s', tariff filter='%s'",
        len(rates),
        utility_name,
        tariff_name,
    )
    return rates


def fetch_rate(label: str, api_key: str | None = None) -> dict:
    """Fetch a specific rate record by its URDB label.

    Args:
        label: URDB rate label (unique identifier).
        api_key: Explicit API key, or None to use env var.

    Returns:
        Raw URDB rate record dict.

    Raises:
        OpenEIError: On API errors, missing key, or empty response.
    """
    data = _make_request({"getpage": label}, api_key=api_key)
    items = data.get("items", [])

    if not items:
        raise OpenEIError(
            f"No rate found for label '{label}'",
            context={"api": "OpenEI URDB", "label": label},
        )

    logger.info("Fetched rate '%s': %s", label, items[0].get("name", "unknown"))
    return items[0]


def _translate_rate_tiers(urdb_tiers: list[dict]) -> list[RateTier]:
    """Translate a URDB tier array to our RateTier models.

    Args:
        urdb_tiers: List of URDB tier dicts with rate, max, adj fields.

    Returns:
        List of RateTier models.
    """
    tiers = []
    for tier in urdb_tiers:
        tiers.append(RateTier(
            rate=tier.get("rate", 0.0),
            max=tier.get("max"),
            adj=tier.get("adj", 0.0),
        ))
    return tiers


def translate_urdb_to_rate_schedule(urdb_record: dict) -> RateSchedule:
    """Translate a raw URDB rate record into our canonical RateSchedule.

    Args:
        urdb_record: Raw URDB rate record dict from the API.

    Returns:
        Validated RateSchedule model.

    Raises:
        OpenEIError: If translation fails due to missing or malformed data.
    """
    try:
        # Energy rate structure (required)
        energy_rs = [
            _translate_rate_tiers(period)
            for period in urdb_record.get("energyratestructure", [[]])
        ]

        # Energy schedules (required)
        energy_wd = urdb_record.get("energyweekdayschedule", [])
        energy_we = urdb_record.get("energyweekendschedule", [])

        # TOU demand (optional group)
        demand_rs = None
        demand_wd = None
        demand_we = None
        if "demandratestructure" in urdb_record:
            demand_rs = [
                _translate_rate_tiers(period)
                for period in urdb_record["demandratestructure"]
            ]
            demand_wd = urdb_record.get("demandweekdayschedule")
            demand_we = urdb_record.get("demandweekendschedule")

        # Flat demand (optional group)
        flat_rs = None
        flat_months = None
        if "flatdemandstructure" in urdb_record:
            flat_rs = [
                _translate_rate_tiers(period)
                for period in urdb_record["flatdemandstructure"]
            ]
            flat_months = urdb_record.get("flatdemandmonths")

        # Fixed charges
        fixed_charge = urdb_record.get("fixedchargefirstmeter", 0.0)
        fixed_units = urdb_record.get("fixedchargeunits", "$/month")
        fixed_charges = FixedCharges(
            fixed_charge_first_meter=fixed_charge,
            fixed_charge_units=fixed_units,
        )

        # Effective date
        startdate = urdb_record.get("startdate")
        effective_date = None
        if startdate:
            effective_date = datetime.fromtimestamp(
                startdate, tz=timezone.utc
            ).strftime("%Y-%m-%d")

        schedule = RateSchedule(
            utility_name=urdb_record.get("utility", "Unknown"),
            tariff_name=urdb_record.get("name", "Unknown"),
            effective_date=effective_date,
            sector=urdb_record.get("sector", "commercial"),
            description=urdb_record.get("description", ""),
            source="openei",
            openei_label=urdb_record.get("label", ""),
            fixed_charges=fixed_charges,
            energyratestructure=energy_rs,
            energyweekdayschedule=energy_wd,
            energyweekendschedule=energy_we,
            demandratestructure=demand_rs,
            demandweekdayschedule=demand_wd,
            demandweekendschedule=demand_we,
            flatdemandstructure=flat_rs,
            flatdemandmonths=flat_months,
        )

        logger.info(
            "Translated URDB rate: %s / %s (label=%s)",
            schedule.utility_name,
            schedule.tariff_name,
            schedule.openei_label,
        )
        return schedule

    except OpenEIError:
        raise
    except Exception as e:
        raise OpenEIError(
            f"Failed to translate URDB record: {e}",
            context={
                "api": "OpenEI URDB",
                "label": urdb_record.get("label", "unknown"),
                "error": str(e),
            },
        ) from e


def fetch_and_translate(
    utility_name: str,
    tariff_name: str,
    api_key: str | None = None,
) -> RateSchedule:
    """Search for a rate by utility and tariff name, fetch it, and translate.

    Finds the best matching rate by case-insensitive substring match on the
    tariff name. If multiple matches, prefers the most recent (highest startdate).

    Args:
        utility_name: Utility name to search.
        tariff_name: Tariff name substring to match.
        api_key: Explicit API key, or None to use env var.

    Returns:
        Translated RateSchedule model.

    Raises:
        OpenEIError: If no matching rate is found, or on API/translation errors.
    """
    rates = search_rates(utility_name=utility_name, tariff_name=tariff_name, api_key=api_key)

    if not rates:
        # Fetch all rates to show available names in error message
        all_rates = search_rates(utility_name=utility_name, api_key=api_key)
        available = [r["name"] for r in all_rates[:20]]
        raise OpenEIError(
            f"No rate matching '{tariff_name}' found for utility '{utility_name}'. "
            f"Available rates: {available}",
            context={
                "api": "OpenEI URDB",
                "utility": utility_name,
                "tariff_name": tariff_name,
            },
        )

    # Pick most recent match
    best = max(rates, key=lambda r: r.get("startdate", 0))
    logger.info("Selected rate '%s' (label=%s)", best["name"], best["label"])

    record = fetch_rate(best["label"], api_key=api_key)
    return translate_urdb_to_rate_schedule(record)
