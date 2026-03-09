"""Extract and format data from PySAM outputs for charts/tables.

Transforms raw loss_data dicts from SimulationResult into clean structures
suitable for matplotlib charts and reportlab tables in the PDF report.
"""

from __future__ import annotations

from pathlib import Path

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

MONTH_LABELS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

# Bounding boxes for US states with utility-scale solar potential.
# Format: (state_name, min_lat, max_lat, min_lon, max_lon)
_US_STATE_BOXES: list[tuple[str, float, float, float, float]] = [
    ("Arizona", 31.3, 37.0, -114.8, -109.0),
    ("California", 32.5, 42.0, -124.4, -114.1),
    ("Colorado", 37.0, 41.0, -109.1, -102.0),
    ("Florida", 24.5, 31.0, -87.6, -80.0),
    ("Georgia", 30.4, 35.0, -85.6, -80.8),
    ("Illinois", 37.0, 42.5, -91.5, -87.5),
    ("Indiana", 37.8, 41.8, -88.1, -84.8),
    ("Massachusetts", 41.2, 42.9, -73.5, -69.9),
    ("Michigan", 41.7, 48.3, -90.4, -82.4),
    ("Minnesota", 43.5, 49.4, -97.2, -89.5),
    ("Nevada", 35.0, 42.0, -120.0, -114.0),
    ("New Jersey", 38.9, 41.4, -75.6, -73.9),
    ("New Mexico", 31.3, 37.0, -109.1, -103.0),
    ("New York", 40.5, 45.0, -79.8, -71.9),
    ("North Carolina", 33.8, 36.6, -84.3, -75.5),
    ("Ohio", 38.4, 42.0, -84.8, -80.5),
    ("Oregon", 42.0, 46.3, -124.6, -116.5),
    ("Pennsylvania", 39.7, 42.3, -80.5, -74.7),
    ("Tennessee", 35.0, 36.7, -90.3, -81.6),
    ("Texas", 25.8, 36.5, -106.6, -93.5),
    ("Utah", 37.0, 42.0, -114.1, -109.0),
    ("Virginia", 36.5, 39.5, -83.7, -75.2),
    ("Washington", 45.5, 49.0, -124.8, -116.9),
]


def get_us_state_from_coords(latitude: float, longitude: float) -> str:
    """Look up US state from latitude/longitude using bounding boxes.

    Args:
        latitude: Site latitude in decimal degrees.
        longitude: Site longitude in decimal degrees.

    Returns:
        State name if coordinates fall within a known bounding box,
        or "Unknown" if no match.
    """
    for state, min_lat, max_lat, min_lon, max_lon in _US_STATE_BOXES:
        if min_lat <= latitude <= max_lat and min_lon <= longitude <= max_lon:
            return state
    logger.warning(
        f"No US state match for coordinates ({latitude}, {longitude})"
    )
    return "Unknown"


def extract_monthly_production(loss_data: dict) -> dict | None:
    """Extract monthly energy production in MWh from loss_data.

    Args:
        loss_data: Dict from SimulationResult.loss_data containing
            "monthly_energy" (12-element list of kWh values).

    Returns:
        Dict with "months" (12 abbreviated labels) and "energy_mwh"
        (12 floats converted from kWh), or None if monthly_energy
        is missing or not 12 elements.
    """
    monthly_energy = loss_data.get("monthly_energy")

    if monthly_energy is None or len(monthly_energy) != 12:
        logger.warning(
            "monthly_energy missing or wrong length in loss_data "
            f"(got {len(monthly_energy) if monthly_energy is not None else 'None'})"
        )
        return None

    energy_mwh = [kwh / 1000 for kwh in monthly_energy]

    return {
        "months": list(MONTH_LABELS),
        "energy_mwh": energy_mwh,
    }


def extract_loss_waterfall(loss_data: dict) -> list[dict]:
    """Build a waterfall chart dataset from PySAM loss_data.

    Uses actual energy deltas between PySAM's reported stage totals
    (annual_dc_nominal → annual_dc_gross → annual_dc_net →
    annual_ac_gross → annual_energy) rather than recomputing from
    percentages. The individual loss percentages are carried as
    sub_loss annotations only.

    The bifacial rear gain is embedded in the "POA & Module Losses"
    group as a negative sub_loss. It partially offsets the other POA
    losses. This is why we use actual energy deltas rather than
    recomputing from percentages — PySAM's stage totals already
    account for bifacial gain correctly.

    Args:
        loss_data: Dict from SimulationResult.loss_data containing
            annual energy totals and loss percentages.

    Returns:
        Ordered list of waterfall steps. Each step is a dict with
        keys: label, energy_kwh, loss_percent, type, sub_losses.
    """

    def _get(key: str) -> float:
        return float(loss_data.get(key, 0.0))

    annual_dc_nominal = _get("annual_dc_nominal")
    annual_dc_gross = _get("annual_dc_gross")
    annual_dc_net = _get("annual_dc_net")
    annual_ac_gross = _get("annual_ac_gross")
    annual_energy = _get("annual_energy")

    def _filter_sub_losses(subs: list[dict]) -> list[dict]:
        """Remove sub_losses where percent rounds to 0.0."""
        return [s for s in subs if round(s["percent"], 1) != 0.0]

    def _loss_pct(before: float, after: float) -> float:
        """Calculate loss percentage between two energy stages."""
        if before == 0.0:
            return 0.0
        return (1 - after / before) * 100

    steps: list[dict] = []

    # a. Start: Nominal DC Energy
    steps.append({
        "label": "Nominal DC Energy",
        "energy_kwh": annual_dc_nominal,
        "loss_percent": 0.0,
        "type": "start",
        "sub_losses": [],
    })

    # b. POA & Module Losses (nominal → gross)
    poa_module_delta = annual_dc_nominal - annual_dc_gross
    if round(poa_module_delta) != 0:
        steps.append({
            "label": "POA & Module Losses",
            "energy_kwh": poa_module_delta,
            "loss_percent": _loss_pct(annual_dc_nominal, annual_dc_gross),
            "type": "loss",
            "sub_losses": _filter_sub_losses([
                {"label": "Snow", "percent": _get("annual_dc_snow_loss_percent")},
                {"label": "Shading", "percent": _get("annual_poa_shading_loss_percent")},
                {"label": "Soiling", "percent": _get("annual_poa_soiling_loss_percent")},
                {"label": "Cover/IAM", "percent": _get("annual_poa_cover_loss_percent")},
                {"label": "Module Efficiency", "percent": _get("annual_dc_module_loss_percent")},
                {"label": "Bifacial Gain", "percent": -_get("annual_poa_rear_gain_percent")},
            ]),
        })

    # c. DC Losses (gross → net)
    dc_delta = annual_dc_gross - annual_dc_net
    if round(dc_delta) != 0:
        steps.append({
            "label": "DC Losses",
            "energy_kwh": dc_delta,
            "loss_percent": _loss_pct(annual_dc_gross, annual_dc_net),
            "type": "loss",
            "sub_losses": _filter_sub_losses([
                {"label": "Mismatch", "percent": _get("annual_dc_mismatch_loss_percent")},
                {"label": "Bifacial Mismatch", "percent": _get("annual_bifacial_electrical_mismatch_percent")},
                {"label": "Diodes & Connections", "percent": _get("annual_dc_diodes_loss_percent")},
                {"label": "DC Wiring", "percent": _get("annual_dc_wiring_loss_percent")},
                {"label": "Tracking Error", "percent": _get("annual_dc_tracking_loss_percent")},
                {"label": "Nameplate/LID", "percent": _get("annual_dc_nameplate_loss_percent")},
            ]),
        })

    # d. Inverter Losses (dc_net → ac_gross)
    inv_delta = annual_dc_net - annual_ac_gross
    if round(inv_delta) != 0:
        steps.append({
            "label": "Inverter Losses",
            "energy_kwh": inv_delta,
            "loss_percent": _loss_pct(annual_dc_net, annual_ac_gross),
            "type": "loss",
            "sub_losses": _filter_sub_losses([
                {"label": "Inverter Clipping", "percent": _get("annual_ac_inv_clip_loss_percent")},
                {"label": "Inverter Efficiency", "percent": _get("annual_ac_inv_eff_loss_percent")},
            ]),
        })

    # e. AC Losses & Availability (ac_gross → annual_energy)
    ac_delta = annual_ac_gross - annual_energy
    if round(ac_delta) != 0:
        steps.append({
            "label": "AC Losses & Availability",
            "energy_kwh": ac_delta,
            "loss_percent": _loss_pct(annual_ac_gross, annual_energy),
            "type": "loss",
            "sub_losses": _filter_sub_losses([
                {"label": "AC Wiring", "percent": _get("annual_ac_wiring_loss_percent")},
                {"label": "Transformer", "percent": _get("annual_xfmr_loss_percent")},
                {"label": "Availability", "percent": _get("annual_ac_perf_adj_loss_percent")},
            ]),
        })

    # f. End: Net AC Energy
    steps.append({
        "label": "Net AC Energy",
        "energy_kwh": annual_energy,
        "loss_percent": 0.0,
        "type": "end",
        "sub_losses": [],
    })

    logger.info(
        f"Loss waterfall: {len(steps)} steps, "
        f"nominal={annual_dc_nominal:,.0f} kWh → net={annual_energy:,.0f} kWh"
    )

    return steps


def extract_site_summary(site_config: dict, loss_data: dict) -> dict:
    """Extract a flat summary dict for the site overview table.

    Args:
        site_config: Dict with SiteConfig fields (site_name, customer,
            latitude, longitude, dc_size_mw, ac_installed_mw, racking,
            tilt, azimuth, gcr, panel_model, number_of_modules,
            bifacial, inverter_model).
        loss_data: Dict from SimulationResult.loss_data containing
            capacity_factor, kwh_per_kw, performance_ratio, annual_energy.

    Returns:
        Flat dict with formatted site summary fields for report tables.
    """
    dc_mw = float(site_config["dc_size_mw"])
    ac_mw = float(site_config["ac_installed_mw"])

    lat = float(site_config["latitude"])
    lon = float(site_config["longitude"])

    return {
        "site_name": site_config["site_name"],
        "customer": site_config["customer"],
        "latitude": lat,
        "longitude": lon,
        "state": get_us_state_from_coords(lat, lon),
        "dc_capacity_mw": dc_mw,
        "ac_capacity_mw": ac_mw,
        "dc_ac_ratio": round(dc_mw / ac_mw, 2),
        "racking": site_config["racking"],
        "tilt": float(site_config["tilt"]),
        "azimuth": float(site_config["azimuth"]),
        "gcr": float(site_config["gcr"]),
        "module_model": site_config["panel_model"],
        "module_count": int(site_config["number_of_modules"]),
        "bifacial": bool(site_config["bifacial"]),
        "inverter_model": site_config["inverter_model"],
        "annual_energy_mwh": float(loss_data.get("annual_energy", 0.0)) / 1000,
        "capacity_factor_pct": float(loss_data.get("capacity_factor", 0.0)),
        "specific_yield_kwh_kwp": float(loss_data.get("kwh_per_kw", 0.0)),
        "performance_ratio_pct": float(loss_data.get("performance_ratio", 0.0)) * 100,
        "avg_daytime_ghi_wm2": float(loss_data.get("avg_daytime_ghi_wm2", 0.0)),
        "annual_ghi_kwh_m2": float(loss_data.get("annual_ghi_kwh_m2", 0.0)),
        "weather_year": "2023",
        "data_source": site_config.get("data_source", "nsrdb"),
        "resource_file_name": (
            Path(site_config["resource_file_path"]).name
            if site_config.get("resource_file_path")
            else None
        ),
    }


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _fmt_int(value: float) -> str:
    """Format a number as integer with comma separators."""
    return f"{int(round(value)):,}"


def _fmt_pct(value: float) -> str:
    """Format a number as percentage with 1 decimal place."""
    return f"{value:.1f}"


# ---------------------------------------------------------------------------
# Narrative text generation
# ---------------------------------------------------------------------------


def generate_narrative(
    site_summary: dict, loss_data: dict, waterfall_data: list[dict]
) -> dict:
    """Generate template-based narrative paragraphs for the PDF report.

    Args:
        site_summary: Output of extract_site_summary() — flat dict with
            site metadata, performance metrics, and weather data.
        loss_data: Raw loss_data dict from PySAM SimulationResult.
        waterfall_data: Output of extract_loss_waterfall() — ordered list
            of waterfall steps with label, energy_kwh, loss_percent, type,
            and sub_losses.

    Returns:
        Dict with four string keys: site_overview, solar_resource,
        production_summary, loss_summary.
    """
    # --- site_overview ---
    bifacial_text = "bifacial" if site_summary["bifacial"] else "monofacial"

    racking = site_summary["racking"]
    is_tracker = "track" in racking.lower()
    racking_text = "single-axis tracker" if is_tracker else "fixed-tilt"

    tilt = site_summary["tilt"]
    azimuth = site_summary["azimuth"]
    if is_tracker:
        tilt_text = f"a \u00b1{_fmt_pct(tilt)}\u00b0 tracker rotation limit"
    else:
        tilt_text = (
            f"{_fmt_pct(tilt)}\u00b0 fixed tilt at "
            f"{_fmt_pct(azimuth)}\u00b0 azimuth"
        )

    lat = site_summary["latitude"]
    lon = site_summary["longitude"]

    site_overview = (
        f"{site_summary['site_name']} is a proposed "
        f"{_fmt_pct(site_summary['dc_capacity_mw'])} MW-DC / "
        f"{_fmt_pct(site_summary['ac_capacity_mw'])} MW-AC solar PV facility "
        f"located in {site_summary['state']} "
        f"({lat}\u00b0N, {abs(lon)}\u00b0W). "
        f"The system uses {racking_text} mounting with "
        f"{site_summary['module_model']} {bifacial_text} modules and "
        f"{site_summary['inverter_model']} inverters, "
        f"at a DC-to-AC ratio of {site_summary['dc_ac_ratio']}. "
        f"The array is configured with a ground coverage ratio of "
        f"{site_summary['gcr']} and {tilt_text}."
    )

    # --- solar_resource ---
    annual_ghi = site_summary["annual_ghi_kwh_m2"]
    if annual_ghi > 2000:
        climate_note = "This represents an excellent solar resource."
    elif annual_ghi > 1600:
        climate_note = "This represents a good solar resource."
    elif annual_ghi > 1200:
        climate_note = "This represents a moderate solar resource."
    else:
        climate_note = "This represents a below-average solar resource."

    solar_resource = (
        f"Based on {site_summary['weather_year']} NSRDB satellite-derived "
        f"weather data, the site receives an annual global horizontal "
        f"irradiance of {_fmt_int(annual_ghi)} kWh/m\u00b2, with an average "
        f"daytime intensity of "
        f"{_fmt_int(site_summary['avg_daytime_ghi_wm2'])} W/m\u00b2. "
        f"{climate_note}"
    )

    # --- production_summary ---
    production_summary = (
        f"The system is projected to produce "
        f"{_fmt_int(site_summary['annual_energy_mwh'])} MWh of net AC energy "
        f"annually, corresponding to a capacity factor of "
        f"{_fmt_pct(site_summary['capacity_factor_pct'])}% and a specific "
        f"yield of "
        f"{_fmt_pct(site_summary['specific_yield_kwh_kwp'])} kWh/kWp. "
        f"The overall performance ratio is "
        f"{_fmt_pct(site_summary['performance_ratio_pct'])}%."
    )

    # --- loss_summary ---
    nominal_kwh = 0.0
    net_kwh = 0.0
    for step in waterfall_data:
        if step["type"] == "start":
            nominal_kwh = step["energy_kwh"]
        elif step["type"] == "end":
            net_kwh = step["energy_kwh"]

    loss_steps = [s for s in waterfall_data if s["type"] == "loss"]
    verbs = ["accounts for", "contributes", "represents"]

    sentences = []
    for i, step in enumerate(loss_steps):
        verb = verbs[i % len(verbs)]
        step_mwh = step["energy_kwh"] / 1000
        sub_parts = []
        for sub in step["sub_losses"]:
            if round(sub["percent"], 1) != 0.0:
                sub_parts.append(
                    f"{sub['label']} ({_fmt_pct(sub['percent'])}%)"
                )
        sub_text = ", ".join(sub_parts)
        sentence = (
            f"{step['label']} {verb} {_fmt_int(step_mwh)} MWh "
            f"({_fmt_pct(step['loss_percent'])}%), comprising {sub_text}."
        )
        sentences.append(sentence)

    loss_summary = (
        f"From a nominal DC energy of {_fmt_int(nominal_kwh / 1000)} MWh, "
        f"the system delivers {_fmt_int(net_kwh / 1000)} MWh after "
        f"accounting for system losses.\n" + "\n".join(sentences)
    )

    total_chars = (
        len(site_overview) + len(solar_resource)
        + len(production_summary) + len(loss_summary)
    )
    logger.info(f"Generated narrative: {total_chars} chars")

    return {
        "site_overview": site_overview,
        "solar_resource": solar_resource,
        "production_summary": production_summary,
        "loss_summary": loss_summary,
    }
