"""Shared subhourly clipping correction logic.

Extracted from Pipeline._apply_subhourly_correction so the optimizer
can call the same logic without instantiating a Pipeline object.
"""

from src.config.schema import SiteConfig
from src.models.subhourly_correction import (
    compute_weather_features,
    get_model_version,
    predict_correction,
)
from src.models.timeseries_adjustment import apply_correction
from src.pysam_integration.simulator import SimulationResult
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def apply_subhourly_clipping_correction(
    result: SimulationResult,
    site: SiteConfig,
) -> dict[str, object]:
    """Apply subhourly clipping correction to a PySAM SimulationResult.

    Mutates result.hourly_data["ac_gross"] in place when the predicted
    correction is non-zero. Returns a metadata dict with correction details
    and raw (pre-correction) annual energy for the summary.

    Failures are logged as warnings and do not halt the caller.

    Args:
        result: Successful simulation result with hourly_data.
        site: Site configuration with weather file and system parameters.

    Returns:
        Dict with correction metadata, or empty dict if correction was
        skipped due to missing data or errors.
    """

    if result.hourly_data is None or site.weather_file_path is None:
        logger.debug(
            f"Subhourly correction skipped for {site.site_name}: "
            "missing hourly data or weather file"
        )
        return {}

    if "dc_net" not in result.hourly_data.columns:
        logger.debug(
            f"Subhourly correction skipped for {site.site_name}: "
            "dc_net column not in hourly data"
        )
        return {}

    try:
        # Capture raw (pre-correction) annual energy
        raw_ac_gross_kwh = float(result.hourly_data["ac_gross"].sum())

        # Compute weather features from the site's weather file
        weather_features = compute_weather_features(site.weather_file_path)

        # Get system parameters
        dcac_ratio = site.dc_size_mw / site.ac_installed_mw
        cf_60min = float(result.loss_data["capacity_factor_ac"])

        # Predict correction (clamped to >= 0 by the model)
        correction_pct = predict_correction(
            dcac_ratio=dcac_ratio,
            gcr=site.gcr,
            racking=site.racking,
            latitude=site.latitude,
            longitude=site.longitude,
            cf_60min=cf_60min,
            weather_features=weather_features,
        )

        # Apply correction to ac_gross if non-zero
        if correction_pct > 0:
            ac_capacity_kw = site.ac_installed_mw * 1000
            adjusted = apply_correction(
                hourly_gen_kwh=result.hourly_data["ac_gross"].tolist(),
                correction_pct=correction_pct,
                dc_hourly_kwh=result.hourly_data["dc_net"].tolist(),
                ac_capacity_kw=ac_capacity_kw,
            )
            result.hourly_data["ac_gross"] = adjusted

        model_version = get_model_version()

        # Compute raw annual energy post-shading for comparability
        shading_factor = 1 - site.shading_percent / 100
        raw_annual_energy_mwh = round(
            raw_ac_gross_kwh * shading_factor / 1000, 3
        )

        logger.info(
            f"Subhourly correction for {site.site_name}: "
            f"{correction_pct:.3f}% (model {model_version})"
        )

        return {
            "subhourly_correction_pct": round(correction_pct, 4),
            "subhourly_model_version": model_version,
            "raw_annual_energy_mwh": raw_annual_energy_mwh,
        }

    except FileNotFoundError as exc:
        logger.warning(
            f"Subhourly correction skipped for {site.site_name}: {exc}"
        )
        return {}
    except Exception as exc:
        logger.warning(
            f"Subhourly correction failed for {site.site_name}: {exc}"
        )
        return {}
