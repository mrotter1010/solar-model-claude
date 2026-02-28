"""PySAM model configuration — maps SiteConfig fields to PySAM Pvsamv1 parameters."""

import math
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import PySAM.Pvsamv1 as pvsam

from src.config.schema import SiteConfig
from src.pysam_integration.cec_database import (
    CECDatabase,
    CECInverterParams,
    CECModuleParams,
)
from src.pysam_integration.exceptions import ValidationError
from src.pysam_integration.string_calculator import StringCalculator, StringConfiguration
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

MAX_DC_AC_RATIO = 2.0


@dataclass
class PySAMModelConfig:
    """Container for a configured PySAM model and its metadata."""

    model: pvsam.Pvsamv1
    site_config: SiteConfig
    module_params: CECModuleParams
    inverter_params: CECInverterParams
    inverter_count: int
    dc_ac_ratio: float
    string_config: StringConfiguration | None = None
    metadata: dict[str, object] = field(default_factory=dict)


class ModelConfigurator:
    """Configures a PySAM Pvsamv1 model from a SiteConfig."""

    def __init__(self, cec_database: CECDatabase | None = None) -> None:
        self.cec_db = cec_database or CECDatabase()
        self.string_calc = StringCalculator()

    def configure_model(self, site_config: SiteConfig) -> PySAMModelConfig:
        """Configure a PySAM Pvsamv1 model from a validated SiteConfig.

        Args:
            site_config: Validated site configuration from CSV input.

        Returns:
            PySAMModelConfig with the configured model and metadata.

        Raises:
            ValidationError: If DC/AC ratio exceeds maximum.
            CECDatabaseError: If module or inverter not found.
        """
        logger.info(f"Configuring PySAM model for site: {site_config.site_name}")

        # Look up equipment from CEC database
        module_params = self.cec_db.get_module_params(site_config.panel_model)
        inverter_params = self.cec_db.get_inverter_params(site_config.inverter_model)

        # Validate DC/AC ratio
        dc_ac_ratio = self._validate_dc_ac_ratio(site_config)

        # Calculate inverter count
        inverter_count = self._calculate_inverter_count(
            site_config, inverter_params
        )

        # Create and configure the PySAM model
        model = pvsam.new()

        self._configure_system_capacity(model, site_config)
        self._configure_module(model, module_params)
        self._configure_inverter(model, inverter_params, inverter_count)
        string_config = self._configure_array(
            model, site_config, module_params
        )
        self._configure_losses(model, site_config)

        if site_config.weather_file_path is not None:
            self._configure_weather_file(model, site_config)

        logger.info(
            f"PySAM model configured: DC/AC={dc_ac_ratio:.2f}, "
            f"inverters={inverter_count}, "
            f"capacity={site_config.system_capacity_kw:.1f} kW, "
            f"strings={string_config.nstrings}×{string_config.modules_per_string}"
        )

        return PySAMModelConfig(
            model=model,
            site_config=site_config,
            module_params=module_params,
            inverter_params=inverter_params,
            inverter_count=inverter_count,
            dc_ac_ratio=dc_ac_ratio,
            string_config=string_config,
        )

    def _validate_dc_ac_ratio(self, site_config: SiteConfig) -> float:
        """Validate DC/AC ratio is within acceptable bounds.

        Args:
            site_config: Site configuration with DC and AC capacities.

        Returns:
            The calculated DC/AC ratio.

        Raises:
            ValidationError: If ratio exceeds MAX_DC_AC_RATIO.
        """
        ratio = site_config.dc_size_mw / site_config.ac_installed_mw
        logger.debug(f"DC/AC ratio: {ratio:.3f}")

        if ratio > MAX_DC_AC_RATIO:
            raise ValidationError(
                f"DC/AC ratio {ratio:.2f} exceeds maximum {MAX_DC_AC_RATIO:.1f}. "
                f"DC={site_config.dc_size_mw} MW, AC={site_config.ac_installed_mw} MW."
            )

        return ratio

    def _calculate_inverter_count(
        self, site_config: SiteConfig, inverter_params: CECInverterParams
    ) -> int:
        """Calculate number of inverters needed.

        Args:
            site_config: Site configuration with AC capacity.
            inverter_params: Inverter parameters with Paco rating.

        Returns:
            Number of inverters (ceiling rounded).
        """
        ac_capacity_w = site_config.ac_installed_mw * 1_000_000
        count = math.ceil(ac_capacity_w / inverter_params.paco)
        logger.debug(
            f"Inverter count: {count} "
            f"(AC={ac_capacity_w:.0f}W / Paco={inverter_params.paco:.0f}W)"
        )
        return count

    def _configure_system_capacity(
        self, model: pvsam.Pvsamv1, site_config: SiteConfig
    ) -> None:
        """Set system capacity from site config."""
        model.SystemDesign.system_capacity = site_config.system_capacity_kw

    def _configure_module(
        self, model: pvsam.Pvsamv1, module_params: CECModuleParams
    ) -> None:
        """Set module parameters on the PySAM model."""
        model.Module.module_model = 1  # CEC Performance Model

        cec = model.CECPerformanceModelWithModuleDatabase
        cec.cec_area = module_params.area
        cec.cec_v_mp_ref = module_params.vmp
        cec.cec_i_mp_ref = module_params.imp
        cec.cec_v_oc_ref = module_params.voc
        cec.cec_i_sc_ref = module_params.isc

    def _configure_inverter(
        self,
        model: pvsam.Pvsamv1,
        inverter_params: CECInverterParams,
        inverter_count: int,
    ) -> None:
        """Set inverter parameters on the PySAM model."""
        model.Inverter.inverter_model = 0  # CEC Database
        model.Inverter.inv_snl_paco = inverter_params.paco

        inv_db = model.InverterCECDatabase
        inv_db.inv_snl_pdco = inverter_params.pdco
        inv_db.inv_snl_vdco = inverter_params.vdco
        inv_db.inv_snl_pso = inverter_params.pso
        inv_db.inv_snl_vdcmax = inverter_params.vdcmax

        model.SystemDesign.inverter_count = inverter_count

    def _configure_array(
        self,
        model: pvsam.Pvsamv1,
        site_config: SiteConfig,
        module_params: CECModuleParams,
    ) -> StringConfiguration:
        """Set array configuration — tracking, tilt, azimuth, GCR, bifaciality, strings."""
        # Tracking mode
        model.SystemDesign.subarray1_track_mode = site_config.tracking_mode

        # Tilt / rotation limit
        if site_config.racking == "tracker":
            model.SystemDesign.subarray1_tilt = 0
            model.SystemDesign.subarray1_rotlim = site_config.rotation_limit
        else:
            model.SystemDesign.subarray1_tilt = site_config.tilt

        # Azimuth and GCR
        model.SystemDesign.subarray1_azimuth = site_config.azimuth
        model.SystemDesign.subarray1_gcr = site_config.gcr

        # Shading mode: 1 = standard (non-linear)
        model.Shading.subarray1_shade_mode = 1

        # Bifaciality (on CEC module group)
        cec = model.CECPerformanceModelWithModuleDatabase
        if site_config.bifacial:
            cec.cec_is_bifacial = 1
            cec.cec_bifaciality = 0.7
        else:
            cec.cec_is_bifacial = 0
            cec.cec_bifaciality = 0.0

        # Module orientation: 0=portrait, 1=landscape (on Layout group)
        model.Layout.subarray1_mod_orient = (
            0 if site_config.module_orientation == "portrait" else 1
        )

        # Terrain slope: flat ground default
        model.SystemDesign.subarray1_slope_tilt = 0.0
        model.SystemDesign.subarray1_slope_azm = 180.0  # South-facing

        # Ground clearance (on CEC module group, tracker only)
        if site_config.racking == "tracker":
            cec.cec_ground_clearance_height = site_config.ground_clearance_height_m

        # String sizing
        string_config = self.string_calc.calculate_strings(
            site_config.dc_size_mw, module_params.pmax
        )
        model.SystemDesign.subarray1_nstrings = string_config.nstrings
        model.SystemDesign.subarray1_modules_per_string = (
            string_config.modules_per_string
        )

        return string_config

    def _configure_losses(
        self, model: pvsam.Pvsamv1, site_config: SiteConfig
    ) -> None:
        """Set loss parameters — wiring, transformer, availability, mismatch, LID."""
        # DC and AC wiring losses
        model.Losses.subarray1_dcwiring_loss = site_config.dc_wiring_loss_percent
        model.Losses.acwiring_loss = site_config.ac_wiring_loss_percent

        # Transformer losses (80/20 split: load/no-load)
        model.Losses.transformer_load_loss = (
            site_config.transformer_losses_percent * 0.8
        )
        model.Losses.transformer_no_load_loss = (
            site_config.transformer_losses_percent * 0.2
        )

        # Availability (CSV is downtime %, PySAM adjust_constant is availability %)
        model.AdjustmentFactors.adjust_constant = site_config.availability_for_pysam

        # Module mismatch and LID
        model.Losses.subarray1_mismatch_loss = site_config.module_mismatch_percent
        model.Losses.subarray1_diodeconn_loss = 0.5  # Default
        model.Losses.subarray1_tracking_loss = 0.0
        model.Losses.subarray1_nameplate_loss = 0.0

    def _configure_weather_file(
        self, model: pvsam.Pvsamv1, site_config: SiteConfig
    ) -> None:
        """Set the solar resource file path and monthly albedo from weather data."""
        model.SolarResource.solar_resource_file = str(site_config.weather_file_path)
        logger.debug(f"Weather file set: {site_config.weather_file_path}")

        # Calculate monthly albedo from hourly weather file data
        monthly_albedo = self._calculate_monthly_albedo(site_config.weather_file_path)
        model.SolarResource.albedo = monthly_albedo
        logger.debug(f"Monthly albedo set: {[round(a, 4) for a in monthly_albedo]}")

    @staticmethod
    def _calculate_monthly_albedo(weather_file_path: Path) -> list[float]:
        """Aggregate hourly surface albedo from weather file to 12 monthly averages.

        Args:
            weather_file_path: Path to PySAM-format weather CSV (2 header rows).

        Returns:
            List of 12 monthly average albedo values. Falls back to 0.2 for
            any month with missing data.
        """
        default_albedo = 0.2
        try:
            df = pd.read_csv(weather_file_path, skiprows=2)
            df["Month"] = pd.to_datetime(
                df["Year"].astype(str) + "-" + df["Month"].astype(str) + "-" + df["Day"].astype(str),
                format="%Y-%m-%d",
            ).dt.month
            monthly_albedo = [
                df[df["Month"] == m]["Surface Albedo"].mean() for m in range(1, 13)
            ]
            # Replace NaN with default for months with no data
            return [
                a if pd.notna(a) else default_albedo for a in monthly_albedo
            ]
        except Exception as exc:
            logger.warning(
                f"Could not calculate monthly albedo from {weather_file_path}: {exc}. "
                f"Using default {default_albedo} for all months."
            )
            return [default_albedo] * 12
