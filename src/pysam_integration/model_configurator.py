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
            model, site_config, module_params, inverter_params
        )
        self._configure_losses(model, site_config)

        if site_config.weather_file_path is not None:
            self._configure_weather_file(model, site_config)

        # Debug: log all critical PySAM parameters for diagnostics
        sd = model.SystemDesign
        logger.info(
            f"PySAM model configured: DC/AC={dc_ac_ratio:.2f}, "
            f"inverters={inverter_count}, "
            f"capacity={site_config.system_capacity_kw:.1f} kW, "
            f"strings={string_config.nstrings}×{string_config.modules_per_string}"
        )
        rotlim = sd.subarray1_rotlim if site_config.racking == "tracker" else "N/A"
        logger.info(
            f"  SystemDesign: system_capacity={sd.system_capacity}, "
            f"track_mode={sd.subarray1_track_mode}, "
            f"tilt={sd.subarray1_tilt}, "
            f"azimuth={sd.subarray1_azimuth}, "
            f"rotlim={rotlim}, "
            f"gcr={sd.subarray1_gcr}"
        )
        logger.info(
            f"  Array: modules_per_string={sd.subarray1_modules_per_string}, "
            f"nstrings={sd.subarray1_nstrings}, "
            f"inverter_count={sd.inverter_count}, "
            f"backtrack={sd.subarray1_backtrack}"
        )
        logger.info(
            f"  Module: model={model.Module.module_model}, "
            f"area={model.SimpleEfficiencyModuleModel.spe_area}, "
            f"eff={model.SimpleEfficiencyModuleModel.spe_eff4:.2f}%, "
            f"vmp={model.SimpleEfficiencyModuleModel.spe_vmp}"
        )
        logger.info(
            f"  Inverter: model={model.Inverter.inverter_model}, "
            f"paco={model.Inverter.inv_snl_paco}, "
            f"vdco={model.InverterCECDatabase.inv_snl_vdco}, "
            f"vdcmax={model.InverterCECDatabase.inv_snl_vdcmax}"
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
        """Set module parameters using the Simple Efficiency module model."""
        model.Module.module_model = 0  # Simple Efficiency Module Model

        spe = model.SimpleEfficiencyModuleModel
        spe.spe_area = module_params.area
        spe.spe_vmp = module_params.vmp
        spe.spe_voc = module_params.voc

        # Efficiency at 5 irradiance levels (W/m²)
        # Slight derating at low irradiance is realistic
        eff = module_params.efficiency * 100  # Convert fraction to %
        spe.spe_rad0 = 200.0
        spe.spe_rad1 = 400.0
        spe.spe_rad2 = 600.0
        spe.spe_rad3 = 800.0
        spe.spe_rad4 = 1000.0
        spe.spe_eff0 = eff * 0.90
        spe.spe_eff1 = eff * 0.95
        spe.spe_eff2 = eff * 0.98
        spe.spe_eff3 = eff * 0.99
        spe.spe_eff4 = eff
        spe.spe_reference = 4  # Reference at 1000 W/m²

        # Thermal model defaults
        spe.spe_temp_coeff = -0.35  # %/°C typical for crystalline Si
        spe.spe_fd = 1.0  # Diffuse utilization factor
        spe.spe_a = -3.56  # NOCT cell temp coefficient a
        spe.spe_b = -0.075  # NOCT cell temp coefficient b
        spe.spe_dT = 3.0  # Cell-to-module temperature delta
        spe.spe_module_structure = 0  # Glass/Cell/Polymer Sheet

    def _configure_inverter(
        self,
        model: pvsam.Pvsamv1,
        inverter_params: CECInverterParams,
        inverter_count: int,
    ) -> None:
        """Set inverter parameters using the CEC/Sandia model.

        PySAM's Inverter.inv_snl_paco is the total system AC power limit,
        while InverterCECDatabase params describe a single inverter for the
        Sandia efficiency model. inverter_count scales the single-inverter
        output to system level.
        """
        model.Inverter.inverter_model = 0  # CEC Database (Sandia)

        inv_db = model.InverterCECDatabase
        inv_db.inv_snl_paco = inverter_params.paco
        inv_db.inv_snl_pdco = inverter_params.pdco
        inv_db.inv_snl_vdco = inverter_params.vdco
        inv_db.inv_snl_pso = inverter_params.pso
        inv_db.inv_snl_vdcmax = inverter_params.vdcmax
        inv_db.inv_snl_c0 = inverter_params.c0
        inv_db.inv_snl_c1 = inverter_params.c1
        inv_db.inv_snl_c2 = inverter_params.c2
        inv_db.inv_snl_c3 = inverter_params.c3
        inv_db.inv_snl_pnt = inverter_params.pnt
        inv_db.inv_tdc_cec_db = [[1500, 52.8, 0]]

        # Set total system AC limit AFTER InverterCECDatabase (PySAM links them)
        model.Inverter.inv_snl_paco = inverter_params.paco * inverter_count
        model.SystemDesign.inverter_count = inverter_count

    def _configure_array(
        self,
        model: pvsam.Pvsamv1,
        site_config: SiteConfig,
        module_params: CECModuleParams,
        inverter_params: CECInverterParams | None = None,
    ) -> StringConfiguration:
        """Set array configuration — tracking, tilt, azimuth, GCR, bifaciality, strings."""
        # Disable additional subarrays (we only use subarray1)
        model.SystemDesign.subarray2_enable = 0
        model.SystemDesign.subarray3_enable = 0
        model.SystemDesign.subarray4_enable = 0

        # Tracking mode
        model.SystemDesign.subarray1_track_mode = site_config.tracking_mode

        # Tilt / rotation limit / backtracking
        if site_config.racking == "tracker":
            model.SystemDesign.subarray1_tilt = 0
            model.SystemDesign.subarray1_rotlim = site_config.rotation_limit
            model.SystemDesign.subarray1_backtrack = 1
        else:
            model.SystemDesign.subarray1_tilt = site_config.tilt
            model.SystemDesign.subarray1_backtrack = 0

        # Azimuth and GCR
        model.SystemDesign.subarray1_azimuth = site_config.azimuth
        model.SystemDesign.subarray1_gcr = site_config.gcr

        # Shading mode: 1 = standard (non-linear)
        model.Shading.subarray1_shade_mode = 1

        # Bifaciality (on Simple Efficiency module group)
        spe = model.SimpleEfficiencyModuleModel
        if site_config.bifacial:
            spe.spe_is_bifacial = 1
            spe.spe_bifaciality = 0.7
            spe.spe_bifacial_transmission_factor = 0.013
        else:
            spe.spe_is_bifacial = 0
            spe.spe_bifaciality = 0.0
            spe.spe_bifacial_transmission_factor = 0.0

        # Module orientation: 0=portrait, 1=landscape (on Layout group)
        model.Layout.subarray1_mod_orient = (
            0 if site_config.module_orientation == "portrait" else 1
        )

        # Terrain slope: flat ground default
        model.SystemDesign.subarray1_slope_tilt = 0.0
        model.SystemDesign.subarray1_slope_azm = 180.0  # South-facing

        # Number of modules along row width (table width)
        model.Layout.subarray1_nmodx = site_config.number_of_modules

        # Ground clearance (on Simple Efficiency module group, tracker only)
        if site_config.racking == "tracker":
            spe.spe_bifacial_ground_clearance_height = site_config.ground_clearance_height_m

        # String sizing (voltage-aware: target ~80% of inverter Vdco)
        string_config = self.string_calc.calculate_strings(
            site_config.dc_size_mw,
            module_params.pmax,
            module_vmp=module_params.vmp,
            inverter_vdco=inverter_params.vdco if inverter_params else None,
        )
        model.SystemDesign.subarray1_nstrings = string_config.nstrings
        model.SystemDesign.subarray1_modules_per_string = (
            string_config.modules_per_string
        )

        # Number of modules along row length (table length = string length)
        model.Layout.subarray1_nmody = string_config.modules_per_string

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
        model.Losses.subarray1_electrical_mismatch = site_config.module_mismatch_percent
        model.Losses.subarray1_diodeconn_loss = 0.5  # Default
        model.Losses.subarray1_tracking_loss = 0.0
        model.Losses.subarray1_nameplate_loss = 0.0

        # Monthly soiling losses (constant 5% MVP default)
        model.Losses.subarray1_soiling = [5.0] * 12
        model.Losses.subarray1_rear_soiling_loss = 0.5

        # Rack self-shading (0% — already handled by GCR)
        model.Losses.subarray1_rack_shading = 0.0

        # DC optimizer loss (0% — not using module-level optimizers)
        model.Losses.dcoptimizer_loss = 0.0

        # Transmission loss (0% — POI assumed on-site)
        model.Losses.transmission_loss = 0.0

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
