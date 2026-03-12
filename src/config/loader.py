"""CSV configuration loader for solar site parameters."""

from pathlib import Path

import pandas as pd

from ..utils.exceptions import ConfigValidationError
from ..utils.logger import setup_logger
from .schema import SiteConfig

logger = setup_logger(__name__)

# Mapping from CSV column names to SiteConfig field names
COLUMN_MAP: dict[str, str] = {
    "Run Name": "run_name",
    "Site Name": "site_name",
    "Customer": "customer",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "BESS Dispatch Required": "bess_dispatch_required",
    "BESS Optimization Required": "bess_optimization_required",
    "DC Size (MW)": "dc_size_mw",
    "AC Installed (MW)": "ac_installed_mw",
    "AC POI (MW)": "ac_poi_mw",
    "Racking": "racking",
    "Tilt": "tilt",
    "Azimuth": "azimuth",
    "Module Orientation": "module_orientation",
    "Number of Modules": "number_of_modules",
    "Ground Clearance Height (m)": "ground_clearance_height_m",
    "Panel Model": "panel_model",
    "Bifacial": "bifacial",
    "Inverter Model": "inverter_model",
    "GCR": "gcr",
    "Shading (%)": "shading_percent",
    "DC Wiring Loss (%)": "dc_wiring_loss_percent",
    "AC Wiring Loss (%)": "ac_wiring_loss_percent",
    "Transformer Losses (%)": "transformer_losses_percent",
    "Degradation (%)": "degradation_percent",
    "Availability (%)": "availability_percent",
    "Module Mismatch (%)": "module_mismatch_percent",
    "LID(%)": "lid_percent",
    "Report": "report",
    "Resource File Path": "resource_file_path",
    "Ground Truth Data File": "ground_truth_data_file",
}


def load_config(csv_path: Path) -> list[SiteConfig]:
    """Load and validate site configurations from CSV.

    Args:
        csv_path: Path to CSV file with site configurations.

    Returns:
        List of validated SiteConfig objects.

    Raises:
        ConfigValidationError: If CSV cannot be read or validation fails.
    """
    if not csv_path.exists():
        raise ConfigValidationError(
            f"Config file not found: {csv_path}",
            context={"path": str(csv_path)},
        )

    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} rows from {csv_path}")
    except Exception as e:
        raise ConfigValidationError(
            f"Failed to read CSV: {e}",
            context={"path": str(csv_path), "error": str(e)},
        )

    # Strip whitespace from column names and rename to snake_case
    df.columns = df.columns.str.strip()
    df = df.rename(columns=COLUMN_MAP)

    # Validate each row
    configs: list[SiteConfig] = []
    for idx, row in df.iterrows():
        try:
            row_dict = row.to_dict()
            # Convert NaN to None for optional fields
            row_dict = {
                k: (None if pd.isna(v) else v) for k, v in row_dict.items()
            }
            config = SiteConfig(**row_dict)
            configs.append(config)
            logger.debug(f"Row {idx + 2}: {config.site_name} validated successfully")
        except Exception as e:
            logger.error(f"Validation failed for row {idx + 2}: {e}")
            raise ConfigValidationError(
                f"Validation failed for row {idx + 2}",
                context={
                    "row_number": idx + 2,
                    "site_name": row_dict.get("site_name", "Unknown"),
                    "error": str(e),
                },
            )

    _validate_site_coordinates(configs)
    _validate_unique_run_names(configs)

    logger.info(f"Successfully validated {len(configs)} site configurations")
    return configs


def _validate_site_coordinates(configs: list[SiteConfig]) -> None:
    """Validate that rows sharing a site name have identical coordinates.

    Args:
        configs: List of validated SiteConfig objects.

    Raises:
        ConfigValidationError: If a site name maps to conflicting lat/lon values.
    """
    seen: dict[str, tuple[float, float]] = {}
    for config in configs:
        name = config.site_name
        coords = (config.latitude, config.longitude)
        if name in seen:
            if seen[name] != coords:
                raise ConfigValidationError(
                    f"Site '{name}' has conflicting coordinates: "
                    f"{seen[name]} vs {coords}",
                    context={
                        "site_name": name,
                        "first_coords": seen[name],
                        "conflicting_coords": coords,
                    },
                )
        else:
            seen[name] = coords


def _validate_unique_run_names(configs: list[SiteConfig]) -> None:
    """Validate that all run names in the batch are unique.

    Args:
        configs: List of validated SiteConfig objects.

    Raises:
        ConfigValidationError: If any run name appears more than once.
    """
    seen: dict[str, int] = {}
    for config in configs:
        seen[config.run_name] = seen.get(config.run_name, 0) + 1

    duplicates = [name for name, count in seen.items() if count > 1]
    if duplicates:
        raise ConfigValidationError(
            f"Duplicate run names found: {duplicates}",
            context={"duplicate_run_names": duplicates},
        )


def get_unique_locations(sites: list[SiteConfig]) -> list[tuple[float, float]]:
    """Extract unique (latitude, longitude) tuples from site configs.

    This avoids duplicate climate data pulls for sites at the same location.

    Args:
        sites: List of validated SiteConfig objects.

    Returns:
        List of unique (latitude, longitude) tuples.
    """
    locations = [site.location for site in sites]
    unique_locations = list(set(locations))

    logger.info(
        f"Found {len(unique_locations)} unique locations from {len(sites)} sites"
    )

    return unique_locations
