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

    logger.info(f"Successfully validated {len(configs)} site configurations")
    return configs


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
