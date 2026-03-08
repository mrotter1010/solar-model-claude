"""Database query functions for retrieving run data."""

import uuid

from sqlalchemy.orm import Session

from src.database.connection import get_session
from src.database.models import Customer, Run, RunInput, Site
from src.utils.exceptions import SolarModelError
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def recreate_run_input(
    run_id: str, database_url: str | None = None
) -> dict:
    """Reconstruct the original CSV input row from a stored run.

    Joins runs → run_inputs → sites → customers to rebuild all 28
    original CSV columns.

    Args:
        run_id: UUID string of the run to reconstruct.
        database_url: Optional database URL override.

    Returns:
        Dict with all 28 original CSV column names as keys.

    Raises:
        SolarModelError: If run_id is not found.
    """
    try:
        run_uuid = uuid.UUID(run_id)
    except ValueError:
        raise SolarModelError(
            f"Invalid run ID format: {run_id!r}",
            context={"run_id": run_id},
        )

    with get_session(url=database_url) as session:
        run = session.query(Run).filter_by(id=run_uuid).first()
        if run is None:
            raise SolarModelError(
                f"Run not found: {run_id}",
                context={"run_id": run_id},
            )

        run_input = session.query(RunInput).filter_by(run_id=run_uuid).first()
        if run_input is None:
            raise SolarModelError(
                f"Run inputs not found for run: {run_id}",
                context={"run_id": run_id},
            )

        site = session.query(Site).filter_by(id=run.site_id).first()
        customer = session.query(Customer).filter_by(id=site.customer_id).first()

        return {
            "Run Name": run.run_name,
            "Site Name": site.name,
            "Customer": customer.name,
            "Latitude": run_input.latitude,
            "Longitude": run_input.longitude,
            "BESS Dispatch Required": run_input.bess_dispatch_required,
            "BESS Optimization Required": run_input.bess_optimization_required,
            "DC Size (MW)": run_input.dc_size_mw,
            "AC Installed (MW)": run_input.ac_installed_mw,
            "AC POI (MW)": run_input.ac_poi_mw,
            "Racking": run_input.racking,
            "Tilt": run_input.tilt,
            "Azimuth": run_input.azimuth,
            "Module Orientation": run_input.module_orientation,
            "Number of Modules": run_input.number_of_modules,
            "Ground Clearance Height (m)": run_input.ground_clearance_height_m,
            "Panel Model": run_input.panel_model,
            "Bifacial": run_input.bifacial,
            "Inverter Model": run_input.inverter_model,
            "GCR": run_input.gcr,
            "Shading (%)": run_input.shading_pct,
            "DC Wiring Loss (%)": run_input.dc_wiring_loss_pct,
            "AC Wiring Loss (%)": run_input.ac_wiring_loss_pct,
            "Transformer Losses (%)": run_input.transformer_losses_pct,
            "Degradation (%)": run_input.degradation_pct,
            "Availability (%)": run_input.availability_pct,
            "Module Mismatch (%)": run_input.module_mismatch_pct,
            "LID(%)": run_input.lid_pct,
        }
