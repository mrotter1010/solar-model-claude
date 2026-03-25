"""Main orchestrator: simulation results -> PDF report.

Calls data extractors, chart builders, and PDF builder in sequence
to produce a complete solar production analysis report.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Optional

from src.bess.report_section import (
    generate_dispatch_profile_chart,
    generate_heatmap_chart,
)
from src.reporting.chart_builder import (
    generate_loss_waterfall_chart,
    generate_monthly_production_chart,
)
from src.reporting.data_extractor import (
    extract_loss_waterfall,
    extract_monthly_production,
    extract_site_summary,
    generate_narrative,
)
from src.reporting.pdf_builder import build_pdf
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def generate_report(
    site_config: dict,
    loss_data: dict,
    output_dir: Path,
    summary: Optional[dict] = None,
) -> Optional[Path]:
    """Generate a complete PDF report from site config and simulation results.

    Orchestrates the full reporting pipeline:
      1. Extract site summary, monthly production, and loss waterfall data
      2. Generate narrative text
      3. Generate chart PNGs to temp files
      4. Assemble PDF report
      5. Clean up temp chart files

    Args:
        site_config: Dict with SiteConfig fields (site_name, customer,
            latitude, longitude, dc_size_mw, etc.).
        loss_data: Dict from SimulationResult.loss_data containing
            annual energy totals, loss percentages, and monthly arrays.
        output_dir: Directory for the output PDF file.
        summary: Optional pipeline summary dict. When present, enables
            BESS dispatch page (if summary["bess_dispatch"] exists).

    Returns:
        Path to the generated PDF on success, or None on failure.
    """
    try:
        # 1. Extract site summary
        site_summary = extract_site_summary(site_config, loss_data)
        site_name = site_summary.get("site_name", "unknown")
        logger.info(f"Extracted site summary for {site_name}")

        # 2. Extract monthly production
        monthly_data = extract_monthly_production(loss_data)
        if monthly_data is None:
            logger.error("Failed to extract monthly production data")
            return None

        # 3. Extract loss waterfall
        waterfall_data = extract_loss_waterfall(loss_data)
        if not waterfall_data:
            logger.error("Failed to extract loss waterfall data")
            return None

        # 4. Generate narrative
        narrative = generate_narrative(site_summary, loss_data, waterfall_data)
        logger.info("Generated narrative text")

        # 5. Generate charts to temp files
        temp_dir = tempfile.mkdtemp(prefix="solar_report_")
        temp_path = Path(temp_dir)

        monthly_chart_path = temp_path / "monthly_production.png"
        monthly_result = generate_monthly_production_chart(monthly_data, monthly_chart_path)
        if monthly_result is None:
            logger.error("Failed to generate monthly production chart")
            return None

        waterfall_chart_path = temp_path / "loss_waterfall.png"
        waterfall_result = generate_loss_waterfall_chart(waterfall_data, waterfall_chart_path)
        if waterfall_result is None:
            logger.error("Failed to generate loss waterfall chart")
            return None

        logger.info(f"Generated chart PNGs in {temp_dir}")

        # 6. Build PDF
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Sanitize site name for filename
        safe_name = site_name.replace(" ", "_").replace("/", "_")
        pdf_path = output_dir / f"{safe_name}_report.pdf"

        # Generate BESS charts if dispatch data is available
        bess_dispatch = None
        bess_heatmap_path = None
        bess_dispatch_chart_path = None

        if summary is not None and "bess_dispatch" in summary:
            bess_dispatch = summary["bess_dispatch"]
            hm = bess_dispatch.get("heatmap_data")
            if hm is not None:
                bess_heatmap_path = temp_path / "bess_heatmap.png"
                generate_heatmap_chart(hm, bess_heatmap_path)

            dp_load = bess_dispatch.get("dispatch_profile_load")
            dp_solar = bess_dispatch.get("dispatch_profile_solar")
            dp_battery = bess_dispatch.get("dispatch_profile_battery")
            dp_month = bess_dispatch.get("dispatch_profile_month", "July")
            dp_export = bess_dispatch.get("dispatch_profile_export")
            if dp_load and dp_solar and dp_battery:
                bess_dispatch_chart_path = temp_path / "bess_dispatch_profile.png"
                generate_dispatch_profile_chart(
                    dp_load, dp_solar, dp_battery, dp_month,
                    bess_dispatch_chart_path,
                    export_kw=dp_export,
                )

            logger.info("Generated BESS charts")

        bess_sizing = (
            summary.get("bess_sizing") if summary is not None else None
        )

        result = build_pdf(
            output_path=pdf_path,
            site_summary=site_summary,
            narrative=narrative,
            monthly_chart_path=monthly_chart_path,
            waterfall_chart_path=waterfall_chart_path,
            bess_dispatch=bess_dispatch,
            bess_heatmap_path=bess_heatmap_path,
            bess_dispatch_chart_path=bess_dispatch_chart_path,
            bess_sizing=bess_sizing,
        )

        if result is None:
            logger.error("Failed to build PDF report")
            return None

        # 7. Clean up temp chart PNGs
        for chart_file in temp_path.glob("*.png"):
            chart_file.unlink()
        temp_path.rmdir()
        logger.info("Cleaned up temp chart files")

        logger.info(f"Report generated: {pdf_path}")
        return pdf_path

    except Exception as exc:
        logger.error(f"Report generation failed: {exc}")
        return None
