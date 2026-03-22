"""reportlab PDF assembly for solar production analysis reports.

Builds a 3-page US Letter PDF:
  Page 1 — Cover / Site Summary table
  Page 2 — Narrative text + monthly production chart
  Page 3 — Loss analysis narrative + waterfall chart
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Image,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Page geometry
PAGE_WIDTH, PAGE_HEIGHT = letter
MARGIN = 1 * inch

# Typography
FONT_TITLE = "Helvetica-Bold"
FONT_HEADER = "Helvetica-Bold"
FONT_BODY = "Helvetica"
SIZE_TITLE = 20
SIZE_SUBTITLE = 14
SIZE_BODY = 10
SIZE_TABLE = 9
SIZE_FOOTER = 8

# Colors
LIGHT_GREY = colors.Color(0.93, 0.93, 0.93)
MED_GREY = colors.Color(0.80, 0.80, 0.80)


def _build_styles() -> dict[str, ParagraphStyle]:
    """Create paragraph styles for the report.

    Returns:
        Dict mapping style name to ParagraphStyle.
    """
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "ReportTitle",
            parent=base["Normal"],
            fontName=FONT_TITLE,
            fontSize=SIZE_TITLE,
            alignment=1,  # center
            spaceAfter=6,
        ),
        "subtitle": ParagraphStyle(
            "ReportSubtitle",
            parent=base["Normal"],
            fontName=FONT_HEADER,
            fontSize=SIZE_SUBTITLE,
            alignment=1,
            spaceAfter=4,
        ),
        "date_line": ParagraphStyle(
            "DateLine",
            parent=base["Normal"],
            fontName=FONT_BODY,
            fontSize=SIZE_BODY,
            alignment=1,
            spaceAfter=24,
        ),
        "section_header": ParagraphStyle(
            "SectionHeader",
            parent=base["Normal"],
            fontName=FONT_HEADER,
            fontSize=SIZE_SUBTITLE,
            spaceBefore=12,
            spaceAfter=6,
        ),
        "body": ParagraphStyle(
            "BodyText",
            parent=base["Normal"],
            fontName=FONT_BODY,
            fontSize=SIZE_BODY,
            leading=14,
            spaceAfter=8,
        ),
        "table_header": ParagraphStyle(
            "TableHeader",
            parent=base["Normal"],
            fontName=FONT_HEADER,
            fontSize=SIZE_TABLE,
        ),
        "table_cell": ParagraphStyle(
            "TableCell",
            parent=base["Normal"],
            fontName=FONT_BODY,
            fontSize=SIZE_TABLE,
        ),
    }


def _format_table_value(key: str, value: object) -> str:
    """Format a site summary value for display in the table.

    Args:
        key: The field name from site_summary dict.
        value: The raw value to format.

    Returns:
        Human-readable string representation.
    """
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, float):
        if "pct" in key or "ratio" in key:
            return f"{value:.2f}"
        if "mw" in key or "mwh" in key:
            return f"{value:,.1f}"
        if "ghi" in key:
            return f"{value:,.1f}"
        if key in ("latitude", "longitude"):
            return f"{value:.3f}"
        if key == "gcr":
            return f"{value:.2f}"
        return f"{value:,.1f}"
    return str(value)


def _format_data_source(site_summary: dict) -> str:
    """Format the resource data source display string.

    Args:
        site_summary: Site summary dict with data_source and
            optional resource_file_name keys.

    Returns:
        "NSRDB" or "Solcast (filename.csv)" depending on data source.
    """
    source = site_summary.get("data_source", "nsrdb")
    if source == "solcast":
        filename = site_summary.get("resource_file_name", "")
        return f"Solcast ({filename})" if filename else "Solcast"
    return "NSRDB"


def _build_site_summary_tables(
    site_summary: dict, styles: dict[str, ParagraphStyle]
) -> list:
    """Build the two site summary tables for page 1.

    Args:
        site_summary: Output of extract_site_summary().
        styles: Dict of ParagraphStyle objects.

    Returns:
        List of reportlab flowable objects (tables with spacers).
    """
    config_rows = [
        ("Site Name", site_summary.get("site_name", "")),
        ("Customer", site_summary.get("customer", "")),
        (
            "Location",
            f"{site_summary.get('state', '')}, "
            f"{_format_table_value('latitude', site_summary.get('latitude', 0))}\u00b0N / "
            f"{_format_table_value('longitude', abs(site_summary.get('longitude', 0)))}\u00b0W",
        ),
        ("DC Capacity (MW)", _format_table_value("dc_capacity_mw", site_summary.get("dc_capacity_mw", 0))),
        ("AC Capacity (MW)", _format_table_value("ac_capacity_mw", site_summary.get("ac_capacity_mw", 0))),
        ("DC/AC Ratio", _format_table_value("dc_ac_ratio", site_summary.get("dc_ac_ratio", 0))),
        ("Racking", site_summary.get("racking", "")),
        ("Tilt / Rotation Limit", f"{site_summary.get('tilt', 0)}\u00b0"),
        ("Azimuth", f"{site_summary.get('azimuth', 0)}\u00b0"),
        ("GCR", _format_table_value("gcr", site_summary.get("gcr", 0))),
        ("Module", f"{site_summary.get('module_model', '')} ({'Bifacial' if site_summary.get('bifacial') else 'Monofacial'})"),
        ("Inverter", site_summary.get("inverter_model", "")),
        ("Resource Data Source", _format_data_source(site_summary)),
    ]

    results_rows = [
        ("Annual Energy (MWh)", _format_table_value("annual_energy_mwh", site_summary.get("annual_energy_mwh", 0))),
        ("Net Capacity Factor (%)", _format_table_value("capacity_factor_pct", site_summary.get("capacity_factor_pct", 0))),
        ("Specific Yield (kWh/kWp)", _format_table_value("specific_yield_kwh_kwp", site_summary.get("specific_yield_kwh_kwp", 0))),
        ("Performance Ratio (%)", _format_table_value("performance_ratio_pct", site_summary.get("performance_ratio_pct", 0))),
        ("Annual GHI (kWh/m\u00b2)", _format_table_value("annual_ghi_kwh_m2", site_summary.get("annual_ghi_kwh_m2", 0))),
        ("Weather Year", site_summary.get("weather_year", "")),
    ]

    usable_width = PAGE_WIDTH - 2 * MARGIN
    col_widths = [usable_width * 0.45, usable_width * 0.55]

    table_style = TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), FONT_HEADER),
        ("FONTSIZE", (0, 0), (-1, -1), SIZE_TABLE),
        ("FONTNAME", (0, 1), (-1, -1), FONT_BODY),
        ("BACKGROUND", (0, 0), (-1, 0), LIGHT_GREY),
        ("GRID", (0, 0), (-1, -1), 0.5, MED_GREY),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ])

    # Add alternating row shading
    for i in range(2, len(config_rows) + 1, 2):
        table_style.add("BACKGROUND", (0, i), (-1, i), colors.Color(0.97, 0.97, 0.97))

    config_table_data = [["System Configuration", ""]] + list(config_rows)
    config_table = Table(config_table_data, colWidths=col_widths)
    config_table.setStyle(table_style)

    results_style = TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), FONT_HEADER),
        ("FONTSIZE", (0, 0), (-1, -1), SIZE_TABLE),
        ("FONTNAME", (0, 1), (-1, -1), FONT_BODY),
        ("BACKGROUND", (0, 0), (-1, 0), LIGHT_GREY),
        ("GRID", (0, 0), (-1, -1), 0.5, MED_GREY),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ])

    for i in range(2, len(results_rows) + 1, 2):
        results_style.add("BACKGROUND", (0, i), (-1, i), colors.Color(0.97, 0.97, 0.97))

    results_table_data = [["Production Results", ""]] + list(results_rows)
    results_table = Table(results_table_data, colWidths=col_widths)
    results_table.setStyle(results_style)

    return [config_table, Spacer(1, 16), results_table]


def _add_footer(canvas: object, doc: object, site_name: str, total_pages: int = 3) -> None:
    """Draw page footer with site name and page number.

    Args:
        canvas: reportlab canvas object.
        doc: reportlab document object.
        site_name: Site name for footer text.
        total_pages: Total number of pages in the document.
    """
    canvas.saveState()
    canvas.setFont(FONT_BODY, SIZE_FOOTER)
    footer_text = f"{site_name} | Page {doc.page} of {total_pages}"
    canvas.drawRightString(PAGE_WIDTH - MARGIN, 0.5 * inch, footer_text)
    canvas.restoreState()


def build_pdf(
    output_path: Path,
    site_summary: dict,
    narrative: dict,
    monthly_chart_path: Path,
    waterfall_chart_path: Path,
    bill_summary: Optional[dict] = None,
    bill_chart_path: Optional[Path] = None,
    load_solar_chart_path: Optional[Path] = None,
    bess_dispatch: Optional[dict] = None,
    bess_heatmap_path: Optional[Path] = None,
    bess_dispatch_chart_path: Optional[Path] = None,
) -> Optional[Path]:
    """Build a solar production analysis PDF report (3-5 pages).

    Page 1: Cover with title, subtitle, date, and site summary tables.
    Page 2: Narrative sections (overview, resource, production) and monthly chart.
    Page 3: Loss analysis narrative and waterfall chart.
    Page 4 (optional): Bill savings analysis with comparison chart.
    Page 5 (optional): Battery storage analysis with heatmap and dispatch profile.

    Args:
        output_path: File path for the output PDF.
        site_summary: Output of extract_site_summary() with site metadata.
        narrative: Output of generate_narrative() with 4 paragraph strings.
        monthly_chart_path: Path to monthly production chart PNG.
        waterfall_chart_path: Path to loss waterfall chart PNG.
        bill_summary: Optional dict from extract_bill_summary() for page 4.
        bill_chart_path: Optional path to monthly bill comparison chart PNG.
        load_solar_chart_path: Optional path to load vs solar chart PNG.
        bess_dispatch: Optional dict from summary["bess_dispatch"] for page 5.
        bess_heatmap_path: Optional path to dispatch heatmap chart PNG.
        bess_dispatch_chart_path: Optional path to dispatch profile chart PNG.

    Returns:
        The output_path on success, or None on failure.
    """
    try:
        # Validate chart files exist
        if not monthly_chart_path.exists():
            logger.error(f"Monthly chart not found: {monthly_chart_path}")
            return None
        if not waterfall_chart_path.exists():
            logger.error(f"Waterfall chart not found: {waterfall_chart_path}")
            return None

        has_bill_page = (
            bill_summary is not None
            and bill_chart_path is not None
            and bill_chart_path.exists()
        )
        has_bess_page = (
            bess_dispatch is not None
            and bess_heatmap_path is not None
            and bess_heatmap_path.exists()
        )
        total_pages = 3 + int(has_bill_page) + int(has_bess_page)

        output_path.parent.mkdir(parents=True, exist_ok=True)

        site_name = site_summary.get("site_name", "Unknown Site")
        customer = site_summary.get("customer", "")

        styles = _build_styles()

        # Build flowable story
        story: list = []

        # === PAGE 1: Cover / Site Summary ===
        story.append(Spacer(1, 0.5 * inch))
        story.append(Paragraph("Solar Production Analysis Report", styles["title"]))
        story.append(Paragraph(f"{site_name} — {customer}", styles["subtitle"]))
        story.append(Paragraph(f"Generated: {date.today().strftime('%B %d, %Y')}", styles["date_line"]))
        story.append(Spacer(1, 0.3 * inch))
        story.extend(_build_site_summary_tables(site_summary, styles))

        # Page break
        from reportlab.platypus import PageBreak
        story.append(PageBreak())

        # === PAGE 2: Narrative + Monthly Chart ===
        story.append(Paragraph("Site Overview", styles["section_header"]))
        story.append(Paragraph(narrative.get("site_overview", ""), styles["body"]))

        story.append(Paragraph("Solar Resource", styles["section_header"]))
        story.append(Paragraph(narrative.get("solar_resource", ""), styles["body"]))

        story.append(Paragraph("Production Summary", styles["section_header"]))
        story.append(Paragraph(narrative.get("production_summary", ""), styles["body"]))

        story.append(Spacer(1, 0.2 * inch))

        # Embed monthly chart — scale to fit within margins
        chart_width = PAGE_WIDTH - 2 * MARGIN
        chart_height = chart_width * 0.5  # maintain 2:1 aspect ratio
        story.append(Image(str(monthly_chart_path), width=chart_width, height=chart_height))

        story.append(PageBreak())

        # === PAGE 3: Loss Analysis ===
        story.append(Paragraph("Loss Analysis", styles["section_header"]))
        # Split loss_summary on newlines into separate paragraphs
        loss_text = narrative.get("loss_summary", "")
        for paragraph_text in loss_text.split("\n"):
            if paragraph_text.strip():
                story.append(Paragraph(paragraph_text.strip(), styles["body"]))

        story.append(Spacer(1, 0.2 * inch))

        # Embed waterfall chart
        story.append(Image(str(waterfall_chart_path), width=chart_width, height=chart_height))

        # === PAGE 4: Bill Savings Analysis (optional) ===
        if has_bill_page:
            story.append(PageBreak())
            story.append(Paragraph("Bill Savings Analysis", styles["section_header"]))
            story.append(Spacer(1, 0.1 * inch))

            # Summary table
            usable_width = PAGE_WIDTH - 2 * MARGIN
            col_widths = [usable_width * 0.55, usable_width * 0.45]

            bill_rows = [
                ["Bill Savings Summary", ""],
                [
                    "Annual Bill Without Solar",
                    f"${bill_summary['annual_bill_without_solar']:,.2f}",
                ],
                [
                    "Annual Bill With Solar",
                    f"${bill_summary['annual_bill_with_solar']:,.2f}",
                ],
                [
                    "Annual Savings",
                    f"${bill_summary['annual_savings']:,.2f}",
                ],
                [
                    "Savings Percentage",
                    f"{bill_summary['savings_percent']:.1f}%",
                ],
                [
                    "Avoided Cost per kWh",
                    f"${bill_summary['avoided_cost_per_kwh']:.4f}",
                ],
            ]

            bill_table = Table(bill_rows, colWidths=col_widths)
            bill_table.setStyle(TableStyle([
                ("FONTNAME", (0, 0), (-1, 0), FONT_HEADER),
                ("FONTSIZE", (0, 0), (-1, -1), SIZE_TABLE),
                ("FONTNAME", (0, 1), (-1, -1), FONT_BODY),
                ("BACKGROUND", (0, 0), (-1, 0), LIGHT_GREY),
                ("GRID", (0, 0), (-1, -1), 0.5, MED_GREY),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]))
            story.append(bill_table)
            story.append(Spacer(1, 0.2 * inch))

            # Monthly bill comparison chart
            story.append(Image(
                str(bill_chart_path), width=chart_width, height=chart_height
            ))

            # Load vs solar chart (if available)
            if (
                load_solar_chart_path is not None
                and load_solar_chart_path.exists()
            ):
                story.append(Spacer(1, 0.2 * inch))
                story.append(Image(
                    str(load_solar_chart_path),
                    width=chart_width,
                    height=chart_height,
                ))

        # === PAGE 5: Battery Storage Analysis (optional) ===
        if has_bess_page:
            story.append(PageBreak())
            story.append(Paragraph("Battery Storage Analysis", styles["section_header"]))
            story.append(Spacer(1, 0.1 * inch))

            # Summary table
            usable_width = PAGE_WIDTH - 2 * MARGIN
            bess_col_widths = [usable_width * 0.55, usable_width * 0.45]

            bd = bess_dispatch
            bess_rows = [
                ["Battery Storage Summary", ""],
                [
                    "Battery Size",
                    f"{bd['bess_power_mw']} MW / {bd['bess_duration_hr']} hr "
                    f"({bd['bess_capacity_kwh']:,.0f} kWh)",
                ],
                ["Strategy", str(bd["bess_strategy"])],
                [
                    "Solar-Only Annual Bill",
                    f"${bd['solar_only_annual_bill']:,.0f}",
                ],
                [
                    "Solar+BESS Annual Bill",
                    f"${bd['solar_plus_bess_annual_bill']:,.0f}",
                ],
                [
                    "BESS Incremental Savings",
                    f"${bd['bess_incremental_savings']:,.0f}",
                ],
                [
                    "Demand Savings",
                    f"${bd['bess_demand_savings']:,.0f}",
                ],
                [
                    "Energy Savings",
                    f"${bd['bess_energy_savings']:,.0f}",
                ],
                [
                    "Annual Cycles",
                    f"{bd['annual_cycles']:.1f}",
                ],
                [
                    "Capacity Utilization",
                    f"{bd['capacity_utilization_pct']:.1f}%",
                ],
                [
                    "Est. Annual Degradation",
                    f"{bd['estimated_annual_degradation_pct']:.2f}%",
                ],
                [
                    "Curtailed Energy",
                    f"{bd['total_curtailed_kwh']:,.0f} kWh",
                ],
            ]

            bess_style = TableStyle([
                ("FONTNAME", (0, 0), (-1, 0), FONT_HEADER),
                ("FONTSIZE", (0, 0), (-1, -1), SIZE_TABLE),
                ("FONTNAME", (0, 1), (-1, -1), FONT_BODY),
                ("BACKGROUND", (0, 0), (-1, 0), LIGHT_GREY),
                ("GRID", (0, 0), (-1, -1), 0.5, MED_GREY),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ])
            for i in range(2, len(bess_rows), 2):
                bess_style.add(
                    "BACKGROUND", (0, i), (-1, i),
                    colors.Color(0.97, 0.97, 0.97),
                )

            bess_table = Table(bess_rows, colWidths=bess_col_widths)
            bess_table.setStyle(bess_style)

            story.append(bess_table)
            story.append(Spacer(1, 0.1 * inch))

            # Heatmap chart
            chart_width = PAGE_WIDTH - 2 * MARGIN
            bess_chart_height = chart_width * 0.35
            story.append(Image(
                str(bess_heatmap_path),
                width=chart_width,
                height=bess_chart_height,
            ))

            # Dispatch profile chart (optional, fits on same page)
            if (
                bess_dispatch_chart_path is not None
                and bess_dispatch_chart_path.exists()
            ):
                story.append(Spacer(1, 0.1 * inch))
                story.append(Image(
                    str(bess_dispatch_chart_path),
                    width=chart_width,
                    height=bess_chart_height,
                ))

        # Build PDF with footer callback
        doc = SimpleDocTemplate(
            str(output_path),
            pagesize=letter,
            leftMargin=MARGIN,
            rightMargin=MARGIN,
            topMargin=MARGIN,
            bottomMargin=MARGIN,
        )

        def on_page(canvas_obj: object, doc_obj: object) -> None:
            _add_footer(canvas_obj, doc_obj, site_name, total_pages=total_pages)

        doc.build(story, onFirstPage=on_page, onLaterPages=on_page)

        file_size_kb = output_path.stat().st_size / 1024
        logger.info(f"PDF report built: {output_path} ({file_size_kb:.1f} KB)")
        return output_path

    except Exception as exc:
        logger.error(f"Failed to build PDF report: {exc}")
        return None
