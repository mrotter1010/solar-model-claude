"""Buildability assessment PDF report section and standalone report generator.

Produces reportlab flowables for the buildable land assessment section,
and can generate a complete standalone PDF matching the existing report branding.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from src.buildability.models import BuildabilityResult
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Page geometry (matches src/reporting/pdf_builder.py)
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
ALT_ROW = colors.Color(0.97, 0.97, 0.97)

# Buildability category colors for table accents
_CAT_COLORS = {
    "buildable": colors.Color(0.18, 0.80, 0.44),       # #2ecc71
    "soft_exclusion": colors.Color(0.95, 0.61, 0.07),   # #f39c12
    "hard_exclusion": colors.Color(0.91, 0.30, 0.24),   # #e74c3c
}


def _build_styles() -> dict[str, ParagraphStyle]:
    """Create paragraph styles matching the existing report branding.

    Returns:
        Dict mapping style name to ParagraphStyle.
    """
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "BuildTitle",
            parent=base["Normal"],
            fontName=FONT_TITLE,
            fontSize=SIZE_TITLE,
            alignment=1,
            spaceAfter=6,
        ),
        "subtitle": ParagraphStyle(
            "BuildSubtitle",
            parent=base["Normal"],
            fontName=FONT_HEADER,
            fontSize=SIZE_SUBTITLE,
            alignment=1,
            spaceAfter=4,
        ),
        "date_line": ParagraphStyle(
            "BuildDateLine",
            parent=base["Normal"],
            fontName=FONT_BODY,
            fontSize=SIZE_BODY,
            alignment=1,
            spaceAfter=24,
        ),
        "section_header": ParagraphStyle(
            "BuildSectionHeader",
            parent=base["Normal"],
            fontName=FONT_HEADER,
            fontSize=SIZE_SUBTITLE,
            spaceBefore=12,
            spaceAfter=6,
        ),
        "body": ParagraphStyle(
            "BuildBodyText",
            parent=base["Normal"],
            fontName=FONT_BODY,
            fontSize=SIZE_BODY,
            leading=14,
            spaceAfter=8,
        ),
        "methodology": ParagraphStyle(
            "BuildMethodology",
            parent=base["Normal"],
            fontName=FONT_BODY,
            fontSize=8,
            leading=10,
            textColor=colors.Color(0.4, 0.4, 0.4),
            spaceBefore=12,
            spaceAfter=4,
        ),
    }


def _make_table_style(num_data_rows: int) -> TableStyle:
    """Build a standard table style with header shading and alternating rows.

    Args:
        num_data_rows: Number of data rows (excluding header).

    Returns:
        Configured TableStyle.
    """
    style = TableStyle([
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
    for i in range(2, num_data_rows + 1, 2):
        style.add("BACKGROUND", (0, i), (-1, i), ALT_ROW)
    return style


def _build_summary_table(result: BuildabilityResult) -> Table:
    """Build the site / buildability summary table.

    Args:
        result: BuildabilityResult with location and area data.

    Returns:
        reportlab Table flowable.
    """
    usable_width = PAGE_WIDTH - 2 * MARGIN
    col_widths = [usable_width * 0.45, usable_width * 0.55]

    rows = [
        ["Site Summary", ""],
        ["Location", f"{result.latitude:.4f}\u00b0N / {abs(result.longitude):.4f}\u00b0W"],
        ["Boundary", f"{result.radius_km:.1f} km radius" if result.radius_km else "KMZ polygon"],
        ["Total Area", f"{result.total_area_acres:,.0f} acres"],
        ["Buildable", f"{result.buildable_acres:,.0f} acres ({result.buildable_pct:.1f}%)"],
        ["Soft Exclusion", f"{result.soft_exclusion_acres:,.0f} acres ({result.soft_exclusion_pct:.1f}%)"],
        ["Hard Exclusion", f"{result.hard_exclusion_acres:,.0f} acres ({result.hard_exclusion_pct:.1f}%)"],
    ]

    table = Table(rows, colWidths=col_widths)
    style = _make_table_style(len(rows) - 1)
    # Color-accent the category rows
    style.add("TEXTCOLOR", (0, 4), (0, 4), _CAT_COLORS["buildable"])
    style.add("TEXTCOLOR", (0, 5), (0, 5), _CAT_COLORS["soft_exclusion"])
    style.add("TEXTCOLOR", (0, 6), (0, 6), _CAT_COLORS["hard_exclusion"])
    table.setStyle(style)
    return table


def _build_class_breakdown_table(result: BuildabilityResult) -> Table:
    """Build the NLCD land cover class breakdown table.

    Args:
        result: BuildabilityResult with class_breakdown list.

    Returns:
        reportlab Table flowable.
    """
    usable_width = PAGE_WIDTH - 2 * MARGIN
    col_widths = [
        usable_width * 0.10,   # Code
        usable_width * 0.40,   # Class Name
        usable_width * 0.20,   # Category
        usable_width * 0.15,   # Acres
        usable_width * 0.15,   # %
    ]

    header = ["Code", "Land Cover Class", "Category", "Acres", "%"]
    sorted_classes = sorted(result.class_breakdown, key=lambda x: x["acres"], reverse=True)

    data_rows = []
    for cls in sorted_classes:
        category_display = cls["category"].replace("_", " ").title()
        data_rows.append([
            str(cls["code"]),
            cls["name"],
            category_display,
            f"{cls['acres']:,.0f}",
            f"{cls['percent']:.1f}",
        ])

    table = Table([header] + data_rows, colWidths=col_widths)
    style = _make_table_style(len(data_rows))
    style.add("ALIGN", (0, 0), (0, -1), "CENTER")
    style.add("ALIGN", (3, 0), (4, -1), "RIGHT")
    table.setStyle(style)
    return table


def _build_slope_table(result: BuildabilityResult) -> Table:
    """Build the slope analysis summary table.

    Args:
        result: BuildabilityResult with slope_stats dict.

    Returns:
        reportlab Table flowable.
    """
    stats = result.slope_stats
    usable_width = PAGE_WIDTH - 2 * MARGIN
    col_widths = [usable_width * 0.45, usable_width * 0.55]

    rows = [
        ["Slope Analysis", ""],
        ["Mean Slope", f"{stats['mean_degrees']:.1f}\u00b0"],
        ["Min / Max", f"{stats['min_degrees']:.1f}\u00b0 / {stats['max_degrees']:.1f}\u00b0"],
        ["Median", f"{stats['median_degrees']:.1f}\u00b0"],
        [
            "Tracker Suitable (< 10\u00b0)",
            f"{stats['pct_below_tracker_limit']:.1f}%",
        ],
        [
            "Fixed-Tilt Suitable (< 20\u00b0)",
            f"{stats['pct_below_fixed_tilt_limit']:.1f}%",
        ],
    ]

    # Add distribution buckets
    for bucket in stats.get("distribution", []):
        rows.append([bucket["range"], f"{bucket['percent']:.1f}%"])

    table = Table(rows, colWidths=col_widths)
    table.setStyle(_make_table_style(len(rows) - 1))
    return table


def generate_buildability_report_section(
    result: BuildabilityResult,
) -> list:
    """Generate reportlab flowables for the buildability assessment section.

    Produces a self-contained section that can be appended to an existing
    report's story list or used standalone.

    Args:
        result: BuildabilityResult from the buildability analysis pipeline.

    Returns:
        List of reportlab flowable objects.
    """
    styles = _build_styles()
    flowables: list = []

    # Section header
    flowables.append(Paragraph("Buildable Land Assessment", styles["section_header"]))
    flowables.append(Spacer(1, 0.1 * inch))

    # Summary table
    flowables.append(_build_summary_table(result))
    flowables.append(Spacer(1, 0.2 * inch))

    # Land cover breakdown table
    flowables.append(
        Paragraph("Land Cover Classification", styles["section_header"])
    )
    flowables.append(_build_class_breakdown_table(result))
    flowables.append(Spacer(1, 0.2 * inch))

    # Slope summary table
    flowables.append(_build_slope_table(result))
    flowables.append(Spacer(1, 0.2 * inch))

    # Embedded images
    chart_width = PAGE_WIDTH - 2 * MARGIN
    chart_height = chart_width * 0.8  # maps are roughly square

    figure_paths = result.figure_paths
    for key, label in [
        ("land_cover", "Land Cover Classification"),
        ("buildability", "Buildability Assessment"),
        ("slope", "Terrain Slope Analysis"),
    ]:
        path_str = figure_paths.get(key)
        if path_str and Path(path_str).exists():
            flowables.append(PageBreak())
            flowables.append(Paragraph(label, styles["section_header"]))
            flowables.append(Spacer(1, 0.1 * inch))
            flowables.append(
                Image(path_str, width=chart_width, height=chart_height)
            )

    # Methodology note
    flowables.append(Spacer(1, 0.2 * inch))
    flowables.append(
        Paragraph(
            "Land cover: USGS NLCD 2021, 30m. "
            "Elevation: USGS 3DEP, ~10m. "
            "Analysis by Vantyra Analytics.",
            styles["methodology"],
        )
    )

    return flowables


def _add_footer(
    canvas: object, doc: object, title: str, total_pages: int,
) -> None:
    """Draw page footer with title and page number.

    Args:
        canvas: reportlab canvas object.
        doc: reportlab document object.
        title: Text for footer left side.
        total_pages: Total page count for display.
    """
    canvas.saveState()
    canvas.setFont(FONT_BODY, SIZE_FOOTER)
    footer_text = f"{title} | Page {doc.page} of {total_pages}"
    canvas.drawRightString(PAGE_WIDTH - MARGIN, 0.5 * inch, footer_text)
    canvas.restoreState()


def generate_standalone_buildability_report(
    result: BuildabilityResult,
    output_path: Path,
) -> Path:
    """Generate a standalone buildability assessment PDF report.

    Creates a complete PDF with cover page header/branding, buildability
    section content, and page numbers matching the existing report style.

    Args:
        result: BuildabilityResult from the buildability analysis pipeline.
        output_path: File path for the output PDF.

    Returns:
        The output_path on success.

    Raises:
        RuntimeError: If PDF generation fails.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    styles = _build_styles()
    story: list = []

    # Cover / header
    story.append(Spacer(1, 0.5 * inch))
    story.append(
        Paragraph("Buildable Land Assessment Report", styles["title"])
    )

    location_str = (
        f"{result.latitude:.4f}\u00b0N / {abs(result.longitude):.4f}\u00b0W"
    )
    boundary_str = (
        f"{result.radius_km:.1f} km radius"
        if result.radius_km
        else "KMZ polygon"
    )
    story.append(
        Paragraph(f"{location_str} — {boundary_str}", styles["subtitle"])
    )
    story.append(
        Paragraph(
            f"Generated: {date.today().strftime('%B %d, %Y')}",
            styles["date_line"],
        )
    )
    story.append(Spacer(1, 0.2 * inch))

    # Buildability section content
    section = generate_buildability_report_section(result)
    story.extend(section)

    # Page count: cover + tables (1-2 pages) + 3 image pages
    num_figures = sum(
        1 for k in ("land_cover", "buildability", "slope")
        if result.figure_paths.get(k) and Path(result.figure_paths[k]).exists()
    )
    total_pages = 2 + num_figures  # cover/tables + image pages

    footer_title = f"Buildability — {result.latitude:.4f}, {result.longitude:.4f}"

    def on_page(canvas_obj: object, doc_obj: object) -> None:
        _add_footer(canvas_obj, doc_obj, footer_title, total_pages)

    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=letter,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=MARGIN,
        bottomMargin=MARGIN,
    )
    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)

    file_size_kb = output_path.stat().st_size / 1024
    logger.info(f"Buildability PDF report built: {output_path} ({file_size_kb:.1f} KB)")
    return output_path
