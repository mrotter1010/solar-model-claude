"""Buildability analysis orchestrator.

Ties together polygon parsing, NLCD land cover, DEM elevation,
exclusion classification, and slope analysis into a single pipeline.
"""

import json
import sys
from pathlib import Path

from src.buildability.dem_client import compute_slope, fetch_dem
from src.buildability.exclusion_engine import classify_land_cover, get_pixel_area_sq_m
from src.buildability.models import BuildabilityConfig, BuildabilityResult
from src.buildability.nlcd_client import fetch_nlcd
from src.buildability.polygon_parser import get_analysis_polygon
from src.buildability.slope_analyzer import analyze_slope
from src.buildability.visualizations import generate_all_visualizations
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def run_buildability_analysis(
    config: BuildabilityConfig,
    output_dir: Path | None = None,
) -> BuildabilityResult:
    """Run a full buildability analysis for a site.

    Orchestrates the entire pipeline: polygon creation, data fetching,
    land cover classification, slope analysis, and visualization.

    Args:
        config: BuildabilityConfig with location and analysis parameters.
        output_dir: Directory for output files (JSON, PNGs). If None,
            visualizations are skipped.

    Returns:
        BuildabilityResult with classification and slope results.
    """
    logger.info(
        f"Starting buildability analysis at "
        f"({config.latitude}, {config.longitude})"
    )

    # 1. Get analysis polygon
    polygon = get_analysis_polygon(config)
    logger.info(f"Analysis polygon: {polygon.geom_type}, area={polygon.area:.6f} sq deg")

    # 2. Fetch NLCD land cover
    nlcd_array, nlcd_meta = fetch_nlcd(polygon)

    # 3. Fetch DEM elevation
    dem_array, dem_meta = fetch_dem(polygon)

    # 4. Compute slope
    slope_array = compute_slope(dem_array, dem_meta["transform"])

    # 5. Compute pixel area and classify land cover
    pixel_area = get_pixel_area_sq_m(nlcd_meta)
    land_cover = classify_land_cover(nlcd_array, pixel_area)

    # 6. Analyze slope
    slope_stats = analyze_slope(slope_array, config.slope_thresholds)

    # 7. Generate visualizations (if output_dir provided)
    figure_paths: dict[str, str] = {}
    if output_dir is not None:
        fig_dir = Path(output_dir) / "figures"
        paths = generate_all_visualizations(
            nlcd_array=nlcd_array,
            nlcd_metadata=nlcd_meta,
            slope_array=slope_array,
            dem_metadata=dem_meta,
            polygon=polygon,
            classification_result=land_cover,
            output_dir=fig_dir,
            tracker_limit=config.tracker_slope_limit,
        )
        figure_paths = {k: str(v) for k, v in paths.items()}

    # 8. Assemble result
    # Convert Affine transforms to lists for JSON serialization
    nlcd_meta_serializable = _make_serializable(nlcd_meta)
    dem_meta_serializable = _make_serializable(dem_meta)

    result = BuildabilityResult(
        latitude=config.latitude,
        longitude=config.longitude,
        radius_km=config.radius_km,
        kmz_file_path=config.kmz_file_path,
        polygon_wkt=polygon.wkt,
        total_area_acres=land_cover["total_area_acres"],
        buildable_acres=land_cover["buildable_acres"],
        soft_exclusion_acres=land_cover["soft_exclusion_acres"],
        hard_exclusion_acres=land_cover["hard_exclusion_acres"],
        unclassified_acres=land_cover["unclassified_acres"],
        buildable_pct=land_cover["summary_pct"]["buildable"],
        soft_exclusion_pct=land_cover["summary_pct"]["soft_exclusion"],
        hard_exclusion_pct=land_cover["summary_pct"]["hard_exclusion"],
        class_breakdown=land_cover["class_breakdown"],
        slope_stats=slope_stats,
        nlcd_metadata=nlcd_meta_serializable,
        dem_metadata=dem_meta_serializable,
        figure_paths=figure_paths,
    )

    logger.info(
        f"Buildability analysis complete: "
        f"{result.buildable_acres:.1f} buildable acres "
        f"({result.buildable_pct}%), "
        f"tracker-suitable={slope_stats['pct_below_tracker_limit']}%"
    )
    return result


def run_buildability_from_site(site_config: "SiteConfig") -> BuildabilityResult | None:
    """Run buildability analysis from a CSV pipeline SiteConfig.

    Args:
        site_config: SiteConfig from the CSV pipeline.

    Returns:
        BuildabilityResult, or None if buildable land assessment is disabled.
    """
    if not site_config.buildable_land_assessment:
        return None

    config = BuildabilityConfig(
        latitude=site_config.latitude,
        longitude=site_config.longitude,
        kmz_file_path=site_config.kmz_file_path,
        radius_km=site_config.analysis_radius_km,
    )
    return run_buildability_analysis(config)


def to_json(result: BuildabilityResult, output_path: Path) -> Path:
    """Serialize a BuildabilityResult to a JSON file.

    Args:
        result: BuildabilityResult to serialize.
        output_path: Path to write the JSON file.

    Returns:
        The output path.
    """
    data = result.model_dump()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info(f"Wrote buildability result to {output_path}")
    return output_path


def _make_serializable(metadata: dict) -> dict:
    """Convert rasterio metadata to JSON-serializable dict.

    Converts Affine transforms to lists and ensures all values
    are basic Python types.

    Args:
        metadata: Raster metadata dict.

    Returns:
        JSON-serializable copy of the metadata.
    """
    result = {}
    for key, value in metadata.items():
        if hasattr(value, "__iter__") and hasattr(value, "a"):
            # Affine transform → list of 6 coefficients
            result[key] = [value.a, value.b, value.c, value.d, value.e, value.f]
        elif isinstance(value, tuple):
            result[key] = list(value)
        else:
            result[key] = value
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run buildable land assessment for a solar site."
    )
    parser.add_argument("--lat", type=float, required=True, help="Latitude")
    parser.add_argument("--lon", type=float, required=True, help="Longitude")
    parser.add_argument("--radius", type=float, default=None, help="Analysis radius in km")
    parser.add_argument("--kmz", type=str, default=None, help="Path to KMZ/KML file")
    parser.add_argument("--output-dir", type=str, default=".", help="Output directory")
    parser.add_argument("--report", action="store_true", default=False, help="Generate standalone PDF report")

    args = parser.parse_args()

    if args.radius is not None and args.kmz is not None:
        print("Error: Cannot specify both --radius and --kmz. Provide one or neither.")
        sys.exit(1)

    config = BuildabilityConfig(
        latitude=args.lat,
        longitude=args.lon,
        radius_km=args.radius,
        kmz_file_path=args.kmz,
    )

    output_dir = Path(args.output_dir)
    result = run_buildability_analysis(config, output_dir=output_dir)

    # Save JSON
    json_path = output_dir / f"buildability_{args.lat:.4f}_{args.lon:.4f}.json"
    to_json(result, json_path)

    # Generate PDF report if requested
    if args.report:
        from src.buildability.report_section import generate_standalone_buildability_report

        pdf_path = output_dir / f"buildability_{args.lat:.4f}_{args.lon:.4f}_report.pdf"
        generate_standalone_buildability_report(result, pdf_path)
        result.report_path = str(pdf_path)

    # Attempt to save to database (non-fatal)
    try:
        from datetime import date as _date

        from src.buildability.db import save_buildability_result
        from src.database.connection import get_session

        run_name = f"buildability_{args.lat}_{args.lon}_{_date.today().strftime('%Y%m%d')}"
        with get_session() as session:
            save_buildability_result(
                result=result,
                run_name=run_name,
                source="standalone",
                session=session,
                json_output_path=str(json_path),
            )
        logger.info(f"Saved buildability run to database: {run_name}")
    except Exception as exc:
        logger.warning(f"Could not save to database (non-fatal): {exc}")

    # Print summary
    print()
    print("=" * 60)
    print("  BUILDABLE LAND ASSESSMENT RESULTS")
    print("=" * 60)
    print(f"  Location:      ({result.latitude}, {result.longitude})")
    radius_display = result.radius_km if result.radius_km else "1.0 (default)"
    print(f"  Radius:        {radius_display} km")
    print(f"  Total area:    {result.total_area_acres:.1f} acres")
    print()
    print(f"  Buildable:     {result.buildable_acres:8.1f} acres ({result.buildable_pct}%)")
    print(f"  Soft excl:     {result.soft_exclusion_acres:8.1f} acres ({result.soft_exclusion_pct}%)")
    print(f"  Hard excl:     {result.hard_exclusion_acres:8.1f} acres ({result.hard_exclusion_pct}%)")
    print()
    print("  Top land cover classes:")
    for cls in result.class_breakdown[:5]:
        print(f"    {cls['code']:3d} {cls['name']:35s} {cls['percent']:5.1f}%")
    print()
    ss = result.slope_stats
    print(f"  Slope mean:    {ss['mean_degrees']}°")
    print(f"  Tracker OK:    {ss['pct_below_tracker_limit']}% (< 10°)")
    print(f"  Fixed OK:      {ss['pct_below_fixed_tilt_limit']}% (< 20°)")
    print()
    print(f"  JSON output:   {json_path}")
    if result.figure_paths:
        print()
        print("  Figures:")
        for name, path in result.figure_paths.items():
            size = Path(path).stat().st_size
            print(f"    {name:15s} {path} ({size:,} bytes)")
    if result.report_path:
        report_size = Path(result.report_path).stat().st_size
        print(f"\n  PDF report:  {result.report_path} ({report_size:,} bytes)")
    print("=" * 60)
