"""Buildability assessment visualizations.

Generates customer-facing maps of land cover classification,
buildability assessment, and terrain slope analysis as PNG files.
"""

import matplotlib
matplotlib.use("Agg")

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pyproj
from matplotlib.colors import ListedColormap, BoundaryNorm
from matplotlib.patches import Patch
from shapely.geometry import Polygon

from src.buildability.exclusion_engine import (
    BUILDABLE,
    HARD_EXCLUSIONS,
    SOFT_EXCLUSIONS,
)
from src.buildability.nlcd_client import NLCD_CLASSES
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Standard NLCD color scheme (RGB 0-255)
_NLCD_COLORS: dict[int, tuple[int, int, int]] = {
    11: (70, 107, 159),
    12: (209, 222, 248),
    21: (222, 197, 165),
    22: (217, 146, 130),
    23: (237, 0, 0),
    24: (171, 0, 0),
    31: (179, 172, 159),
    41: (104, 171, 95),
    42: (28, 99, 48),
    43: (189, 204, 145),
    52: (204, 186, 136),
    71: (227, 227, 194),
    81: (219, 217, 61),
    82: (171, 113, 40),
    90: (186, 216, 234),
    95: (112, 163, 186),
}

# Buildability category colors (hex)
_BUILDABLE_COLOR = "#2ecc71"
_SOFT_EXCLUSION_COLOR = "#f39c12"
_HARD_EXCLUSION_COLOR = "#e74c3c"
_UNCLASSIFIED_COLOR = "#95a5a6"

# Category lookup for recoloring
_CODE_TO_CATEGORY: dict[int, str] = {}
for _code in BUILDABLE:
    _CODE_TO_CATEGORY[_code] = "buildable"
for _code in SOFT_EXCLUSIONS:
    _CODE_TO_CATEGORY[_code] = "soft_exclusion"
for _code in HARD_EXCLUSIONS:
    _CODE_TO_CATEGORY[_code] = "hard_exclusion"

# CRS objects for polygon reprojection (PROJ strings)
_WGS84 = pyproj.CRS("+proj=longlat +datum=WGS84 +no_defs")
_WEB_MERCATOR = pyproj.CRS(
    "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 "
    "+x_0=0 +y_0=0 +k=1 +units=m +no_defs"
)
_TO_3857 = pyproj.Transformer.from_crs(_WGS84, _WEB_MERCATOR, always_xy=True)

_FIG_SIZE = (10, 8)
_FIG_DPI = 150


def generate_land_cover_map(
    nlcd_array: np.ndarray,
    metadata: dict,
    polygon: Polygon,
    output_path: Path,
) -> Path:
    """Generate a land cover classification map with NLCD standard colors.

    Args:
        nlcd_array: 2D uint8 array of NLCD class codes.
        metadata: NLCD raster metadata with 'transform' key.
        polygon: Analysis polygon in EPSG:4326.
        output_path: Path to save the PNG.

    Returns:
        The output path.
    """
    transform = metadata["transform"]
    present_codes = sorted(set(int(v) for v in np.unique(nlcd_array) if int(v) in NLCD_CLASSES))

    # Build colormap: map each present code to its NLCD color
    all_codes = sorted(_NLCD_COLORS.keys())
    colors_normed = [
        tuple(c / 255.0 for c in _NLCD_COLORS[code]) for code in all_codes
    ]
    cmap = ListedColormap(colors_normed)
    # Boundaries centered around each code value
    boundaries = [all_codes[0] - 0.5] + [
        (all_codes[i] + all_codes[i + 1]) / 2 for i in range(len(all_codes) - 1)
    ] + [all_codes[-1] + 0.5]
    norm = BoundaryNorm(boundaries, cmap.N)

    # Compute raster extent in EPSG:3857
    extent = _get_raster_extent(transform, nlcd_array.shape)

    fig, ax = plt.subplots(1, 1, figsize=_FIG_SIZE)
    ax.imshow(nlcd_array, cmap=cmap, norm=norm, extent=extent, interpolation="nearest")

    # Overlay polygon (reprojected to 3857)
    _overlay_polygon_3857(ax, polygon)

    # Legend for present classes only
    legend_patches = [
        Patch(
            facecolor=tuple(c / 255.0 for c in _NLCD_COLORS[code]),
            edgecolor="black",
            linewidth=0.5,
            label=f"{code} - {NLCD_CLASSES[code]}",
        )
        for code in present_codes
    ]
    ax.legend(
        handles=legend_patches,
        loc="upper left",
        fontsize=7,
        framealpha=0.9,
        borderpad=0.5,
    )

    ax.set_xticks([])
    ax.set_yticks([])
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=_FIG_DPI, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Saved land cover map: {output_path}")
    return output_path


def generate_buildability_map(
    nlcd_array: np.ndarray,
    metadata: dict,
    polygon: Polygon,
    classification_result: dict,
    output_path: Path,
) -> Path:
    """Generate a buildability assessment map with three-category coloring.

    Args:
        nlcd_array: 2D uint8 array of NLCD class codes.
        metadata: NLCD raster metadata with 'transform' key.
        polygon: Analysis polygon in EPSG:4326.
        classification_result: Output from classify_land_cover().
        output_path: Path to save the PNG.

    Returns:
        The output path.
    """
    transform = metadata["transform"]

    # Reclassify to 0=unclassified, 1=buildable, 2=soft, 3=hard
    reclass = np.zeros_like(nlcd_array, dtype=np.uint8)
    for code, category in _CODE_TO_CATEGORY.items():
        if category == "buildable":
            reclass[nlcd_array == code] = 1
        elif category == "soft_exclusion":
            reclass[nlcd_array == code] = 2
        elif category == "hard_exclusion":
            reclass[nlcd_array == code] = 3

    colors = [_UNCLASSIFIED_COLOR, _BUILDABLE_COLOR, _SOFT_EXCLUSION_COLOR, _HARD_EXCLUSION_COLOR]
    cmap = ListedColormap(colors)
    norm = BoundaryNorm([-0.5, 0.5, 1.5, 2.5, 3.5], cmap.N)

    extent = _get_raster_extent(transform, nlcd_array.shape)

    fig, ax = plt.subplots(1, 1, figsize=_FIG_SIZE)
    ax.imshow(reclass, cmap=cmap, norm=norm, extent=extent, interpolation="nearest")

    _overlay_polygon_3857(ax, polygon)

    # Legend with acre totals
    legend_patches = [
        Patch(facecolor=_BUILDABLE_COLOR, edgecolor="black", linewidth=0.5,
              label=f"Buildable ({classification_result['buildable_acres']:.0f} ac)"),
        Patch(facecolor=_SOFT_EXCLUSION_COLOR, edgecolor="black", linewidth=0.5,
              label=f"Soft Exclusion ({classification_result['soft_exclusion_acres']:.0f} ac)"),
        Patch(facecolor=_HARD_EXCLUSION_COLOR, edgecolor="black", linewidth=0.5,
              label=f"Hard Exclusion ({classification_result['hard_exclusion_acres']:.0f} ac)"),
    ]
    if classification_result["unclassified_pixels"] > 0:
        legend_patches.append(
            Patch(facecolor=_UNCLASSIFIED_COLOR, edgecolor="black", linewidth=0.5,
                  label=f"Unclassified ({classification_result['unclassified_acres']:.0f} ac)")
        )
    ax.legend(
        handles=legend_patches,
        loc="upper left",
        fontsize=9,
        framealpha=0.9,
        borderpad=0.5,
    )

    ax.set_xticks([])
    ax.set_yticks([])
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=_FIG_DPI, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Saved buildability map: {output_path}")
    return output_path


def generate_slope_map(
    slope_array: np.ndarray,
    metadata: dict,
    polygon: Polygon,
    output_path: Path,
    tracker_limit: float = 10.0,
) -> Path:
    """Generate a terrain slope analysis map.

    Args:
        slope_array: 2D float array of slope values in degrees.
        metadata: DEM raster metadata with 'transform' key.
        polygon: Analysis polygon in EPSG:4326.
        output_path: Path to save the PNG.
        tracker_limit: Slope limit for tracker suitability annotation.

    Returns:
        The output path.
    """
    transform = metadata["transform"]
    extent = _get_raster_extent(transform, slope_array.shape)

    fig, ax = plt.subplots(1, 1, figsize=_FIG_SIZE)
    im = ax.imshow(
        slope_array,
        cmap="RdYlGn_r",
        vmin=0,
        vmax=30,
        extent=extent,
        interpolation="nearest",
    )

    # Overlay polygon (4326 — same CRS as DEM)
    _overlay_polygon_4326(ax, polygon)

    # Colorbar
    cbar = fig.colorbar(im, ax=ax, shrink=0.8, pad=0.02)
    cbar.set_label("Slope (degrees)", fontsize=10)

    # Mark tracker limit on colorbar
    cbar.ax.axhline(y=tracker_limit, color="black", linewidth=1.5, linestyle="--")
    cbar.ax.annotate(
        f"Tracker limit ({tracker_limit:.0f}\u00b0)",
        xy=(1.0, tracker_limit),
        xycoords=("axes fraction", "data"),
        xytext=(35, 0),
        textcoords="offset points",
        fontsize=8,
        va="center",
        arrowprops=dict(arrowstyle="-", color="black", lw=0.8),
    )

    ax.set_xticks([])
    ax.set_yticks([])
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=_FIG_DPI, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Saved slope map: {output_path}")
    return output_path


def generate_all_visualizations(
    nlcd_array: np.ndarray,
    nlcd_metadata: dict,
    slope_array: np.ndarray,
    dem_metadata: dict,
    polygon: Polygon,
    classification_result: dict,
    output_dir: Path,
    tracker_limit: float = 10.0,
) -> dict[str, Path]:
    """Generate all three buildability visualizations.

    Args:
        nlcd_array: 2D uint8 array of NLCD class codes.
        nlcd_metadata: NLCD raster metadata.
        slope_array: 2D float array of slope values in degrees.
        dem_metadata: DEM raster metadata.
        polygon: Analysis polygon in EPSG:4326.
        classification_result: Output from classify_land_cover().
        output_dir: Directory to save the PNG files.
        tracker_limit: Slope limit for tracker suitability.

    Returns:
        Dict mapping figure name to its file path.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "land_cover": generate_land_cover_map(
            nlcd_array, nlcd_metadata, polygon,
            output_dir / "land_cover_map.png",
        ),
        "buildability": generate_buildability_map(
            nlcd_array, nlcd_metadata, polygon, classification_result,
            output_dir / "buildability_map.png",
        ),
        "slope": generate_slope_map(
            slope_array, dem_metadata, polygon,
            output_dir / "slope_map.png",
            tracker_limit=tracker_limit,
        ),
    }

    logger.info(f"Generated {len(paths)} visualizations in {output_dir}")
    return paths


def _get_raster_extent(
    transform: object, shape: tuple[int, int]
) -> tuple[float, float, float, float]:
    """Compute matplotlib imshow extent from rasterio transform.

    Args:
        transform: Rasterio Affine transform.
        shape: (rows, cols) of the raster array.

    Returns:
        (left, right, bottom, top) for matplotlib extent.
    """
    rows, cols = shape
    left = transform.c
    right = transform.c + cols * transform.a
    top = transform.f
    bottom = transform.f + rows * transform.e
    return (left, right, bottom, top)


def _overlay_polygon_3857(ax: plt.Axes, polygon: Polygon) -> None:
    """Overlay polygon boundary on an EPSG:3857 plot.

    Reprojects polygon from EPSG:4326 to EPSG:3857 before plotting.

    Args:
        ax: Matplotlib axes.
        polygon: Polygon in EPSG:4326.
    """
    xs, ys = polygon.exterior.xy
    xs_3857, ys_3857 = _TO_3857.transform(list(xs), list(ys))
    ax.plot(xs_3857, ys_3857, color="black", linewidth=1.5, linestyle="--")


def _overlay_polygon_4326(ax: plt.Axes, polygon: Polygon) -> None:
    """Overlay polygon boundary on an EPSG:4326 plot.

    No reprojection needed — polygon is already in 4326.

    Args:
        ax: Matplotlib axes.
        polygon: Polygon in EPSG:4326.
    """
    xs, ys = polygon.exterior.xy
    ax.plot(xs, ys, color="black", linewidth=1.5, linestyle="--")
