"""Tests for buildability analysis module (no network access required)."""

import zipfile
from pathlib import Path

import numpy as np
import pytest
from pydantic import ValidationError

from src.buildability.exclusion_engine import (
    BUILDABLE,
    HARD_EXCLUSIONS,
    SOFT_EXCLUSIONS,
    classify_land_cover,
    get_pixel_area_sq_m,
)
from src.buildability.models import BuildabilityConfig, BuildabilityResult
from src.buildability.polygon_parser import (
    create_buffer,
    get_analysis_polygon,
    get_analysis_polygon_from_site,
    parse_kmz,
)
from src.buildability.slope_analyzer import analyze_slope
from src.config.schema import SiteConfig


# ============================================================
# Fixtures
# ============================================================

# Base dict for creating SiteConfig objects with all required fields
_SITE_BASE = {
    "run_name": "Test1",
    "site_name": "Test Site",
    "customer": "TestCo",
    "latitude": 33.45,
    "longitude": -112.07,
    "dc_size_mw": 10.0,
    "ac_installed_mw": 8.0,
    "ac_poi_mw": 8.0,
    "racking": "tracker",
    "tilt": 60.0,
    "azimuth": 180.0,
    "module_orientation": "portrait",
    "number_of_modules": 2,
    "ground_clearance_height_m": 1.5,
    "panel_model": "Test Panel",
    "bifacial": True,
    "inverter_model": "Test Inverter",
    "gcr": 0.35,
    "shading_percent": 1.0,
    "dc_wiring_loss_percent": 1.5,
    "ac_wiring_loss_percent": 1.5,
    "transformer_losses_percent": 0.0,
    "degradation_percent": 0.3,
    "availability_percent": 98.0,
    "module_mismatch_percent": 1.5,
    "lid_percent": 1.0,
}


def _make_kml(coords: list[tuple[float, float]]) -> str:
    """Build a minimal KML string with a single Polygon.

    Args:
        coords: List of (lon, lat) tuples for the polygon exterior.

    Returns:
        KML XML string.
    """
    coord_str = " ".join(f"{lon},{lat},0" for lon, lat in coords)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<kml xmlns="http://www.opengis.net/kml/2.2">'
        "<Document><Placemark><Polygon>"
        "<outerBoundaryIs><LinearRing>"
        f"<coordinates>{coord_str}</coordinates>"
        "</LinearRing></outerBoundaryIs>"
        "</Polygon></Placemark></Document></kml>"
    )


# ============================================================
# A. Schema Validation — BuildabilityConfig
# ============================================================


class TestBuildabilityConfig:
    """Tests for BuildabilityConfig Pydantic model."""

    def test_valid_lat_lon_only(self) -> None:
        """Valid config with just lat/lon uses defaults for everything else."""
        config = BuildabilityConfig(latitude=33.45, longitude=-112.07)

        assert config.latitude == 33.45
        assert config.longitude == -112.07
        assert config.radius_km is None
        assert config.kmz_file_path is None
        assert config.slope_thresholds == [5.0, 10.0, 15.0]
        assert config.tracker_slope_limit == 10.0
        assert config.fixed_tilt_slope_limit == 20.0

    def test_valid_with_radius(self) -> None:
        """Valid config with radius_km set."""
        config = BuildabilityConfig(
            latitude=40.0, longitude=-105.0, radius_km=2.5
        )

        assert config.radius_km == 2.5
        assert config.kmz_file_path is None

    def test_valid_with_kmz(self) -> None:
        """Valid config with kmz_file_path set."""
        config = BuildabilityConfig(
            latitude=40.0, longitude=-105.0, kmz_file_path="/path/to/site.kmz"
        )

        assert config.kmz_file_path == "/path/to/site.kmz"
        assert config.radius_km is None

    def test_invalid_both_radius_and_kmz(self) -> None:
        """Setting both radius and kmz raises ValidationError."""
        with pytest.raises(ValidationError, match="Cannot specify both"):
            BuildabilityConfig(
                latitude=40.0,
                longitude=-105.0,
                radius_km=1.5,
                kmz_file_path="/path/to/site.kmz",
            )

    def test_invalid_negative_radius(self) -> None:
        """Negative radius raises ValidationError."""
        with pytest.raises(ValidationError, match="radius_km must be > 0"):
            BuildabilityConfig(
                latitude=40.0, longitude=-105.0, radius_km=-1.0
            )

    def test_empty_string_radius_treated_as_none(self) -> None:
        """Empty string for radius is coerced to None (CSV behavior)."""
        config = BuildabilityConfig(
            latitude=40.0, longitude=-105.0, radius_km=""
        )

        assert config.radius_km is None

    def test_empty_string_kmz_treated_as_none(self) -> None:
        """Empty string for kmz_file_path is coerced to None (CSV behavior)."""
        config = BuildabilityConfig(
            latitude=40.0, longitude=-105.0, kmz_file_path="  "
        )

        assert config.kmz_file_path is None


# ============================================================
# A. Schema Validation — SiteConfig buildability fields
# ============================================================


class TestSiteConfigBuildability:
    """Tests for buildability fields on SiteConfig."""

    def test_buildable_land_assessment_false_ignores_cols(self) -> None:
        """When buildable_land_assessment=False, kmz + radius are accepted without error."""
        # Both kmz and radius present but assessment is disabled — no validation error
        config = SiteConfig(
            **_SITE_BASE,
            buildable_land_assessment=False,
            kmz_file_path="/some/path.kmz",
            analysis_radius_km=2.0,
        )

        assert config.buildable_land_assessment is False
        assert config.kmz_file_path == "/some/path.kmz"
        assert config.analysis_radius_km == 2.0

    def test_buildable_land_assessment_true_both_raises(self) -> None:
        """When buildable_land_assessment=True, both kmz and radius raises ValidationError."""
        with pytest.raises(ValidationError, match="Cannot specify both"):
            SiteConfig(
                **_SITE_BASE,
                buildable_land_assessment=True,
                kmz_file_path="/some/path.kmz",
                analysis_radius_km=2.0,
            )

    def test_buildable_land_assessment_true_radius_only(self) -> None:
        """When buildable_land_assessment=True, radius alone is valid."""
        config = SiteConfig(
            **_SITE_BASE,
            buildable_land_assessment=True,
            analysis_radius_km=1.5,
        )

        assert config.buildable_land_assessment is True
        assert config.analysis_radius_km == 1.5

    def test_buildable_land_assessment_coerces_string(self) -> None:
        """String 'TRUE' from CSV is coerced to bool True."""
        config = SiteConfig(
            **_SITE_BASE,
            buildable_land_assessment="TRUE",
        )

        assert config.buildable_land_assessment is True

    def test_buildable_land_assessment_empty_string_is_false(self) -> None:
        """Empty string from CSV is coerced to False."""
        config = SiteConfig(
            **_SITE_BASE,
            buildable_land_assessment="",
        )

        assert config.buildable_land_assessment is False


# ============================================================
# B. Polygon Parser
# ============================================================


class TestCreateBuffer:
    """Tests for create_buffer circular polygon generation."""

    def test_returns_valid_polygon(self) -> None:
        """Buffer returns a valid, non-empty Polygon in EPSG:4326 bounds."""
        polygon = create_buffer(33.45, -112.07, radius_km=1.0)

        assert polygon.is_valid
        assert not polygon.is_empty
        # Bounds should be reasonable lat/lon values
        minx, miny, maxx, maxy = polygon.bounds
        assert -180 <= minx <= 180
        assert -90 <= miny <= 90
        assert -180 <= maxx <= 180
        assert -90 <= maxy <= 90

    def test_larger_radius_gives_larger_bounds(self) -> None:
        """A 2km radius produces roughly 2x the extent of a 1km radius."""
        poly_1km = create_buffer(33.45, -112.07, radius_km=1.0)
        poly_2km = create_buffer(33.45, -112.07, radius_km=2.0)

        # Compare bounding box widths (longitude span)
        width_1km = poly_1km.bounds[2] - poly_1km.bounds[0]
        width_2km = poly_2km.bounds[2] - poly_2km.bounds[0]

        # 2km should be roughly 2x wider (within 10% tolerance for projection)
        ratio = width_2km / width_1km
        assert 1.8 < ratio < 2.2

    def test_polygon_area_scales_with_radius(self) -> None:
        """Area should scale roughly as radius^2."""
        poly_1km = create_buffer(33.45, -112.07, radius_km=1.0)
        poly_2km = create_buffer(33.45, -112.07, radius_km=2.0)

        # Area ratio should be ~4x (2^2)
        area_ratio = poly_2km.area / poly_1km.area
        assert 3.5 < area_ratio < 4.5


class TestParseKmz:
    """Tests for KMZ/KML polygon parsing."""

    def test_parse_kml_file(self, tmp_path: Path) -> None:
        """Parse a valid .kml file and get a polygon back."""
        # Arrange — create a simple rectangular polygon KML
        coords = [
            (-112.1, 33.4),
            (-112.0, 33.4),
            (-112.0, 33.5),
            (-112.1, 33.5),
            (-112.1, 33.4),  # close the ring
        ]
        kml_path = tmp_path / "test_site.kml"
        kml_path.write_text(_make_kml(coords))

        # Act
        polygon = parse_kmz(kml_path)

        # Assert
        assert polygon.is_valid
        assert not polygon.is_empty
        assert len(polygon.exterior.coords) == 5  # 4 vertices + closing point

    def test_parse_kmz_file(self, tmp_path: Path) -> None:
        """Parse a programmatically created .kmz (zip) file."""
        # Arrange — create a .kmz archive containing a .kml
        coords = [
            (-104.6, 40.3),
            (-104.5, 40.3),
            (-104.5, 40.4),
            (-104.6, 40.4),
            (-104.6, 40.3),
        ]
        kml_content = _make_kml(coords)
        kmz_path = tmp_path / "test_site.kmz"
        with zipfile.ZipFile(kmz_path, "w") as zf:
            zf.writestr("doc.kml", kml_content)

        # Act
        polygon = parse_kmz(kmz_path)

        # Assert
        assert polygon.is_valid
        assert polygon.bounds[0] == pytest.approx(-104.6, abs=0.01)

    def test_nonexistent_file_raises(self) -> None:
        """ValueError for a file that doesn't exist."""
        with pytest.raises(ValueError, match="not found"):
            parse_kmz("/nonexistent/path/site.kmz")

    def test_no_polygon_raises(self, tmp_path: Path) -> None:
        """ValueError for a KML with no <Polygon> element."""
        # Arrange — KML with only a Point, no Polygon
        kml_content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<kml xmlns="http://www.opengis.net/kml/2.2">'
            "<Document><Placemark><Point>"
            "<coordinates>-112.0,33.4,0</coordinates>"
            "</Point></Placemark></Document></kml>"
        )
        kml_path = tmp_path / "no_polygon.kml"
        kml_path.write_text(kml_content)

        # Act / Assert
        with pytest.raises(ValueError, match="No <Polygon>"):
            parse_kmz(kml_path)


class TestGetAnalysisPolygon:
    """Tests for routing functions that select polygon source."""

    def test_routes_to_buffer_when_radius(self) -> None:
        """Config with radius_km routes to create_buffer."""
        config = BuildabilityConfig(
            latitude=33.45, longitude=-112.07, radius_km=1.0
        )

        polygon = get_analysis_polygon(config)

        assert polygon.is_valid
        # Centroid should be near the specified lat/lon
        cx, cy = polygon.centroid.coords[0]
        assert cx == pytest.approx(-112.07, abs=0.02)
        assert cy == pytest.approx(33.45, abs=0.02)

    def test_routes_to_buffer_default_radius(self) -> None:
        """Config with no radius or kmz defaults to 1km buffer."""
        config = BuildabilityConfig(latitude=33.45, longitude=-112.07)

        polygon = get_analysis_polygon(config)

        assert polygon.is_valid
        # Should be roughly 1km radius — check bounding box extent
        minx, miny, maxx, maxy = polygon.bounds
        lat_span_deg = maxy - miny
        # 1km radius ≈ 0.018 degrees of latitude
        assert 0.01 < lat_span_deg < 0.03

    def test_routes_to_kmz(self, tmp_path: Path) -> None:
        """Config with kmz_file_path routes to parse_kmz."""
        # Arrange
        coords = [
            (-112.1, 33.4),
            (-112.0, 33.4),
            (-112.0, 33.5),
            (-112.1, 33.5),
            (-112.1, 33.4),
        ]
        kml_path = tmp_path / "site.kml"
        kml_path.write_text(_make_kml(coords))

        config = BuildabilityConfig(
            latitude=33.45,
            longitude=-112.07,
            kmz_file_path=str(kml_path),
        )

        # Act
        polygon = get_analysis_polygon(config)

        # Assert
        assert polygon.is_valid
        assert len(polygon.exterior.coords) == 5


class TestGetAnalysisPolygonFromSite:
    """Tests for get_analysis_polygon_from_site with SiteConfig."""

    def test_returns_none_when_disabled(self) -> None:
        """Returns None when buildable_land_assessment=False."""
        site = SiteConfig(**_SITE_BASE, buildable_land_assessment=False)

        result = get_analysis_polygon_from_site(site)

        assert result is None

    def test_returns_polygon_when_enabled(self) -> None:
        """Returns a valid Polygon when buildable_land_assessment=True."""
        site = SiteConfig(
            **_SITE_BASE,
            buildable_land_assessment=True,
            analysis_radius_km=1.0,
        )

        polygon = get_analysis_polygon_from_site(site)

        assert polygon is not None
        assert polygon.is_valid

    def test_uses_default_radius_when_none_specified(self) -> None:
        """Uses 1km default when enabled but no radius or kmz."""
        site = SiteConfig(**_SITE_BASE, buildable_land_assessment=True)

        polygon = get_analysis_polygon_from_site(site)

        assert polygon is not None
        assert polygon.is_valid
        # Should be roughly 1km radius
        lat_span = polygon.bounds[3] - polygon.bounds[1]
        assert 0.01 < lat_span < 0.03


# ============================================================
# C. Exclusion Engine
# ============================================================


class TestClassifyLandCover:
    """Tests for land cover classification with synthetic data."""

    @pytest.fixture()
    def synthetic_nlcd(self) -> np.ndarray:
        """Create a synthetic NLCD array with known pixel counts.

        Layout (18 pixels total):
        - 10 pixels class 82 (Cultivated Crops → buildable)
        -  5 pixels class 11 (Open Water → hard exclusion)
        -  3 pixels class 42 (Evergreen Forest → soft exclusion)
        """
        arr = np.zeros((6, 3), dtype=np.uint8)
        arr[0:3, :] = 82   # 9 pixels
        arr[3, 0] = 82      # 1 more → 10 total
        arr[3, 1:] = 11     # 2 pixels
        arr[4, :] = 11      # 3 more → 5 total
        arr[5, :] = 42      # 3 pixels
        return arr

    def test_pixel_counts(self, synthetic_nlcd: np.ndarray) -> None:
        """Verify pixel counts per category match expected values."""
        # Arrange — 30m NLCD pixel = 900 sq m
        pixel_area = 900.0

        # Act
        result = classify_land_cover(synthetic_nlcd, pixel_area)

        # Assert
        assert result["total_pixels"] == 18
        assert result["buildable_pixels"] == 10
        assert result["hard_exclusion_pixels"] == 5
        assert result["soft_exclusion_pixels"] == 3
        assert result["unclassified_pixels"] == 0

    def test_acre_calculations(self, synthetic_nlcd: np.ndarray) -> None:
        """Verify acre calculations are correct."""
        pixel_area = 900.0  # 30m x 30m

        result = classify_land_cover(synthetic_nlcd, pixel_area)

        # 10 pixels * 900 sq m / 4046.86 sq m per acre ≈ 2.22 acres
        expected_buildable = 10 * 900 / 4046.86
        assert result["buildable_acres"] == pytest.approx(expected_buildable, abs=0.1)

        expected_total = 18 * 900 / 4046.86
        assert result["total_area_acres"] == pytest.approx(expected_total, abs=0.1)

    def test_category_assignments(self, synthetic_nlcd: np.ndarray) -> None:
        """Verify each class is assigned to the correct category."""
        result = classify_land_cover(synthetic_nlcd, 900.0)

        breakdown_by_code = {c["code"]: c for c in result["class_breakdown"]}

        assert breakdown_by_code[82]["category"] == "buildable"
        assert breakdown_by_code[11]["category"] == "hard_exclusion"
        assert breakdown_by_code[42]["category"] == "soft_exclusion"

    def test_percentages(self, synthetic_nlcd: np.ndarray) -> None:
        """Verify percentage calculations."""
        result = classify_land_cover(synthetic_nlcd, 900.0)

        # 10/18 = 55.6%, 5/18 = 27.8%, 3/18 = 16.7%
        assert result["summary_pct"]["buildable"] == pytest.approx(55.6, abs=0.1)
        assert result["summary_pct"]["hard_exclusion"] == pytest.approx(27.8, abs=0.1)
        assert result["summary_pct"]["soft_exclusion"] == pytest.approx(16.7, abs=0.1)

    def test_summary_pct_sums_to_100(self, synthetic_nlcd: np.ndarray) -> None:
        """Summary percentages should sum to 100%."""
        result = classify_land_cover(synthetic_nlcd, 900.0)

        total_pct = sum(result["summary_pct"].values())
        assert total_pct == pytest.approx(100.0, abs=0.5)

    def test_class_breakdown_sorted_by_pixels_desc(
        self, synthetic_nlcd: np.ndarray
    ) -> None:
        """Class breakdown should be sorted by pixel count descending."""
        result = classify_land_cover(synthetic_nlcd, 900.0)

        pixel_counts = [c["pixels"] for c in result["class_breakdown"]]
        assert pixel_counts == sorted(pixel_counts, reverse=True)


class TestGetPixelAreaSqM:
    """Tests for pixel area calculation in different CRS."""

    def test_epsg_3857_at_equator(self) -> None:
        """At equator, cos²(0°)=1, so EPSG:3857 area = x_res * y_res."""
        from unittest.mock import MagicMock

        transform = MagicMock()
        transform.a = 30.0
        transform.e = -30.0
        # center_y = transform.f + transform.e * (height/2)
        # For center at equator (y=0): transform.f = 0 - (-30)*50 = 1500
        transform.f = 1500.0

        metadata = {
            "transform": transform,
            "crs": "EPSG:3857",
            "height": 100,
        }

        area = get_pixel_area_sq_m(metadata)

        assert area == pytest.approx(900.0, rel=0.01)

    def test_epsg_3857_at_60_degrees(self) -> None:
        """At 60°N, cos²(60°)=0.25, so EPSG:3857 area = 900 * 0.25 = 225."""
        from unittest.mock import MagicMock

        # 60°N in EPSG:3857: y = R * ln(tan(π/4 + 60°/2)) ≈ 8,399,738
        center_y_3857 = 8_399_738.0
        transform = MagicMock()
        transform.a = 30.0
        transform.e = -30.0
        # center_y = f + e * (height/2) → f = center_y - e * 50
        transform.f = center_y_3857 - (-30.0) * 50

        metadata = {
            "transform": transform,
            "crs": "EPSG:3857",
            "height": 100,
        }

        area = get_pixel_area_sq_m(metadata)

        # cos²(60°) = 0.25, so 900 * 0.25 = 225
        assert area == pytest.approx(225.0, rel=0.01)

    def test_geographic_crs_epsg_4326(self) -> None:
        """For geographic CRS, pixel area accounts for latitude scaling."""
        from unittest.mock import MagicMock

        transform = MagicMock()
        # ~30m resolution in degrees: 30m / 111000m ≈ 0.00027°
        transform.a = 0.00027027
        transform.e = -0.00027027
        transform.f = 41.0  # top edge latitude

        metadata = {
            "transform": transform,
            "crs": "EPSG:4326",
            "height": 100,
        }

        area = get_pixel_area_sq_m(metadata)

        # At ~40° latitude, cos(40°) ≈ 0.766
        # Expected: 0.00027 * 111000 * cos(40°) * 0.00027 * 111000 ≈ 30*30*0.766 ≈ 689
        assert 600 < area < 800


# ============================================================
# D. Slope Analyzer
# ============================================================


class TestAnalyzeSlope:
    """Tests for slope analysis with synthetic data."""

    @pytest.fixture()
    def synthetic_slope(self) -> np.ndarray:
        """Create a synthetic slope array with known distribution.

        100 pixels total:
        - 70 pixels at 2° (bucket 0-5)
        - 15 pixels at 7° (bucket 5-10)
        - 10 pixels at 12° (bucket 10-15)
        -  5 pixels at 18° (bucket 15+)
        """
        arr = np.zeros((10, 10), dtype=np.float64)
        arr[:7, :] = 2.0   # 70 pixels at 2°
        arr[7, :5] = 7.0   # 5 pixels at 7°
        arr[7, 5:] = 7.0   # 5 more → 10... wait, need 15 at 7
        arr[8, :] = 7.0    # 10 more → but that's 20. Let me recalculate.
        # Actually let me just set it cleanly
        arr[:] = 0.0
        arr.flat[:70] = 2.0
        arr.flat[70:85] = 7.0
        arr.flat[85:95] = 12.0
        arr.flat[95:100] = 18.0
        return arr

    def test_summary_statistics(self, synthetic_slope: np.ndarray) -> None:
        """Verify min, max, mean, median match expected values."""
        result = analyze_slope(synthetic_slope)

        assert result["min_degrees"] == pytest.approx(2.0)
        assert result["max_degrees"] == pytest.approx(18.0)

        # Mean: (70*2 + 15*7 + 10*12 + 5*18) / 100 = (140+105+120+90)/100 = 4.55
        assert result["mean_degrees"] == pytest.approx(4.55, abs=0.01)

        # Median: 50th percentile — pixel 50 is at 2°
        assert result["median_degrees"] == pytest.approx(2.0)

    def test_distribution_buckets_sum_to_100(
        self, synthetic_slope: np.ndarray
    ) -> None:
        """Distribution bucket percentages should sum to 100%."""
        result = analyze_slope(synthetic_slope)

        total_pct = sum(b["percent"] for b in result["distribution"])
        assert total_pct == pytest.approx(100.0, abs=0.5)

    def test_distribution_bucket_counts(
        self, synthetic_slope: np.ndarray
    ) -> None:
        """Verify pixel counts in each distribution bucket."""
        result = analyze_slope(synthetic_slope)

        dist = result["distribution"]
        # Default thresholds [5, 10, 15] create 4 buckets
        assert len(dist) == 4

        # 0-5°: 70 pixels (70%), 5-10°: 15 (15%), 10-15°: 10 (10%), 15+°: 5 (5%)
        assert dist[0]["pixels"] == 70
        assert dist[0]["percent"] == pytest.approx(70.0)
        assert dist[1]["pixels"] == 15
        assert dist[1]["percent"] == pytest.approx(15.0)
        assert dist[2]["pixels"] == 10
        assert dist[2]["percent"] == pytest.approx(10.0)
        assert dist[3]["pixels"] == 5
        assert dist[3]["percent"] == pytest.approx(5.0)

    def test_tracker_suitability(self, synthetic_slope: np.ndarray) -> None:
        """Tracker suitability: % of pixels below 10°."""
        result = analyze_slope(synthetic_slope)

        # 70 + 15 = 85 pixels below 10° → 85%
        assert result["pct_below_tracker_limit"] == pytest.approx(85.0)

    def test_fixed_tilt_suitability(self, synthetic_slope: np.ndarray) -> None:
        """Fixed-tilt suitability: % of pixels below 20°."""
        result = analyze_slope(synthetic_slope)

        # All 100 pixels below 20° → 100%
        assert result["pct_below_fixed_tilt_limit"] == pytest.approx(100.0)

    def test_custom_thresholds(self) -> None:
        """Custom thresholds produce the correct number of buckets."""
        slope = np.array([[1, 3], [6, 12]], dtype=np.float64)

        result = analyze_slope(slope, thresholds=[2.0, 8.0])

        # [2, 8] → 3 buckets: 0-2, 2-8, 8+
        assert len(result["distribution"]) == 3

        # Bucket 0-2°: 1 pixel (value=1), Bucket 2-8°: 2 pixels (3 and 6), Bucket 8+°: 1 pixel (12)
        assert result["distribution"][0]["pixels"] == 1
        assert result["distribution"][1]["pixels"] == 2
        assert result["distribution"][2]["pixels"] == 1

    def test_all_flat_terrain(self) -> None:
        """All-zero slope array gives 100% tracker and fixed-tilt suitability."""
        slope = np.zeros((5, 5), dtype=np.float64)

        result = analyze_slope(slope)

        assert result["min_degrees"] == 0.0
        assert result["max_degrees"] == 0.0
        assert result["mean_degrees"] == 0.0
        assert result["pct_below_tracker_limit"] == 100.0
        assert result["pct_below_fixed_tilt_limit"] == 100.0


# ============================================================
# E. Nodata Handling — classify_land_cover excludes masked pixels
# ============================================================


class TestClassifyLandCoverNodata:
    """Tests for nodata (value 0) handling in land cover classification."""

    def test_zero_pixels_excluded_from_total(self) -> None:
        """Pixels with value 0 are not counted in total_pixels."""
        # 6 valid pixels + 4 zero (nodata) pixels
        arr = np.array([
            [82, 82, 0, 0],
            [82, 11, 0, 0],
            [42, 82, 11, 42],
        ], dtype=np.uint8)

        result = classify_land_cover(arr, 900.0)

        # Only 8 non-zero pixels should be counted
        assert result["total_pixels"] == 8

    def test_zero_pixels_excluded_from_acres(self) -> None:
        """Total acreage reflects only valid (non-zero) pixels."""
        arr = np.zeros((10, 10), dtype=np.uint8)
        arr[:5, :5] = 82  # 25 valid pixels

        result = classify_land_cover(arr, 900.0)

        expected_acres = 25 * 900.0 / 4046.86
        assert result["total_area_acres"] == pytest.approx(expected_acres, abs=0.1)
        assert result["total_pixels"] == 25

    def test_zero_pixels_excluded_from_percentages(self) -> None:
        """Percentages are relative to valid pixels, not total array size."""
        # 10 buildable + 10 hard exclusion + 80 nodata
        arr = np.zeros((10, 10), dtype=np.uint8)
        arr.flat[:10] = 82   # buildable
        arr.flat[10:20] = 11  # hard exclusion

        result = classify_land_cover(arr, 900.0)

        assert result["total_pixels"] == 20
        assert result["summary_pct"]["buildable"] == pytest.approx(50.0)
        assert result["summary_pct"]["hard_exclusion"] == pytest.approx(50.0)

    def test_all_zero_returns_empty_result(self) -> None:
        """Array of all zeros returns zero-valued result."""
        arr = np.zeros((5, 5), dtype=np.uint8)

        result = classify_land_cover(arr, 900.0)

        assert result["total_pixels"] == 0
        assert result["total_area_acres"] == 0.0
        assert result["class_breakdown"] == []

    def test_no_zeros_works_unchanged(self) -> None:
        """Array with no zeros produces same result as before."""
        arr = np.full((3, 3), 82, dtype=np.uint8)

        result = classify_land_cover(arr, 900.0)

        assert result["total_pixels"] == 9
        assert result["buildable_pixels"] == 9
        expected_acres = 9 * 900.0 / 4046.86
        assert result["total_area_acres"] == pytest.approx(expected_acres, abs=0.1)


# ============================================================
# F. Slope Analyzer — NaN handling for masked pixels
# ============================================================


class TestAnalyzeSlopeNaN:
    """Tests for NaN (masked pixel) handling in slope analysis."""

    def test_nan_pixels_excluded_from_statistics(self) -> None:
        """NaN pixels are excluded from min/max/mean/median."""
        slope = np.array([
            [2.0, 5.0, np.nan],
            [3.0, 8.0, np.nan],
            [np.nan, np.nan, np.nan],
        ])

        result = analyze_slope(slope)

        assert result["min_degrees"] == pytest.approx(2.0)
        assert result["max_degrees"] == pytest.approx(8.0)
        # Mean of [2, 5, 3, 8] = 4.5
        assert result["mean_degrees"] == pytest.approx(4.5)

    def test_nan_pixels_excluded_from_distribution(self) -> None:
        """Distribution percentages are relative to valid pixels only."""
        slope = np.full((10, 10), np.nan)
        # 40 valid pixels: 30 at 2° and 10 at 7°
        slope.flat[:30] = 2.0
        slope.flat[30:40] = 7.0

        result = analyze_slope(slope)

        dist = result["distribution"]
        # 30/40 = 75% in 0-5°, 10/40 = 25% in 5-10°
        assert dist[0]["pixels"] == 30
        assert dist[0]["percent"] == pytest.approx(75.0)
        assert dist[1]["pixels"] == 10
        assert dist[1]["percent"] == pytest.approx(25.0)

    def test_all_nan_returns_empty_result(self) -> None:
        """Array of all NaN returns zero-valued result."""
        slope = np.full((5, 5), np.nan)

        result = analyze_slope(slope)

        assert result["min_degrees"] == 0.0
        assert result["mean_degrees"] == 0.0
        assert result["distribution"] == []

    def test_no_nan_works_unchanged(self) -> None:
        """Array with no NaN produces same result as before."""
        slope = np.full((10, 10), 3.0)

        result = analyze_slope(slope)

        assert result["min_degrees"] == pytest.approx(3.0)
        assert result["max_degrees"] == pytest.approx(3.0)
        assert result["pct_below_tracker_limit"] == pytest.approx(100.0)


# ============================================================
# G. Polygon Masking — _create_polygon_mask
# ============================================================


class TestCreatePolygonMask:
    """Tests for the raster polygon masking function."""

    def test_mask_epsg_4326_circle(self) -> None:
        """Mask clips a circular buffer polygon in EPSG:4326 raster."""
        import math

        from affine import Affine

        from src.buildability.analyzer import _create_polygon_mask

        # Create a 2km circular buffer at the equator
        polygon = create_buffer(0.0, 0.0, radius_km=2.0)
        bounds = polygon.bounds  # (west, south, east, north)

        # Build a raster grid covering the polygon bbox + small buffer
        res_deg = 30.0 / 111_000  # ~30m in degrees
        west = bounds[0] - 2 * res_deg
        south = bounds[1] - 2 * res_deg
        east = bounds[2] + 2 * res_deg
        north = bounds[3] + 2 * res_deg

        width = int(round((east - west) / res_deg))
        height = int(round((north - south) / res_deg))
        transform = Affine(res_deg, 0.0, west, 0.0, -res_deg, north)

        metadata = {
            "transform": transform,
            "crs": "EPSG:4326",
            "height": height,
            "width": width,
        }

        mask = _create_polygon_mask(polygon, metadata)

        # Masked pixel count × pixel area should ≈ π × 2000²
        pixel_area = get_pixel_area_sq_m(metadata)
        masked_area_sq_m = int(np.sum(mask)) * pixel_area
        expected_area_sq_m = math.pi * 2000**2

        # Within 3% — discretization at 30m on a 2km circle is small
        assert masked_area_sq_m == pytest.approx(expected_area_sq_m, rel=0.03)

    def test_mask_shape_matches_raster(self) -> None:
        """Mask output shape matches (height, width) from metadata."""
        from affine import Affine

        from src.buildability.analyzer import _create_polygon_mask

        polygon = create_buffer(33.45, -112.07, radius_km=1.0)
        bounds = polygon.bounds
        res = 0.0003
        west = bounds[0] - res
        north = bounds[3] + res
        width = 100
        height = 80
        transform = Affine(res, 0.0, west, 0.0, -res, north)

        metadata = {
            "transform": transform,
            "crs": "EPSG:4326",
            "height": height,
            "width": width,
        }

        mask = _create_polygon_mask(polygon, metadata)

        assert mask.shape == (80, 100)
        assert mask.dtype == bool

    def test_mask_excludes_corners(self) -> None:
        """Circle mask excludes bounding-box corner pixels."""
        from affine import Affine

        from src.buildability.analyzer import _create_polygon_mask

        polygon = create_buffer(0.0, 0.0, radius_km=1.0)
        bounds = polygon.bounds
        res = 30.0 / 111_000
        transform = Affine(res, 0.0, bounds[0], 0.0, -res, bounds[3])
        width = int(round((bounds[2] - bounds[0]) / res))
        height = int(round((bounds[3] - bounds[1]) / res))

        metadata = {
            "transform": transform,
            "crs": "EPSG:4326",
            "height": height,
            "width": width,
        }

        mask = _create_polygon_mask(polygon, metadata)

        # Corners should be False (outside circle)
        assert not mask[0, 0]
        assert not mask[0, -1]
        assert not mask[-1, 0]
        assert not mask[-1, -1]
        # Center should be True
        assert mask[height // 2, width // 2]


# ============================================================
# H. Integration — Circular buffer acreage matches π × r²
# ============================================================


class TestCircularBufferAcreage:
    """Integration test: masked + corrected area ≈ geometric expectation."""

    def test_2km_buffer_at_equator(self) -> None:
        """2km buffer at equator: total acreage within 5% of π × r²."""
        import math

        from affine import Affine

        from src.buildability.analyzer import _create_polygon_mask

        radius_m = 2000.0
        polygon = create_buffer(0.0, 0.0, radius_km=2.0)
        bounds = polygon.bounds
        res_deg = 30.0 / 111_000

        west = bounds[0] - 5 * res_deg
        south = bounds[1] - 5 * res_deg
        east = bounds[2] + 5 * res_deg
        north = bounds[3] + 5 * res_deg

        width = int(round((east - west) / res_deg))
        height = int(round((north - south) / res_deg))
        transform = Affine(res_deg, 0.0, west, 0.0, -res_deg, north)

        metadata = {
            "transform": transform,
            "crs": "EPSG:4326",
            "height": height,
            "width": width,
        }

        # Mask to polygon, classify, check acreage
        mask = _create_polygon_mask(polygon, metadata)
        nlcd = np.full((height, width), 82, dtype=np.uint8)
        nlcd[~mask] = 0

        pixel_area = get_pixel_area_sq_m(metadata)
        result = classify_land_cover(nlcd, pixel_area)

        expected_acres = math.pi * radius_m**2 / 4046.86
        assert result["total_area_acres"] == pytest.approx(
            expected_acres, rel=0.05
        )

    def test_1km_buffer_at_mid_latitude(self) -> None:
        """1km buffer at 40°N: total acreage within 5% of π × r²."""
        import math

        from affine import Affine

        from src.buildability.analyzer import _create_polygon_mask

        radius_m = 1000.0
        polygon = create_buffer(40.0, -105.0, radius_km=1.0)
        bounds = polygon.bounds
        res_deg = 30.0 / 111_000

        west = bounds[0] - 5 * res_deg
        south = bounds[1] - 5 * res_deg
        east = bounds[2] + 5 * res_deg
        north = bounds[3] + 5 * res_deg

        width = int(round((east - west) / res_deg))
        height = int(round((north - south) / res_deg))
        transform = Affine(res_deg, 0.0, west, 0.0, -res_deg, north)

        metadata = {
            "transform": transform,
            "crs": "EPSG:4326",
            "height": height,
            "width": width,
        }

        mask = _create_polygon_mask(polygon, metadata)
        nlcd = np.full((height, width), 71, dtype=np.uint8)
        nlcd[~mask] = 0

        pixel_area = get_pixel_area_sq_m(metadata)
        result = classify_land_cover(nlcd, pixel_area)

        expected_acres = math.pi * radius_m**2 / 4046.86
        assert result["total_area_acres"] == pytest.approx(
            expected_acres, rel=0.05
        )
