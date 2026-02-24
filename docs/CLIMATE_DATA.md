# Climate Data Integration

## Overview

The climate data pipeline fetches hourly solar resource data from NREL's National Solar Radiation Database (NSRDB) and formats it for PySAM consumption. The pipeline handles caching, deduplication, and failure recovery.

## NSRDB Data Fields

The following fields are retrieved from the NSRDB PSM v3.2.2 API:

| Field | Unit | Description |
|-------|------|-------------|
| GHI | W/m2 | Global Horizontal Irradiance |
| DNI | W/m2 | Direct Normal Irradiance |
| DHI | W/m2 | Diffuse Horizontal Irradiance |
| Temperature | C | Air temperature at 2m |
| Wind Speed | m/s | Wind speed at 10m |
| Surface Albedo | dimensionless | Ground reflectivity (0-1) |

A `Precipitation` column is added during formatting. When NCEI precipitation data is available, real hourly values are used; otherwise, zeros are used as a fallback.

## Cache System

### Filename Format

Cache files follow the pattern:

```
nsrdb_{latitude}_{longitude}_{YYYYMMDD}.csv
```

Examples:
- `nsrdb_33.45_-111.98_20250223.csv` (Phoenix)
- `nsrdb_-33.87_-151.21_20250223.csv` (negative coordinates)

### Age Policy

- Default maximum age: **365 days**
- Files older than `max_age_days` are considered stale and trigger a fresh API call
- Configurable via `ClimateConfig.cache_max_age_days`

### Nearest Cache Fallback

When an API call fails, the system can fall back to a nearby cached file:

- Uses **Haversine formula** to calculate great-circle distance between coordinates
- Default maximum distance: **50 km**
- Selects the closest cached file within range
- Configurable via `ClimateConfig.max_cache_distance_km`

## Haversine Distance Calculation

The `_calculate_distance()` function computes the great-circle distance between two lat/lon points:

```
a = sin2(dlat/2) + cos(lat1) * cos(lat2) * sin2(dlon/2)
c = 2 * atan2(sqrt(a), sqrt(1-a))
d = R * c  (R = 6371 km)
```

## API Failure Recovery

When an NSRDB API call fails, the orchestrator offers interactive recovery:

1. **Retry** - Re-attempt the API call (up to 3 retries)
2. **Use nearest cache** - Fall back to a nearby cached file (if one exists within 50 km)
3. **Abort** - Stop processing and raise an error

The flow:

```
API call fails
    -> Check for nearest cached file within max_distance_km
    -> Prompt user with available options
    -> If retry: attempt again (up to MAX_RETRIES=3)
    -> If nearest cache: return the cached file path
    -> If abort: raise ClimateDataError
```

## Orchestrator Deduplication

The `ClimateOrchestrator.fetch_climate_data()` method deduplicates API calls:

1. Extract all `(latitude, longitude)` tuples from the site list
2. Deduplicate to unique locations using `get_unique_locations()`
3. For each unique location:
   - Check cache for an exact-match fresh file
   - If cache hit: use cached file (skip API call)
   - If cache miss: fetch from NSRDB API
   - On API failure: invoke recovery workflow
4. Return mapping of `(lat, lon) -> Path` for all locations

Multiple sites at the same coordinates share a single weather file.

## Example Usage

```python
from src.climate.nsrdb_client import NSRDBClient
from src.climate.cache_manager import CacheManager
from src.climate.weather_formatter import WeatherFormatter
from src.climate.orchestrator import ClimateOrchestrator
from src.config.loader import load_config
from pathlib import Path

# Load site configurations
sites = load_config(Path("input/sites.csv"))

# Create pipeline components
client = NSRDBClient(api_key="your-key", email="you@example.com")
cache = CacheManager(cache_dir=Path("data/climate"))
formatter = WeatherFormatter()

# Orchestrate data retrieval
orchestrator = ClimateOrchestrator(client, cache, formatter)
results = orchestrator.fetch_climate_data(sites, year=2024)

# results: {(33.45, -111.98): Path("data/climate/nsrdb_33.45_-111.98_20250223.csv"), ...}
```

## Configuration

All climate settings are managed via `ClimateConfig` (Pydantic model):

| Setting | Default | Env Var | Description |
|---------|---------|---------|-------------|
| `api_key` | `DEMO_KEY` | `NSRDB_API_KEY` | NREL API key |
| `api_email` | `demo@example.com` | `NSRDB_API_EMAIL` | NREL account email |
| `default_year` | `2024` | - | Weather data year |
| `cache_max_age_days` | `365` | - | Max cache file age |
| `cache_dir` | `data/climate` | - | Cache directory |
| `max_cache_distance_km` | `50.0` | - | Max nearest-cache distance |
| `ncei_token` | `WewidNCeiBHMUnnVbgNyjKxxHCSXXCad` | `NCEI_API_TOKEN` | NOAA NCEI API token |
| `precipitation_enabled` | `True` | - | Enable precipitation fetch |
| `max_station_distance_km` | `100.0` | - | Max NCEI station distance |

Environment variables override default values when set.

## Precipitation Data (NOAA NCEI)

### Overview

The pipeline optionally fetches real hourly precipitation data from NOAA's National Centers for Environmental Information (NCEI) Climate Data Online (CDO) API. This replaces the default all-zeros Precipitation column with actual observed values.

The design is **best-effort**: any failure in the precipitation pipeline returns `None` and the formatter falls back to zeros. Precipitation failures never break the main climate data pipeline.

### NCEI API

- **Base URL**: `https://www.ncei.noaa.gov/cdo-web/api/v2`
- **Dataset**: `PRECIP_HLY` (Hourly Precipitation)
- **Datatype**: `HPCP` (Hourly Precipitation Amount)
- **Authentication**: Token-based via `headers = {'token': api_token}`
- **Rate limit**: 5 requests/second (enforced client-side with 200ms minimum interval)

### Station Search

The `PrecipitationClient` finds the nearest weather station with hourly precipitation data:

1. Convert `max_station_distance_km` to a lat/lon bounding box (`degrees ≈ km / 111`)
2. Query the NCEI `/stations` endpoint for stations with HPCP data within the bounding box
3. Calculate Haversine distance to each candidate station
4. Select the closest station within `max_station_distance_km`

### Data Alignment

NCEI precipitation records are sparse (only non-zero hours may be reported). The client aligns these to a complete hourly series:

1. Create a full hourly index for the year (8760 or 8784 hours)
2. Parse NCEI ISO timestamps with `pd.to_datetime()`
3. Reindex sparse data to the complete hourly index
4. Fill gaps with `0.0` (no precipitation)

### Failure Behavior

The precipitation client **never raises exceptions**. All failures are caught, logged as warnings, and return `None`:

- No station found within range → `None`
- NCEI API error (timeout, HTTP error, connection error) → `None`
- Empty data response → `None`
- Any unexpected error → `None`

The orchestrator logs a warning when precipitation is unavailable and the formatter falls back to zeros.

### Precipitation Configuration

| Setting | Default | Env Var | Description |
|---------|---------|---------|-------------|
| `ncei_token` | `WewidNCeiBHMUnnVbgNyjKxxHCSXXCad` | `NCEI_API_TOKEN` | NOAA NCEI API token |
| `precipitation_enabled` | `True` | - | Enable/disable precipitation fetch |
| `max_station_distance_km` | `100.0` | - | Max distance for NCEI station search |

### Cache Interaction

Precipitation is only fetched on NSRDB cache misses. When a cached PySAM weather file is reused, it already contains its Precipitation column (either real data or zeros from the original fetch). This avoids redundant NCEI API calls for cached locations.
