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

A `Precipitation` column (all zeros) is added during formatting since PySAM requires it but NSRDB does not provide it.

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

Environment variables override default values when set.
