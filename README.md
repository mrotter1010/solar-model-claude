# Solar Production Model

Python-based solar production modeling tool using NREL's PySAM detailed photovoltaic model for utility-scale solar projects.

## Architecture Overview

```
CSV Input → Climate Data (NSRDB/Solcast + Open-Meteo) → PySAM Simulation → Subhourly Correction → 8760 Timeseries + PDF Report → Database Storage
```

1. **CSV Input**: Site parameters including location (lat/lon), system design, panel/inverter models, and loss assumptions
2. **Climate Data**: Fetches TMY/historical weather from NSRDB v4.0.0 or Solcast TMY files, plus snow/soiling data from Open-Meteo ERA5
3. **PySAM Simulation**: Runs NREL's System Advisor Model detailed PV simulation (CEC Performance Model)
4. **Subhourly Correction**: ML model adjusts for inverter clipping losses missed by hourly-resolution modeling
5. **Output**: Per-site 8760 hourly timeseries CSV, customer PDF report with waterfall loss chart
6. **Database**: Stores all run inputs, results, and metrics in TimescaleDB/PostgreSQL

## Setup

### Prerequisites
- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Anaconda](https://www.anaconda.com/)
- Python 3.12.4
- PostgreSQL 17 with TimescaleDB extension

### Installation

```bash
# Create conda environment
conda env create -f environment.yml

# Activate environment
conda activate solar-model
```

### NSRDB API Setup

The climate data pipeline uses NREL's National Solar Radiation Database (NSRDB) API. To configure:

1. **Get an API key** at [https://developer.nrel.gov/signup/](https://developer.nrel.gov/signup/)
2. **Set environment variables:**

```bash
export NSRDB_API_KEY="your-api-key-here"
export NSRDB_API_EMAIL="your-email@example.com"
```

Without these variables, the tool defaults to `DEMO_KEY` which has lower rate limits.

**Cache behavior:** Weather data is cached in `data/climate/` as `nsrdb_{lat}_{lon}_{YYYYMMDD}.csv`. Cached files are reused if less than 365 days old. Sites sharing the same coordinates are deduplicated to avoid redundant API calls. See [docs/CLIMATE_DATA.md](docs/CLIMATE_DATA.md) for details.

## Usage

### Input
CSV file with site parameters:
- Location (latitude, longitude)
- System design (capacity, tilt, azimuth, GCR)
- Panel and inverter model specifications (matched against CEC databases)
- Loss assumptions (soiling, shading, wiring, etc.)

### Output
- **8760 timeseries CSV**: Hourly AC production, POA irradiance, cell temperature, inverter efficiency
- **PDF report**: Customer-facing report with system summary, production metrics, and waterfall loss chart
- **Database records**: Run inputs, results, and loss data stored in PostgreSQL for querying and comparison

## Database

### Connecting

```bash
psql -U solar_model -d solar_model -h localhost
```

Standard PostgreSQL — any Postgres client works.

### Schema

Five tables: `customers`, `sites`, `runs`, `run_inputs`, `run_results`. Managed via Alembic migrations.

### Useful Queries

List all customers:
```sql
SELECT id, name, created_at FROM customers ORDER BY name;
```

List all sites for a customer:
```sql
SELECT s.id, s.name, s.latitude, s.longitude
FROM sites s JOIN customers c ON s.customer_id = c.id
WHERE c.name = 'SunCorp'
ORDER BY s.name;
```

List all runs for a site (most recent first):
```sql
SELECT r.id, r.run_name, r.status, r.weather_year, r.timestamp
FROM runs r JOIN sites s ON r.site_id = s.id
WHERE s.name = 'SiteTest_Phoenix'
ORDER BY r.timestamp DESC;
```

Get run inputs and results by run ID:
```sql
SELECT r.run_name, s.name AS site_name, c.name AS customer,
       ri.dc_size_mw, ri.ac_installed_mw, ri.racking, ri.panel_model,
       rr.annual_energy_mwh, rr.net_capacity_factor, rr.specific_yield, rr.performance_ratio
FROM runs r
  JOIN sites s ON r.site_id = s.id
  JOIN customers c ON s.customer_id = c.id
  JOIN run_inputs ri ON ri.run_id = r.id
  JOIN run_results rr ON rr.run_id = r.id
WHERE r.id = 'your-run-uuid-here';
```

Get just run results by run ID:
```sql
SELECT annual_energy_mwh, net_capacity_factor, specific_yield, performance_ratio,
       capacity_factor_dc, capacity_factor_ac, annual_snow_loss_pct,
       timeseries_file_path, report_file_path
FROM run_results
WHERE run_id = 'your-run-uuid-here';
```

### Recreate Query (Python)

Reconstruct the full 28-column CSV input row from a run ID:

```python
from src.database.queries import recreate_run_input

row = recreate_run_input("your-run-uuid-here")
# Returns dict with all 28 original CSV column names as keys
```

## Testing

Run tests with coverage:
```bash
pytest tests/ -v --cov=src
```

Run tests for specific module:
```bash
pytest tests/test_config.py -v
```

Generate HTML coverage report:
```bash
pytest tests/ --cov=src --cov-report=html
open htmlcov/index.html
```

### Test Organization
- `tests/conftest.py`: Shared pytest fixtures
- `tests/fixtures/`: Sample CSV files for testing
- `tests/test_*.py`: Test modules mirroring src/ structure

## Running the Climate Pipeline

Fetch weather data for all sites in a CSV:

```bash
python scripts/test_climate_data.py --csv "Energy Analytics Inputs Single Row Test - Sheet1.csv"
python scripts/test_climate_data.py --csv "Energy Analytics Inputs Multi Row Test - Sheet1.csv" --year 2023
```

The pipeline will:
1. Parse and validate site configurations from the CSV
2. Deduplicate locations (sites at the same lat/lon share one API call)
3. Check the local cache (`data/climate/`) before calling the NSRDB API
4. Fetch snow depth data from Open-Meteo ERA5 for monthly soiling/snow loss estimates
5. Format weather files for PySAM consumption

## Milestones

1. **Core Infrastructure** — Config validation, logging, error handling
2. **Climate Data Integration** — NSRDB API client, caching, weather formatting, pipeline entry point
3. **PySAM Model Execution** — CEC Performance Model configuration, string sizing, batch simulation
4. **Output Processing** — 8760 timeseries generation, summary metrics, shading haircut
5. **Climate & Reporting** — Open-Meteo ERA5 snow/soiling, NSRDB v4.0.0 migration, customer PDF reports with waterfall loss charts
6. **Database Integration** — TimescaleDB/PostgreSQL schema, SQLAlchemy ORM, Alembic migrations, run tracking, CSV input reconstruction
7. **Solcast TMY Ingest** — Solcast TMY file parser, auto-detection of resource file path in CSV, pipeline branching by data source, DB and PDF updates
8. **Subhourly Resolution Correction** — ML model correcting for inverter clipping losses missed by hourly-resolution PySAM simulation
   - Gradient Boosting model trained on 6,480 paired PySAM simulations (45 CONUS sites × 72 configurations × 2 temporal resolutions)
   - Leave-one-site-out cross-validation: Global R² = 0.85, RMSE = 0.25%
   - Loss-only correction clamped to ≥ 0% per industry practice (DNV Hourly Modeling Correction standard)
   - Applied after PySAM, before output generation — reflected in waterfall chart ("Subhourly Clipping" step), PDF narrative, and database columns
   - Both raw (pre-correction) and adjusted annual energy preserved for audit trail
   - 447 tests passing
