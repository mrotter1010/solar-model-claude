# Solar Production Model

Python-based solar production modeling tool using NREL's PySAM detailed photovoltaic model for utility-scale solar projects. Accepts a multi-row CSV of site configurations, fetches weather data, runs physics-based simulations, applies trained ML corrections, and produces per-site 8760 hourly timeseries CSVs, PDF reports, and database records.

## Pipeline Overview

```
CSV Input
  → Config validation (Pydantic)
  → Climate data fetch (NSRDB v4.0.0 or Solcast TMY) + ERA5 snow/precip
  → NSRDB bias correction (ML, v1)
  → PySAM detailed PV simulation (CEC Performance Model)
  → Subhourly clipping correction (ML, v2)
  → Shading haircut
  → Output: 8760 timeseries CSV + PDF report + database write
```

### Step-by-step

1. **CSV parsing and validation** — Each row is validated against a Pydantic schema (`SiteConfig`). Panel and inverter models are matched against CEC databases (~20k modules, ~2k inverters). String sizing computes modules per string and string count from DC capacity and module/inverter voltage specs.

2. **Climate data retrieval** — For each unique (lat, lon), fetches hourly weather from NSRDB GOES Aggregated v4.0.0 (GHI, DNI, DHI, temperature, wind speed, albedo). If a Solcast TMY file path is provided in the CSV, that file is used instead. Snow depth and precipitation are fetched from Open-Meteo ERA5 reanalysis for monthly soiling/snow loss estimates. Weather files are cached locally.

3. **NSRDB bias correction** — Monthly GHI/DNI correction factors predicted by trained ML models (Gradient Boosting for GHI, Random Forest for DNI). DHI recomputed from irradiance closure. Skipped for Solcast-sourced data.

4. **PySAM simulation** — Runs NREL SAM's `Pvsamv1` detailed PV model. CEC Performance Model with single-diode module parameters. Configures tracking mode, self-shading, bifacial gains, snow loss (from ERA5), monthly soiling (from precipitation thresholds), and all system losses from the CSV.

5. **Subhourly clipping correction** — ML model predicts the percentage of annual AC energy that hourly resolution misses due to sub-hourly irradiance variability (inverter clipping during brief high-irradiance periods). Applied as a loss to the hourly timeseries using a physically targeted peak-shaving algorithm.

6. **Shading haircut** — Flat percentage reduction from the CSV `Shading (%)` column.

7. **Output generation** — Per-site 8760 hourly timeseries CSV (AC production, POA irradiance, cell temperature, DC/AC power, inverter efficiency). Summary metrics JSON. Optional PDF report with system summary table, monthly production bar chart, waterfall loss chart, and methodology narrative.

8. **Database storage** — Run inputs, results, and metrics stored in PostgreSQL/TimescaleDB. Both raw (pre-correction) and adjusted annual energy preserved for audit trail.

## Input CSV Format

The CSV accepts up to 31 columns. Column mapping is defined in `src/config/loader.py`.

| # | Column | Type | Required | Description |
|---|--------|------|----------|-------------|
| 1 | Run Name | string | yes | Unique identifier for this simulation run |
| 2 | Site Name | string | yes | Site identifier (rows sharing a name must have identical coordinates) |
| 3 | Customer | string | yes | Customer/project name |
| 4 | Latitude | float | yes | Site latitude (-90 to 90) |
| 5 | Longitude | float | yes | Site longitude (-180 to 180) |
| 6 | BESS Dispatch Required | float | no | Battery storage parameter (stored, not used in simulation) |
| 7 | BESS Optimization Required | float | no | Battery storage parameter (stored, not used in simulation) |
| 8 | DC Size (MW) | float | yes | DC system capacity in MW |
| 9 | AC Installed (MW) | float | yes | AC inverter capacity in MW |
| 10 | AC POI (MW) | float | yes | Point of interconnection limit in MW |
| 11 | Racking | string | yes | `fixed` or `tracker` (case-insensitive) |
| 12 | Tilt | float | yes | Fixed: static tilt angle (0-90). Tracker: rotation limit (degrees from horizontal) |
| 13 | Azimuth | float | yes | Array azimuth in degrees (0-360, 180 = south-facing) |
| 14 | Module Orientation | string | yes | `portrait` or `landscape` |
| 15 | Number of Modules | int | yes | Modules in height on racking (1 or 2) |
| 16 | Ground Clearance Height (m) | float | yes | Distance from ground to lowest module edge |
| 17 | Panel Model | string | yes | Must match a name in `docs/CEC Modules.csv` |
| 18 | Bifacial | bool | yes | `True` or `False` |
| 19 | Inverter Model | string | yes | Must match a name in `docs/CEC Inverters.csv` |
| 20 | GCR | float | yes | Ground coverage ratio (0 to 1, exclusive) |
| 21 | Shading (%) | float | yes | Post-simulation shading loss (0-100) |
| 22 | DC Wiring Loss (%) | float | yes | DC cable losses (0-100) |
| 23 | AC Wiring Loss (%) | float | yes | AC cable losses (0-100) |
| 24 | Transformer Losses (%) | float | yes | Step-up transformer losses (0-100) |
| 25 | Degradation (%) | float | yes | Annual module degradation (0-100) |
| 26 | Availability (%) | float | yes | System unavailability/downtime (0-100). Inverted for PySAM. |
| 27 | Module Mismatch (%) | float | yes | Module mismatch loss (0-100) |
| 28 | LID(%) | float | yes | Light-induced degradation (0-100) |
| 29 | Report | bool | no | `TRUE` to generate PDF report for this row |
| 30 | Resource File Path | path | no | Path to Solcast TMY SAM CSV file. If set, NSRDB is skipped. |
| 31 | Ground Truth Data File | path | no | Path to ground truth irradiance CSV for site-specific bias correction |

## Climate Data Sources

### NSRDB (primary)
- **Endpoint:** GOES Aggregated v4.0.0 (`nsrdb-GOES-aggregated-v4-0-0`)
- **Data fetched:** GHI, DNI, DHI, air temperature, wind speed, surface albedo
- **Resolution:** Hourly, single year (default: 2024)
- **Caching:** Files cached in `data/climate/` as `nsrdb_{lat}_{lon}_{YYYYMMDD}.csv`, reused if < 365 days old. Sites at the same coordinates are deduplicated.

### Solcast (optional)
- **Format:** SAM-format TMY CSV files provided via the `Resource File Path` CSV column
- **Validation:** Checks 2-row header metadata, required columns (with alias support), row count, and lat/lon proximity
- **Behavior:** When set, bypasses NSRDB fetch and bias correction entirely

### Open-Meteo ERA5 (supplemental)
- **Data fetched:** Hourly snow depth (cm), monthly precipitation totals (inches)
- **Usage:** Snow depth feeds PySAM's snow loss model. Precipitation drives monthly soiling loss via a threshold table (< 0.5" = 3.0% loss, >= 2.0" = 1.0% loss).
- **API:** Free REST API, no key required. Results cached as JSON in `data/climate/era5/`.

### Open-Meteo Elevation API
- **Usage:** Looks up site elevation for the NSRDB bias correction model
- **Caching:** In-memory cache keyed on (lat, lon)

## ML Models

### NSRDB Bias Correction (v1.0.0)

Corrects systematic overestimation in NSRDB satellite irradiance relative to ground-truth measurements.

| Property | Value |
|----------|-------|
| **GHI model** | Gradient Boosting Regressor |
| **DNI model** | Random Forest Regressor |
| **Features** | latitude, longitude, elevation, month, NSRDB irradiance value |
| **Training data** | 20 SURFRAD/SOLRAD/MIDC ground stations, 2018-2023 |
| **GHI improvement** | 39.4% reduction in bias vs uncorrected |
| **DNI improvement** | 36.6% reduction in bias vs uncorrected |
| **Correction range** | Monthly factors typically 0.75-1.0 (NSRDB overestimates) |
| **Safety clamps** | Factors clamped to [0.5, 1.5]; corrections < 3% from unity are skipped |
| **DHI handling** | Recomputed from irradiance closure after GHI/DNI correction |

**Artifacts:** `src/models/artifacts/nsrdb_bias_correction_{ghi,dni}_v1.joblib`, `nsrdb_bias_correction_v1_metadata.json`

### Subhourly Clipping Correction (v2.0.0)

Predicts the percentage of annual AC energy that hourly-resolution PySAM simulation overestimates due to missing sub-hourly irradiance variability. Applied as a loss-only correction (clamped >= 0%).

| Property | Value |
|----------|-------|
| **Model** | Gradient Boosting Regressor (200 trees, depth 5) |
| **Training data** | 760 paired PySAM simulations (1-min vs 60-min) across 19 CONUS ground stations |
| **Cross-validation** | Leave-one-station-out (19 folds) |
| **Global R²** | 0.874 |
| **RMSE** | 0.288% |
| **Mean correction** | 0.78% (range: 0.0% to 3.3%) |
| **Top features** | DC/AC ratio (47%), pct_clear_hours (16%), mean_dni (14%) |
| **Clamping** | Target clamped >= 0 at training time; prediction also clamped >= 0 |

**14 input features:** dcac_ratio, gcr, racking, latitude, longitude, cf_60min, annual_ghi, mean_kt, std_kt, ghi_cv, mean_dni, pct_clear_hours, climate_cloudy, climate_variable

Six weather features are computed from the hourly weather file using Spencer (1971) solar geometry for extraterrestrial irradiance and clearness index calculation.

**v2 vs v1:** v2 is trained on real 1-minute ground station irradiance (19 stations). v1 used NSRDB 5-minute satellite data (45 sites, 3,240 samples). Ground-truth 1-min data captures sharper irradiance peaks than satellite-smoothed data, yielding higher corrections (mean 0.78% vs 0.3%).

**Artifacts:** `src/models/artifacts/subhourly_correction_v2.joblib`, `subhourly_correction_v2_metadata.json`. v1 artifacts are retained alongside v2.

## Outputs

### 8760 Timeseries CSV
Per-site hourly CSV with columns including AC production, POA irradiance, cell temperature, DC/AC power, and inverter efficiency.

### PDF Report
Customer-facing PDF generated by reportlab, containing:
- Site summary table (location, system design, equipment)
- Monthly production bar chart
- Waterfall loss chart (DC nominal → net AC, showing each loss step including subhourly clipping)
- Methodology narrative (auto-generated, includes bias correction and subhourly correction details)

### Database Records
PostgreSQL/TimescaleDB with 5 tables managed via Alembic migrations:

| Table | Purpose |
|-------|---------|
| `customers` | Customer registry |
| `sites` | Site locations (linked to customer) |
| `runs` | Simulation run tracking (UUID PK, status, weather year, git hash) |
| `run_inputs` | Full input parameters for each run (system design, losses) |
| `run_results` | Results: annual energy, capacity factors, specific yield, performance ratio, bias/subhourly correction metadata, monthly data (JSON), file paths |

Both `raw_annual_energy_mwh` (pre-correction) and `annual_energy_mwh` (post-correction) are stored for audit purposes.

## Setup

### Prerequisites
- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Anaconda](https://www.anaconda.com/)
- Python 3.12.4
- PostgreSQL 17 with TimescaleDB extension (for database features)

### Installation

```bash
conda env create -f environment.yml
conda activate solar-model
```

### NSRDB API Credentials

```bash
export NSRDB_API_KEY="your-api-key-here"
export NSRDB_API_EMAIL="your-email@example.com"
```

Get a free key at [https://developer.nrel.gov/signup/](https://developer.nrel.gov/signup/). Without these variables, the tool uses a default key with lower rate limits.

### CEC Equipment Databases

Module and inverter parameters are loaded from:
- `docs/CEC Modules.csv` (~20,000 modules)
- `docs/CEC Inverters.csv` (~2,000 inverters)

Panel Model and Inverter Model values in the input CSV must exactly match names in these files.

## Usage

### CLI

```bash
# Full pipeline: CSV → climate → simulation → outputs
python -m src.main input.csv

# Specify output directory
python -m src.main input.csv --output-dir results/

# Skip climate fetch (weather files must already exist)
python -m src.main input.csv --skip-climate

# Set log level
python -m src.main input.csv --log-level DEBUG
```

### Python API

```python
from pathlib import Path
from src.pipeline import SolarModelingPipeline

pipeline = SolarModelingPipeline(output_dir=Path("outputs"))
results = pipeline.run(csv_path=Path("input.csv"))
# results: {total_sites, successful, failed, timeseries_files, summaries, report_files, error_files}
```

### Climate Pipeline Only

```bash
python scripts/test_climate_data.py --csv "sites.csv" --year 2023
```

## Project Structure

```
src/
├── main.py                          # CLI entry point
├── pipeline.py                      # SolarModelingPipeline orchestrator
├── config/
│   ├── loader.py                    # CSV → SiteConfig parsing, column mapping
│   └── schema.py                    # Pydantic SiteConfig model (31 fields)
├── climate/
│   ├── nsrdb_client.py              # NSRDB GOES Aggregated v4.0.0 API client
│   ├── solcast_parser.py            # Solcast TMY SAM CSV validator
│   ├── open_meteo_client.py         # ERA5 snow/precip + elevation API
│   ├── weather_formatter.py         # Raw NSRDB → SAM-format CSV
│   ├── cache_manager.py             # Weather file caching
│   ├── orchestrator.py              # Climate pipeline coordinator
│   ├── soiling_calculator.py        # Precipitation → monthly soiling loss
│   └── config.py                    # ClimateConfig (env vars, cache settings)
├── pysam_integration/
│   ├── model_configurator.py        # SiteConfig → PySAM Pvsamv1 configuration
│   ├── simulator.py                 # PySAM execution, timeseries extraction
│   ├── string_calculator.py         # Module/string sizing from capacity + voltage
│   ├── cec_database.py              # CEC module/inverter CSV database loader
│   └── exceptions.py                # PySAM-specific exceptions
├── models/
│   ├── nsrdb_bias_correction.py     # Monthly GHI/DNI bias correction (v1)
│   ├── subhourly_correction.py      # Subhourly clipping loss correction (v2)
│   ├── timeseries_adjustment.py     # Peak-shaving algorithm for 8760 correction
│   └── artifacts/                   # Trained model files (.joblib) + metadata (.json)
├── outputs/
│   └── output_writer.py             # 8760 CSV + error JSON generation
├── reporting/
│   ├── report_generator.py          # PDF orchestrator
│   ├── data_extractor.py            # Loss waterfall, monthly data, narrative text
│   ├── chart_builder.py             # Matplotlib charts (monthly bar, waterfall)
│   └── pdf_builder.py               # ReportLab PDF assembly
├── database/
│   ├── models.py                    # SQLAlchemy ORM (5 tables)
│   ├── connection.py                # Database connection management
│   ├── writer.py                    # Save run results to DB
│   └── queries.py                   # Query helpers (recreate_run_input)
└── utils/
    ├── exceptions.py                # Custom exception hierarchy
    └── logger.py                    # Logging configuration

docs/
├── CEC Modules.csv                  # NREL CEC module parameter database
├── CEC Inverters.csv                # NREL CEC inverter parameter database
└── CLIMATE_DATA.md                  # Climate data pipeline documentation

research/                            # Research scripts (not part of production pipeline)
├── m8_subhourly/                    # M8: Original subhourly model (v1, 5-min NSRDB)
├── m9_bias_correction/              # M9: NSRDB bias correction model training
└── m11_subhourly/                   # M11: Subhourly model v2 (1-min ground stations)

tests/                               # 544 tests
├── conftest.py                      # Shared pytest fixtures
├── fixtures/                        # Sample CSV files
└── test_*.py                        # Test modules mirroring src/ structure
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=term-missing

# Single test file
pytest tests/test_config.py -v

# Single test
pytest tests/test_config.py::test_name

# HTML coverage report
pytest tests/ --cov=src --cov-report=html
open htmlcov/index.html
```

544 tests covering config validation, climate clients, PySAM configuration, simulation execution, output formatting, bias correction, subhourly correction, reporting, database operations, and end-to-end smoke tests. Tests write intermediate outputs to `outputs/test_results/` for manual inspection.

## Database

### Connecting

```bash
psql -U solar_model -d solar_model -h localhost
```

### Useful Queries

```sql
-- List all runs for a site (most recent first)
SELECT r.id, r.run_name, r.status, r.weather_year, r.timestamp
FROM runs r JOIN sites s ON r.site_id = s.id
WHERE s.name = 'SiteTest_Phoenix'
ORDER BY r.timestamp DESC;

-- Get run results by run ID
SELECT annual_energy_mwh, net_capacity_factor, specific_yield, performance_ratio,
       capacity_factor_dc, capacity_factor_ac, annual_snow_loss_pct,
       bias_correction_applied, subhourly_correction_pct,
       raw_annual_energy_mwh, timeseries_file_path, report_file_path
FROM run_results WHERE run_id = 'your-run-uuid-here';
```

### Recreate CSV Input Row

```python
from src.database.queries import recreate_run_input
row = recreate_run_input("your-run-uuid-here")
```

## Development Workflow

- **Milestone-driven** development with feature branches per milestone (`feature/milestone-X-description`)
- **Conventional commits** format (`feat:`, `fix:`, `test:`, `docs:`)
- Feature branches merge to `main` after all tests pass
- Claude Code used for implementation via structured prompt sequences

## Milestones

| # | Name | Status | Description |
|---|------|--------|-------------|
| M1 | Core Infrastructure | Done | Config validation, logging, error handling, Pydantic schema |
| M2 | Climate Data Integration | Done | NSRDB API client, caching, weather formatting, pipeline entry point |
| M3 | PySAM Model Execution | Done | CEC Performance Model, string sizing, batch simulation |
| M4 | Output Processing | Done | 8760 timeseries CSV, summary metrics, shading haircut |
| M5 | Climate & Reporting | Done | Open-Meteo ERA5 snow/soiling, NSRDB v4.0.0 migration, PDF reports with waterfall charts |
| M6 | Database Integration | Done | PostgreSQL/TimescaleDB schema, SQLAlchemy ORM, Alembic migrations, run tracking |
| M7 | Solcast TMY Ingest | Done | Solcast file parser, auto-detection, pipeline branching by data source |
| M8 | Subhourly Correction v1 | Done | GB model trained on 5-min NSRDB satellite data (45 sites, 3,240 paired simulations) |
| M9 | NSRDB Bias Correction | Done | GHI/DNI bias correction trained on 20 ground stations (SURFRAD/SOLRAD/MIDC, 2018-2023) |
| M11 | Subhourly Correction v2 | Done | Retrained on 1-min ground station data (19 stations, 760 paired simulations). Global R²=0.874 |

### Roadmap

| # | Name | Description |
|---|------|-------------|
| M10 | Ground Truth Bias Correction | Site-specific bias correction using customer-provided ground truth irradiance data |
| M12 | Batch Processing & Reporting | Multi-site batch runs with consolidated reporting |
| M13 | BESS Integration | Battery storage dispatch and optimization modeling |
| M14+ | API & Dashboard | REST API for programmatic access, web dashboard for results visualization |
