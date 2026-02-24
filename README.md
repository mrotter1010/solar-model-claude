# Solar Production Model

Python-based solar production modeling tool using NREL's PySAM detailed photovoltaic model for utility-scale solar projects.

## Architecture Overview

```
CSV Input → Climate Data Pull (NSRDB) → PySAM Execution → 8760 Hourly Timeseries Output
```

1. **CSV Input**: Site parameters including location (lat/lon), system design, panel/inverter models, and loss assumptions
2. **Climate Data Pull**: Fetches TMY/historical weather data from NREL's National Solar Radiation Database (NSRDB) API
3. **PySAM Execution**: Runs NREL's System Advisor Model detailed PV simulation
4. **Output**: Per-site 8760 hourly production timeseries

## Setup

### Prerequisites
- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Anaconda](https://www.anaconda.com/)
- Python 3.12.4

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
- Panel and inverter model specifications
- Loss assumptions (soiling, shading, wiring, etc.)

### Output
Per-site 8760 hourly production timeseries (kWh).

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

## Planned Milestones

1. **Core Infrastructure** - Config validation, logging, error handling
2. **Climate Data Integration** - NSRDB API client for weather data retrieval
3. **PySAM Model Execution** - Detailed PV model configuration and simulation
4. **Output Processing** - 8760 timeseries generation, summary metrics
5. **Parallelization & Optimization** - Multi-site concurrent processing
