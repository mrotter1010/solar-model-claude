# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Solar production modeling tool using NREL's PySAM. Pipeline: CSV input → NSRDB climate data → PySAM detailed PV simulation → 8760 hourly timeseries output.

## Commands

```bash
# Environment setup
conda env create -f environment.yml
conda activate solar-model

# Run tests
pytest
pytest tests/test_config.py              # single test file
pytest tests/test_config.py::test_name   # single test
pytest --cov=src --cov-report=term-missing  # with coverage

# Linting and formatting
ruff check src/ tests/
black src/ tests/
mypy src/
```

## Architecture

```
src/
├── config/    # Input CSV parsing, Pydantic validation models
├── climate/   # NSRDB API client for weather data
├── sam/       # PySAM model configuration and execution
└── utils/     # Logging setup, custom exceptions, shared helpers
```

## Coding Standards

- Type hints required on all functions
- `pathlib.Path` for all file operations (never `os.path`)
- No `print()` statements — use the logging framework
- Pydantic for all data validation
- Google-style docstrings on all public functions
- **Simplicity first**: use the simplest structure that solves the problem. Don't add abstractions, classes, or patterns unless they provide clear value. Flat is better than nested.

## Git Workflow

- Feature branches per milestone: `feature/milestone-X-description`
- Conventional commits format (e.g., `feat:`, `fix:`, `test:`, `docs:`)
- No commits directly to main

## Testing

- pytest with **95%+ coverage target**
- Test files mirror src structure: `tests/test_<module>.py`
- Fixtures in `tests/fixtures/`
- Tests that process data must write intermediate outputs to `outputs/test_results/` for manual inspection (e.g., `test_config_valid_sites.json`, `test_climate_phoenix_2023.csv`)
- Self-documenting tests: clear arrange/act/assert with comments explaining what's validated

## Error Handling

- Fail-fast with specific error messages
- Custom exceptions in `src/utils/exceptions.py`
- Always include context (row number, field name, validation failure reason)
- Log errors before raising
