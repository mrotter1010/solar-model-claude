# Development History

Milestone-based development log for solar-model-claude. Moved from README.md during M23.

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
| M12 | Benchmarking | Done | Parity validation at reference sites vs SolarGIS/SolarAnywhere/PVWatts/SAM + model-vs-measured ground truth |
| — | Buildable Land Assessment | Done | NLCD land cover + 3DEP slope analysis, KMZ polygon support, exclusion engine, setback buffers |
| — | Rate Engine & Bill Savings | Done | TOU rate schedules (JSON + OpenEI API), DOE typical load profiles, energy + demand + flat demand charges, monthly savings analysis, PDF report page |
| M14a | BESS Dispatch | Done | LP-based battery dispatch (PuLP/CBC), 3 strategies, SOC carryover, bill comparison, 12×24 heatmap, PDF report page, DB persistence |
| M14b | BESS Enhancements | Done | NEM export credits (flat/match_import/detailed, monthly banking, annual true-up), ITC solar-only charging constraint, grid-only standalone BESS, LP export revenue optimization, dynamic PDF page counting |
| M14c | BESS Sizing Optimization | Done | Brute force sweep over (power, duration) combos, NPV ranking, project economics |
| M14e | FTM/Wholesale Dispatch | Done | LMP-based dispatch via gridstatus (PJM/ERCOT/CAISO), energy arbitrage LP, FTM sizing optimization, revenue-based NPV |
| M15 | API Phase 1 | Done | FastAPI REST API overlay, 10 endpoints, equipment search, PDF/CSV/JSON retrieval |
| M16 | API Phase 2 | Done | Pipeline refactor (run_from_configs), rate builder API, inline rates, file upload, auth, Docker |
| M17 | LLM Orchestrator | Done | GPT-5 natural language interface, plan-then-execute workflow, 4 endpoints, 67 tests |
| M18 | React Frontend | Done | Vite + React + Tailwind chat UI, plan approval, results display, file upload, CORS |
| M19 | Backend Bug Fixes | Done | GPT-5 exact number quoting, equipment search caching, timeseries negative value clamp, CEC manufacturer name correction, filename sanitization |
| M20 | Backlog Cleanup | Done | DB schema migration (35+5 missing columns, 2 type fixes), standalone GET /lmp/prices endpoint (PJM/ERCOT/CAISO with zone auto-detect), SSE streaming (executor async generator, POST /chat/approve/stream, frontend ExecutionProgress), equipment search upgrade (tokenized AND-matching, numeric STC/Paco filters), orchestrator planning/execution mode separation |
| M21 | Reliability & UX | Done | Orchestrator reliability, buildability fixes, BESS timeseries, frontend UX |
| M22 | Frontend Redesign | Done | Dark theme, layout overhaul, UX polish |
| M22a | Orchestrator UX | Done | Orchestrator UX polish, smart file handling |
| M23 | Beta Deployment | Done | Environment config, invite code auth, access gate, Nginx reverse proxy, Docker production config, deploy script |
| M24 | Chat Persistence | Done | Anonymous user identity, conversation + message DB persistence (TimescaleDB), conversation CRUD endpoints, sidebar UI, LLM title generation, session_id→conversation_id rename. Bug fixes: tool-chain message filtering, LMP result summarization, synthesis instruction persistence. 1,983 tests. |
| M25 | Solar Layout Optimization | Done | GCR × DC/AC ratio sweep engine, three optimization modes (production/LCOE/NPV), solar+BESS joint optimization, capacity-from-acreage sizing, economics engine, PDF report with heatmap + comparison charts, KML centroid auto-extraction, orchestrator tool with auto-chaining. 2,200 tests. |
| M26 | Batch Processing | Done | CSV/Excel batch upload (up to 25 sites), 4 analysis types (buildability/production/optimization/optimization_bess), 39-column validation engine, fail-forward execution, formatted Excel workbook output with per-analysis-type tabs, orchestrator integration, frontend auto-detection. 2,398 tests. |

## Roadmap

| # | Name | Description |
|---|------|-------------|
| M10 | Solcast Bias Correction | Bias correction for Solcast TMY data, analogous to M9 for NSRDB. Blocked on Solcast account access. |
| M13 | Multiyear P50/P75/P90 | Monte Carlo exceedance probabilities with interannual variability and epistemic uncertainty factors |
| M14d | Detailed Degradation | Rainflow counting, calendar aging, C-rate effects |

## M26: Batch Processing

**Commit:** `bacb7b1`

### Core Features

- **CSV/Excel batch upload** — Upload a CSV or Excel file with up to 25 sites per batch. Supports 4 analysis types: `buildability`, `production`, `optimization`, and `optimization_bess` (FTM only; BTM deferred).
- **39-column validation engine** — Schema validation with CEC equipment lookup (module/inverter name matching against ~20k modules and ~2k inverters), field type/range checks, and 25-row limit enforcement. All rows validated upfront before execution begins.
- **Sequential execution with fail-forward error handling** — Rows execute sequentially. If a row fails, the error is captured and execution continues to the next row. Partial results are always returned.
- **Formatted Excel workbook output** — Results written to an openpyxl workbook with per-analysis-type tabs (one sheet per analysis type), Input Echo tab, and Errors tab. Frozen headers, auto-filter, and number formatting for immediate comparison.

### API & Orchestrator

- **REST endpoints** — `POST /analyses/batch/run` (JSON `file_path` pattern matching existing KMZ upload flow), `GET` template download, `GET` results download.
- **Orchestrator tool** — `run_batch` tool with batch routing rule. File content preview enables informed plan approval by GPT-5.
- **Upload auto-detection** — CSV files with `analysis_type` + `lat` + `lon` columns are automatically classified as `batch_input` with text extraction for GPT-5 preview.

### Frontend

- **Batch upload flow** — Upload auto-detection for batch CSV files. Template download link in header. Batch results card with structured validation error display.

### Bug Fixes (Integration Testing)

- **Optimization albedo** — Missing weather fetch in optimization path caused albedo to default to zero. Added weather data retrieval before optimization runs.
- **Capacity factor double-conversion** — PySAM returns capacity factor as a percentage (e.g., 25.4); the pipeline was dividing by 100 again, producing values like 0.254%. Fixed to use PySAM's value directly where appropriate.
- **NaN-to-string defaults** — Pandas NaN values in optional columns caused type errors during Excel formatting. Added string defaults for missing values.

### Stats

- **Tests:** 2,398 (189 new batch tests)
- **Diff:** 6,960 insertions, 31 files changed

## M25: Solar Layout Optimization

**Commits:** `c2c786c` (core), `395d039` (M25 final), `9d2984a` (nginx/auth fix), `25a6a5f` (prompt cleanup)

### Core Features

- **Capacity-from-acreage sizing** — Converts buildable acres → MW_DC based on GCR, module dimensions, utilization factor (default 75%), and land intensity lookup. Automatically sizes the system for each GCR in the sweep grid.
- **Three-mode solar optimizer** — Sweeps 7 GCR × 10 DC/AC ratio = 70 combinations per run. Returns multiple winner configurations:
  - **Production mode** (default): max annual energy (MWh) and max specific yield (kWh/kWp)
  - **LCOE mode**: min levelized cost of energy ($/MWh) with ITC, degradation, and O&M
  - **NPV mode**: max net present value with energy revenue, escalation, and tax credits
- **Solar+BESS joint optimization** — Extends the 70-point solar sweep with 10 BESS power/duration combinations per valid design = 700 total configurations. Supports BTM (rate-based bill savings) and FTM (LMP-based wholesale revenue) dispatch modes.
- **Economics engine** — LCOE, NPV, IRR, and simple payback calculations with ITC (default 30%), annual degradation, O&M escalation, and discount rate. Shared across solar-only and solar+BESS modes.
- **Shared subhourly clipping correction** — Extracted the ML clipping correction from `pipeline.py` into `src/optimization/clipping_correction.py` for reuse by the optimizer without duplicating pipeline logic.

### API & Orchestrator

- **REST endpoint** — `POST /analyses/optimize` with full parameter schema (GCR/DCAC ranges, economics, BESS config, report_winner selection).
- **Orchestrator tool** — `run_optimization` with auto-chaining: KML upload → buildability → optimization in a single plan. `buildability_run_id` auto-populates latitude, longitude, and buildable_acres.
- **PDF report** — Optimization summary table + LCOE/NPV heatmap + winner configuration comparison chart. `report_winner` parameter selects which winner gets the detailed report (auto/max_production/max_yield/lcoe/npv).

### Infrastructure & UX

- **KML centroid auto-extraction** — `BuildabilityLocation` makes lat/lon optional. When a KML/KMZ is provided without coordinates, the buildability analyzer computes the polygon centroid automatically.
- **Market-neutral system prompt** — Removed community solar and C&I-specific framing from the orchestrator system prompt. The platform now presents itself as a general solar feasibility tool.
- **Nginx proxy timeouts** — Increased `proxy_read_timeout` and `proxy_send_timeout` to 900s (15 min) on all proxy locations to handle long-running optimization sweeps.
- **Nginx env_file fix** — Added `env_file: .env.production` to the nginx service in `docker-compose.prod.yml` to prevent `VITE_ANALYSIS_API_KEY` from being baked as empty on standalone rebuilds.
- **Download UX dedup** — Removed "Available Downloads" section from LLM result formatting rules; the frontend `ResultsCard` already shows download buttons.

### Default Configuration

- **Equipment**: LONGi LR5-72HBD-550M (551W bifacial) + Sungrow SG250HX-US [800V]
- **Economics**: $1,200/kW DC, $20/kW-yr O&M, 7% discount rate, 25-year life, 30% ITC, $340/kWh BESS

### Key Technical Decisions

- **Brute-force sweep over surrogate model** — The 70-point PySAM sweep takes ~37s total. This is fast enough that a surrogate model or Bayesian optimization would add complexity without meaningful speed improvement. Each PySAM run is ~0.5s.
- **Multiple winners rather than single optimum** — The optimizer returns `max_production`, `max_yield`, `lcoe`, and `npv` winners separately. Different stakeholders care about different metrics, and the "best" design depends on project economics that may change.
- **Capacity sizing from acreage** — Rather than requiring the user to specify MW_DC, the optimizer calculates it from buildable acres and GCR. This enables the full workflow: upload KML → get buildable acres → sweep designs → pick winner.
- **Clipping correction extraction** — The subhourly ML correction was tightly coupled to `pipeline.py`. Extracting it into a standalone helper with the same interface allows the optimizer to apply the correction without instantiating the full pipeline.

## M24: Chat Persistence, Sidebar, Conversation Management

**Commits:** `e98b21e` (core), `e962e20` (m24a), `7338a53` (m24b), `63fc1ce` (m24c), `97e5b8e` (m24d), `2a4fe3c` (m24e)

### Core Features

- **Anonymous user identity** — UUID cookie generated client-side, sent as `X-User-Id` header. `UserIdentityMiddleware` validates format and attaches to request state. No login required.
- **Conversation persistence** — `conversations` + `messages` tables in TimescaleDB via Alembic migration. Async DB access layer (`orchestrator/database.py`) using SQLAlchemy async + asyncpg.
- **Conversation CRUD** — List, get (with messages), delete, and rename endpoints on orchestrator. Three-dot menu in sidebar for rename/delete with inline confirmation.
- **Message persistence** — All `/chat` and `/chat/approve` flows persist messages to DB with metadata (`responseType`, `steps`, `fileAttachment`). Sequence numbers for ordering.
- **API contract rename** — `session_id` replaced with `conversation_id` across all request/response models, frontend API calls, and orchestrator routes.
- **Collapsible sidebar** — Conversation list with switching, "New Chat" button, auto-generated titles via gpt-4o-mini background task (`asyncio.create_task`).
- **Session continuity** — When resuming a conversation with an expired in-memory session, messages are hydrated from DB before the next planner call.

### Bug Fixes (M24a–M24e)

- **M24a** (`e962e20`): Filter `role: "tool"` and `assistant(tool_calls)` messages from planning calls — sending tool-chain messages without `tools=` causes OpenAI validation errors. Reordered executor to call `add_message` before `yield` to prevent orphaned tool_calls on client disconnect.
- **M24b** (`7338a53`): Extended tool-chain message filter to `generate_execution_calls()`. Defense-in-depth: stale tool/tool_call messages from previous executions waste tokens and can confuse GPT-5.
- **M24c** (`63fc1ce`): Added `_summarize_tool_result()` to strip `prices` (8760 floats) and `timestamps` (8760 strings) from `get_lmp_prices` results before session storage. Reduced tool message from ~55K tokens to ~140 tokens (99.7% reduction). Without this, two LMP calls exceeded GPT-5's context window.
- **M24d** (`97e5b8e`): Fixed empty synthesis after LMP execution. Root cause: the execution instruction ("provide a synthesis of results") was only in `generate_execution_calls()` — `continue_execution()` did not include it. Also removed contradictory "Only emit tool_calls" wording. Now both methods include clear synthesis instructions.
- **M24e** (`2a4fe3c`): Fixed 23 failing orchestrator route tests. Added mock `conversation_db` to test fixtures, `X-User-Id` header to test HTTP clients, renamed `session_id` → `conversation_id` in all request payloads and response assertions.

### Key Technical Decisions

- **Async DB layer** — Used SQLAlchemy async engine + asyncpg rather than sync DB calls, since the orchestrator is already fully async (FastAPI + OpenAI async client). Raw SQL via `text()` rather than ORM models to keep the layer thin.
- **Sequence numbers** — Messages use explicit sequence numbers rather than relying on `created_at` ordering. Avoids timestamp precision issues and makes message ordering deterministic.
- **Summarize at session storage, not at API** — LMP tool results are summarized in the executor before storing in session messages, not at the API endpoint. The full data remains available via the API for download endpoints and frontend charts.
- **Synthesis instruction on every loop iteration** — Rather than storing the execution instruction in session messages, `continue_execution()` appends a fresh reminder. This keeps the instruction visible to GPT-5 without polluting the conversation history.

### Lessons Learned

- **Tool-chain messages are invisible landmines.** OpenAI's API rejects `role: "tool"` messages when `tools=` is not provided, but the error only surfaces when the planner is called after an execution. The filter must be applied in every code path that sends messages without tools.
- **LLM context windows need active management.** A single 8760-element array in a tool result consumes ~55K tokens. Two LMP calls exceeded GPT-5's context window. Tool results stored in session messages compound on every subsequent turn — they must be compact.
- **Execution instructions vanish in multi-turn tool loops.** The executor's `continue_execution()` only sends session messages + system prompt. Any instruction from the initial `generate_execution_calls()` call is lost after the first iteration. GPT-5 needs the synthesis instruction visible on every turn.

## Pipeline Overview

```
React Frontend (:5173)                 LLM Orchestrator (:8001)
  → Chat UI, plan approval, SSE streaming → GPT-5 plan-then-execute
  → File upload (drag-and-drop)           → Calls analysis API endpoints
  │                                       │
  └──────────┬────────────────────────────┘
             ▼
CSV Input                              JSON API Request (:8000)
  → Config validation (Pydantic)         → FastAPI (auth, validation)
  → pipeline.run()                       → adapter → SiteConfig
  │                                      │
  └──────────┬───────────────────────────┘
             ▼
    pipeline.run_from_configs([SiteConfig, ...])
      → Climate data fetch (NSRDB v4.0.0 or Solcast TMY) + ERA5 snow/precip
      → NSRDB bias correction (ML, v1)
      → PySAM detailed PV simulation (CEC Performance Model)
      → Subhourly clipping correction (ML, v2)
      → Shading haircut
      → Bill calculation (TOU energy + demand charges, NEM export credits, savings analysis)
      → BESS dispatch optimization (LP-based, PuLP/CBC; NEM/ITC/grid-only modes, if enabled)
      → FTM wholesale dispatch (LMP-based revenue optimization, if dispatch_mode=ftm)
      → Buildable land assessment (NLCD land cover + 3DEP slope, if enabled)
      → Output: 8760 timeseries CSV + PDF report + database write
```

## Input CSV Format

The CSV accepts up to 70 columns. Column mapping is defined in `src/config/loader.py`.

| # | Column | Type | Required | Description |
|---|--------|------|----------|-------------|
| 1 | Run Name | string | yes | Unique identifier for this simulation run |
| 2 | Site Name | string | yes | Site identifier (rows sharing a name must have identical coordinates) |
| 3 | Customer | string | yes | Customer/project name |
| 4 | Latitude | float | yes | Site latitude (-90 to 90) |
| 5 | Longitude | float | yes | Site longitude (-180 to 180) |
| 6 | BESS Dispatch Required | bool | no | `TRUE` to run BESS dispatch optimization |
| 7 | BESS Optimization Required | float | no | Reserved for M14b sizing optimization |
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
| 32 | Buildable Land Assessment | bool | no | `TRUE` to run NLCD/slope buildability analysis |
| 33 | KMZ File Path | path | no | Path to KMZ polygon file for site boundary |
| 34 | Analysis Radius (km) | float | no | Circular analysis radius if no KMZ provided (default 1.5 km) |
| 35 | Bill Calculation | bool | no | `TRUE` to run bill savings analysis |
| 36 | Rate File Path | path | no | Path to rate schedule JSON file |
| 37 | Utility Name | string | no | OpenEI utility name (alternative to rate file) |
| 38 | Tariff Name | string | no | OpenEI tariff name (used with Utility Name) |
| 39 | Load Profile Path | path | no | Path to custom hourly load profile CSV |
| 40 | Load Type | string | no | DOE building type for typical load profile (e.g., `MediumOffice`) |
| 41 | Annual Consumption (kWh) | float | no | Annual load to scale typical profile |
| 42 | Peak Demand (kW) | float | no | Peak demand for load profile scaling |
| 43 | BESS Power (MW) | float | no | Battery inverter power rating |
| 44 | BESS Duration (hr) | float | no | Battery storage duration in hours |
| 45 | BESS RTE (%) | float | no | Round-trip efficiency (default 88%) |
| 46 | BESS Min SOC (%) | float | no | Minimum state of charge (default 10%) |
| 47 | BESS Max SOC (%) | float | no | Maximum state of charge (default 90%) |
| 48 | BESS Strategy | string | no | Dispatch strategy: `global`, `peak_shaving`, `tou_arbitrage` (default `global`) |
| 49 | BESS Installed Cost ($/kWh) | float | no | Installed cost per kWh for degradation penalty (default $275) |
| 50 | BESS Cycles Warranty | int | no | Warranted cycle count for degradation cost (default 5000) |
| 51 | BESS Solar Only Charging | bool | no | `TRUE` to restrict battery charging to excess solar only (ITC compliance) |
| 52 | BESS Grid Only Charging | bool | no | `TRUE` for standalone BESS (grid charging only, no solar in dispatch) |
| 53 | Discount Rate (%) | float | no | Discount rate for NPV calculations (default 7%) |
| 54 | Project Lifetime (years) | int | no | Project lifetime for NPV calculations (default 25) |
| 55 | Rate Escalation (%) | float | no | Annual revenue/rate escalation (default 2%) |
| 56 | Solar Cost ($/kW DC) | float | no | Solar installed cost per kW DC capacity |
| 57 | Solar Cost ($/kW AC) | float | no | Solar installed cost per kW AC capacity |
| 58 | Solar O&M ($/kW-DC/yr) | float | no | Annual solar O&M cost per kW DC |
| 59 | BESS O&M ($/kW/yr) | float | no | Annual BESS O&M cost per kW power |
| 60 | BESS Power Min (MW) | float | no | Minimum BESS power for sizing sweep |
| 61 | BESS Power Max (MW) | float | no | Maximum BESS power for sizing sweep |
| 62 | BESS Duration Min (hr) | float | no | Minimum BESS duration for sizing sweep (default 2) |
| 63 | BESS Duration Max (hr) | float | no | Maximum BESS duration for sizing sweep (default 5) |
| 64 | Dispatch Mode | string | no | `btm` (behind-the-meter, default) or `ftm` (front-of-meter wholesale) |
| 65 | ISO | string | no | ISO/RTO market: `pjm`, `ercot`, `caiso` (required for FTM) |
| 66 | LMP Zone | string | no | Pricing zone override (e.g., `AEP`, `LZ_HOUSTON`, `NP15`). Auto-detected if omitted. |
| 67 | LMP Node IDs | string | no | Specific pricing node IDs (reserved for future use) |
| 68 | LMP Market | string | no | Market type: `DAY_AHEAD_HOURLY` (default), `REAL_TIME_5_MIN` |
| 69 | LMP Year | int | no | Calendar year for LMP data (default: previous year) |
| 70 | Ancillary Revenue ($/kW/yr) | float | no | Flat ancillary services revenue assumption per kW per year |

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

### LMP Data (FTM dispatch)
- **Source:** ISO market data via [gridstatus](https://github.com/gridstatus/gridstatus)
- **ISOs supported:** PJM (day-ahead hourly), ERCOT (day-ahead SPP), CAISO (day-ahead hourly)
- **Resolution:** Hourly, full calendar year
- **Caching:** Files cached in `data/lmp/cache/` as `{iso}_{zone}_{market}_{year}.csv`
- **Zone auto-detection:** PJM via EIA service territory shapefile (when available), ERCOT/CAISO via geographic rules. User can override with explicit zone in CSV.
- **PJM API key:** Set via `PJM_API_KEY` env var. Default key included for convenience.

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

**Artifacts:** `src/models/artifacts/subhourly_correction_v2.joblib`, `subhourly_correction_v2_metadata.json`

## API Endpoints

### Analysis API (port 8000)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/analyses/production` | Run production-only simulation |
| POST | `/analyses/bill-savings` | Run production + bill savings analysis |
| POST | `/analyses/bess` | Run BESS dispatch or sizing optimization |
| POST | `/analyses/buildability` | Run buildable land assessment |
| POST | `/analyses/optimize` | Run solar layout optimization (GCR × DC/AC sweep) |
| GET | `/analyses/load-types` | List available DOE reference building types |
| GET | `/analyses/{run_id}/results` | Retrieve saved results JSON |
| GET | `/analyses/{run_id}/report` | Download PDF report |
| GET | `/analyses/{run_id}/timeseries` | Download 8760 timeseries CSV |
| GET | `/analyses/equipment/modules` | Search CEC module database |
| GET | `/analyses/equipment/inverters` | Search CEC inverter database |
| GET | `/lmp/prices` | Standalone LMP price query |
| POST | `/rates/build` | Build and validate a rate schedule |
| POST | `/uploads/{file_type}` | Upload a file (rate, kmz, load-profile) |
| GET | `/health` | Health check |

### Orchestrator (port 8001)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/chat` | Send a message (creates/resumes conversation) |
| POST | `/chat/approve` | Approve and execute a pending plan |
| POST | `/chat/approve/stream` | Approve and execute with SSE streaming |
| GET | `/sessions/{conversation_id}` | Get in-memory session info |
| GET | `/conversations` | List conversations for user |
| GET | `/conversations/{conversation_id}` | Get conversation with messages |
| DELETE | `/conversations/{conversation_id}` | Delete a conversation |
| POST | `/conversations/{conversation_id}/title` | Update conversation title |
| GET | `/health` | Health check |

## Project Structure

```
src/
├── main.py                          # CLI entry point
├── pipeline.py                      # SolarModelingPipeline orchestrator
├── api/                             # FastAPI REST API
├── config/                          # CSV parsing, Pydantic validation
├── climate/                         # NSRDB, Solcast, ERA5 clients
├── pysam_integration/               # PySAM model configuration and execution
├── models/                          # ML correction models + artifacts
├── outputs/                         # 8760 CSV generation
├── reporting/                       # PDF report generation
├── rates/                           # Rate engine, bill calculation, NEM
├── lmp/                             # LMP data fetching and caching
├── bess/                            # BESS dispatch and sizing optimization
├── buildability/                    # Land cover and slope analysis
├── optimization/                   # Layout optimization (GCR × DC/AC sweep, economics)
├── database/                        # SQLAlchemy ORM, migrations
└── utils/                           # Logging, exceptions

orchestrator/                        # GPT-5 LLM orchestrator
frontend/                            # React + Vite + Tailwind chat UI
```

## Database Schema

PostgreSQL/TimescaleDB with 8 tables managed via Alembic migrations:

| Table | Purpose |
|-------|---------|
| `customers` | Customer registry |
| `sites` | Site locations |
| `runs` | Simulation run tracking |
| `run_inputs` | Full input parameters |
| `run_results` | Results, metrics, file paths |
| `bill_calculation_runs` | Rate schedule metadata |
| `conversations` | Chat conversations (anonymous_user_id, title, timestamps) |
| `messages` | Conversation messages (role, content, metadata, sequence) |
