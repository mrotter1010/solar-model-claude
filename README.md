# Vantyra Analytics

AI-powered utility-scale solar feasibility platform. Describe your project in plain English and get bankable-quality production modeling, buildability screening, rate/bill analysis, and battery storage optimization.

Live at [https://app.vantyraanalytics.com](https://app.vantyraanalytics.com)

## Architecture

Four Docker services behind Cloudflare SSL:

```
                    Cloudflare (DNS + SSL)
                           │
                      Nginx (:80)
                    ┌──────┴──────┐
              /api/ │             │ /orchestrator/
                    ▼             ▼
           FastAPI API      FastAPI Orchestrator
            (:8000)              (:8001)
          PySAM engine       GPT-5 function calling
          Climate data       Plan-then-execute
          Analysis pipeline  Chat persistence
                    │             │
                    └──────┬──────┘
                           ▼
                     TimescaleDB
                    (PostgreSQL 16)
```

- **Nginx** — Reverse proxy + static frontend (React build)
- **API** — PySAM simulation, NSRDB/Solcast climate data, rate engine, BESS dispatch, buildability analysis
- **Orchestrator** — GPT-5 natural language interface with function calling against the API. Conversation persistence with anonymous user identity, DB message hydration on resume.
- **TimescaleDB** — Run tracking, results storage, conversation persistence, audit trail
- **Frontend** — React + Vite + Tailwind. Chat UI with collapsible sidebar, conversation history, plan approval, SSE streaming.

## Capabilities

**Production Modeling** — NREL PySAM Pvsamv1 detailed photovoltaic simulation. NSRDB TMY + CONUS bias correction, Solcast TMY support. Fixed-tilt and single-axis tracker. CEC module/inverter database (~20k modules, ~2k inverters). Subhourly clipping correction, bifacial, snow/soiling losses. PDF report + 8760 CSV export.

**Buildability Screening** — NLCD 2021 land cover classification, USGS 3DEP slope analysis, setback buffers. KMZ/KML polygon support. Buildable acreage estimates by land cover type.

**Rate & Bill Savings** — OpenEI/URDB rate lookup or custom JSON schedules. TOU energy + demand charges, NEM export credits (flat/match_import/detailed with monthly banking). DOE reference load profiles or custom CSV. Monthly savings analysis.

**BESS Dispatch & Optimization** — LP-based battery dispatch (PuLP/CBC). Behind-the-meter: peak shaving, TOU arbitrage, global optimization with NEM-aware export revenue. Front-of-meter: wholesale LMP-based dispatch via gridstatus (PJM, ERCOT, CAISO). NPV sizing optimization across power/duration combinations.

## Local Development

### Prerequisites

- Docker and Docker Compose
- Node.js 20+
- Python 3.12 (Conda recommended)

### Quick Start

```bash
# Start backend services
cp .env.example .env  # Set OPENAI_API_KEY, NSRDB_API_KEY, NSRDB_API_EMAIL
docker compose -f docker-compose.yml -f docker-compose.dev.yml up

# Start frontend (separate terminal)
cd frontend && npm install && npm run dev
```

- Frontend: [http://localhost:5173](http://localhost:5173)
- API: [http://localhost:8000](http://localhost:8000)
- Orchestrator: [http://localhost:8001](http://localhost:8001)

## Production Deployment

Hosted on Hetzner VPS with Cloudflare DNS + SSL termination.

```bash
# Deploy latest main to production
./scripts/deploy.sh
```

Production secrets live in `.env.production` on the VPS (never committed). See `.env.production.example` for the template.

## Tests

```bash
pytest tests/
```

1,983 tests covering config validation, climate clients, PySAM simulation, ML corrections, rate engine, BESS dispatch, buildability analysis, REST API endpoints, LLM orchestrator, and end-to-end integration.

## Tech Stack

| Layer | Technology |
|-------|------------|
| Simulation | NREL PySAM, CEC Performance Model |
| Climate | NSRDB v4.0.0, Solcast TMY, Open-Meteo ERA5 |
| ML | scikit-learn (Gradient Boosting, Random Forest) |
| Backend | FastAPI, SQLAlchemy, Alembic, PuLP/CBC |
| LLM | OpenAI GPT-5 (function calling) |
| Frontend | React 19, Vite, Tailwind CSS |
| Database | PostgreSQL 16 + TimescaleDB |
| Infrastructure | Docker Compose, Nginx, Cloudflare |
| Market Data | gridstatus (PJM, ERCOT, CAISO) |
