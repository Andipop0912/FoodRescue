# FoodRescue

A full-stack data-engineering platform that matches surplus food from restaurants, cafes, supermarkets and bakeries with NGOs, food banks and community kitchens in Navi Mumbai.

Donors list surplus food. The system cleans the data, finds the best nearby NGOs using a weighted scoring model, and tracks the donation through to delivery. Every stage of the pipeline is observable in real time.

---

## Architecture

```
CSV / API ──► MongoDB (raw)
                 │
                 ▼
           Polars cleaning ──► rejected rows (reason stored)
                 │
                 ▼
           PostgreSQL ──────────────────────────────────────────────────────────────────────┐
                 │                                                                          │
                 ▼                                                                          ▼
           RabbitMQ ──► matching worker ──► top-5 NGO matches            Delta Lake (versioned Parquet)
                                                                                           │
Apache NiFi (watches data/inbox/, POSTs CSVs)                                              ▼
                                                                            Elasticsearch (full-text search)
Apache Airflow (daily DAG: health → validate → analytics → report + DuckDB)
                                                                                           │
FastAPI (REST + WebSocket) ◄──────────────────────────────────────────────────────────────┘
     │
     ▼
React + Vite (SPA — donor / NGO / admin views, live map, WebSocket dashboard)
```

**Services:** PostgreSQL · MongoDB · RabbitMQ · Apache NiFi · Elasticsearch · Apache Airflow · FastAPI · React/Vite — all managed by Docker Compose profiles.

---

## Features

### Data pipeline
- **Ingestion** — `POST /ingest/batch` accepts JSON, CSV body or file upload; every raw row is saved to MongoDB before cleaning.
- **Polars cleaning** — required-field checks, deduplication, category standardisation, expired/unsafe food rejection with a stored reason.
- **RabbitMQ events** — a `donation.created` event is published after each clean row is committed to PostgreSQL; a dead-letter queue (`donation.dead`) catches failures.
- **Matching worker** — 15 km haversine radius, expiry and food-type filters, composite score `= 0.4 × distance + 0.35 × urgency + 0.25 × quantity fit`; top-5 matches saved per donation.
- **Delta Lake** — append-only versioned Parquet history written after each ingestion and status transition; supports time-travel queries.
- **Elasticsearch** — donations indexed with stable IDs for upsert-safe re-indexing; BM25 full-text search with category, area, status and `min_kg` filters.
- **DuckDB analytics** — in-memory OLAP engine loads PostgreSQL donation data and runs aggregation queries; exposed via admin API endpoints.
- **Apache NiFi** — watches `data/inbox/` and POSTs each CSV file to `/ingest/batch`; failed files land in `data/nifi-failed/`.
- **Apache Airflow** — daily DAG (`foodrescue_analytics_pipeline`) with five tasks: pipeline health check → data validation → SQL analytics → formatted report → DuckDB analytics (parallel to SQL).

### Application
- **JWT auth** — bcrypt passwords, role-based access (`donor`, `ngo`, `admin`); all API routes and frontend pages are protected.
- **Donation lifecycle** — `listed → matched → accepted → picked_up → delivered`.
- **Donor view** — create a donation, track status, see the top-5 matched NGOs with score breakdowns.
- **NGO view** — ranked match list with scoring detail, one-click accept, pickup and delivery tracking, post a food need.
- **Admin dashboard** — live demo controls (start/stop, 1×/2×/5×), real-time counters (batches · accepted · rejected · matches · kg rescued · meals), event log, rejected-rows table, Leaflet map of donations.
- **WebSocket** — live dashboard updates pushed from the server; polling fallback for environments that block WS.
- **Live simulator** — generates realistic Navi Mumbai donation batches (~10% dirty data), writes CSVs to `data/inbox/` and sends them to the API.

### Quality
- **163 pytest tests** (2 skipped) covering auth, cleaning, matching, ingest→match flow, simulator, JWT secret validation and analytics endpoints.

---

## Tech stack

| Layer | Technologies |
|---|---|
| API | FastAPI · SQLAlchemy · Alembic · Pydantic |
| Databases | PostgreSQL 16 · MongoDB 7 |
| Messaging | RabbitMQ 3.13 |
| Data processing | Polars · DuckDB · Delta Lake (`deltalake`) |
| Search | Elasticsearch 8.14 |
| Orchestration | Apache Airflow 2.9.3 (SequentialExecutor) |
| File ingestion | Apache NiFi 1.28.1 |
| Frontend | React 18 · Vite · Leaflet · CSS custom properties |
| Infrastructure | Docker Compose (profiles) |

---

## Quick start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with WSL2 (Windows) or Docker Engine (Linux/macOS)
- Git
- Free ports: **5173, 8000, 5433, 27017, 5672, 15672**

### 1. Clone and configure

```powershell
git clone https://github.com/Andipop0912/FoodRescue.git
cd FoodRescue
Copy-Item .env.example .env
```

### 2. Start the core stack

```powershell
docker compose up -d --build
```

This starts PostgreSQL, MongoDB, RabbitMQ, the FastAPI backend, the matching worker, and the React frontend. Database tables and demo accounts are created automatically on first boot.

### 3. Open the app

| Service | URL | Credentials |
|---|---|---|
| Frontend | http://localhost:5173 | see demo logins below |
| API docs (Swagger) | http://localhost:8000/docs | — |
| RabbitMQ management | http://localhost:15672 | `guest` / `guest` |
| PostgreSQL | `localhost:5433` | `foodrescue` / `foodrescue` |
| MongoDB | `localhost:27017` | — |

**Demo logins** (password `demo123`):

| Role | Email |
|---|---|
| Donor | `donor@foodrescue.demo` |
| NGO | `ngo@foodrescue.demo` |
| Admin | `admin@foodrescue.demo` |

---

## Optional services

### NiFi (file-based ingestion)

```powershell
docker compose --profile pipeline up -d nifi
```

UI at http://localhost:8080 (user `admin`, password from `.env`). NiFi watches `data/inbox/` and POSTs each CSV to the API. Set `SIM_DIRECT_INGEST=false` in `.env` to route the simulator through NiFi instead of the API directly.

### Elasticsearch (full-text search)

```powershell
docker compose --profile search up -d elasticsearch
```

API at http://localhost:9200. Once running, call `POST /analytics/es/index` (admin token required) to index all donations, then `GET /analytics/es/search?q=rice&area=Vashi` to search.

### Airflow (analytics pipeline)

```powershell
docker compose --profile airflow up -d airflow
```

UI at http://localhost:8081 (user `admin`, password `foodrescue123`). Trigger the `foodrescue_analytics_pipeline` DAG manually from the UI or wait for the daily schedule.

### Start everything at once

```powershell
docker compose --profile all up -d --build
```

---

## Running the tests

```powershell
docker compose exec backend pytest -v
```

Or from the host (requires the backend image to be built):

```powershell
./scripts/test.ps1
```

Expected result: **163 passed, 2 skipped**.

---

## Demo walkthrough

1. **Donor** — sign in, create a donation, watch it transition to *Matched* and expand the 5 ranked NGO suggestions.
2. **NGO** — sign in, accept the donation, mark it picked up, then delivered.
3. **Admin** — sign in, press **▶ Start Live Demo** (choose a preset or leave defaults). Watch the counters, event log and map update in real time. Open `data/inbox/` to see CSV files arriving from the simulator.
4. **RabbitMQ** — open http://localhost:15672, inspect the `donation.created` queue and the dead-letter exchange.
5. **Airflow** — open http://localhost:8081, trigger `foodrescue_analytics_pipeline`, and watch all 5 tasks turn green.
6. **Elasticsearch** — index donations via `POST /analytics/es/index`, then search with `GET /analytics/es/search?category=cooked_meals`.
7. **DuckDB** — call `GET /analytics/duckdb` (admin token) to see in-memory aggregation results returned in milliseconds.

---

## Project layout

```
backend/
  app/
    routers/        auth, donations, ngo, ingest, demo, analytics
    services/       cleaning, matching, ingest, mongo, delta, search, duckdb_analytics
    worker/         RabbitMQ consumer / matching worker
    models/         SQLAlchemy ORM models
  alembic/          database migrations
  tests/            163 pytest tests
airflow/
  dags/             foodrescue_analytics_pipeline.py
frontend/
  src/
    components/     Layout, MapView, icons, bits
    pages/          Login, Donor, Ngo, Admin
simulator/          donation / NGO data generator and live-demo runner
data/
  inbox/            CSV drop folder (NiFi / simulator)
  delta/            Delta Lake Parquet tables (gitignored)
nifi/               NiFi flow configuration
scripts/            test runner (test.ps1)
docker-compose.yml  profiles: core · search · pipeline · airflow · all
```

---

## Donation matching score

Each NGO candidate is scored against a donation using three factors:

| Factor | Weight | Signal |
|---|---|---|
| Distance | 40% | Haversine distance (0 = 15 km, 1 = same location) |
| Urgency | 35% | Time remaining before expiry |
| Quantity fit | 25% | How well the NGO's stated capacity matches the donation size |

The top 5 NGOs by score are saved as match suggestions. The NGO can accept, triggering the donation status transition to `accepted`.

---

## Environment variables

Key variables in `.env` (copy from `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `JWT_SECRET` | `dev-secret-…` | Must be ≥ 32 bytes and non-placeholder in production |
| `FOODRESCUE_ENV` | `production` | Set to `development` only for local test runs outside Docker |
| `POSTGRES_PASSWORD` | `foodrescue` | PostgreSQL password |
| `RABBITMQ_USER` / `_PASS` | `guest` / `guest` | RabbitMQ credentials |
| `ELASTICSEARCH_URL` | `http://elasticsearch:9200` | Elasticsearch endpoint seen by the backend |
| `SIM_DIRECT_INGEST` | `true` | `false` = simulator writes CSVs only; NiFi must pick them up |
| `NIFI_PASSWORD` | `nifi-local-dev-password-change-me` | NiFi single-user password |

---

## Stopping and resetting

```powershell
docker compose down              # stop containers, keep data volumes
docker compose --profile all down   # stop all profiles
```

> **Note:** `docker compose down -v` deletes all data volumes (PostgreSQL, MongoDB, Delta Lake, Elasticsearch index, Airflow metadata). Only run it if you want a clean slate.
