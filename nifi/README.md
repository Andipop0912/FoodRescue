# FoodRescue — Apache NiFi Ingestion Integration

## What this does

Apache NiFi watches `data/inbox/` for CSV files dropped by the demo simulator
(or any external source) and forwards each file to the FastAPI `/ingest/batch`
endpoint.  This replaces the simulator's direct HTTP call when
`SIM_DIRECT_INGEST=false`.

```
data/inbox/*.csv
       │
       ▼  GetFile (every 10 s)
     NiFi
       │  POST multipart/form-data
       ▼
  /ingest/batch  (FastAPI)
       │
  ┌────┴────┐
  │         │
  ▼         ▼
Postgres  MongoDB
(clean)   (raw)
```

## Starting NiFi

NiFi is under the `pipeline` Docker Compose profile and does not start with the
default `core` profile:

```powershell
# Start core + NiFi:
$env:COMPOSE_PROFILES = "pipeline"
docker compose up -d

# Or use the all profile:
$env:COMPOSE_PROFILES = "all"
docker compose up -d
```

NiFi takes 60–120 seconds to start.  Access the UI at http://localhost:8080/nifi/

Default credentials (override in `.env`):
```
NIFI_USER=admin
NIFI_PASSWORD=nifi-local-dev-password-change-me
```

**Never use the default password in a shared or production environment.**

## Deploying the flow

After NiFi is healthy, run the bootstrap script once to create the ingestion
flow programmatically via the NiFi REST API:

```powershell
# Obtain a FoodRescue admin JWT first:
$token = (Invoke-RestMethod -Uri http://localhost:8000/auth/token `
    -Method POST -ContentType "application/x-www-form-urlencoded" `
    -Body "username=admin@foodrescue.demo&password=demo123").access_token

# Deploy the flow:
python nifi/deploy_flow.py `
    --nifi-url http://localhost:8080 `
    --username admin `
    --password nifi-local-dev-password-change-me `
    --backend-url http://localhost:8000 `
    --auth-token $token
```

The script creates a process group named **FoodRescue Inbox Ingestion** in the
NiFi root canvas with four processors and the required connections.

Then start the flow in the NiFi UI, or via the REST API:

```bash
PG_ID=<process-group-id-from-script-output>
curl -X PUT http://localhost:8080/nifi-api/flow/process-groups/$PG_ID \
     -H "Authorization: Bearer $NIFI_TOKEN" \
     -H "Content-Type: application/json" \
     -d "{\"id\":\"$PG_ID\",\"state\":\"RUNNING\"}"
```

## Switching from direct ingest to NiFi-only

Set `SIM_DIRECT_INGEST=false` in `.env` so the simulator only writes CSVs to
`data/inbox/` without POSTing them directly:

```env
SIM_DIRECT_INGEST=false
```

NiFi will then be the sole ingestion path.

## Data contract

See `nifi/data-contract.yaml` for the full contract covering field names,
types, validation rules, idempotency, and error handling.

## Files in this directory

| File | Purpose |
|------|---------|
| `data-contract.yaml` | Authoritative ingestion schema and NiFi integration spec |
| `deploy_flow.py`     | One-time flow bootstrap via NiFi REST API |
| `README.md`          | This file |

## Live integration status

Live end-to-end integration (NiFi container running, flow deployed, CSV
processed, records in PostgreSQL) was **not verified** in this session because
the NiFi container cannot be started in the CI environment.

To complete live verification:
1. `docker compose --profile pipeline up -d`
2. Wait for NiFi healthcheck to pass (~120 s).
3. Run `deploy_flow.py` as documented above.
4. Start the demo (Admin UI → Start Live Demo).
5. Set `SIM_DIRECT_INGEST=false` and confirm donations appear in the dashboard.
6. Drop a malformed CSV in `data/inbox/` and confirm it appears in
   `data/nifi-failed/` and is visible at `GET /ingest/rejected`.

## Production notes

- Re-enable HTTPS: remove `NIFI_WEB_HTTPS_PORT: ""` from docker-compose.yml
  and configure a proper keystore.
- Use NiFi Parameter Contexts (sensitive=true) for `auth.token` instead of
  passing it as a plain environment variable.
- The NiFi volumes (`nifi_state`, `nifi_db`, etc.) are Docker-managed; back
  them up before any NiFi version upgrade.
- Limit exposure: do not publish port 8080 externally in production.
