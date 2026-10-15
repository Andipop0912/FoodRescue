# FoodRescue — Phase 4: Apache Airflow

## Status: DESIGN ONLY — not yet deployed

The DAG in `dags/foodrescue_pipeline.py` is syntactically valid Python and
documents the intended orchestration design.  It has NOT been executed.

**Do not add Airflow to Docker Compose until:**
1. NiFi live end-to-end integration is verified (needs Docker).
2. RabbitMQ DLQ topology is verified against a live broker.
3. Delta Lake integration is confirmed non-fatal on write failure.
4. This DAG design has been reviewed by the team.

---

## What Airflow orchestrates

Airflow ORCHESTRATES existing services — it does not replace them:

| Existing service | Airflow interaction |
|-----------------|-------------------|
| PostgreSQL      | Direct SQLAlchemy queries (same pool as worker) |
| RabbitMQ        | None — ingest publishes, worker consumes |
| Delta Lake      | Calls `upsert_donations()` from `app.services.delta` |
| NiFi            | Optional: trigger via NiFi REST API for large batch jobs |
| FastAPI         | Optional: read `/stats/summary` for quality checks |

---

## DAG: `foodrescue_pipeline` (hourly)

```
[expire_stale_donations]  ──▶  [run_match_sweeper]
         │                              │
         └──────────┬───────────────────┘
                    ▼
           [check_data_quality]   ──fail──▶  [alert_on_quality_failure]
                    │
                    ▼
        [export_donations_to_delta]
                    │
                    ▼
        [export_status_history_to_delta]
                    │
                    ▼
          [refresh_analytics_summary]
```

### Tasks

| Task | What it does | Idempotent? |
|------|-------------|------------|
| `expire_stale_donations` | UPDATE donations SET status=expired WHERE expiry_time < now | ✓ |
| `run_match_sweeper` | Calls existing `consumer.sweep()` | ✓ |
| `check_data_quality` | Reject rate < 50%, NGOs exist, no stuck donations | ✓ |
| `export_donations_to_delta` | Upsert last-48h donations to Delta Lake on `donation_id` | ✓ |
| `export_status_history_to_delta` | Append event-log rows to Delta status-history table | append-only |
| `refresh_analytics_summary` | Calls existing `stats.summary()` | ✓ |
| `alert_on_quality_failure` | Fires only when quality gate fails | ✓ |

### Retry policy

```python
retries=3
retry_delay=timedelta(minutes=2)
retry_exponential_backoff=True
execution_timeout=timedelta(minutes=10)
dagrun_timeout=timedelta(hours=1)
```

---

## Deployment plan (not yet executed)

### 1. Add to docker-compose.yml (when ready)

```yaml
airflow:
  image: apache/airflow:2.10.3
  profiles: [pipeline, all]
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://foodrescue:foodrescue@postgres:5432/airflow
    AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    DATABASE_URL: postgresql+psycopg2://foodrescue:foodrescue@postgres:5432/foodrescue
    FOODRESCUE_ENV: development
  volumes:
    - ./airflow/dags:/opt/airflow/dags:ro
    - ./backend:/opt/airflow/app:ro    # Mount app code so DAG tasks can import it
    - ./data/delta:/data/delta
    - airflow_db:/opt/airflow/logs
  command: >
    bash -c "airflow db migrate &&
             airflow users create --username admin --password admin
               --firstname Air --lastname Flow --role Admin --email airflow@local &&
             airflow scheduler &
             airflow webserver"
  ports: ["8081:8080"]
  depends_on:
    postgres: {condition: service_healthy}
```

### 2. Environment variables required

```env
AIRFLOW_FERNET_KEY=<32-byte base64 key; generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
```

### 3. DAG considerations

- The Airflow worker imports `app.*` modules directly, so `PYTHONPATH` must
  include the `backend/` directory.
- `FOODRESCUE_ENV=development` in the worker environment prevents the JWT
  guard from requiring a strong secret in the worker process.
- Delta Lake writes go to the shared `data/delta/` volume.

---

## Remaining work before deploying Airflow

1. Verify NiFi end-to-end (needs Docker daemon running).
2. Verify RabbitMQ DLQ topology with live broker.
3. Add an Airflow-specific DB (or reuse PostgreSQL with a separate `airflow` database).
4. Decide on an executor: LocalExecutor (simple, single-node) vs CeleryExecutor (distributed).
5. Configure alerting (Slack/email) for quality gate failures.
6. Write unit tests for each DAG task function independently (they are plain Python callables).
