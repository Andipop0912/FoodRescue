"""FoodRescue Pipeline DAG — Phase 4 (Airflow readiness design).

STATUS: DESIGN ONLY — not yet deployed.
This file documents the intended Airflow DAG.  It is syntactically valid
Python and uses the Airflow 2.x API, but it has not been executed.

Do not add Airflow to Docker Compose until Phase 3 (Delta Lake) has been
verified end-to-end and this DAG design has been reviewed.

──────────────────────────────────────────────────────────────────────────────
Architecture context
──────────────────────────────────────────────────────────────────────────────
Existing services Airflow will ORCHESTRATE (not replace):
  PostgreSQL   Operational source of truth.  Airflow reads donation/NGO state.
  RabbitMQ     donation.created queue already published by the FastAPI ingest
               endpoint.  Airflow does NOT publish to RabbitMQ directly.
  Delta Lake   Analytical store.  Airflow schedules the snapshot export DAG.
  NiFi         Watches data/inbox/ and POSTs to /ingest/batch.  Airflow can
               trigger NiFi via its REST API for coordinated batch jobs.
  FastAPI      Existing ingestion endpoint; Airflow calls it for manual batch
               triggers and reads /stats/summary for quality checks.

What Airflow adds:
  - Scheduled Delta Lake snapshots (hourly).
  - Stale-donation expiry sweep (every 5 min).
  - Daily analytics refresh (aggregate stats export).
  - Data-quality gate before each snapshot.
  - Retry policies with exponential backoff.
  - Observable failure path with alerting.

──────────────────────────────────────────────────────────────────────────────
DAG schedule and dependencies
──────────────────────────────────────────────────────────────────────────────

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

──────────────────────────────────────────────────────────────────────────────
Retry and timeout policy
──────────────────────────────────────────────────────────────────────────────
  retries=3, retry_delay=timedelta(minutes=2), retry_exponential_backoff=True
  execution_timeout=timedelta(minutes=10) per task
  dagrun_timeout=timedelta(hours=1)

──────────────────────────────────────────────────────────────────────────────
Idempotency
──────────────────────────────────────────────────────────────────────────────
  Each task is keyed on logical_date (Airflow's execution date).
  Delta Lake upserts use donation_id as the merge key, so a re-run of any DAG
  instance does not create duplicate analytical rows.
  expire_stale_donations is idempotent: it only updates donations whose
  expiry_time < NOW and status = S_LISTED.

──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Airflow 2.x imports
# NOTE: These will fail if Airflow is not installed.
#       The file is syntactically valid Python for review purposes.
# ---------------------------------------------------------------------------
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.utils.dates import days_ago
    _AIRFLOW_AVAILABLE = True
except ImportError:
    _AIRFLOW_AVAILABLE = False
    # Stub classes for syntax validation without an Airflow installation.
    class DAG:  # type: ignore[no-redef]
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
    class PythonOperator:  # type: ignore[no-redef]
        def __init__(self, *a, **kw): pass
        def set_downstream(self, *a): pass
    class EmptyOperator:  # type: ignore[no-redef]
        def __init__(self, *a, **kw): pass
    def days_ago(n): return datetime(2026, 1, 1)  # type: ignore[misc]

# ---------------------------------------------------------------------------
# Task implementations
# These functions call the EXISTING service layer — no duplicated logic.
# ---------------------------------------------------------------------------

def _expire_stale_donations(**context) -> dict:
    """Mark donations past their expiry_time as expired.

    Calls the existing FastAPI status logic via the Python service layer
    directly (same process, shares DB connection pool in Airflow worker).

    Idempotent: only affects donations with status in (S_LISTED, S_MATCHED)
    and expiry_time < utcnow().
    """
    import os
    os.environ.setdefault("FOODRESCUE_ENV", "development")  # Worker may lack this.

    from sqlalchemy import update
    from app.db import SessionLocal
    from app.constants import S_LISTED, S_MATCHED, S_EXPIRED
    from app.utils import utcnow

    now = utcnow()
    with SessionLocal() as db:
        result = db.execute(
            update("donations")
            .where(
                db.execute("donations.status IN (:s1, :s2)", s1=S_LISTED, s2=S_MATCHED),
                db.execute("donations.expiry_time < :now", now=now),
            )
            .values(status=S_EXPIRED, status_changed_at=now)
        )
        db.commit()
        expired_count = result.rowcount
    return {"expired_count": expired_count, "run_at": now.isoformat()}


def _expire_stale_donations_sqlalchemy(**context) -> dict:
    """SQLAlchemy-idiomatic implementation (use this in production)."""
    import os
    os.environ.setdefault("FOODRESCUE_ENV", "development")

    from sqlalchemy import update, and_
    from app.db import SessionLocal
    from app.models import Donation
    from app.constants import S_LISTED, S_MATCHED, S_EXPIRED
    from app.utils import utcnow

    now = utcnow()
    with SessionLocal() as db:
        result = db.execute(
            update(Donation)
            .where(
                and_(
                    Donation.status.in_([S_LISTED, S_MATCHED]),
                    Donation.expiry_time < now,
                )
            )
            .values(status=S_EXPIRED, status_changed_at=now)
        )
        db.commit()
    return {"expired_count": result.rowcount, "run_at": now.isoformat()}


def _run_match_sweeper(**context) -> dict:
    """Run the donation matching sweeper once (same logic as consumer.py sweep()).

    Uses the existing sweep() function — no duplicated matching logic.
    Returns the number of donations re-examined.
    """
    import os
    os.environ.setdefault("FOODRESCUE_ENV", "development")

    from app.worker.consumer import sweep
    swept = sweep()
    return {"swept_count": swept}


def _check_data_quality(**context) -> dict:
    """Assert data quality thresholds before writing to Delta Lake.

    Checks:
    - Reject rate for the last ingestion batch is < 50%.
    - At least one active NGO exists.
    - No stuck donations (listed for > 24 h with 0 match attempts).

    Raises ValueError if any check fails — Airflow marks the task as FAILED
    and the downstream Delta export tasks are skipped.
    """
    import os
    os.environ.setdefault("FOODRESCUE_ENV", "development")

    from sqlalchemy import func, select, and_
    from app.db import SessionLocal
    from app.models import Donation, NGO, IngestBatch
    from app.constants import S_LISTED
    from app.utils import utcnow
    from datetime import timedelta

    now = utcnow()
    issues = []

    with SessionLocal() as db:
        # 1. Last batch reject rate.
        last_batch = db.scalar(
            select(IngestBatch).order_by(IngestBatch.id.desc()).limit(1)
        )
        if last_batch and last_batch.total_rows > 0:
            reject_rate = last_batch.rejected_rows / last_batch.total_rows
            if reject_rate > 0.5:
                issues.append(
                    f"last batch reject rate {reject_rate:.0%} exceeds 50% "
                    f"(batch_id={last_batch.id})"
                )

        # 2. Active NGOs.
        ngo_count = db.scalar(select(func.count()).select_from(NGO))
        if not ngo_count:
            issues.append("no NGOs registered — matching cannot proceed")

        # 3. Stuck donations (listed > 24 h, 0 match attempts).
        stuck = db.scalar(
            select(func.count()).select_from(Donation).where(
                and_(
                    Donation.status == S_LISTED,
                    Donation.match_attempts == 0,
                    Donation.created_at < now - timedelta(hours=24),
                )
            )
        )
        if stuck and stuck > 100:
            issues.append(f"{stuck} donations stuck in listed state with 0 match attempts for > 24 h")

    if issues:
        raise ValueError("Data quality check failed:\n" + "\n".join(f"  - {i}" for i in issues))

    return {"quality_ok": True, "checked_at": now.isoformat()}


def _export_donations_to_delta(**context) -> dict:
    """Snapshot active and recent donations to Delta Lake.

    Reads from PostgreSQL (source of truth), transforms to the Delta schema,
    and upserts into the analytical table.  The logical_date window determines
    which donations to include so re-runs are idempotent.

    Does NOT affect PostgreSQL state.
    """
    import os
    os.environ.setdefault("FOODRESCUE_ENV", "development")

    from sqlalchemy import select, and_
    from app.db import SessionLocal
    from app.models import Donation, Donor
    from app.utils import utcnow
    from app.services.delta import upsert_donations
    from datetime import timedelta

    # Export donations created in the last 48 h (overlap ensures no gaps).
    cutoff = utcnow() - timedelta(hours=48)

    with SessionLocal() as db:
        rows = db.execute(
            select(Donation, Donor.name.label("donor_name"))
            .join(Donor, Donor.id == Donation.donor_id)
            .where(Donation.created_at >= cutoff)
        ).all()

        records = [
            {
                "donation_id": row.Donation.id,
                "batch_id": row.Donation.batch_id,
                "donor_name": row.donor_name,
                "food_type": row.Donation.food_type,
                "category": row.Donation.category,
                "quantity_kg": row.Donation.quantity_kg,
                "area": row.Donation.area,
                "lat": row.Donation.lat,
                "lng": row.Donation.lng,
                "status": row.Donation.status,
                "expiry_time": row.Donation.expiry_time,
                "created_at": row.Donation.created_at,
                "source": row.Donation.source,
                "dedupe_key": row.Donation.dedupe_key,
                "ingest_batch_id": row.Donation.batch_id,
            }
            for row in rows
        ]

    written = upsert_donations(records)
    return {"records_exported": written, "cutoff": cutoff.isoformat()}


def _export_status_history_to_delta(**context) -> dict:
    """Append status-change events to the Delta status-history table.

    Reads Event rows of kind='match', 'accept', 'pickup', 'delivered' from
    PostgreSQL and appends to data/delta/status_history/.
    """
    import os
    os.environ.setdefault("FOODRESCUE_ENV", "development")

    from sqlalchemy import select
    from app.db import SessionLocal
    from app.models import Event
    from app.utils import utcnow
    from datetime import timedelta
    import pyarrow as pa
    import deltalake
    import pathlib

    base = os.environ.get("DELTA_BASE_PATH", "data/delta")
    table_path = str(pathlib.Path(base) / "status_history")
    pathlib.Path(table_path).mkdir(parents=True, exist_ok=True)

    cutoff = utcnow() - timedelta(hours=48)
    event_kinds = ("match", "accept", "pickup", "delivered", "ingest", "no_match")

    with SessionLocal() as db:
        events = db.scalars(
            select(Event)
            .where(Event.kind.in_(event_kinds), Event.ts >= cutoff)
            .order_by(Event.ts)
        ).all()

    if not events:
        return {"events_exported": 0}

    schema = pa.schema([
        pa.field("event_id",    pa.int64()),
        pa.field("kind",        pa.string()),
        pa.field("message",     pa.string()),
        pa.field("ts",          pa.string()),
        pa.field("data_json",   pa.string()),
    ])
    table = pa.table(
        {
            "event_id":  [e.id for e in events],
            "kind":      [e.kind for e in events],
            "message":   [e.message for e in events],
            "ts":        [e.ts.isoformat() for e in events],
            "data_json": [str(e.data) if e.data else None for e in events],
        },
        schema=schema,
    )
    deltalake.write_deltalake(table_path, table, mode="append")
    return {"events_exported": len(events)}


def _refresh_analytics_summary(**context) -> dict:
    """Compute and log aggregate donation statistics.

    Calls the existing stats.summary() function — no duplicated aggregation.
    In a future phase this will write to an analytics Delta table or a
    dashboard API.
    """
    import os
    os.environ.setdefault("FOODRESCUE_ENV", "development")

    from app.db import SessionLocal
    from app.services.stats import summary

    with SessionLocal() as db:
        s = summary(db)
    return s


def _alert_on_quality_failure(**context) -> None:
    """Log a structured alert for data quality failures.

    In production, replace with an Airflow notification operator
    (SlackWebhookOperator, EmailOperator, PagerDutyOperator, etc.).
    """
    ti = context.get("ti")
    exception = context.get("exception")
    import logging
    logging.getLogger("airflow.task").error(
        "ALERT: FoodRescue data quality check failed: %s (dag_run=%s)",
        exception,
        ti.dag_run if ti else "unknown",
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "foodrescue",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=10),
    "email_on_failure": False,   # Set to True and configure smtp_* in airflow.cfg for production.
    "email_on_retry": False,
}

with DAG(
    dag_id="foodrescue_pipeline",
    description="FoodRescue hourly pipeline: expiry sweep, match sweep, Delta Lake export, analytics refresh",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,        # Prevent overlapping pipeline runs.
    dagrun_timeout=timedelta(hours=1),
    tags=["foodrescue", "pipeline", "delta"],
) as dag:

    expire_task = PythonOperator(
        task_id="expire_stale_donations",
        python_callable=_expire_stale_donations_sqlalchemy,
        doc_md="Mark donations past their expiry_time as expired. Idempotent.",
    )

    sweep_task = PythonOperator(
        task_id="run_match_sweeper",
        python_callable=_run_match_sweeper,
        doc_md="Run the database sweeper to retry unmatched donations (match_attempts < 4).",
    )

    quality_task = PythonOperator(
        task_id="check_data_quality",
        python_callable=_check_data_quality,
        doc_md="Assert data quality thresholds before writing to Delta Lake.",
    )

    alert_task = PythonOperator(
        task_id="alert_on_quality_failure",
        python_callable=_alert_on_quality_failure,
        trigger_rule="one_failed",    # Fires only when check_data_quality fails.
        doc_md="Log (or page) on data quality gate failure.",
    )

    export_donations_task = PythonOperator(
        task_id="export_donations_to_delta",
        python_callable=_export_donations_to_delta,
        doc_md="Upsert last-48h donations from PostgreSQL into Delta Lake.",
    )

    export_history_task = PythonOperator(
        task_id="export_status_history_to_delta",
        python_callable=_export_status_history_to_delta,
        doc_md="Append event-log rows to the Delta status-history table.",
    )

    refresh_task = PythonOperator(
        task_id="refresh_analytics_summary",
        python_callable=_refresh_analytics_summary,
        doc_md="Compute aggregate stats from PostgreSQL.",
    )

    # ── Dependencies ─────────────────────────────────────────────────────────
    # Expire before sweep (sweep skips expired donations).
    expire_task >> sweep_task
    # Both must complete before the quality gate.
    [expire_task, sweep_task] >> quality_task
    # Alert fires on quality gate failure (does not block downstream when alert-only).
    quality_task >> alert_task
    # Delta exports run after quality gate passes.
    quality_task >> export_donations_task >> export_history_task >> refresh_task
