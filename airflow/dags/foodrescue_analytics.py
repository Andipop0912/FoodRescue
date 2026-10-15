"""FoodRescue analytics pipeline DAG.

Tasks
-----
1. check_pipeline_health    — HTTP GET /health on the backend; assert status=ok.
2. validate_ingested_data   — Integrity checks: orphaned matches, stale listed
                              donations, recent rejected rows.
3. run_analytics            — Aggregation queries: matching rate, category
                              breakdown, top NGOs, match-score distribution.
4. report_results           — Pull XComs from previous tasks and log a
                              formatted summary report.

Design notes
------------
* Read-only against the FoodRescue PostgreSQL DB — no writes, no side effects.
* Safe to rerun at any time; idempotent.
* Does NOT trigger donation matching or replay RabbitMQ events.
* SequentialExecutor is used (SQLite metadata DB); tasks run one at a time.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from decimal import Decimal

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Configuration — injected via docker-compose environment variables
# ---------------------------------------------------------------------------
_DB_DSN = os.environ["FOODRESCUE_DB_URL"]       # postgresql://user:pass@host/db
_API_URL = os.environ.get("FOODRESCUE_API_URL", "http://backend:8000")
_ES_URL = os.environ.get("FOODRESCUE_ES_URL", "http://elasticsearch:9200")

_DEFAULT_ARGS = {
    "owner": "foodrescue",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect():
    """Return a psycopg2 connection to the FoodRescue PostgreSQL database."""
    import psycopg2
    return psycopg2.connect(_DB_DSN)


def _jsonify(obj):
    """Recursively convert Decimal / datetime to JSON-safe types."""
    if isinstance(obj, dict):
        return {k: _jsonify(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_jsonify(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


# ---------------------------------------------------------------------------
# Task 1 — pipeline health
# ---------------------------------------------------------------------------

def check_pipeline_health(**_ctx):
    url = f"{_API_URL}/health"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            body = json.loads(resp.read())
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Backend unreachable at {url}: {exc}") from exc
    status = body.get("status", "unknown")
    if status != "ok":
        raise RuntimeError(f"Backend health check failed: {body}")
    logging.info("Health OK: %s", body)
    return {"status": status, "url": url}


# ---------------------------------------------------------------------------
# Task 2 — data validation
# ---------------------------------------------------------------------------

def validate_ingested_data(**_ctx):
    with _connect() as conn:
        with conn.cursor() as cur:
            # Status distribution
            cur.execute(
                "SELECT status, COUNT(*) FROM donations GROUP BY status ORDER BY status"
            )
            by_status = {row[0]: row[1] for row in cur.fetchall()}

            # Orphaned match rows (donation deleted mid-flight is a bug)
            cur.execute("""
                SELECT COUNT(*) FROM matches m
                LEFT JOIN donations d ON d.id = m.donation_id
                WHERE d.id IS NULL
            """)
            orphaned_matches = cur.fetchone()[0]

            # Stale listed donations: status=listed but match_attempts=0 and not expired
            cur.execute("""
                SELECT COUNT(*) FROM donations
                WHERE status = 'listed'
                  AND match_attempts = 0
                  AND expiry_time > NOW()
            """)
            stale_listed = cur.fetchone()[0]

            # Rejected rows in the last 24 hours
            cur.execute("""
                SELECT COUNT(*) FROM rejected_rows
                WHERE created_at > NOW() - INTERVAL '24 hours'
            """)
            recent_rejected = cur.fetchone()[0]

            # Ingest batches total
            cur.execute("SELECT COUNT(*), COALESCE(SUM(accepted_donations), 0) FROM ingest_batches")
            batch_count, total_accepted = cur.fetchone()

    result = _jsonify({
        "donations_by_status": by_status,
        "orphaned_matches": orphaned_matches,
        "stale_listed_donations": stale_listed,
        "recent_rejected_rows_24h": recent_rejected,
        "ingest_batches": batch_count,
        "total_accepted_donations": total_accepted,
    })

    if orphaned_matches > 0:
        raise ValueError(
            f"DATA QUALITY FAILURE: {orphaned_matches} orphaned match row(s) detected"
        )
    logging.info("Validation passed: %s", result)
    return result


# ---------------------------------------------------------------------------
# Task 3 — analytics
# ---------------------------------------------------------------------------

def run_analytics(**_ctx):
    with _connect() as conn:
        with conn.cursor() as cur:
            # Overall donation summary
            cur.execute("""
                SELECT
                    COUNT(*)                                                  AS total,
                    COUNT(*) FILTER (WHERE status IN
                        ('matched','accepted','picked_up','delivered'))       AS matched_or_above,
                    COALESCE(ROUND(AVG(match_attempts)::numeric, 2), 0)       AS avg_attempts,
                    COALESCE(ROUND(SUM(quantity_kg)
                        FILTER (WHERE status = 'delivered')::numeric, 1), 0) AS kg_delivered,
                    COALESCE(ROUND(SUM(quantity_kg)::numeric, 1), 0)          AS kg_total
                FROM donations
            """)
            row = cur.fetchone()
            total, matched, avg_att, kg_del, kg_total = row

            # Category breakdown
            cur.execute("""
                SELECT
                    COALESCE(category, 'uncategorised') AS category,
                    COUNT(*)                             AS n,
                    COALESCE(ROUND(SUM(quantity_kg)::numeric, 1), 0) AS total_kg,
                    COUNT(*) FILTER (WHERE status IN
                        ('matched','accepted','picked_up','delivered')) AS matched_n
                FROM donations
                GROUP BY category
                ORDER BY n DESC
            """)
            categories = [
                {"category": r[0], "n": r[1], "total_kg": r[2], "matched_n": r[3]}
                for r in cur.fetchall()
            ]

            # Match score statistics (suggested matches only)
            cur.execute("""
                SELECT
                    COALESCE(ROUND(AVG(score)::numeric, 4), 0) AS avg_score,
                    COALESCE(ROUND(MIN(score)::numeric, 4), 0) AS min_score,
                    COALESCE(ROUND(MAX(score)::numeric, 4), 0) AS max_score,
                    COUNT(*)                                    AS total_suggested
                FROM matches
                WHERE status = 'suggested'
            """)
            sc = cur.fetchone()
            score_stats = {
                "avg_score": sc[0], "min_score": sc[1],
                "max_score": sc[2], "total_suggested": sc[3],
            }

            # Top NGOs by accepted donations
            cur.execute("""
                SELECT n.name, n.area,
                    COUNT(*) AS accepted_count,
                    COALESCE(ROUND(SUM(d.quantity_kg)::numeric, 1), 0) AS total_kg
                FROM donations d
                JOIN ngos n ON n.id = d.accepted_ngo_id
                WHERE d.status IN ('accepted', 'picked_up', 'delivered')
                GROUP BY n.id, n.name, n.area
                ORDER BY accepted_count DESC
                LIMIT 5
            """)
            top_ngos = [
                {"name": r[0], "area": r[1], "accepted_count": r[2], "total_kg": r[3]}
                for r in cur.fetchall()
            ]

    analytics = _jsonify({
        "summary": {
            "total": total,
            "matched_or_above": matched,
            "avg_match_attempts": avg_att,
            "kg_delivered": kg_del,
            "kg_total": kg_total,
        },
        "categories": categories,
        "match_score_stats": score_stats,
        "top_ngos": top_ngos,
    })
    logging.info("Analytics: %s", analytics)
    return analytics


# ---------------------------------------------------------------------------
# Task 4 — report
# ---------------------------------------------------------------------------

def report_results(**ctx):
    ti = ctx["ti"]
    health = ti.xcom_pull(task_ids="check_pipeline_health") or {}
    validation = ti.xcom_pull(task_ids="validate_ingested_data") or {}
    analytics = ti.xcom_pull(task_ids="run_analytics") or {}

    summary = analytics.get("summary", {})
    total = summary.get("total", 0)
    matched = summary.get("matched_or_above", 0)
    match_rate = round(matched / total * 100, 1) if total else 0.0

    lines = [
        "",
        "=" * 62,
        f"FoodRescue Analytics — {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC",
        "=" * 62,
        f"  API health          : {health.get('status', 'N/A')}",
        f"  Total donations     : {total}",
        f"  Matched or above    : {matched}  ({match_rate}%)",
        f"  Avg match attempts  : {summary.get('avg_match_attempts', 'N/A')}",
        f"  kg delivered        : {summary.get('kg_delivered', 'N/A')}",
        f"  Ingest batches      : {validation.get('ingest_batches', 'N/A')}",
        f"  Orphaned matches    : {validation.get('orphaned_matches', 'N/A')}  (0 = healthy)",
        f"  Stale listed        : {validation.get('stale_listed_donations', 'N/A')}",
        f"  Rejected rows (24h) : {validation.get('recent_rejected_rows_24h', 'N/A')}",
    ]

    score = analytics.get("match_score_stats", {})
    lines += [
        f"  Match score avg/min/max : "
        f"{score.get('avg_score','N/A')} / {score.get('min_score','N/A')} / {score.get('max_score','N/A')}",
        "  Category breakdown:",
    ]
    for cat in analytics.get("categories", []):
        lines.append(
            f"    {cat['category']:<18} {cat['n']:>4} donations  "
            f"{cat['total_kg']:>8} kg  matched={cat['matched_n']}"
        )

    top = analytics.get("top_ngos", [])
    if top:
        lines.append("  Top NGOs (accepted):")
        for ngo in top:
            lines.append(
                f"    {ngo['name']:<30} {ngo['accepted_count']:>3} accepted  "
                f"{ngo['total_kg']:>7} kg"
            )

    lines.append("=" * 62)
    report = "\n".join(lines)
    logging.info(report)

    return {
        "match_rate_pct": match_rate,
        "total_donations": total,
        "orphaned_matches": validation.get("orphaned_matches", -1),
    }


# ---------------------------------------------------------------------------
# Task 5 — DuckDB in-memory analytics
# ---------------------------------------------------------------------------

def run_duckdb_analytics(**_ctx):
    """Load donations from PostgreSQL into DuckDB in-memory and compute analytics."""
    import psycopg2
    import duckdb

    with psycopg2.connect(_DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, status,
                       COALESCE(category, 'uncategorised') AS category,
                       quantity_kg::float, match_attempts
                FROM donations
            """)
            don_rows = cur.fetchall()
            cur.execute("SELECT score::float, status FROM matches")
            match_rows = cur.fetchall()

    if not don_rows:
        logging.warning("DuckDB: no donations in database")
        return {"engine": "duckdb", "total": 0}

    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE donations (
            id INTEGER, status VARCHAR, category VARCHAR,
            quantity_kg DOUBLE, match_attempts INTEGER
        )
    """)
    con.executemany("INSERT INTO donations VALUES (?,?,?,?,?)", don_rows)

    con.execute("CREATE TABLE matches (score DOUBLE, status VARCHAR)")
    if match_rows:
        con.executemany("INSERT INTO matches VALUES (?,?)", match_rows)

    summary = con.execute("""
        SELECT COUNT(*) total,
               COUNT(*) FILTER (WHERE status IN ('matched','accepted','picked_up','delivered')) matched_above,
               ROUND(AVG(quantity_kg)::numeric, 2) avg_kg,
               ROUND(SUM(quantity_kg)::numeric, 1) total_kg,
               ROUND(AVG(match_attempts)::numeric, 2) avg_attempts
        FROM donations
    """).fetchone()

    cats = con.execute("""
        SELECT category, COUNT(*) n,
               ROUND(SUM(quantity_kg)::numeric, 1) total_kg,
               COUNT(*) FILTER (WHERE status IN ('matched','accepted','picked_up','delivered')) matched_n
        FROM donations GROUP BY category ORDER BY n DESC
    """).fetchall()

    score_row = con.execute("""
        SELECT ROUND(AVG(score)::numeric, 4), COUNT(*)
        FROM matches WHERE status = 'suggested'
    """).fetchone()

    con.close()

    total = summary[0] or 0
    matched = summary[1] or 0
    match_rate = round(matched / total * 100, 1) if total else 0.0

    result = {
        "engine": "duckdb",
        "total_donations": total,
        "matched_or_above": matched,
        "match_rate_pct": match_rate,
        "avg_quantity_kg": float(summary[2] or 0),
        "total_quantity_kg": float(summary[3] or 0),
        "avg_match_attempts": float(summary[4] or 0),
        "by_category": [
            {"category": r[0], "n": r[1], "total_kg": float(r[2] or 0), "matched_n": r[3]}
            for r in cats
        ],
        "avg_match_score": float(score_row[0] or 0),
        "total_suggested_matches": score_row[1],
    }
    logging.info("DuckDB analytics: %s", result)
    return result


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="foodrescue_analytics_pipeline",
    default_args=_DEFAULT_ARGS,
    description="Data quality validation and donation analytics for FoodRescue",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["foodrescue", "analytics"],
    doc_md=__doc__,
) as dag:

    t1 = PythonOperator(
        task_id="check_pipeline_health",
        python_callable=check_pipeline_health,
    )
    t2 = PythonOperator(
        task_id="validate_ingested_data",
        python_callable=validate_ingested_data,
    )
    t3 = PythonOperator(
        task_id="run_analytics",
        python_callable=run_analytics,
    )
    t4 = PythonOperator(
        task_id="report_results",
        python_callable=report_results,
    )
    t5 = PythonOperator(
        task_id="duckdb_analytics",
        python_callable=run_duckdb_analytics,
    )

    t1 >> t2 >> t3 >> t4
    t3 >> t5
