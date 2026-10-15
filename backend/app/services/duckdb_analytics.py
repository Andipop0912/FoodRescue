"""DuckDB in-memory analytics over FoodRescue PostgreSQL data.

Fetches donation/match data from PostgreSQL via psycopg2, loads it into a
DuckDB in-memory database, then runs aggregation queries.  Read-only.
"""
from __future__ import annotations

import os
from typing import Any


def _dsn() -> str:
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql+psycopg2://foodrescue:foodrescue@localhost:5433/foodrescue",
    )
    return url.replace("postgresql+psycopg2://", "postgresql://")


def run_donation_analytics() -> dict[str, Any]:
    """Return donation analytics computed by DuckDB (in-memory)."""
    import duckdb
    import psycopg2

    with psycopg2.connect(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.id, d.status,
                       COALESCE(d.category, 'uncategorised') AS category,
                       d.quantity_kg::float AS quantity_kg,
                       d.match_attempts,
                       COALESCE(d.area, 'unknown') AS area
                FROM donations d
            """)
            don_rows = cur.fetchall()

            cur.execute("""
                SELECT score::float, status FROM matches
            """)
            match_rows = cur.fetchall()

    if not don_rows:
        return {"engine": "duckdb", "error": "no donations"}

    con = duckdb.connect(":memory:")

    con.execute("""
        CREATE TABLE donations (
            id INTEGER, status VARCHAR, category VARCHAR,
            quantity_kg DOUBLE, match_attempts INTEGER, area VARCHAR
        )
    """)
    con.executemany("INSERT INTO donations VALUES (?,?,?,?,?,?)", don_rows)

    con.execute("""
        CREATE TABLE matches (score DOUBLE, status VARCHAR)
    """)
    if match_rows:
        con.executemany("INSERT INTO matches VALUES (?,?)", match_rows)

    summary = con.execute("""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE status IN ('matched','accepted','picked_up','delivered')) AS matched_above,
               ROUND(AVG(quantity_kg), 2) AS avg_kg,
               ROUND(SUM(quantity_kg), 1) AS total_kg,
               ROUND(AVG(match_attempts), 2) AS avg_attempts
        FROM donations
    """).fetchone()

    cats = con.execute("""
        SELECT category, COUNT(*) n, ROUND(SUM(quantity_kg), 1) total_kg,
               COUNT(*) FILTER (WHERE status IN ('matched','accepted','picked_up','delivered')) matched_n
        FROM donations GROUP BY category ORDER BY n DESC
    """).fetchall()

    by_status = con.execute("""
        SELECT status, COUNT(*) n, ROUND(SUM(quantity_kg), 1) total_kg
        FROM donations GROUP BY status ORDER BY n DESC
    """).fetchall()

    top_areas = con.execute("""
        SELECT area, COUNT(*) n, ROUND(SUM(quantity_kg), 1) total_kg
        FROM donations GROUP BY area ORDER BY n DESC LIMIT 5
    """).fetchall()

    score_row = con.execute("""
        SELECT ROUND(AVG(score), 4), ROUND(MIN(score), 4), ROUND(MAX(score), 4), COUNT(*)
        FROM matches WHERE status = 'suggested'
    """).fetchone()

    con.close()

    total, matched = summary[0], summary[1]
    match_rate = round(matched / total * 100, 1) if total else 0.0

    return {
        "engine": "duckdb",
        "summary": {
            "total_donations": total,
            "matched_or_above": matched,
            "match_rate_pct": match_rate,
            "avg_quantity_kg": summary[2],
            "total_quantity_kg": summary[3],
            "avg_match_attempts": summary[4],
        },
        "by_category": [
            {"category": r[0], "n": r[1], "total_kg": r[2], "matched_n": r[3]}
            for r in cats
        ],
        "by_status": [
            {"status": r[0], "n": r[1], "total_kg": r[2]}
            for r in by_status
        ],
        "top_areas": [
            {"area": r[0], "n": r[1], "total_kg": r[2]}
            for r in top_areas
        ],
        "match_score_stats": {
            "avg": score_row[0],
            "min": score_row[1],
            "max": score_row[2],
            "total_suggested": score_row[3],
        },
    }
