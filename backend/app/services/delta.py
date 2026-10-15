"""Delta Lake analytical storage layer (Phase 3).

Stores versioned donation history alongside PostgreSQL, which remains the
operational source of truth.

Architecture
------------
PostgreSQL : Transactional donation state and current business operations.
MongoDB    : Raw ingestion documents (source provenance).
Delta Lake : Versioned analytical tables for historical analysis and audit.

The Delta table is append-only by default.  Upserts (MERGE) use a stable
record key (donation_id, batch_id) to prevent duplicates during reprocessing.

Storage
-------
The default path is <DELTA_BASE_PATH>/donations (configurable via the
DELTA_BASE_PATH environment variable, default: data/delta).

This is a local filesystem path suitable for development and single-node
deployments.  For production with multiple workers, use shared object storage
(S3, Azure ADLS, GCS) with the appropriate deltalake storage options.

Concurrency limitations
-----------------------
The delta-rs library (deltalake Python package) provides optimistic concurrency
control via Delta transaction log.  Concurrent writes from multiple processes to
the same local path may collide; use object storage with conditional-writes for
multi-writer deployments.

Known limitations
-----------------
* Local Delta tables do not survive volume loss.  Mount a persistent volume or
  use object storage for production.
* Schema evolution (adding nullable columns) is supported; removing or renaming
  columns requires a full table rewrite.
* Time travel is limited by VACUUM retention (default: 30 days of log files).
"""
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Storage configuration
# ---------------------------------------------------------------------------

_DEFAULT_BASE = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "delta")
DELTA_BASE_PATH = os.environ.get("DELTA_BASE_PATH", _DEFAULT_BASE)

DONATIONS_TABLE_PATH = str(Path(DELTA_BASE_PATH) / "donations")

# Stable record key for upsert deduplication.
MERGE_KEY_COLS = ["donation_id"]

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

DONATION_SCHEMA = pa.schema([
    pa.field("donation_id",     pa.int64(),    nullable=False),
    pa.field("batch_id",        pa.int64(),    nullable=True),
    pa.field("donor_name",      pa.string(),   nullable=True),
    pa.field("food_type",       pa.string(),   nullable=False),
    pa.field("category",        pa.string(),   nullable=True),
    pa.field("quantity_kg",     pa.float64(),  nullable=False),
    pa.field("area",            pa.string(),   nullable=True),
    pa.field("lat",             pa.float64(),  nullable=True),
    pa.field("lng",             pa.float64(),  nullable=True),
    pa.field("status",          pa.string(),   nullable=False),
    pa.field("expiry_time",     pa.string(),   nullable=True),   # ISO string (naive UTC)
    pa.field("created_at",      pa.string(),   nullable=True),   # ISO string (naive UTC)
    pa.field("ingest_batch_id", pa.int64(),    nullable=True),
    pa.field("source",          pa.string(),   nullable=True),
    pa.field("dedupe_key",      pa.string(),   nullable=True),
    pa.field("processed_at",    pa.string(),   nullable=False),  # When this row was written
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_arrow(records: list[dict]) -> pa.Table:
    """Convert a list of donation dicts to a PyArrow table matching DONATION_SCHEMA."""
    now_iso = datetime.utcnow().isoformat()
    rows = {col.name: [] for col in DONATION_SCHEMA}
    for r in records:
        rows["donation_id"].append(int(r["donation_id"]))
        rows["batch_id"].append(r.get("batch_id"))
        rows["donor_name"].append(r.get("donor_name") or r.get("org_name"))
        rows["food_type"].append(str(r.get("food_type", "")))
        rows["category"].append(r.get("category"))
        rows["quantity_kg"].append(float(r.get("quantity_kg", 0.0)))
        rows["area"].append(r.get("area"))
        rows["lat"].append(float(r["lat"]) if r.get("lat") is not None else None)
        rows["lng"].append(float(r["lng"]) if r.get("lng") is not None else None)
        rows["status"].append(str(r.get("status", "listed")))
        exp = r.get("expiry_time")
        rows["expiry_time"].append(exp.isoformat() if isinstance(exp, datetime) else str(exp) if exp else None)
        cre = r.get("created_at")
        rows["created_at"].append(cre.isoformat() if isinstance(cre, datetime) else str(cre) if cre else None)
        rows["ingest_batch_id"].append(r.get("ingest_batch_id") or r.get("batch_id"))
        rows["source"].append(r.get("source"))
        rows["dedupe_key"].append(r.get("dedupe_key"))
        rows["processed_at"].append(now_iso)
    return pa.table(rows, schema=DONATION_SCHEMA)


def _import_deltalake():
    """Lazy import so the rest of the app works even if deltalake isn't installed."""
    try:
        import deltalake
        return deltalake
    except ImportError as exc:
        raise ImportError(
            "deltalake package is required for Delta Lake integration. "
            "Install it with: pip install 'deltalake>=0.20,<0.22' pyarrow"
        ) from exc


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def write_donations(records: list[dict], table_path: str = DONATIONS_TABLE_PATH,
                    mode: str = "append") -> int:
    """Write donation records to the Delta table.

    Parameters
    ----------
    records     : List of donation dicts (from ingest_rows() summary or DB query).
    table_path  : Local filesystem path (or object storage URI) for the Delta table.
    mode        : "append" (default) or "overwrite".

    Returns the number of records written.

    Raises on write failure without modifying any previously committed version.
    """
    if not records:
        return 0
    dl = _import_deltalake()
    table = _to_arrow(records)
    Path(table_path).mkdir(parents=True, exist_ok=True)
    dl.write_deltalake(table_path, table, mode=mode, schema=DONATION_SCHEMA)
    log.info("delta: wrote %d records to %s (mode=%s)", len(records), table_path, mode)
    return len(records)


def upsert_donations(records: list[dict], table_path: str = DONATIONS_TABLE_PATH) -> int:
    """Upsert donation records using donation_id as the merge key.

    Safe for idempotent reprocessing: records with the same donation_id update
    existing rows rather than creating duplicates.

    If the table does not exist yet, it is created by this call (behaves like
    write_donations with mode='append').
    """
    if not records:
        return 0
    dl = _import_deltalake()
    table = _to_arrow(records)
    path = Path(table_path)
    path.mkdir(parents=True, exist_ok=True)

    if not (path / "_delta_log").exists():
        # First write — no existing table to merge into.
        return write_donations(records, table_path, mode="append")

    dt = dl.DeltaTable(table_path)
    (
        dt.merge(
            source=table,
            predicate="target.donation_id = source.donation_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    log.info("delta: upserted %d records in %s", len(records), table_path)
    return len(records)


def read_donations(table_path: str = DONATIONS_TABLE_PATH,
                   version: int | None = None) -> pa.Table:
    """Read the Delta table, optionally at a historical version (time travel).

    Parameters
    ----------
    table_path : Path to the Delta table.
    version    : Integer version number (0 = initial write).  None = current.

    Returns a PyArrow Table.  Raises if the table does not exist.
    """
    dl = _import_deltalake()
    if version is not None:
        dt = dl.DeltaTable(table_path, version=version)
    else:
        dt = dl.DeltaTable(table_path)
    return dt.to_pyarrow_table()


def get_table_version(table_path: str = DONATIONS_TABLE_PATH) -> int:
    """Return the current version number of the Delta table."""
    dl = _import_deltalake()
    return dl.DeltaTable(table_path).version()


def get_table_history(table_path: str = DONATIONS_TABLE_PATH) -> list[dict]:
    """Return the transaction history of the Delta table."""
    dl = _import_deltalake()
    return dl.DeltaTable(table_path).history()


def delete_donations(predicate: str, table_path: str = DONATIONS_TABLE_PATH) -> None:
    """Delete rows matching a predicate expression.

    Example: delete_donations("donation_id = 42")

    WARNING: This modifies the table.  Use with care and prefer soft deletes
    (status update) in the operational PostgreSQL layer for business logic.
    """
    dl = _import_deltalake()
    dt = dl.DeltaTable(table_path)
    dt.delete(predicate)
    log.info("delta: deleted rows matching '%s' from %s", predicate, table_path)


def table_exists(table_path: str = DONATIONS_TABLE_PATH) -> bool:
    """Return True if a Delta table exists at the given path."""
    return (Path(table_path) / "_delta_log").exists()
