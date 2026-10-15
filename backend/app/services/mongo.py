"""MongoDB access: raw ingest rows and event payloads live here (Postgres holds clean data)."""
import logging

from pymongo import MongoClient

from app.config import settings

log = logging.getLogger(__name__)
_client: MongoClient | None = None


def get_mongo():
    """Returns the Mongo database. Tests monkeypatch this function with a mongomock database."""
    global _client
    if _client is None:
        _client = MongoClient(settings.mongo_url, serverSelectionTimeoutMS=3000)
    return _client[settings.mongo_db]


def ensure_indexes() -> None:
    """Create indexes on raw_ingest and events if they don't already exist.
    Non-fatal: called at startup; a down MongoDB must not prevent the app from starting."""
    try:
        mdb = get_mongo()
        mdb.raw_ingest.create_index("batch_id")
        mdb.raw_ingest.create_index([("batch_id", 1), ("row_number", 1)], unique=True, name="batch_row_unique")
        mdb.events.create_index("donation_id")
        mdb.events.create_index("batch_id")
    except Exception as exc:
        log.warning("MongoDB ensure_indexes failed (non-fatal): %s", exc)


def reset_collections() -> None:
    mdb = get_mongo()
    for name in ("raw_ingest", "events"):
        mdb[name].delete_many({})
