"""Cross-store consistency: one uniquely identifiable donation traced through every store.

Verifies that a single ingest run produces correlated records in:
  1. PostgreSQL (Donation + IngestBatch)
  2. MongoDB raw_ingest (every row, including the original dict)
  3. MongoDB events (donation.created payload)
  4. Delta Lake (captured via monkeypatch — actual write tested in test_delta.py)
"""

from datetime import datetime, timedelta

UNIQUE_ORG = "CrossStore Probe Org"
UNIQUE_FOOD = "Probe Bajra Ladoo 99"
UNIQUE_QTY = 17.0


def _iso(hours=8):
    return (datetime.utcnow() + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _probe_row():
    return {"record_type": "donation", "org_name": UNIQUE_ORG, "food_type": UNIQUE_FOOD,
            "category": "bakery", "quantity_kg": UNIQUE_QTY, "expiry_time": _iso(8), "area": "Nerul"}


def test_unique_record_consistent_across_all_stores(client, session, admin, published, mongo_db, monkeypatch):
    """A single probe donation must appear with matching IDs and values in every store."""
    delta_written = []
    import app.services.delta as delta_mod
    monkeypatch.setattr(delta_mod, "upsert_donations", lambda records, **kw: delta_written.extend(records))

    r = client.post("/ingest/batch", headers=admin, json=[_probe_row()])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["accepted_donations"] == 1
    batch_id = body["batch_id"]
    donation_id = body["donation_ids"][0]

    # --- 1. PostgreSQL: Donation record ---
    from app.models import Donation, IngestBatch
    pg = session.query(Donation).filter_by(id=donation_id).one()
    assert pg.food_type == UNIQUE_FOOD
    assert pg.category == "bakery"
    assert pg.quantity_kg == UNIQUE_QTY
    assert pg.batch_id == batch_id
    assert pg.donor.name == UNIQUE_ORG  # Donor.name maps to org_name

    pg_batch = session.query(IngestBatch).filter_by(id=batch_id).one()
    assert pg_batch.accepted_donations == 1

    # --- 2. MongoDB raw_ingest: original row stored verbatim ---
    raw_doc = mongo_db.raw_ingest.find_one({"batch_id": batch_id, "row_number": 1})
    assert raw_doc is not None, "raw_ingest document not found"
    assert raw_doc["batch_id"] == batch_id
    assert raw_doc["raw"]["org_name"] == UNIQUE_ORG
    assert raw_doc["raw"]["food_type"] == UNIQUE_FOOD
    assert float(raw_doc["raw"]["quantity_kg"]) == UNIQUE_QTY

    # --- 3. MongoDB events: donation.created with correct IDs ---
    event_doc = mongo_db.events.find_one({"batch_id": batch_id})
    assert event_doc is not None, "events document not found"
    assert event_doc["event"] == "donation.created"
    assert event_doc["donation_id"] == donation_id
    assert event_doc["batch_id"] == batch_id
    assert event_doc["category"] == "bakery"
    assert event_doc["quantity_kg"] == UNIQUE_QTY

    # --- 4. Delta Lake: upsert called with matching donation_id and food_type ---
    assert len(delta_written) == 1, f"expected 1 delta record, got {len(delta_written)}"
    dr = delta_written[0]
    assert dr["donation_id"] == donation_id
    assert dr["food_type"] == UNIQUE_FOOD
    assert dr["category"] == "bakery"
    assert dr["quantity_kg"] == UNIQUE_QTY
    assert dr["batch_id"] == batch_id
