"""Verify MongoDB document structure, index creation, batch correlation, and error resilience.

All tests use the mongomock fixture from conftest.py — no live MongoDB required.
"""

from datetime import datetime, timedelta

from app.services.mongo import ensure_indexes


def iso(hours=6):
    return (datetime.utcnow() + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _row(org="Mongo Test Org", food="Steamed Rice", cat="cooked_meals", qty=20, hours=6, area="Vashi"):
    return {"record_type": "donation", "org_name": org, "food_type": food,
            "category": cat, "quantity_kg": qty, "expiry_time": iso(hours), "area": area}


class TestRawIngestDocumentStructure:
    def test_raw_ingest_doc_has_all_required_fields(self, client, admin, published, mongo_db):
        r = client.post("/ingest/batch", headers=admin, json=[_row()])
        assert r.status_code == 200
        doc = mongo_db.raw_ingest.find_one({"batch_id": r.json()["batch_id"]})
        assert doc is not None
        for field in ("batch_id", "row_number", "source", "received_at", "raw"):
            assert field in doc, f"raw_ingest document missing field: {field}"

    def test_raw_ingest_row_number_is_one_indexed(self, client, admin, published, mongo_db):
        r = client.post("/ingest/batch", headers=admin, json=[_row()])
        docs = list(mongo_db.raw_ingest.find({"batch_id": r.json()["batch_id"]}))
        assert docs[0]["row_number"] == 1

    def test_raw_ingest_raw_field_preserves_original_row(self, client, admin, published, mongo_db):
        r = client.post("/ingest/batch", headers=admin, json=[_row(org="Probe Bakery", food="Rye Loaf")])
        doc = mongo_db.raw_ingest.find_one({"batch_id": r.json()["batch_id"]})
        assert doc["raw"]["org_name"] == "Probe Bakery"
        assert doc["raw"]["food_type"] == "Rye Loaf"

    def test_raw_ingest_stores_all_rows_including_rejected(self, client, admin, published, mongo_db):
        rows = [_row(), {"record_type": "donation", "org_name": "Bad", "quantity_kg": "abc",
                         "category": "bakery", "expiry_time": iso(4), "area": "Vashi"}]
        r = client.post("/ingest/batch", headers=admin, json=rows)
        assert r.json()["rejected_rows"] >= 1
        count = mongo_db.raw_ingest.count_documents({"batch_id": r.json()["batch_id"]})
        assert count == 2

    def test_batch_id_in_raw_ingest_matches_pg_batch(self, client, admin, published, mongo_db, session):
        from app.models import IngestBatch
        r = client.post("/ingest/batch", headers=admin, json=[_row()])
        batch_id = r.json()["batch_id"]
        pg_batch = session.query(IngestBatch).filter_by(id=batch_id).one()
        assert mongo_db.raw_ingest.count_documents({"batch_id": pg_batch.id}) == 1


class TestEventsDocumentStructure:
    def test_events_doc_has_all_required_fields(self, client, admin, published, mongo_db):
        r = client.post("/ingest/batch", headers=admin, json=[_row()])
        doc = mongo_db.events.find_one({"batch_id": r.json()["batch_id"]})
        assert doc is not None
        for field in ("event", "donation_id", "category", "quantity_kg", "area", "batch_id", "ts"):
            assert field in doc, f"events document missing field: {field}"

    def test_events_doc_event_type_is_donation_created(self, client, admin, published, mongo_db):
        r = client.post("/ingest/batch", headers=admin, json=[_row()])
        doc = mongo_db.events.find_one({"batch_id": r.json()["batch_id"]})
        assert doc["event"] == "donation.created"

    def test_events_only_written_for_accepted_donations(self, client, admin, published, mongo_db):
        expired = {"record_type": "donation", "org_name": "Old Org", "food_type": "Old Rice",
                   "category": "cooked_meals", "quantity_kg": 5,
                   "expiry_time": (datetime.utcnow() - timedelta(hours=2)).isoformat() + "Z", "area": "Vashi"}
        r = client.post("/ingest/batch", headers=admin, json=[_row(), expired])
        assert r.json()["accepted_donations"] == 1 and r.json()["rejected_rows"] == 1
        assert mongo_db.events.count_documents({"batch_id": r.json()["batch_id"]}) == 1


class TestMongoDownIsNonFatal:
    def test_mongo_down_does_not_block_postgres_write(self, client, admin, published, session, monkeypatch):
        import app.services.mongo as mongo_mod

        def _raise():
            raise ConnectionError("Mongo is unreachable")

        monkeypatch.setattr(mongo_mod, "get_mongo", _raise)
        r = client.post("/ingest/batch", headers=admin, json=[_row()])
        assert r.status_code == 200
        assert r.json()["accepted_donations"] == 1
        from app.models import Donation
        assert session.query(Donation).count() == 1


class TestEnsureIndexes:
    def test_ensure_indexes_creates_batch_id_index_on_raw_ingest(self, mongo_db, monkeypatch):
        import app.services.mongo as mongo_mod
        monkeypatch.setattr(mongo_mod, "get_mongo", lambda: mongo_db)
        ensure_indexes()
        info = mongo_db.raw_ingest.index_information()
        assert any("batch_id" in name for name in info), f"No batch_id index found: {list(info)}"

    def test_ensure_indexes_creates_donation_id_index_on_events(self, mongo_db, monkeypatch):
        import app.services.mongo as mongo_mod
        monkeypatch.setattr(mongo_mod, "get_mongo", lambda: mongo_db)
        ensure_indexes()
        info = mongo_db.events.index_information()
        assert any("donation_id" in name for name in info), f"No donation_id index found: {list(info)}"

    def test_ensure_indexes_tolerates_mongo_down(self, monkeypatch):
        import app.services.mongo as mongo_mod

        def _raise():
            raise ConnectionError("Mongo is unreachable")

        monkeypatch.setattr(mongo_mod, "get_mongo", _raise)
        ensure_indexes()  # must not raise
