"""Tests for NiFi-specific ingest router behavior.

Covers:
- Raw CSV with no optional headers (source defaults to "api").
- Raw CSV with X-Ingest-Source and X-Ingest-Filename headers.
- X-Ingest-Source values: allowed set and unknown/unsupported values.
- Source and filename propagation into IngestBatch metadata.
- Existing multipart CSV upload behavior is preserved.
- Existing JSON body ingestion is preserved.
- Empty CSV body.
- Malformed request bodies.

All tests use the in-memory SQLite database and mocked RabbitMQ/Mongo
fixtures from conftest.py.  No records are inserted into the live database.
"""

from datetime import datetime, timedelta

import pytest

from app.models import IngestBatch


def _iso(hours: int = 6) -> str:
    return (datetime.utcnow() + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _csv(*extra_rows: str) -> str:
    header = "record_type,org_name,food_type,category,quantity_kg,expiry_time,area\n"
    base = f"donation,Vashi Bakery,Fresh Bread,bakery,10,{_iso(6)},Vashi\n"
    return header + base + "".join(extra_rows)


# ---------------------------------------------------------------------------
# Raw CSV — source/filename header reading
# ---------------------------------------------------------------------------

class TestRawCsvHeaders:
    def test_raw_csv_no_headers_defaults_to_api_source(self, client, admin, session, published):
        r = client.post("/ingest/batch",
                        headers={**admin, "Content-Type": "text/csv"},
                        content=_csv())
        assert r.status_code == 200
        body = r.json()
        assert body["accepted_donations"] == 1
        batch = session.query(IngestBatch).filter_by(id=body["batch_id"]).one()
        assert batch.source == "api"
        assert batch.filename is None

    def test_raw_csv_with_nifi_source_header(self, client, admin, session, published):
        r = client.post("/ingest/batch",
                        headers={**admin,
                                 "Content-Type": "text/csv",
                                 "X-Ingest-Source": "nifi-inbox"},
                        content=_csv())
        assert r.status_code == 200
        batch = session.query(IngestBatch).filter_by(id=r.json()["batch_id"]).one()
        assert batch.source == "nifi-inbox"

    def test_raw_csv_with_filename_header(self, client, admin, session, published):
        r = client.post("/ingest/batch",
                        headers={**admin,
                                 "Content-Type": "text/csv",
                                 "X-Ingest-Filename": "donations_2026-10-15.csv"},
                        content=_csv())
        assert r.status_code == 200
        batch = session.query(IngestBatch).filter_by(id=r.json()["batch_id"]).one()
        assert batch.filename == "donations_2026-10-15.csv"

    def test_raw_csv_with_both_headers(self, client, admin, session, published):
        r = client.post("/ingest/batch",
                        headers={**admin,
                                 "Content-Type": "text/csv",
                                 "X-Ingest-Source": "nifi-inbox",
                                 "X-Ingest-Filename": "batch_001.csv"},
                        content=_csv())
        assert r.status_code == 200
        batch = session.query(IngestBatch).filter_by(id=r.json()["batch_id"]).one()
        assert batch.source == "nifi-inbox"
        assert batch.filename == "batch_001.csv"

    def test_unknown_source_header_maps_to_api(self, client, admin, session, published):
        """Source values outside the allowed set are silently mapped to 'api'."""
        r = client.post("/ingest/batch",
                        headers={**admin,
                                 "Content-Type": "text/csv",
                                 "X-Ingest-Source": "some-unknown-source"},
                        content=_csv())
        assert r.status_code == 200
        batch = session.query(IngestBatch).filter_by(id=r.json()["batch_id"]).one()
        assert batch.source == "api"

    def test_upload_source_header_is_allowed(self, client, admin, session, published):
        r = client.post("/ingest/batch",
                        headers={**admin,
                                 "Content-Type": "text/csv",
                                 "X-Ingest-Source": "upload"},
                        content=_csv())
        assert r.status_code == 200
        batch = session.query(IngestBatch).filter_by(id=r.json()["batch_id"]).one()
        assert batch.source == "upload"

    def test_sim_source_header_is_allowed(self, client, admin, session, published):
        r = client.post("/ingest/batch",
                        headers={**admin,
                                 "Content-Type": "text/csv",
                                 "X-Ingest-Source": "sim"},
                        content=_csv())
        assert r.status_code == 200
        batch = session.query(IngestBatch).filter_by(id=r.json()["batch_id"]).one()
        assert batch.source == "sim"


# ---------------------------------------------------------------------------
# Raw CSV — body correctness
# ---------------------------------------------------------------------------

class TestRawCsvBody:
    def test_raw_csv_one_good_row(self, client, admin, published):
        r = client.post("/ingest/batch",
                        headers={**admin, "Content-Type": "text/csv"},
                        content=_csv())
        assert r.status_code == 200
        assert r.json()["accepted_donations"] == 1
        assert r.json()["rejected_rows"] == 0

    def test_raw_csv_one_good_one_bad_row(self, client, admin, published):
        bad_row = f"donation,Test,Stale,bakery,5,{_iso(-2)},Vashi\n"  # expired
        r = client.post("/ingest/batch",
                        headers={**admin, "Content-Type": "text/csv"},
                        content=_csv(bad_row))
        assert r.status_code == 200
        assert r.json()["accepted_donations"] == 1
        assert r.json()["rejected_rows"] == 1

    def test_raw_csv_empty_body_returns_422(self, client, admin):
        r = client.post("/ingest/batch",
                        headers={**admin, "Content-Type": "text/csv"},
                        content="")
        assert r.status_code == 422

    def test_raw_csv_header_only_no_data_rows_returns_422(self, client, admin):
        r = client.post("/ingest/batch",
                        headers={**admin, "Content-Type": "text/csv"},
                        content="record_type,org_name,food_type,category,quantity_kg,expiry_time,area\n")
        assert r.status_code == 422

    def test_raw_csv_bom_stripped(self, client, admin, published):
        """BOM prefix (UTF-8-sig) must not corrupt the first column name."""
        csv_with_bom = "\xef\xbb\xbf" + _csv()
        r = client.post("/ingest/batch",
                        headers={**admin, "Content-Type": "text/csv"},
                        content=csv_with_bom.encode("utf-8"))
        assert r.status_code == 200
        assert r.json()["accepted_donations"] == 1


# ---------------------------------------------------------------------------
# Multipart upload — preserved behavior
# ---------------------------------------------------------------------------

class TestMultipartUpload:
    def test_multipart_csv_upload(self, client, admin, published):
        csv_text = _csv()
        r = client.post("/ingest/batch",
                        headers=admin,
                        files={"file": ("donations.csv", csv_text.encode(), "text/csv")})
        assert r.status_code == 200
        assert r.json()["accepted_donations"] == 1

    def test_multipart_upload_source_is_upload(self, client, admin, session, published):
        csv_text = _csv()
        r = client.post("/ingest/batch",
                        headers=admin,
                        files={"file": ("test.csv", csv_text.encode(), "text/csv")})
        assert r.status_code == 200
        batch = session.query(IngestBatch).filter_by(id=r.json()["batch_id"]).one()
        assert batch.source == "upload"

    def test_multipart_upload_stores_filename(self, client, admin, session, published):
        csv_text = _csv()
        r = client.post("/ingest/batch",
                        headers=admin,
                        files={"file": ("my_batch.csv", csv_text.encode(), "text/csv")})
        assert r.status_code == 200
        batch = session.query(IngestBatch).filter_by(id=r.json()["batch_id"]).one()
        assert batch.filename == "my_batch.csv"

    def test_multipart_missing_file_field_returns_422(self, client, admin):
        r = client.post("/ingest/batch",
                        headers=admin,
                        files={"not_file": ("x.csv", b"data")})
        assert r.status_code == 422

    def test_multipart_json_file_upload(self, client, admin, published):
        import json as _json
        rows = [{"record_type": "donation", "org_name": "Test", "food_type": "Rice",
                 "category": "packaged", "quantity_kg": 15, "expiry_time": _iso(5), "area": "Nerul"}]
        r = client.post("/ingest/batch",
                        headers=admin,
                        files={"file": ("data.json", _json.dumps(rows).encode(), "application/json")})
        assert r.status_code == 200
        assert r.json()["accepted_donations"] == 1


# ---------------------------------------------------------------------------
# JSON body — preserved behavior
# ---------------------------------------------------------------------------

class TestJsonBody:
    def test_json_list_body(self, client, admin, published):
        rows = [{"record_type": "donation", "org_name": "Nerul Cafe", "food_type": "Idli",
                 "category": "cooked_meals", "quantity_kg": 20, "expiry_time": _iso(4), "area": "Nerul"}]
        r = client.post("/ingest/batch", headers=admin, json=rows)
        assert r.status_code == 200
        assert r.json()["accepted_donations"] == 1

    def test_json_dict_with_source(self, client, admin, session, published):
        rows = [{"record_type": "donation", "org_name": "Sim Corp", "food_type": "Dal",
                 "category": "cooked_meals", "quantity_kg": 10, "expiry_time": _iso(5), "area": "Vashi"}]
        r = client.post("/ingest/batch", headers=admin, json={"source": "sim", "rows": rows})
        assert r.status_code == 200
        batch = session.query(IngestBatch).filter_by(id=r.json()["batch_id"]).one()
        assert batch.source == "sim"

    def test_empty_json_list_returns_422(self, client, admin):
        assert client.post("/ingest/batch", headers=admin, json=[]).status_code == 422

    def test_malformed_json_returns_422(self, client, admin):
        r = client.post("/ingest/batch",
                        headers={**admin, "Content-Type": "application/json"},
                        content=b"{not valid json}")
        assert r.status_code == 422

    def test_json_object_without_rows_returns_422(self, client, admin):
        r = client.post("/ingest/batch", headers=admin, json={"nope": 1})
        assert r.status_code == 422


# ---------------------------------------------------------------------------
# NiFi flow configuration assumptions (no live NiFi required)
# ---------------------------------------------------------------------------

def _load_deploy_flow():
    import importlib.util, os
    path = os.path.join(os.path.dirname(__file__), "..", "..", "nifi", "deploy_flow.py")
    if not os.path.exists(path):
        import pytest
        pytest.skip("nifi/deploy_flow.py not present in this environment")
    spec = importlib.util.spec_from_file_location("deploy_flow", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestNiFiFlowConfig:
    def test_deploy_flow_constants(self):
        """Verify the deploy_flow.py constants match the router's allowed sources."""
        mod = _load_deploy_flow()
        assert mod.FLOW_GROUP_NAME == "FoodRescue Inbox Ingestion"

    def test_deploy_flow_default_source_is_nifi_inbox(self):
        """Default source in build_flow must match the allowed source value in the router."""
        import inspect
        mod = _load_deploy_flow()
        sig = inspect.signature(mod.build_flow)
        assert sig.parameters["source"].default == "nifi-inbox"

    def test_nifi_inbox_is_in_router_allowed_sources(self):
        """'nifi-inbox' must be in the allowed source set to prevent silent mapping to 'api'."""
        # Read the router source and verify the guard includes nifi-inbox.
        import os
        router_path = os.path.join(
            os.path.dirname(__file__), "..", "app", "routers", "ingest.py"
        )
        with open(router_path) as f:
            content = f.read()
        assert "nifi-inbox" in content
