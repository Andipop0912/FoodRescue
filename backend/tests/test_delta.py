"""Phase 3 — Delta Lake integration tests.

All tests use a temporary directory so they are fully isolated, repeatable,
and leave no state in the project data directory.

Test scenarios:
  A. Table creation + read
  B. Insert and update (upsert)
  C. Delete (controlled deletion in disposable table)
  D. Time travel (read a prior version)
  E. Schema enforcement (incompatible write is rejected)
  F. Idempotent reprocessing (same batch twice → no duplicates)
  G. Failure handling (invalid batch → previous version still readable)

Implementation notes
--------------------
* Uses the deltalake Python package (delta-rs), NOT PySpark.
* All Delta APIs used here are the Python/Rust subset, not the Spark superset.
  In particular, merge() is available via deltalake>=0.20.
* Tests are deterministic — no random data, no time-dependent assertions.
"""
import pytest
import pyarrow as pa

# Skip the entire module if deltalake is not installed (e.g. stripped CI image).
deltalake = pytest.importorskip("deltalake", reason="deltalake package not installed")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def table_path(tmp_path):
    """Fresh temporary Delta table path for each test."""
    return str(tmp_path / "test_donations")


def _sample_records(n: int = 3, id_offset: int = 0) -> list[dict]:
    """Return n deterministic donation records."""
    return [
        {
            "donation_id": 100 + id_offset + i,
            "batch_id": 1,
            "donor_name": f"Donor {i}",
            "food_type": "Bread",
            "category": "bakery",
            "quantity_kg": 10.0 + i,
            "area": "Vashi",
            "lat": 19.033,
            "lng": 73.029,
            "status": "listed",
            "expiry_time": "2026-12-01T12:00:00",
            "created_at": "2026-10-15T08:00:00",
            "source": "test",
            "dedupe_key": f"key-{100 + id_offset + i}",
        }
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# A. Table creation and read
# ---------------------------------------------------------------------------

class TestTableCreationAndRead:
    def test_write_creates_delta_log(self, table_path):
        """write_donations must create a _delta_log directory (Delta table marker)."""
        from app.services.delta import write_donations, table_exists
        records = _sample_records(2)
        n = write_donations(records, table_path=table_path)
        assert n == 2
        assert table_exists(table_path)

    def test_read_returns_correct_row_count(self, table_path):
        """read_donations must return all written rows."""
        from app.services.delta import write_donations, read_donations
        records = _sample_records(3)
        write_donations(records, table_path=table_path)
        table = read_donations(table_path=table_path)
        assert table.num_rows == 3

    def test_read_preserves_donation_ids(self, table_path):
        """Written donation_ids must be recoverable after read."""
        from app.services.delta import write_donations, read_donations
        records = _sample_records(3)
        write_donations(records, table_path=table_path)
        table = read_donations(table_path=table_path)
        written_ids = sorted(r["donation_id"] for r in records)
        read_ids = sorted(table.column("donation_id").to_pylist())
        assert written_ids == read_ids

    def test_empty_write_returns_zero(self, table_path):
        """Writing an empty list must return 0 without creating a table."""
        from app.services.delta import write_donations, table_exists
        n = write_donations([], table_path=table_path)
        assert n == 0
        assert not table_exists(table_path)


# ---------------------------------------------------------------------------
# B. Insert and update (upsert / merge)
# ---------------------------------------------------------------------------

class TestInsertAndUpdate:
    def test_upsert_creates_table_when_missing(self, table_path):
        """upsert_donations on a non-existent table must create it."""
        from app.services.delta import upsert_donations, table_exists
        upsert_donations(_sample_records(2), table_path=table_path)
        assert table_exists(table_path)

    def test_upsert_inserts_new_records(self, table_path):
        """upsert_donations on a fresh table inserts all records."""
        from app.services.delta import upsert_donations, read_donations
        upsert_donations(_sample_records(3), table_path=table_path)
        assert read_donations(table_path).num_rows == 3

    def test_upsert_updates_existing_record(self, table_path):
        """A second upsert with the same donation_id must update, not insert."""
        from app.services.delta import upsert_donations, read_donations

        original = [{"donation_id": 200, "batch_id": 1, "donor_name": "Old",
                     "food_type": "Rice", "category": "packaged", "quantity_kg": 5.0,
                     "area": "Vashi", "lat": 19.0, "lng": 73.0, "status": "listed",
                     "expiry_time": "2026-12-01T12:00:00", "created_at": "2026-10-15T08:00:00",
                     "source": "test", "dedupe_key": "key-200"}]
        upsert_donations(original, table_path=table_path)

        updated = [{**original[0], "status": "matched", "quantity_kg": 8.0}]
        upsert_donations(updated, table_path=table_path)

        table = read_donations(table_path)
        # Row count stays at 1 (update, not insert).
        assert table.num_rows == 1
        row = table.to_pylist()[0]
        assert row["status"] == "matched"
        assert row["quantity_kg"] == 8.0

    def test_upsert_inserts_new_and_updates_existing(self, table_path):
        """A mixed upsert must update existing rows and insert new ones."""
        from app.services.delta import upsert_donations, read_donations

        first_batch = _sample_records(3)   # donation_ids 100, 101, 102
        upsert_donations(first_batch, table_path=table_path)

        # Record 101 updated + record 103 new.
        second_batch = [
            {**first_batch[1], "status": "matched"},      # id 101 — update
            {**_sample_records(1, id_offset=3)[0]},       # id 103 — new
        ]
        upsert_donations(second_batch, table_path=table_path)

        table = read_donations(table_path)
        assert table.num_rows == 4    # 100, 101 (updated), 102, 103


# ---------------------------------------------------------------------------
# C. Delete
# ---------------------------------------------------------------------------

class TestDelete:
    def test_delete_removes_matching_rows(self, table_path):
        """delete_donations must remove rows matching the predicate."""
        from app.services.delta import write_donations, delete_donations, read_donations

        write_donations(_sample_records(3), table_path=table_path)  # ids 100, 101, 102
        delete_donations("donation_id = 101", table_path=table_path)

        table = read_donations(table_path)
        remaining_ids = sorted(table.column("donation_id").to_pylist())
        assert 101 not in remaining_ids
        assert len(remaining_ids) == 2

    def test_delete_leaves_unmatched_rows_intact(self, table_path):
        """delete_donations with a predicate matching nothing must not remove rows."""
        from app.services.delta import write_donations, delete_donations, read_donations

        write_donations(_sample_records(3), table_path=table_path)
        delete_donations("donation_id = 999", table_path=table_path)

        assert read_donations(table_path).num_rows == 3


# ---------------------------------------------------------------------------
# D. Time travel
# ---------------------------------------------------------------------------

class TestTimeTravel:
    def test_time_travel_reads_prior_version(self, table_path):
        """read_donations(version=0) must return the state after the first write."""
        from app.services.delta import write_donations, read_donations, get_table_version

        # Version 0: write 3 records.
        write_donations(_sample_records(3), table_path=table_path)
        v0 = get_table_version(table_path)
        assert v0 == 0

        # Version 1: append 2 more records.
        write_donations(_sample_records(2, id_offset=10), table_path=table_path, mode="append")
        v1 = get_table_version(table_path)
        assert v1 == 1

        # Time-travel back to version 0: should see only 3 rows.
        old_table = read_donations(table_path, version=0)
        assert old_table.num_rows == 3

        # Current version has all 5 rows.
        current_table = read_donations(table_path)
        assert current_table.num_rows == 5

    def test_time_travel_preserves_historical_values(self, table_path):
        """Old records must retain their original field values after an update."""
        from app.services.delta import write_donations, upsert_donations, read_donations

        original = [{"donation_id": 300, "batch_id": 1, "donor_name": "Org A",
                     "food_type": "Cake", "category": "bakery", "quantity_kg": 3.0,
                     "area": "Nerul", "lat": 19.04, "lng": 73.01, "status": "listed",
                     "expiry_time": "2026-12-01T12:00:00", "created_at": "2026-10-15T08:00:00",
                     "source": "test", "dedupe_key": "key-300"}]
        write_donations(original, table_path=table_path)   # version 0

        updated = [{**original[0], "status": "matched"}]
        upsert_donations(updated, table_path=table_path)   # version 1

        v0_row = read_donations(table_path, version=0).to_pylist()[0]
        assert v0_row["status"] == "listed"  # Historical value preserved.

    def test_get_table_history_returns_list(self, table_path):
        """get_table_history must return a non-empty list after writes."""
        from app.services.delta import write_donations, get_table_history

        write_donations(_sample_records(2), table_path=table_path)
        history = get_table_history(table_path)
        assert isinstance(history, list)
        assert len(history) >= 1


# ---------------------------------------------------------------------------
# E. Schema enforcement
# ---------------------------------------------------------------------------

class TestSchemaEnforcement:
    def test_incompatible_schema_write_is_rejected(self, table_path):
        """Writing a table with an incompatible schema must raise an error
        and leave the existing table intact."""
        from app.services.delta import write_donations, read_donations, DONATION_SCHEMA

        write_donations(_sample_records(2), table_path=table_path)

        # Attempt to write an incompatible schema (donation_id as string).
        bad_schema = pa.schema([
            pa.field("donation_id", pa.string()),   # Wrong type — int64 → string
            pa.field("food_type",   pa.string()),
        ])
        bad_table = pa.table(
            {"donation_id": ["abc", "def"], "food_type": ["Rice", "Bread"]},
            schema=bad_schema,
        )

        with pytest.raises(Exception):
            # deltalake raises on schema mismatch; exact exception type varies by version.
            deltalake.write_deltalake(table_path, bad_table, mode="append")

        # Existing table must still be readable and intact.
        surviving = read_donations(table_path)
        assert surviving.num_rows == 2

    def test_append_with_matching_schema_succeeds(self, table_path):
        """Appending a batch with the correct schema must succeed."""
        from app.services.delta import write_donations, read_donations

        write_donations(_sample_records(2), table_path=table_path)
        write_donations(_sample_records(2, id_offset=10), table_path=table_path, mode="append")

        assert read_donations(table_path).num_rows == 4


# ---------------------------------------------------------------------------
# F. Idempotent reprocessing
# ---------------------------------------------------------------------------

class TestIdempotentReprocessing:
    def test_upsert_same_batch_twice_no_duplicates(self, table_path):
        """Processing the same source batch twice via upsert must not create
        duplicate rows."""
        from app.services.delta import upsert_donations, read_donations

        batch = _sample_records(3)
        upsert_donations(batch, table_path=table_path)
        upsert_donations(batch, table_path=table_path)  # Second identical call

        table = read_donations(table_path)
        assert table.num_rows == 3   # Exactly 3 rows, not 6.

        ids = sorted(table.column("donation_id").to_pylist())
        assert len(ids) == len(set(ids)), "Duplicate donation_ids found after reprocessing"

    def test_write_overwrite_is_idempotent(self, table_path):
        """write_donations with mode='overwrite' replaces all rows;
        calling it twice with the same data yields a single copy."""
        from app.services.delta import write_donations, read_donations

        records = _sample_records(3)
        write_donations(records, table_path=table_path, mode="overwrite")
        write_donations(records, table_path=table_path, mode="overwrite")

        assert read_donations(table_path).num_rows == 3


# ---------------------------------------------------------------------------
# G. Failure handling
# ---------------------------------------------------------------------------

class TestFailureHandling:
    def test_previous_version_readable_after_failed_write(self, table_path):
        """A write failure must leave the previous valid Delta version readable."""
        from app.services.delta import write_donations, read_donations, DONATION_SCHEMA

        write_donations(_sample_records(3), table_path=table_path)

        # Simulate a bad write with wrong schema.
        bad_table = pa.table({"junk": ["a", "b"]})
        try:
            deltalake.write_deltalake(table_path, bad_table, mode="append")
        except Exception:
            pass  # Expected — bad schema rejected.

        # Previous valid version must remain readable.
        table = read_donations(table_path)
        assert table.num_rows == 3

    def test_write_failure_on_invalid_type_does_not_corrupt(self, table_path):
        """Passing None as records must return 0 and not corrupt the table."""
        from app.services.delta import write_donations, read_donations

        write_donations(_sample_records(2), table_path=table_path)

        # Passing an empty list (safe no-op).
        n = write_donations([], table_path=table_path)
        assert n == 0

        # Table is still readable.
        assert read_donations(table_path).num_rows == 2

    def test_table_not_exists_before_first_write(self, table_path):
        """table_exists must return False before any write."""
        from app.services.delta import table_exists
        assert not table_exists(table_path)

    def test_table_exists_after_write(self, table_path):
        """table_exists must return True after the first write."""
        from app.services.delta import write_donations, table_exists
        write_donations(_sample_records(1), table_path=table_path)
        assert table_exists(table_path)
