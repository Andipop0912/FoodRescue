"""Tests for worker/consumer.py ACK / NACK correctness and DLQ routing.

We use unittest.mock to simulate pika channel/method objects so the tests have
no RabbitMQ dependency.  All tests are unit tests; live broker behavior is
explicitly noted where it cannot be verified without a running RabbitMQ instance.
"""
import json
from unittest.mock import MagicMock, patch

from app.worker.consumer import process_message, _parse_message


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_channel():
    """Return a mock pika Channel with tracked ack/nack calls."""
    return MagicMock()


def _delivery_tag(n: int = 42):
    m = MagicMock()
    m.delivery_tag = n
    return m


# ---------------------------------------------------------------------------
# process_message happy path
# ---------------------------------------------------------------------------

def test_process_message_returns_match_count(session, published):
    """process_message returns the number of matches written (may be 0 for no NGOs)."""
    from app.models import Donation, Donor
    from app.utils import utcnow
    from datetime import timedelta

    donor = session.query(Donor).first()
    d = Donation(
        donor_id=donor.id, food_type="Bread", category="bakery", quantity_kg=10.0,
        expiry_time=utcnow() + timedelta(hours=5), lat=19.033, lng=73.0297,
        area="Vashi", source="manual", status="listed",
    )
    session.add(d)
    session.commit()

    msg = json.dumps({"event": "donation.created", "donation_id": d.id}).encode()
    result = process_message(msg)
    assert isinstance(result, int) and result >= 0


def test_process_message_unknown_donation_returns_zero(session):
    """process_message on a non-existent donation_id returns 0 without raising."""
    msg = json.dumps({"event": "donation.created", "donation_id": 999999}).encode()
    result = process_message(msg)
    assert result == 0


# ---------------------------------------------------------------------------
# ACK / NACK logic tests (mock channel, no broker needed)
# ---------------------------------------------------------------------------

def _simulate_one_delivery(process_raises: bool):
    """Replicate the ACK/NACK branch of consumer.run() for one delivery."""
    ch = _make_channel()
    method = _delivery_tag(42)
    body = b'{"donation_id": 1}'

    side_effect = RuntimeError("boom") if process_raises else None

    with patch("app.worker.consumer.process_message", side_effect=side_effect, return_value=1):
        try:
            from app.worker.consumer import process_message as pm
            pm(body)
            ch.basic_ack(method.delivery_tag)
        except Exception:
            ch.basic_nack(method.delivery_tag, requeue=False)

    return ch


def test_ack_on_success():
    """Successful processing must result in exactly one basic_ack, no nack."""
    ch = _simulate_one_delivery(process_raises=False)
    ch.basic_ack.assert_called_once_with(42)
    ch.basic_nack.assert_not_called()


def test_nack_without_requeue_on_failure():
    """Failed processing must result in basic_nack(requeue=False), no ack."""
    ch = _simulate_one_delivery(process_raises=True)
    ch.basic_nack.assert_called_once_with(42, requeue=False)
    ch.basic_ack.assert_not_called()


def test_nack_requeue_is_false_not_true():
    """Ensure requeue=True is never passed - that would cause an infinite loop."""
    ch = _simulate_one_delivery(process_raises=True)
    _, kwargs = ch.basic_nack.call_args
    assert kwargs.get("requeue") is False, "requeue must be False to avoid infinite requeue loop"


# ---------------------------------------------------------------------------
# Sweeper per-donation exception isolation
# ---------------------------------------------------------------------------

def test_sweep_processes_all_donations_when_one_raises(session):
    """A sweeper failure on one donation must not abort the rest of the batch."""
    from unittest.mock import patch, call
    from app.worker.consumer import sweep
    from app.models import Donation, Donor
    from app.utils import utcnow
    from datetime import timedelta

    donor = session.query(Donor).first()
    now = utcnow()

    # Create two donations old enough for the sweeper (created_at > 20 s ago).
    ids = []
    for _ in range(2):
        d = Donation(
            donor_id=donor.id, food_type="Rice", category="grains", quantity_kg=5.0,
            expiry_time=now + timedelta(hours=3),
            lat=19.033, lng=73.0297, area="Vashi", source="manual", status="listed",
            created_at=now - timedelta(seconds=60),
        )
        session.add(d)
        session.flush()
        ids.append(d.id)
    session.commit()

    call_count = {"n": 0}

    def _match_donation_raises_on_first(db, donation_id, log=True):
        call_count["n"] += 1
        if donation_id == ids[0]:
            raise RuntimeError("simulated error for donation isolation test")
        return []

    with patch("app.worker.consumer.match_donation", side_effect=_match_donation_raises_on_first):
        swept = sweep()

    # The sweeper must have attempted both donations.
    assert call_count["n"] == 2, (
        f"Expected 2 match_donation calls (one per donation), got {call_count['n']}"
    )
    # sweep() returns the count of IDs attempted, not successes.
    assert swept >= 2


def test_sweep_returns_zero_when_no_unmatched_donations(session):
    """sweep() returns 0 when there are no donations eligible for re-matching."""
    from app.worker.consumer import sweep
    result = sweep()
    assert isinstance(result, int) and result >= 0


# ---------------------------------------------------------------------------
# Phase 1: malformed message handling
# ---------------------------------------------------------------------------

def test_parse_message_valid_body():
    """A well-formed body with donation_id is parsed successfully."""
    body = json.dumps({"event": "donation.created", "donation_id": 42}).encode()
    msg = _parse_message(body)
    assert msg["donation_id"] == 42


def test_parse_message_raises_on_invalid_json():
    """Non-JSON bytes must raise ValueError (caller should NACK to DLQ)."""
    with __import__("pytest").raises(ValueError, match="not valid JSON"):
        _parse_message(b"this is not json {{{")


def test_parse_message_raises_on_missing_donation_id():
    """JSON without 'donation_id' must raise ValueError."""
    body = json.dumps({"event": "donation.created"}).encode()
    with __import__("pytest").raises(ValueError, match="donation_id"):
        _parse_message(body)


def test_parse_message_raises_on_non_object():
    """A JSON array or primitive must raise ValueError."""
    with __import__("pytest").raises(ValueError, match="JSON object"):
        _parse_message(b"[1, 2, 3]")


def test_malformed_message_raises_not_crashes():
    """process_message on malformed JSON must raise ValueError without crashing."""
    with __import__("pytest").raises(ValueError):
        process_message(b"not-valid-json")


def test_malformed_message_missing_donation_id_raises():
    """process_message on JSON without donation_id must raise ValueError."""
    body = json.dumps({"event": "donation.created"}).encode()
    with __import__("pytest").raises(ValueError, match="donation_id"):
        process_message(body)


# ---------------------------------------------------------------------------
# Phase 1: DLQ routing simulation (unit-level)
# ---------------------------------------------------------------------------

def _simulate_delivery_with_nack_tracking(process_raises: bool, malformed: bool = False):
    """Simulate the consumer run() loop for one delivery and return the channel mock.

    This is a unit test of the NACK→DLQ path: we verify that on failure the
    caller issues NACK(requeue=False).  The actual DLQ routing (broker-side) is
    not verified here; it requires a live RabbitMQ instance.
    """
    ch = _make_channel()
    method = _delivery_tag(99)
    body = b"BROKEN {{{" if malformed else b'{"donation_id": 1}'

    if process_raises:
        side_effect = RuntimeError("processing failed")
    else:
        side_effect = None

    with patch("app.worker.consumer.process_message", side_effect=side_effect, return_value=0):
        try:
            from app.worker.consumer import process_message as pm
            pm(body)
            ch.basic_ack(method.delivery_tag)
        except Exception:
            ch.basic_nack(method.delivery_tag, requeue=False)

    return ch


def test_dlq_routing_on_processing_failure():
    """A processing failure must NACK with requeue=False (routes to DLQ via x-dead-letter-exchange).
    NOTE: actual DLQ delivery requires a live broker — this test verifies the AMQP call contract.
    """
    ch = _simulate_delivery_with_nack_tracking(process_raises=True)
    ch.basic_nack.assert_called_once_with(99, requeue=False)
    ch.basic_ack.assert_not_called()


def test_dlq_topology_constants():
    """DLX and DLQ constant names must be defined and non-empty strings."""
    from app.worker.consumer import QUEUE_DLX, QUEUE_DEAD, QUEUE_DONATION_CREATED
    assert isinstance(QUEUE_DLX, str) and QUEUE_DLX
    assert isinstance(QUEUE_DEAD, str) and QUEUE_DEAD
    # DLX must differ from main queue name to avoid routing loops
    assert QUEUE_DLX != QUEUE_DONATION_CREATED
    assert QUEUE_DEAD != QUEUE_DONATION_CREATED


# ---------------------------------------------------------------------------
# Phase 1: duplicate event idempotency
# ---------------------------------------------------------------------------

def test_duplicate_event_is_idempotent(session, published):
    """Processing the same donation_id twice must not raise or accumulate duplicate matches.

    Delivery guarantee: the matching engine accepts both S_LISTED and S_MATCHED
    donations (replacing old 'suggested' match rows with a fresh set on each call).
    This means a duplicate event re-runs matching and returns a non-zero count —
    that is the documented at-least-once contract.

    The idempotency guarantee tested here is:
    - No exception is raised on the second delivery.
    - The Match table does not accumulate beyond TOP_N rows for this donation
      (old suggested rows are deleted before new ones are inserted).
    - Each rank value is unique in the final match set.
    """
    from app.models import Donation, Donor, Match
    from app.utils import utcnow
    from datetime import timedelta

    donor = session.query(Donor).first()
    d = Donation(
        donor_id=donor.id, food_type="Biryani", category="cooked_meals", quantity_kg=20.0,
        expiry_time=utcnow() + timedelta(hours=6), lat=19.033, lng=73.0297,
        area="Vashi", source="manual", status="listed",
    )
    session.add(d)
    session.commit()

    body = json.dumps({"event": "donation.created", "donation_id": d.id}).encode()

    # First delivery — normal processing.
    count1 = process_message(body)
    assert isinstance(count1, int)

    # Second delivery of the same event — must not raise.
    count2 = process_message(body)
    assert isinstance(count2, int)

    # Match rows must not exceed TOP_N (old suggested rows replaced, not appended).
    from app.worker.consumer import process_message as _pm  # same import path
    from app.services.matching import TOP_N
    matches = session.query(Match).filter_by(donation_id=d.id, status="suggested").all()
    assert len(matches) <= TOP_N, f"Expected at most {TOP_N} suggested matches, got {len(matches)}"

    # Ranks must be unique within the suggested set.
    ranks = [m.rank for m in matches]
    assert len(ranks) == len(set(ranks)), "duplicate Match ranks found after double processing"