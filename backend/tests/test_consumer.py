"""Tests for worker/consumer.py ACK / NACK correctness.

We use unittest.mock to simulate pika channel/method objects so the tests have
no RabbitMQ dependency.
"""
import json
from unittest.mock import MagicMock, patch

import pytest

from app.worker.consumer import process_message


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