"""Tests for the DLQ replay utility (scripts/replay_dlq.py).

All tests use unittest.mock to simulate pika broker interactions — no live
RabbitMQ broker is required.

Test scenarios:
1. Successful replay — valid message ACKed after confirmed republish.
2. Publish failure — DLQ message NACKed with requeue=True on transient error.
3. Malformed message (invalid JSON) — moved to unroutable queue, DLQ ACKed.
4. Malformed message (missing donation_id) — moved to unroutable queue.
5. Duplicate event — republished message is idempotent in the consumer.
6. ACK ordering — ACK only fires after confirm, never before.
7. Dry-run — no queue modifications, messages left in DLQ.
8. Queue drain — processes all messages up to max_messages limit.
9. Empty DLQ — terminates immediately with zero counts.

NOTE: These are unit tests of the replay logic.  Live broker behavior
(actual DLQ routing, persistent delivery, broker restart recovery) requires
a running RabbitMQ instance and is not verified here.
"""

import json
import sys
import os
from unittest.mock import MagicMock, patch, call
import pytest

# Add scripts/ to path so we can import replay_dlq directly.
_SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from replay_dlq import (
    parse_and_validate,
    replay_dlq,
    QUEUE_DLQ,
    QUEUE_MAIN,
    QUEUE_UNROUTABLE,
    ReplayStats,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_method(tag: int = 1):
    m = MagicMock()
    m.delivery_tag = tag
    return m


def _make_props(headers: dict | None = None):
    p = MagicMock()
    p.headers = headers
    p.content_type = "application/json"
    return p


def _valid_body(donation_id: int = 42) -> bytes:
    return json.dumps({"event": "donation.created", "donation_id": donation_id}).encode()


def _make_channel_with_messages(messages: list[tuple]):
    """Return a mock channel whose basic_get returns messages from the list, then (None, None, None)."""
    ch = MagicMock()
    returns = list(messages) + [(None, None, None)]
    ch.basic_get.side_effect = returns
    return ch


def _patched_connection(ch):
    """Context manager: patch pika.BlockingConnection to return ch."""
    conn_mock = MagicMock()
    conn_mock.__enter__ = MagicMock(return_value=conn_mock)
    conn_mock.__exit__ = MagicMock(return_value=False)
    conn_mock.channel.return_value = ch

    def _make_conn(params):
        return conn_mock

    return patch("pika.BlockingConnection", side_effect=_make_conn)


# ---------------------------------------------------------------------------
# parse_and_validate unit tests
# ---------------------------------------------------------------------------

class TestParseAndValidate:
    def test_valid_message(self):
        body = _valid_body(42)
        r = parse_and_validate(body)
        assert r.valid is True
        assert r.msg["donation_id"] == 42
        assert r.error == ""

    def test_invalid_json(self):
        r = parse_and_validate(b"not json {{{")
        assert r.valid is False
        assert "JSON" in r.error

    def test_missing_donation_id(self):
        body = json.dumps({"event": "donation.created"}).encode()
        r = parse_and_validate(body)
        assert r.valid is False
        assert "donation_id" in r.error

    def test_non_object_json(self):
        r = parse_and_validate(b"[1, 2, 3]")
        assert r.valid is False

    def test_empty_body(self):
        r = parse_and_validate(b"")
        assert r.valid is False

    def test_unicode_decode_error(self):
        r = parse_and_validate(b"\xff\xfe invalid bytes")
        assert r.valid is False


# ---------------------------------------------------------------------------
# replay_dlq integration tests (mocked broker)
# ---------------------------------------------------------------------------

class TestSuccessfulReplay:
    def test_valid_message_is_replayed_and_acked(self):
        """A valid DLQ message must be republished to QUEUE_MAIN and ACKed."""
        method = _make_method(tag=7)
        props = _make_props()
        body = _valid_body(99)
        ch = _make_channel_with_messages([(method, props, body)])

        with _patched_connection(ch):
            stats = replay_dlq("amqp://localhost/", max_messages=1)

        # Verify republish to main queue.
        publish_calls = [c for c in ch.basic_publish.call_args_list
                         if c.kwargs.get("routing_key") == QUEUE_MAIN
                         or (c.args and c.args[0] == "" and len(c.args) > 1 and c.args[1] == QUEUE_MAIN)
                         or (c.kwargs.get("routing_key") == QUEUE_MAIN)]
        # Check using keyword or positional argument
        all_rkeys = []
        for c in ch.basic_publish.call_args_list:
            rk = c.kwargs.get("routing_key") or (c.args[1] if len(c.args) > 1 else None)
            all_rkeys.append(rk)
        assert QUEUE_MAIN in all_rkeys, f"Expected publish to {QUEUE_MAIN}, got: {all_rkeys}"

        # ACK must have been called.
        ch.basic_ack.assert_called_with(7)
        ch.basic_nack.assert_not_called()

        assert stats.replayed == 1
        assert stats.failed == 0
        assert stats.unroutable == 0

    def test_replayed_message_includes_traceability_header(self):
        """Replayed messages must carry x-replayed-from header."""
        method = _make_method(tag=1)
        props = _make_props(headers={})
        body = _valid_body(10)
        ch = _make_channel_with_messages([(method, props, body)])

        with _patched_connection(ch):
            replay_dlq("amqp://localhost/", max_messages=1)

        # Find the publish call to QUEUE_MAIN.
        for c in ch.basic_publish.call_args_list:
            rk = c.kwargs.get("routing_key") or (c.args[1] if len(c.args) > 1 else None)
            if rk == QUEUE_MAIN:
                pub_props = c.kwargs.get("properties") or (c.args[2] if len(c.args) > 2 else None)
                if pub_props:
                    assert pub_props.headers.get("x-replayed-from") == QUEUE_DLQ
                return
        pytest.fail("No publish call to QUEUE_MAIN found")


class TestPublishFailure:
    def test_publish_failure_nacks_with_requeue(self):
        """If republishing fails, the DLQ message must be NACKed with requeue=True."""
        method = _make_method(tag=5)
        props = _make_props()
        body = _valid_body(55)
        ch = _make_channel_with_messages([(method, props, body)])
        ch.basic_publish.side_effect = RuntimeError("broker refused message")

        with _patched_connection(ch):
            stats = replay_dlq("amqp://localhost/", max_messages=1)

        ch.basic_nack.assert_called_with(5, requeue=True)
        ch.basic_ack.assert_not_called()
        assert stats.failed == 1
        assert stats.replayed == 0

    def test_ack_never_called_before_publish_succeeds(self):
        """ACK must never be issued before publish confirmation."""
        method = _make_method(tag=3)
        props = _make_props()
        body = _valid_body(33)
        ch = _make_channel_with_messages([(method, props, body)])

        call_order = []
        def _track_publish(**kwargs):
            call_order.append("publish")
        def _track_ack(tag):
            call_order.append("ack")

        ch.basic_publish.side_effect = _track_publish
        ch.basic_ack.side_effect = _track_ack

        with _patched_connection(ch):
            replay_dlq("amqp://localhost/", max_messages=1)

        assert call_order.index("publish") < call_order.index("ack"), (
            "ACK was issued before or without a successful publish"
        )


class TestMalformedMessages:
    def test_invalid_json_moves_to_unroutable(self):
        """A non-JSON message must be moved to QUEUE_UNROUTABLE, not retried."""
        method = _make_method(tag=11)
        props = _make_props()
        body = b"NOT JSON"
        ch = _make_channel_with_messages([(method, props, body)])

        with _patched_connection(ch):
            stats = replay_dlq("amqp://localhost/", max_messages=1)

        # Must be published to the unroutable sink.
        all_rkeys = [
            c.kwargs.get("routing_key") or (c.args[1] if len(c.args) > 1 else None)
            for c in ch.basic_publish.call_args_list
        ]
        assert QUEUE_UNROUTABLE in all_rkeys

        # Must be ACKed (removed from DLQ).
        ch.basic_ack.assert_called_with(11)
        assert stats.unroutable == 1

    def test_missing_donation_id_moves_to_unroutable(self):
        """A JSON message missing donation_id must be moved to QUEUE_UNROUTABLE."""
        method = _make_method(tag=12)
        props = _make_props()
        body = json.dumps({"event": "donation.created"}).encode()
        ch = _make_channel_with_messages([(method, props, body)])

        with _patched_connection(ch):
            stats = replay_dlq("amqp://localhost/", max_messages=1)

        all_rkeys = [
            c.kwargs.get("routing_key") or (c.args[1] if len(c.args) > 1 else None)
            for c in ch.basic_publish.call_args_list
        ]
        assert QUEUE_UNROUTABLE in all_rkeys
        assert stats.unroutable == 1

    def test_malformed_message_never_published_to_main(self):
        """Malformed messages must never be republished to QUEUE_MAIN."""
        method = _make_method(tag=13)
        props = _make_props()
        body = b"broken"
        ch = _make_channel_with_messages([(method, props, body)])

        with _patched_connection(ch):
            replay_dlq("amqp://localhost/", max_messages=1)

        all_rkeys = [
            c.kwargs.get("routing_key") or (c.args[1] if len(c.args) > 1 else None)
            for c in ch.basic_publish.call_args_list
        ]
        assert QUEUE_MAIN not in all_rkeys


class TestDryRun:
    def test_dry_run_does_not_ack(self):
        """In dry-run mode, no ACK must be issued — messages stay in the DLQ."""
        method = _make_method(tag=99)
        props = _make_props()
        body = _valid_body(77)
        ch = _make_channel_with_messages([(method, props, body)])

        with _patched_connection(ch):
            stats = replay_dlq("amqp://localhost/", max_messages=1, dry_run=True)

        ch.basic_ack.assert_not_called()
        # NACKed with requeue=True to leave message in DLQ.
        ch.basic_nack.assert_called_with(99, requeue=True)
        # replayed counter still incremented for reporting.
        assert stats.replayed == 1

    def test_dry_run_does_not_publish_to_main(self):
        """In dry-run mode, nothing must be published to QUEUE_MAIN."""
        method = _make_method(tag=88)
        props = _make_props()
        body = _valid_body(66)
        ch = _make_channel_with_messages([(method, props, body)])

        with _patched_connection(ch):
            replay_dlq("amqp://localhost/", max_messages=1, dry_run=True)

        all_rkeys = [
            c.kwargs.get("routing_key") or (c.args[1] if len(c.args) > 1 else None)
            for c in ch.basic_publish.call_args_list
        ]
        assert QUEUE_MAIN not in all_rkeys


class TestQueueDrain:
    def test_max_messages_limits_processing(self):
        """replay_dlq must stop after processing max_messages messages."""
        messages = [(_make_method(tag=i), _make_props(), _valid_body(i)) for i in range(1, 6)]
        ch = _make_channel_with_messages(messages)

        with _patched_connection(ch):
            stats = replay_dlq("amqp://localhost/", max_messages=3)

        total = stats.replayed + stats.failed + stats.unroutable
        assert total == 3

    def test_empty_dlq_returns_zero_stats(self):
        """An empty DLQ must return zero counts without error."""
        ch = _make_channel_with_messages([])

        with _patched_connection(ch):
            stats = replay_dlq("amqp://localhost/")

        assert stats == ReplayStats(replayed=0, unroutable=0, failed=0)
        ch.basic_ack.assert_not_called()
        ch.basic_nack.assert_not_called()

    def test_mixed_batch_counts_correctly(self):
        """A batch with valid, malformed, and failing messages returns correct counts."""
        messages = [
            (_make_method(1), _make_props(), _valid_body(1)),        # valid -> replayed
            (_make_method(2), _make_props(), b"bad json"),            # malformed -> unroutable
            (_make_method(3), _make_props(), _valid_body(3)),        # valid -> replayed
        ]
        ch = _make_channel_with_messages(messages)

        with _patched_connection(ch):
            stats = replay_dlq("amqp://localhost/", max_messages=3)

        assert stats.replayed == 2
        assert stats.unroutable == 1
        assert stats.failed == 0


class TestDuplicateEventIdempotency:
    def test_same_donation_id_replayed_twice_no_crash(self):
        """Replaying the same donation_id twice must not raise an error.
        The consumer's idempotency (SELECT FOR UPDATE + match_attempts) handles
        the duplicate at the processing layer.
        """
        body = _valid_body(999)
        messages = [
            (_make_method(1), _make_props(), body),
            (_make_method(2), _make_props(), body),
        ]
        ch = _make_channel_with_messages(messages)

        with _patched_connection(ch):
            stats = replay_dlq("amqp://localhost/", max_messages=2)

        assert stats.failed == 0
        assert stats.replayed == 2


class TestTopologyDeclaration:
    def test_ensure_topology_declared_on_connect(self):
        """_ensure_topology must be called (DLX exchange + queues) on every connection."""
        ch = _make_channel_with_messages([])

        with _patched_connection(ch):
            replay_dlq("amqp://localhost/")

        # DLX exchange declared.
        exchange_calls = [c.kwargs.get("exchange") or (c.args[0] if c.args else None)
                          for c in ch.exchange_declare.call_args_list]
        assert "donation.dlx" in exchange_calls

        # Main queue declared with DLX argument.
        queue_calls = [c for c in ch.queue_declare.call_args_list]
        main_decls = [c for c in queue_calls
                      if (c.kwargs.get("queue") or (c.args[0] if c.args else None)) == QUEUE_MAIN]
        assert main_decls, "Main queue was not declared"
        for decl in main_decls:
            args = decl.kwargs.get("arguments") or {}
            assert args.get("x-dead-letter-exchange") == "donation.dlx"
