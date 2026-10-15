#!/usr/bin/env python3
"""Dead-Letter Queue replay utility for FoodRescue.

Reads messages from the ``donation.dead`` DLQ and republishes validated ones
to the main ``donation.created`` queue using publisher confirms.  The DLQ
message is ACKed only after the republished message is confirmed by the broker.
On any publishing failure the DLQ message is NACKed with requeue=True so it
stays in the DLQ for the next replay attempt.

This utility is OPERATOR-TRIGGERED.  It must never run automatically on
startup — replay decisions require human review because some DLQ messages
represent permanently invalid data.

Delivery guarantee
------------------
Each successfully replayed message receives the same at-least-once guarantee
as an original publish.  The consumer uses SELECT FOR UPDATE and
match_attempts to handle duplicate processing safely.

Replay safety
-------------
* Malformed messages (non-JSON, missing donation_id) are moved to the
  ``donation.unroutable`` queue rather than being retried indefinitely.
* Replayed events carry the original donation_id so the consumer's
  idempotency logic (SELECT FOR UPDATE + match_attempts) prevents corruption.
* --max-messages caps the number processed per invocation.
* --dry-run inspects and logs messages without modifying any queue.

Usage
-----
Local (venv):
    python scripts/replay_dlq.py
    python scripts/replay_dlq.py --dry-run
    python scripts/replay_dlq.py --max-messages 10

Inside Docker (after ``docker compose --profile core up -d``):
    docker exec foodrescue-worker-1 \\
        python /app/scripts/replay_dlq.py --max-messages 50

Environment variables (read from environment or .env via --env-file):
    RABBITMQ_URL   amqp://guest:guest@localhost:5672/  (default)

Exit codes:
    0  All messages processed (or dry-run completed).
    1  Configuration or connection error.
    2  Partial failure: some messages could not be replayed (check logs).
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import NamedTuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s replay_dlq: %(message)s",
)
log = logging.getLogger("replay_dlq")

# ---------------------------------------------------------------------------
# Constants — must match consumer.py topology
# ---------------------------------------------------------------------------

QUEUE_DLQ = "donation.dead"
QUEUE_DLX = "donation.dlx"
QUEUE_MAIN = "donation.created"
QUEUE_UNROUTABLE = "donation.unroutable"   # Permanent-failure sink for invalid messages.

REQUIRED_FIELDS = ("donation_id",)


# ---------------------------------------------------------------------------
# Message validation
# ---------------------------------------------------------------------------

class ParseResult(NamedTuple):
    valid: bool
    msg: dict
    error: str


def parse_and_validate(body: bytes) -> ParseResult:
    """Parse a DLQ message body and validate required fields.

    Returns a ParseResult with valid=True and the parsed dict on success,
    or valid=False and an error description on failure.
    """
    try:
        msg = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return ParseResult(False, {}, f"invalid JSON: {exc}")
    if not isinstance(msg, dict):
        return ParseResult(False, {}, f"expected JSON object, got {type(msg).__name__}")
    missing = [f for f in REQUIRED_FIELDS if f not in msg]
    if missing:
        return ParseResult(False, msg, f"missing required fields: {missing}")
    return ParseResult(True, msg, "")


# ---------------------------------------------------------------------------
# Topology helpers
# ---------------------------------------------------------------------------

def _ensure_topology(ch) -> None:
    """Declare all queues and exchanges needed for replay.

    Idempotent — safe to call on every connection.  Matches consumer topology
    so both processes agree on queue arguments.
    """
    ch.exchange_declare(exchange=QUEUE_DLX, exchange_type="direct", durable=True)
    ch.queue_declare(queue=QUEUE_DLQ, durable=True)
    ch.queue_bind(queue=QUEUE_DLQ, exchange=QUEUE_DLX, routing_key=QUEUE_MAIN)
    ch.queue_declare(
        queue=QUEUE_MAIN,
        durable=True,
        arguments={"x-dead-letter-exchange": QUEUE_DLX},
    )
    # Unroutable sink: permanently invalid messages land here for manual inspection.
    ch.queue_declare(queue=QUEUE_UNROUTABLE, durable=True)


# ---------------------------------------------------------------------------
# Core replay logic
# ---------------------------------------------------------------------------

class ReplayStats(NamedTuple):
    replayed: int
    unroutable: int
    failed: int


def replay_dlq(
    rabbitmq_url: str,
    max_messages: int = 0,
    dry_run: bool = False,
    prefetch: int = 1,
) -> ReplayStats:
    """Replay up to max_messages messages from the DLQ.

    Parameters
    ----------
    rabbitmq_url  : AMQP URL string.
    max_messages  : Maximum messages to process (0 = drain the queue).
    dry_run       : If True, inspect and log without modifying any queue.
    prefetch      : basic_qos prefetch_count (keep at 1 for safe replay).

    Returns a ReplayStats namedtuple with counts of each outcome.

    Raises pika.exceptions.AMQPError on connection failure.
    """
    import pika

    if dry_run:
        log.info("DRY-RUN mode: queues will not be modified")

    conn = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
    try:
        ch = conn.channel()
        _ensure_topology(ch)
        ch.basic_qos(prefetch_count=prefetch)

        # Use confirm mode on the replay channel so each republish is broker-confirmed.
        if not dry_run:
            ch.confirm_delivery()

        stats = {"replayed": 0, "unroutable": 0, "failed": 0}

        while True:
            if max_messages and (stats["replayed"] + stats["unroutable"] + stats["failed"]) >= max_messages:
                log.info("reached --max-messages=%d; stopping", max_messages)
                break

            # basic_get for one message at a time.
            method, props, body = ch.basic_get(QUEUE_DLQ, auto_ack=False)
            if method is None:
                log.info("DLQ is empty; replay complete")
                break

            tag = method.delivery_tag
            dlq_count = (props.headers or {}).get("x-death", [{}])[0].get("count", 1) if props.headers else 1
            result = parse_and_validate(body)

            if not result.valid:
                log.warning("invalid message (tag=%s): %s | body=%r", tag, result.error, body[:200])
                if dry_run:
                    log.info("  [DRY-RUN] would move to %s", QUEUE_UNROUTABLE)
                    ch.basic_nack(tag, requeue=True)    # Leave in DLQ for human review in dry-run.
                else:
                    # Move permanently invalid messages to the unroutable sink.
                    _publish_to_unroutable(ch, body, props, result.error)
                    ch.basic_ack(tag)
                    stats["unroutable"] += 1
                    log.info("moved invalid message to %s (tag=%s)", QUEUE_UNROUTABLE, tag)
                continue

            donation_id = result.msg.get("donation_id")
            log.info("processing donation_id=%s (dlq-deaths=%s, tag=%s)", donation_id, dlq_count, tag)

            if dry_run:
                log.info("  [DRY-RUN] would republish donation_id=%s to %s", donation_id, QUEUE_MAIN)
                ch.basic_nack(tag, requeue=True)    # Leave in DLQ; dry-run makes no changes.
                stats["replayed"] += 1
                continue

            # Republish with publisher confirms before ACKing the DLQ message.
            try:
                _republish(ch, body, props)
                # ACK the DLQ message only after broker confirms the republish.
                ch.basic_ack(tag)
                stats["replayed"] += 1
                log.info("replayed donation_id=%s (tag=%s)", donation_id, tag)
            except Exception as exc:
                # Keep the DLQ message for the next replay attempt.
                log.error("republish failed for donation_id=%s (tag=%s): %s; requeueing", donation_id, tag, exc)
                ch.basic_nack(tag, requeue=True)
                stats["failed"] += 1

    finally:
        conn.close()

    result_stats = ReplayStats(**stats)
    log.info(
        "replay complete: replayed=%d unroutable=%d failed=%d",
        result_stats.replayed, result_stats.unroutable, result_stats.failed,
    )
    return result_stats


def _republish(ch, body: bytes, original_props) -> None:
    """Republish a DLQ message to the main queue with publisher confirms.

    Preserves original content-type and message body.  Adds a
    x-replayed-from header for traceability.
    """
    import pika
    orig_headers = dict(original_props.headers or {})
    orig_headers["x-replayed-from"] = QUEUE_DLQ
    orig_headers["x-replay-ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    ch.basic_publish(
        exchange="",
        routing_key=QUEUE_MAIN,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type=getattr(original_props, "content_type", "application/json") or "application/json",
            headers=orig_headers,
        ),
    )


def _publish_to_unroutable(ch, body: bytes, original_props, reason: str) -> None:
    """Publish a permanently invalid message to the unroutable sink queue."""
    import pika
    headers = dict(original_props.headers or {})
    headers["x-replay-rejection-reason"] = reason[:500]
    headers["x-replay-ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    ch.basic_publish(
        exchange="",
        routing_key=QUEUE_UNROUTABLE,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type=getattr(original_props, "content_type", "application/json") or "application/json",
            headers=headers,
        ),
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay messages from the FoodRescue dead-letter queue",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--rabbitmq-url",
        default=os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
        help="AMQP URL (default: RABBITMQ_URL env var or amqp://guest:guest@localhost:5672/)",
    )
    parser.add_argument(
        "--max-messages", type=int, default=0,
        help="Maximum messages to process per run (0 = drain queue)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Inspect messages without modifying any queue",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable DEBUG logging",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        import pika  # noqa: F401
    except ImportError:
        log.error("pika is not installed; run: pip install pika")
        return 1

    try:
        stats = replay_dlq(
            rabbitmq_url=args.rabbitmq_url,
            max_messages=args.max_messages,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        log.error("replay failed: %s", exc)
        return 1

    return 2 if stats.failed else 0


if __name__ == "__main__":
    sys.exit(main())
