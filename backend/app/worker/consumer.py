"""Matching worker: consumes 'donation.created' from RabbitMQ and saves the top-5 matches.

Also sweeps (every 30 s) for donations that were never matched, e.g. when the broker was
down at ingest time or a new NGO need appeared, giving the pipeline a best-effort
self-healing path.

Queue topology (Phase 1 — reliable event processing)
-----------------------------------------------------
Main queue  : donation.created  (durable, x-dead-letter-exchange=donation.dlx)
DL exchange : donation.dlx      (direct, durable)
DL queue    : donation.dead     (durable, bound to donation.dlx / donation.created)

ACK / NACK strategy
-------------------
* On success          : basic_ack  — message removed from donation.created.
* On processing fail  : basic_nack(requeue=False) — message routed to donation.dead
  via the dead-letter exchange.  The DLQ preserves the original payload for manual
  inspection or replay.
* On malformed body   : basic_nack(requeue=False) — malformed messages cannot be
  retried by the sweeper (no donation_id), so they go directly to the DLQ.

Recovery path
-------------
The 30-second sweeper queries the database directly for donations whose
match_attempts < MAX_ATTEMPTS and status = S_LISTED, providing bounded, logged
retries independent of the queue state.

Delivery-guarantee contract
----------------------------
This implementation provides *at-least-once* delivery with *bounded retries*.
It does NOT provide exactly-once delivery:
- A consumer crash after processing but before ACK may cause reprocessing.
- match_donation() uses SELECT FOR UPDATE and checks match_attempts to make
  repeated processing safe (idempotent at the match layer).

Known limitations
-----------------
* The DLQ provides observability but not automatic replay.
* Sweeper retries stop after MAX_ATTEMPTS (4) per donation.
* If the donation.created queue was previously declared without x-dead-letter-exchange,
  you must delete and redeclare it (or use a new queue name) in production.
* Sweeper is suspended while the AMQP reconnect loop is active (broker down).
"""
import json
import logging
import time
from datetime import timedelta

import pika
from sqlalchemy import select

from app import db as dbmod
from app.config import settings
from app.constants import QUEUE_DONATION_CREATED, S_LISTED
from app.models import Donation
from app.services.matching import match_donation
from app.utils import utcnow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s worker: %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)
log = logging.getLogger("worker")

SWEEP_EVERY_S = 30
MAX_ATTEMPTS = 4

# Dead-letter topology constants
QUEUE_DLX = "donation.dlx"
QUEUE_DEAD = "donation.dead"


def _declare_topology(ch: pika.adapters.blocking_connection.BlockingChannel) -> None:
    """Declare all queues, exchanges, and bindings for the reliable pipeline.

    Must be called before consuming.  Safe to call on every reconnect because all
    declarations are idempotent when arguments match.

    WARNING: If donation.created was previously declared without
    x-dead-letter-exchange, RabbitMQ will raise PRECONDITION_FAILED (406).
    In that case, delete the queue in the management UI and let this function
    recreate it.
    """
    ch.exchange_declare(exchange=QUEUE_DLX, exchange_type="direct", durable=True)
    ch.queue_declare(queue=QUEUE_DEAD, durable=True)
    ch.queue_bind(queue=QUEUE_DEAD, exchange=QUEUE_DLX, routing_key=QUEUE_DONATION_CREATED)
    ch.queue_declare(
        queue=QUEUE_DONATION_CREATED,
        durable=True,
        arguments={"x-dead-letter-exchange": QUEUE_DLX},
    )


def _parse_message(body: bytes) -> dict:
    """Parse and validate a message body.

    Raises ValueError for malformed JSON or missing required fields so the caller
    can NACK the message to the DLQ rather than retrying indefinitely.
    """
    try:
        msg = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"malformed message body (not valid JSON): {body[:200]!r}") from exc
    if not isinstance(msg, dict):
        raise ValueError(f"message must be a JSON object, got {type(msg).__name__}")
    if "donation_id" not in msg:
        raise ValueError(f"message missing required field 'donation_id': {msg}")
    return msg


def process_message(body: bytes) -> int:
    """Handle one donation.created payload. Returns number of matches saved.

    Raises ValueError for malformed bodies and any exception from match_donation
    on processing failures.  The caller is responsible for ACK/NACK.
    """
    msg = _parse_message(body)
    with dbmod.SessionLocal() as db:
        return len(match_donation(db, int(msg["donation_id"])))


def sweep() -> int:
    """Re-attempt matching for donations that were never matched.

    Each donation is processed independently so that a single poisoned record
    (e.g. one whose data triggers a repeatable exception in match_donation) does
    not prevent the remaining donations in the same sweep batch from being
    processed.  Failures are logged with the donation ID for manual inspection.
    """
    now = utcnow()
    with dbmod.SessionLocal() as db:
        ids = db.scalars(select(Donation.id).where(
            Donation.status == S_LISTED, Donation.match_attempts < MAX_ATTEMPTS, Donation.expiry_time > now,
            Donation.created_at < now - timedelta(seconds=20)).limit(200)).all()
        ok = 0
        for i in ids:
            try:
                match_donation(db, i, log=False)
                ok += 1
            except Exception:
                log.exception("sweeper: error processing donation_id=%s; skipping", i)
                db.rollback()  # reset aborted transaction so subsequent donations are not poisoned
    return len(ids)


def run() -> None:
    while True:
        try:
            conn = pika.BlockingConnection(pika.URLParameters(settings.rabbitmq_url))
            ch = conn.channel()
            _declare_topology(ch)
            ch.basic_qos(prefetch_count=20)
            log.info("connected; waiting for %s (DLQ: %s)", QUEUE_DONATION_CREATED, QUEUE_DEAD)
            last_sweep = time.time()
            for method, _props, body in ch.consume(QUEUE_DONATION_CREATED, inactivity_timeout=5):
                if method is not None:
                    try:
                        n = process_message(body)
                        log.info("matched donation -> %d matches", n)
                        ch.basic_ack(method.delivery_tag)
                    except Exception:
                        # NACK without requeue: message is routed to donation.dead via
                        # the dead-letter exchange for manual inspection or replay.
                        log.exception(
                            "failed to process message; nacking to DLQ %s", QUEUE_DEAD
                        )
                        ch.basic_nack(method.delivery_tag, requeue=False)
                if time.time() - last_sweep > SWEEP_EVERY_S:
                    last_sweep = time.time()
                    try:
                        swept = sweep()
                        if swept:
                            log.info("sweeper re-checked %d donations", swept)
                    except Exception:
                        log.exception("sweep failed")
        except (pika.exceptions.AMQPError, OSError) as exc:
            log.warning("broker unavailable (%s); retrying in 3s", exc)
            time.sleep(3)


if __name__ == "__main__":
    run()
