"""Matching worker: consumes 'donation.created' from RabbitMQ and saves the top-5 matches.

Also sweeps (every 30 s) for donations that were never matched, e.g. when the broker was
down at ingest time or a new NGO need appeared, giving the pipeline a best-effort
self-healing path.

ACK / NACK strategy
-------------------
* On success  : basic_ack  — message is removed from the queue.
* On failure  : basic_nack(requeue=False) — message is discarded from the queue.

  No dead-letter queue (DLQ) is configured; discarding on failure is the safest
  compatible behaviour for the current topology and prevents infinite requeue loops.

  The AMQP message payload is permanently gone after a NACK.  Recovery relies on the
  30-second sweeper querying the database directly for donations whose match_attempts
  column is below MAX_ATTEMPTS and whose status is still S_LISTED.

Known limitations of the sweeper-based recovery
------------------------------------------------
* NOT GUARANTEED: recovery depends on the donation row remaining in S_LISTED status
  and match_attempts < MAX_ATTEMPTS.  A donation that somehow reaches S_MATCHED before
  the transaction rolls back (only possible in extreme edge cases) will not be re-queried.
* HARD CUTOFF: once match_attempts reaches MAX_ATTEMPTS (4), the sweeper permanently
  stops attempting matches for that donation.
* BROKER DEPENDENCY: the sweeper runs inside the consumer event loop; if RabbitMQ is
  unreachable, the worker's AMQP reconnect backoff suspends sweeper execution entirely.
"""
import json
import logging
import time

import pika
from sqlalchemy import select

from app import db as dbmod
from app.config import settings
from app.constants import QUEUE_DONATION_CREATED, S_LISTED
from app.models import Donation
from app.services.matching import match_donation
from app.utils import utcnow
from datetime import timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s worker: %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)
log = logging.getLogger("worker")

SWEEP_EVERY_S = 30
MAX_ATTEMPTS = 4


def process_message(body: bytes) -> int:
    """Handle one donation.created payload. Returns number of matches saved."""
    msg = json.loads(body)
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
    return len(ids)


def run() -> None:
    while True:
        try:
            conn = pika.BlockingConnection(pika.URLParameters(settings.rabbitmq_url))
            ch = conn.channel()
            ch.queue_declare(queue=QUEUE_DONATION_CREATED, durable=True)
            ch.basic_qos(prefetch_count=20)
            log.info("connected; waiting for %s", QUEUE_DONATION_CREATED)
            last_sweep = time.time()
            for method, _props, body in ch.consume(QUEUE_DONATION_CREATED, inactivity_timeout=5):
                if method is not None:
                    try:
                        n = process_message(body)
                        log.info("matched donation -> %d matches", n)
                        ch.basic_ack(method.delivery_tag)
                    except Exception:
                        # NACK without requeue: discard the message from the queue.
                        # The sweeper retries via match_attempts < MAX_ATTEMPTS so there
                        # is no silent loss — just bounded, logged retries.
                        log.exception(
                            "failed to process message; nacking without requeue "
                            "(sweeper will retry up to MAX_ATTEMPTS times)"
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
