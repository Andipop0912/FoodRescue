"""Event log (Postgres, shared across processes) and the RabbitMQ publisher.

Publisher reliability (Phase 1)
--------------------------------
publish_donation_created() uses *publisher confirms* (AMQP confirm mode).
With confirms enabled, basic_publish blocks until the broker acknowledges the
message.  If the broker NACKs or the message is unroutable, pika raises
UnroutableError or NackError, both of which propagate to the caller.

The caller (ingest.py) catches all exceptions from this function and treats a
publish failure as non-fatal: the donation is committed to PostgreSQL, and the
30-second sweeper will retry matching via match_attempts < MAX_ATTEMPTS.

Connection management
---------------------
A new pika BlockingConnection is created per call.  This is safe for the
current load profile (infrequent bulk publishes).  A persistent channel pool
would reduce connection overhead at higher throughput and can be introduced
when benchmarking shows it necessary.
"""
import json
import logging

import pika
from sqlalchemy.orm import Session

from app.config import settings
from app.constants import QUEUE_DONATION_CREATED
from app.models import Event
from app.utils import utcnow

log = logging.getLogger(__name__)


def log_event(db: Session, kind: str, message: str, data: dict | None = None, commit: bool = True) -> Event:
    ev = Event(kind=kind, message=message, data=data, ts=utcnow())
    db.add(ev)
    if commit:
        db.commit()
    else:
        db.flush()
    return ev


def publish_donation_created(messages: list[dict]) -> None:
    """Publish one 'donation.created' message per donation with publisher confirms.

    Uses confirm mode so each basic_publish blocks until the broker acknowledges
    delivery.  Raises pika.exceptions.UnroutableError or NackError if the broker
    rejects a message; raises pika.exceptions.AMQPError on connection failure.

    Callers treat all exceptions as non-fatal: the sweeper recovers unmatched
    donations from the database.
    """
    if not messages:
        return
    conn = pika.BlockingConnection(pika.URLParameters(settings.rabbitmq_url))
    try:
        ch = conn.channel()
        # Declare queue with dead-letter exchange consistent with consumer topology.
        # The declaration is idempotent; a mismatch raises PRECONDITION_FAILED.
        ch.queue_declare(
            queue=QUEUE_DONATION_CREATED,
            durable=True,
            arguments={"x-dead-letter-exchange": "donation.dlx"},
        )
        ch.confirm_delivery()  # Enable publisher confirms for this channel.
        for m in messages:
            ch.basic_publish(
                exchange="",
                routing_key=QUEUE_DONATION_CREATED,
                body=json.dumps(m, default=str),
                properties=pika.BasicProperties(
                    delivery_mode=2,          # Persistent — survives broker restart.
                    content_type="application/json",
                ),
            )
        log.debug("published %d donation.created messages with confirms", len(messages))
    finally:
        conn.close()
