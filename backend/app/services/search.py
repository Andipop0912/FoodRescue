"""Elasticsearch service: index donations, search by text and filters.

Index: foodrescue_donations
Document ID: donation_id (stable — upsert on re-index prevents duplicates).
"""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

INDEX = "foodrescue_donations"


def _client():
    from elasticsearch import Elasticsearch
    url = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
    return Elasticsearch(url, request_timeout=10, verify_certs=False)


def ensure_index() -> None:
    """Create index with mapping if it does not exist."""
    es = _client()
    if es.indices.exists(index=INDEX):
        return
    es.indices.create(index=INDEX, body={
        "mappings": {
            "properties": {
                "donation_id":    {"type": "integer"},
                "donor_name":     {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "food_type":       {"type": "text"},
                "category":       {"type": "keyword"},
                "area":           {"type": "keyword"},
                "status":         {"type": "keyword"},
                "quantity_kg":    {"type": "float"},
                "match_attempts": {"type": "integer"},
                "created_at":     {"type": "date"},
                "expiry_time":    {"type": "date"},
            }
        }
    })
    logger.info("Created Elasticsearch index %s", INDEX)


def index_donations(db_session) -> int:
    """Upsert all donations into Elasticsearch. Returns count indexed."""
    from sqlalchemy import select
    from app.models import Donation, Donor

    es = _client()
    ensure_index()

    rows = db_session.execute(
        select(
            Donation.id, Donation.status, Donation.category,
            Donation.quantity_kg, Donation.area, Donation.food_type,
            Donation.match_attempts, Donation.created_at, Donation.expiry_time,
            Donor.name.label("donor_name"),
        ).join(Donor, Donor.id == Donation.donor_id)
    ).all()

    from elasticsearch.helpers import bulk

    def _actions():
        for r in rows:
            yield {
                "_index": INDEX,
                "_id": str(r.id),
                "_source": {
                    "donation_id":    r.id,
                    "donor_name":     r.donor_name,
                    "food_type":      r.food_type or "",
                    "category":       r.category or "uncategorised",
                    "area":           r.area or "unknown",
                    "status":         r.status,
                    "quantity_kg":    float(r.quantity_kg) if r.quantity_kg else 0.0,
                    "match_attempts": r.match_attempts or 0,
                    "created_at":     r.created_at.isoformat() if r.created_at else None,
                    "expiry_time":    r.expiry_time.isoformat() if r.expiry_time else None,
                },
            }

    success, _ = bulk(es, _actions())
    logger.info("Indexed %d donations into Elasticsearch", success)
    return success


def search_donations(
    q: str | None = None,
    category: str | None = None,
    area: str | None = None,
    status: str | None = None,
    min_kg: float | None = None,
    size: int = 20,
) -> dict[str, Any]:
    """Full-text + filtered search over indexed donations."""
    es = _client()

    must: list[dict] = []
    filters: list[dict] = []

    if q:
        must.append({"multi_match": {
            "query": q,
            "fields": ["food_type^2", "donor_name", "category", "area"],
            "fuzziness": "AUTO",
        }})
    if category:
        filters.append({"term": {"category": category}})
    if area:
        filters.append({"term": {"area": area}})
    if status:
        filters.append({"term": {"status": status}})
    if min_kg is not None:
        filters.append({"range": {"quantity_kg": {"gte": min_kg}}})

    body: dict = {
        "query": {
            "bool": {
                "must": must or [{"match_all": {}}],
                "filter": filters,
            }
        },
        "size": min(size, 100),
        "sort": [{"_score": "desc"}, {"donation_id": "desc"}],
    }

    resp = es.search(index=INDEX, body=body)
    hits = resp["hits"]
    return {
        "total": hits["total"]["value"],
        "hits": [
            {"id": h["_id"], "score": h["_score"], **h["_source"]}
            for h in hits["hits"]
        ],
    }
