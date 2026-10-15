"""Analytics and search endpoints (admin only)."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.deps import get_db, require_roles

router = APIRouter(prefix="/analytics", tags=["analytics"])

_admin = Depends(require_roles("admin"))


@router.get("/duckdb")
def duckdb_analytics(_=_admin):
    """Run DuckDB in-memory analytics over current PostgreSQL donation data."""
    try:
        from app.services.duckdb_analytics import run_donation_analytics
        return run_donation_analytics()
    except ImportError as exc:
        raise HTTPException(503, f"DuckDB not available: {exc}")
    except Exception as exc:
        raise HTTPException(500, str(exc))


@router.post("/es/index")
def es_index_donations(db: Session = Depends(get_db), _=_admin):
    """Index all donations into Elasticsearch (upsert — safe to repeat)."""
    try:
        from app.services.search import index_donations
        count = index_donations(db)
        return {"indexed": count}
    except ImportError as exc:
        raise HTTPException(503, f"Elasticsearch client not available: {exc}")
    except Exception as exc:
        raise HTTPException(500, str(exc))


@router.get("/es/search")
def es_search_donations(
    q: str | None = Query(None, description="Full-text query"),
    category: str | None = Query(None),
    area: str | None = Query(None),
    status: str | None = Query(None),
    min_kg: float | None = Query(None),
    size: int = Query(20, ge=1, le=100),
    _=_admin,
):
    """Search donations in Elasticsearch."""
    try:
        from app.services.search import search_donations
        return search_donations(q=q, category=category, area=area,
                                status=status, min_kg=min_kg, size=size)
    except ImportError as exc:
        raise HTTPException(503, f"Elasticsearch client not available: {exc}")
    except Exception as exc:
        raise HTTPException(500, str(exc))
