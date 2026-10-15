"""Regression tests for the donations router and serialize() helper."""
import json
from datetime import timedelta

import pytest

from app.models import Donation, Donor, Match
from app.utils import utcnow


def _make_donation(session, donor_id):
    d = Donation(
        donor_id=donor_id, food_type="Test Rice", category="grains", quantity_kg=10.0,
        expiry_time=utcnow() + timedelta(hours=6), lat=19.033, lng=73.0297,
        area="Vashi", source="manual", status="matched",
    )
    session.add(d)
    session.flush()
    return d


def _add_match(session, donation_id, ngo_id, rank, status):
    m = Match(donation_id=donation_id, ngo_id=ngo_id, rank=rank, score=0.5,
              distance_km=3.0, distance_score=0.8, urgency_score=0.5, fit_score=0.8,
              status=status)
    session.add(m)
    session.flush()
    return m


# ---------------------------------------------------------------------------
# match_count regression — must count only suggested matches
# ---------------------------------------------------------------------------

def test_match_count_excludes_declined_matches(client, donor, session):
    """match_count in /donations/mine must count only 'suggested' matches, not 'declined'.

    Regression for: serialize() previously counted ALL Match rows regardless of
    status, causing match_count to be inflated after an NGO accepted a donation
    (declined the remaining suggestions).
    """
    from app.deps import donor_profile
    from app import db as dbmod

    # Get donor profile from the seeded DB.
    with dbmod.SessionLocal() as db:
        from app.models import User
        from sqlalchemy import select
        u = db.scalars(select(User).where(User.email == "donor@foodrescue.demo")).first()
        assert u is not None
        dprofile = donor_profile(db, u)
        assert dprofile is not None
        donor_id = dprofile.id

        # Create donation and add 3 matches: 2 suggested + 1 declined.
        d = _make_donation(db, donor_id)
        ngo_row = db.scalars(select(__import__('app.models', fromlist=['NGO']).NGO)).first()
        _add_match(db, d.id, ngo_row.id, 1, "suggested")
        _add_match(db, d.id, ngo_row.id, 2, "suggested")
        _add_match(db, d.id, ngo_row.id, 3, "declined")
        db.commit()
        donation_id = d.id

    r = client.get("/donations/mine", headers=donor)
    assert r.status_code == 200
    mine = [x for x in r.json() if x["id"] == donation_id]
    assert mine, "Donation not found in /donations/mine response"
    assert mine[0]["match_count"] == 2, (
        f"Expected match_count=2 (only suggested), got {mine[0]['match_count']}. "
        "Declined matches must not be counted."
    )


def test_match_count_zero_when_all_matches_declined(client, donor, session):
    """match_count must be 0 when all matches are declined."""
    from app.deps import donor_profile
    from app import db as dbmod

    with dbmod.SessionLocal() as db:
        from app.models import User, NGO
        from sqlalchemy import select
        u = db.scalars(select(User).where(User.email == "donor@foodrescue.demo")).first()
        dprofile = donor_profile(db, u)
        donor_id = dprofile.id

        d = _make_donation(db, donor_id)
        ngo_row = db.scalars(select(NGO)).first()
        _add_match(db, d.id, ngo_row.id, 1, "declined")
        db.commit()
        donation_id = d.id

    r = client.get("/donations/mine", headers=donor)
    assert r.status_code == 200
    mine = [x for x in r.json() if x["id"] == donation_id]
    assert mine[0]["match_count"] == 0, "All-declined donation must show match_count=0"
