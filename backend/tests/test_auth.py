import pytest

from app.security import create_access_token, hash_password, verify_password
from tests.conftest import login


def test_bcrypt_roundtrip():
    h = hash_password("demo123")
    assert h != "demo123" and h.startswith("$2")
    assert verify_password("demo123", h) and not verify_password("wrong", h)


@pytest.mark.parametrize("email,role", [("donor@foodrescue.demo", "donor"), ("ngo@foodrescue.demo", "ngo"),
                                        ("admin@foodrescue.demo", "admin")])
def test_seed_accounts_can_login(client, email, role):
    r = client.post("/auth/login", json={"email": email, "password": "demo123"})
    assert r.status_code == 200 and r.json()["role"] == role
    me = client.get("/auth/me", headers={"Authorization": f"Bearer {r.json()['access_token']}"})
    assert me.json()["email"] == email


def test_wrong_password_and_unknown_user(client):
    assert client.post("/auth/login", json={"email": "donor@foodrescue.demo", "password": "nope"}).status_code == 401
    assert client.post("/auth/login", json={"email": "ghost@x.com", "password": "demo123"}).status_code == 401


def test_oauth_form_login_for_swagger(client):
    r = client.post("/auth/token", data={"username": "admin@foodrescue.demo", "password": "demo123"})
    assert r.status_code == 200 and r.json()["access_token"]


def test_missing_and_garbage_token(client):
    assert client.get("/auth/me").status_code == 401
    assert client.get("/auth/me", headers={"Authorization": "Bearer garbage"}).status_code == 401


def test_expired_token_rejected(client):
    tok = create_access_token("admin@foodrescue.demo", "admin", minutes=-5)
    assert client.get("/auth/me", headers={"Authorization": f"Bearer {tok}"}).status_code == 401


def test_role_guards(client, admin, donor, ngo):
    # admin-only
    assert client.post("/demo/start", headers=donor).status_code == 403
    assert client.post("/demo/start", headers=ngo).status_code == 403
    assert client.get("/stats/summary", headers=donor).status_code == 403
    assert client.get("/stats/summary", headers=admin).status_code == 200
    assert client.get("/donations", headers=donor).status_code == 403
    # donor-only
    assert client.get("/donations/mine", headers=ngo).status_code == 403
    assert client.post("/donations", headers=ngo, json={}).status_code in (403, 422)
    # ngo-only
    assert client.get("/ngo/matches", headers=donor).status_code == 403
    assert client.get("/ngo/matches", headers=ngo).status_code == 200
    # ingest: ngo not allowed
    assert client.post("/ingest/batch", headers=ngo, json=[{}]).status_code == 403
    # unauthenticated
    assert client.get("/ngo/matches").status_code == 401


def test_websocket_requires_admin_token(client, donor, admin):
    from starlette.websockets import WebSocketDisconnect
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws/live?token=bad"):
            pass
    donor_token = donor["Authorization"].split()[1]
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect(f"/ws/live?token={donor_token}"):
            pass
    admin_token = admin["Authorization"].split()[1]
    with client.websocket_connect(f"/ws/live?token={admin_token}") as ws:
        data = ws.receive_json()
        assert "stats" in data and "demo" in data


# ---------------------------------------------------------------------------
# JWT secret startup validator
# ---------------------------------------------------------------------------

def test_jwt_secret_validator_rejects_short_secret_in_production(monkeypatch):
    """A secret shorter than 32 bytes must raise ValueError in production mode."""
    from pydantic import ValidationError
    from app.config import Settings
    monkeypatch.setenv("FOODRESCUE_ENV", "production")
    with pytest.raises((ValidationError, ValueError)):
        Settings(jwt_secret="tooshort", database_url="postgresql+psycopg2://u:p@prod-host/db")


def test_jwt_secret_validator_rejects_dev_placeholder_in_production(monkeypatch):
    """The known dev placeholder must raise ValueError in production mode."""
    from pydantic import ValidationError
    from app.config import Settings, _DEV_SECRET
    monkeypatch.setenv("FOODRESCUE_ENV", "production")
    with pytest.raises((ValidationError, ValueError)):
        Settings(jwt_secret=_DEV_SECRET, database_url="postgresql+psycopg2://u:p@prod-host/db")


def test_jwt_secret_validator_rejects_empty_in_production(monkeypatch):
    """An empty jwt_secret must raise ValueError in production mode."""
    from pydantic import ValidationError
    from app.config import Settings
    monkeypatch.setenv("FOODRESCUE_ENV", "production")
    with pytest.raises((ValidationError, ValueError)):
        Settings(jwt_secret="", database_url="postgresql+psycopg2://u:p@prod-host/db")


# --- New regression tests: production env overrides localhost bypass ---

def test_jwt_secret_validator_rejects_short_secret_production_localhost(monkeypatch):
    """FOODRESCUE_ENV=production must reject weak secrets even with localhost in DATABASE_URL.
    This is the regression test for the localhost-bypass bug that was present before this fix.
    """
    from pydantic import ValidationError
    from app.config import Settings
    monkeypatch.setenv("FOODRESCUE_ENV", "production")
    with pytest.raises((ValidationError, ValueError)):
        Settings(jwt_secret="tooshort", database_url="postgresql+psycopg2://u:p@localhost:5432/db")


def test_jwt_secret_validator_rejects_placeholder_production_localhost(monkeypatch):
    """FOODRESCUE_ENV=production must reject the dev placeholder even with localhost DATABASE_URL."""
    from pydantic import ValidationError
    from app.config import Settings, _DEV_SECRET
    monkeypatch.setenv("FOODRESCUE_ENV", "production")
    with pytest.raises((ValidationError, ValueError)):
        Settings(jwt_secret=_DEV_SECRET, database_url="postgresql+psycopg2://u:p@localhost:5432/db")


def test_jwt_secret_validator_rejects_empty_production_localhost(monkeypatch):
    """FOODRESCUE_ENV=production must reject an empty secret even with localhost DATABASE_URL."""
    from pydantic import ValidationError
    from app.config import Settings
    monkeypatch.setenv("FOODRESCUE_ENV", "production")
    with pytest.raises((ValidationError, ValueError)):
        Settings(jwt_secret="", database_url="postgresql+psycopg2://u:p@localhost:5432/db")


def test_jwt_secret_validator_accepts_strong_secret_production_localhost(monkeypatch):
    """FOODRESCUE_ENV=production must accept a strong (>= 32 byte) non-placeholder secret."""
    from app.config import Settings
    monkeypatch.setenv("FOODRESCUE_ENV", "production")
    strong = "a" * 32
    s = Settings(jwt_secret=strong, database_url="postgresql+psycopg2://u:p@localhost:5432/db")
    assert s.jwt_secret == strong


def test_jwt_secret_validator_allows_dev_placeholder_locally(monkeypatch):
    """The dev placeholder is accepted when FOODRESCUE_ENV is development.

    Regression: when run inside a Docker container that exports FOODRESCUE_ENV=production
    (e.g. the worker container), os.getenv picks up the host environment and causes the
    validator to reject the dev placeholder even in tests that are testing dev-mode
    behaviour.  Explicitly setting FOODRESCUE_ENV=development isolates the test from
    whatever the container exports.
    """
    from app.config import Settings, _DEV_SECRET
    monkeypatch.setenv("FOODRESCUE_ENV", "development")
    s = Settings(jwt_secret=_DEV_SECRET, database_url="postgresql+psycopg2://u:p@localhost/db")
    assert s.jwt_secret == _DEV_SECRET


def test_jwt_secret_validator_falls_back_to_dev_secret_when_empty_locally(monkeypatch):
    """An empty jwt_secret in development must silently default to the dev placeholder.

    Regression: same container-env isolation issue as above — must pin FOODRESCUE_ENV
    to development so the test is not affected by the container's production setting.
    """
    from app.config import Settings, _DEV_SECRET
    monkeypatch.setenv("FOODRESCUE_ENV", "development")
    s = Settings(jwt_secret="", database_url="postgresql+psycopg2://u:p@localhost/db")
    assert s.jwt_secret == _DEV_SECRET

