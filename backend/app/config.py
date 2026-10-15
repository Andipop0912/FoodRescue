import os

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# The literal default used in development.  Any production deploy that forgets to
# set JWT_SECRET will be caught by the validator below.
_DEV_SECRET = "dev-secret-change-me-please-32-bytes-min"

# Recognised values for FOODRESCUE_ENV.
_ENV_PRODUCTION = "production"
_ENV_DEVELOPMENT = "development"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    database_url: str = "postgresql+psycopg2://foodrescue:foodrescue@localhost:5433/foodrescue"
    mongo_url: str = "mongodb://localhost:27017"
    mongo_db: str = "foodrescue"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    # Empty default forces a ValueError at startup unless JWT_SECRET env var is set.
    # Tests that don't care about JWT can monkeypatch this field directly.
    jwt_secret: str = ""
    jwt_expire_minutes: int = 720
    inbox_dir: str = "data/inbox"
    api_internal_url: str = "http://127.0.0.1:8000"
    sim_direct_ingest: bool = True
    match_radius_km: float = 15.0
    cors_origins: str = "http://localhost:5173,http://localhost:5174"

    @model_validator(mode="after")
    def _validate_jwt_secret(self) -> "Settings":
        """Enforce a strong JWT secret in production.

        Priority order (highest first):
        1. FOODRESCUE_ENV=production  →  always enforce strong secret,
           regardless of DATABASE_URL contents (closes the localhost-bypass).
        2. FOODRESCUE_ENV=development →  always allow dev placeholder.
        3. FOODRESCUE_ENV absent / other value  →  fall back to the DB-URL
           heuristic so that local docker-compose works without extra config.
        """
        secret = self.jwt_secret
        foodrescue_env = os.getenv("FOODRESCUE_ENV", _ENV_DEVELOPMENT)

        if foodrescue_env == _ENV_PRODUCTION:
            # Explicit production: enforce unconditionally, even if DATABASE_URL
            # contains 'localhost' (e.g. a tunnel or a misconfigured deploy).
            is_production = True
        elif foodrescue_env == _ENV_DEVELOPMENT:
            # Explicit development: never enforce.
            is_production = False
        else:
            # Unknown value — fall back to DB-URL heuristic for backward compat.
            is_production = not (
                "localhost" in self.database_url
                or "127.0.0.1" in self.database_url
            )

        if is_production:
            if not secret or secret == _DEV_SECRET:
                raise ValueError(
                    "JWT_SECRET must be set to a strong secret (>= 32 bytes) in production. "
                    "Current value is missing or is the insecure development placeholder."
                )
            if len(secret) < 32:
                raise ValueError(
                    f"JWT_SECRET is too short ({len(secret)} bytes); minimum is 32 bytes."
                )

        # In non-production mode fall back gracefully to the dev placeholder so
        # that docker-compose and the test suite work out of the box.
        if not secret:
            self.jwt_secret = _DEV_SECRET
        return self


settings = Settings()
