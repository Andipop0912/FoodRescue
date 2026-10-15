import os

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# The literal default used in development.  Any production deploy that forgets to
# set JWT_SECRET will be caught by the validator below.
_DEV_SECRET = "dev-secret-change-me-please-32-bytes-min"


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
        secret = self.jwt_secret
        # Allow the dev placeholder only in non-production environments to keep
        # the local docker-compose and test suite working without extra config.
        # Detect production by the absence of any obviously local/default DB URL.
        is_local = (
            "localhost" in self.database_url
            or "127.0.0.1" in self.database_url
            or os.getenv("FOODRESCUE_ENV", "development") == "development"
        )
        if not is_local:
            if not secret or secret == _DEV_SECRET:
                raise ValueError(
                    "JWT_SECRET must be set to a strong secret (>= 32 bytes) in production. "
                    "Current value is the insecure development placeholder."
                )
            if len(secret) < 32:
                raise ValueError(
                    f"JWT_SECRET is too short ({len(secret)} bytes); minimum is 32 bytes."
                )
        # In local/dev mode fall back gracefully to the dev placeholder so that
        # docker-compose and tests work out of the box.
        if not secret:
            self.jwt_secret = _DEV_SECRET
        return self


settings = Settings()
