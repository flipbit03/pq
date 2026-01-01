"""PQ configuration using pydantic-settings."""

from pydantic_settings import BaseSettings


class PQSettings(BaseSettings):
    """Configuration for PQ task queue."""

    database_url: str = "postgresql://postgres:postgres@localhost:5433/postgres"

    model_config = {"env_prefix": "PQ_"}
