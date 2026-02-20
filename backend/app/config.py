"""Application configuration."""

from functools import lru_cache
from urllib.parse import urlparse, unquote

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Application
    app_name: str = "MMS API"
    app_version: str = "0.1.0"
    debug: bool = False

    # PostgreSQL
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_user: str = "mms"
    postgres_password: str = "mms_secret"
    postgres_db: str = "mms"
    postgres_url: str = ""  # Full URL override (for external managed PG with SSL)

    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL.
        
        If POSTGRES_URL is set, use it directly (e.g. managed DB with SSL).
        Otherwise, construct from individual POSTGRES_* parts.
        """
        if self.postgres_url:
            return self.postgres_url
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def psycopg2_conn_params(self) -> dict:
        """Get connection params dict for psycopg2 (sync driver).

        Parses POSTGRES_URL if set (managed PG on prod),
        otherwise falls back to individual POSTGRES_* env vars (local Docker).
        Works on both environments without code changes.
        """
        if self.postgres_url:
            # Parse asyncpg URL → psycopg2 params
            # Input:  postgresql+asyncpg://user:pass@host:port/db?ssl=require
            url = self.postgres_url
            parsed = urlparse(url)
            params = {
                "host": parsed.hostname or "localhost",
                "port": parsed.port or 5432,
                "user": unquote(parsed.username or ""),
                "password": unquote(parsed.password or ""),
                "database": (parsed.path or "/").lstrip("/"),
            }
            # Handle sslmode from query params
            if parsed.query:
                from urllib.parse import parse_qs
                qs = parse_qs(parsed.query)
                if "ssl" in qs or "sslmode" in qs:
                    params["sslmode"] = "require"
            return params
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "user": self.postgres_user,
            "password": self.postgres_password,
            "database": self.postgres_db,
        }

    # ClickHouse
    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_db: str = "mms_analytics"

    # Redis
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0

    @property
    def redis_url(self) -> str:
        """Get Redis connection URL."""
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    # Celery
    celery_broker_url: str = ""
    celery_result_backend: str = ""

    def get_celery_broker_url(self) -> str:
        """Get Celery broker URL."""
        return self.celery_broker_url or self.redis_url

    def get_celery_result_backend(self) -> str:
        """Get Celery result backend URL."""
        return self.celery_result_backend or self.redis_url

    # Security
    secret_key: str = "your-secret-key-change-in-production"
    access_token_expire_minutes: int = 120


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
