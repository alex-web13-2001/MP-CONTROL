"""Application configuration."""

from functools import lru_cache

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

    @property
    def postgres_url(self) -> str:
        """Get PostgreSQL connection URL."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

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
    access_token_expire_minutes: int = 30


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
