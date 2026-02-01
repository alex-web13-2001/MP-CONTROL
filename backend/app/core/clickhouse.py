"""ClickHouse database connection for analytics."""

import clickhouse_connect
from clickhouse_connect.driver import Client

from app.config import get_settings

settings = get_settings()


def get_clickhouse_client() -> Client:
    """Get ClickHouse client instance."""
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )


class ClickHouseManager:
    """Manager for ClickHouse operations."""

    def __init__(self):
        self._client: Client | None = None

    @property
    def client(self) -> Client:
        """Get or create ClickHouse client."""
        if self._client is None:
            self._client = get_clickhouse_client()
        return self._client

    def execute(self, query: str, parameters: dict | None = None):
        """Execute a query and return results."""
        return self.client.query(query, parameters=parameters)

    def insert(self, table: str, data: list[dict], column_names: list[str] | None = None):
        """Insert data into a table."""
        if not data:
            return
        if column_names is None:
            column_names = list(data[0].keys())
        rows = [[row.get(col) for col in column_names] for row in data]
        self.client.insert(table, rows, column_names=column_names)

    def close(self):
        """Close the client connection."""
        if self._client:
            self._client.close()
            self._client = None


# Global instance
clickhouse_manager = ClickHouseManager()
