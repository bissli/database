"""
Backwards compatibility module.

The schema cache has been consolidated into database.cache.
This module provides a SchemaCache wrapper for backwards compatibility.
"""
from typing import Any, Self

from database.cache import Cache


class SchemaCache:
    """Backwards compatibility wrapper for schema caching.

    Wraps the unified Cache class to provide the old SchemaCache API.
    """

    _instance: Self | None = None

    @classmethod
    def get_instance(cls) -> Self:
        """Get singleton instance.
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        self.cache = Cache.get_instance()

    def get_or_create_connection_cache(self, connection_id: str) -> Any:
        """Get or create a cache for a specific connection.
        """
        return self.cache.get_schema_cache(connection_id)

    def clear(self) -> None:
        """Clear all cache entries.
        """
        self.cache.clear_all()

    def clear_for_table(self, table_name: str) -> None:
        """Clear cache entries for a specific table.
        """
        self.cache.clear_for_table(table_name)


__all__ = ['SchemaCache']
