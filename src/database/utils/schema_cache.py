"""
Cache for database schema metadata.

This module provides caching for detailed column metadata including type information,
nullability, precision, scale, etc. It works alongside the strategy-based caching
system to reduce database round-trips when schema information is needed.

While strategy caching (via cacheable_strategy decorator) focuses on higher-level
schema information (lists of columns, primary keys, etc.), this cache stores more
detailed per-column metadata that is queried directly from database system catalogs.

Both caching systems use the shared CacheManager for consistent cache behavior and
clearing capabilities. When schema changes are detected, both caches should be
cleared in a coordinated way to ensure consistency, which is why cache clearing
operations are consolidated in the CacheManager.
"""
import logging
import threading
from typing import Any

from database.options import use_iterdict_data_loader
from database.utils.cache import CacheManager
from database.utils.connection_utils import get_dialect_name

logger = logging.getLogger(__name__)


class SchemaCache:
    """Cache for database schema metadata"""

    _instance = None
    _lock = threading.RLock()

    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self):
        # Use the unified cache manager
        self.cache_manager = CacheManager.get_instance()

    def get_or_create_connection_cache(self, connection_id):
        """Get or create a cache for a specific connection

        Args:
            connection_id: Unique identifier for the connection

        Returns
            Cache instance for the specified connection
        """
        return self.cache_manager.get_schema_cache(connection_id)

    def get_column_metadata(self, connection: Any, table_name: str, connection_id: int | None = None) -> dict[str, dict[str, Any]]:
        """Get column metadata for a table, from cache or by querying.

        Args:
            connection: Database connection
            table_name: Table name to get metadata for
            connection_id: Optional connection ID for cache key (defaults to id(connection))

        Returns
            dict: Dictionary of column metadata indexed by column name
        """
        connection_id = connection_id or id(connection)
        conn_cache = self.get_or_create_connection_cache(connection_id)

        cache_key = f'{table_name.lower()}'

        if cache_key in conn_cache:
            return conn_cache[cache_key]

        # Not in cache, query the database
        metadata = self._query_column_metadata(connection, table_name)
        conn_cache[cache_key] = metadata
        return metadata

    def _query_column_metadata(self, connection: Any, table_name: str) -> dict[str, dict[str, Any]]:
        """Query database system catalogs for column metadata

        Args:
            connection: Database connection object
            table_name: Name of the table to get metadata for

        Returns
            dict: Dictionary of column metadata indexed by column name
        """
        dialect = get_dialect_name(connection)
        if dialect == 'postgresql':
            return self._query_postgresql_metadata(connection, table_name)
        elif dialect == 'sqlite':
            return self._query_sqlite_metadata(connection, table_name)
        else:
            return {}

    def _query_postgresql_metadata(self, connection: Any, table_name: str) -> dict[str, dict[str, Any]]:
        """Query PostgreSQL system catalog for column metadata.

        Args:
            connection: PostgreSQL database connection
            table_name: Table name to query metadata for

        Returns
            dict: Dictionary of column metadata indexed by column name
        """
        from database.operations.query import select

        try:
            # Extract schema and table names
            parts = table_name.split('.')
            if len(parts) == 1:
                schema, table = 'public', parts[0].strip('"')
            else:
                schema, table = parts[0].strip('"'), parts[1].strip('"')

            sql = """
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """

            @use_iterdict_data_loader
            def get_metadata(cn, query, *params):
                return select(cn, query, *params)

            result = get_metadata(connection, sql, schema, table)

            # Convert to dictionary by column name
            metadata = {}
            for row in result:
                col_name = row['column_name']
                metadata[col_name] = {
                    'name': col_name,
                    'type_name': row['data_type'],
                    'max_length': row['character_maximum_length'],
                    'precision': row['numeric_precision'],
                    'scale': row['numeric_scale'],
                    'is_nullable': row['is_nullable'] == 'YES'
                }
            return metadata
        except Exception as e:
            logger.debug(f'Error querying PostgreSQL meta {e}')
            return {}

    def _query_sqlite_metadata(self, connection: Any, table_name: str) -> dict[str, dict[str, Any]]:
        """Query SQLite system catalog for column metadata.

        Args:
            connection: SQLite database connection
            table_name: Table name to query metadata for

        Returns
            dict: Dictionary of column metadata indexed by column name
        """
        from database.operations.query import select

        try:
            # SQLite doesn't have schemas
            table = table_name.strip('"')
            sql = f"PRAGMA table_info('{table}')"

            @use_iterdict_data_loader
            def get_metadata(cn, query):
                return select(cn, query)

            result = get_metadata(connection, sql)

            # Convert to dictionary by column name
            metadata = {}
            for row in result:
                col_name = row['name']
                # Parse type string for precision/scale if available
                type_info = row['type'].upper()
                precision = None
                scale = None

                # Extract precision/scale from types like NUMERIC(10,2)
                if '(' in type_info and ')' in type_info:
                    try:
                        type_base = type_info.split('(')[0]
                        type_params = type_info.split('(')[1].split(')')[0]
                        if ',' in type_params:
                            prec, scl = type_params.split(',')
                            precision = int(prec.strip())
                            scale = int(scl.strip())
                        else:
                            precision = int(type_params.strip())
                    except (ValueError, IndexError):
                        pass

                metadata[col_name] = {
                    'name': col_name,
                    'type_name': type_info.split('(')[0] if '(' in type_info else type_info,
                    'max_length': None,  # SQLite doesn't provide this directly
                    'precision': precision,
                    'scale': scale,
                    'is_nullable': not row['notnull']
                }
            return metadata
        except Exception as e:
            logger.debug(f'Error querying SQLite meta {e}')
            return {}

    def clear(self) -> None:
        """Clear all cache entries.

        This will clear all schema caches across all connections.
        """
        # Use the CacheManager to clear all schema caches
        self.cache_manager.clear_all()

    def clear_for_table(self, table_name: str) -> None:
        """Clear cache entries for a specific table.

        This is useful when a table's schema has changed but you don't want to
        clear the entire cache.

        Args:
            table_name: Name of the table to clear cache entries for
        """
        # Delegate to the CacheManager for all clearing operations
        # This ensures coordinated clearing of both strategy and schema caches
        logger.debug(f'SchemaCache delegating clearing for table {table_name} to CacheManager')
        self.cache_manager.clear_caches_for_table(table_name)
