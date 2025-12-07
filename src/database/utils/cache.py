"""
Caching utilities for database operations.

This module provides a unified cache manager for all database-related cache needs,
supporting both strategy-based caching and detailed schema metadata caching.
The CacheManager provides consistent cache creation, access, and clearing capabilities
across the codebase, reducing duplication and ensuring consistent cache behavior.
"""
import logging
import threading

import cachetools

logger = logging.getLogger(__name__)


class CacheManager:
    """Unified cache manager for the database module"""

    _instance = None
    _caches = {}
    _lock = threading.RLock()

    @classmethod
    def get_instance(cls):
        """Get singleton instance

        Returns
            CacheManager: The singleton instance of CacheManager
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_cache(self, name, maxsize=100, ttl=300):
        """Get or create a TTL cache with the given name

        Args:
            name: Name of the cache to retrieve or create
            maxsize: Maximum size for the cache (if creating new)
            ttl: Time-to-live in seconds for cache entries (if creating new)

        Returns
            TTLCache: The requested cache instance
        """
        if name not in self._caches:
            with self._lock:
                if name not in self._caches:
                    self._caches[name] = cachetools.TTLCache(maxsize=maxsize, ttl=ttl)
        return self._caches[name]

    def get_schema_cache(self, connection_id=None):
        """Get schema cache for a connection

        Args:
            connection_id: ID of the connection to get schema cache for,
                           or None for global schema cache

        Returns
            TTLCache: The schema cache for the specified connection
        """
        if connection_id:
            cache_name = f'schema_{connection_id}'
        else:
            cache_name = 'schema_global'
        return self.get_cache(cache_name, maxsize=50, ttl=600)  # 10 minute TTL for schema

    def get_schema_cache_ids(self):
        """Get all connection IDs that have schema caches

        Returns
            list: List of connection IDs that have schema caches
        """
        schema_cache_ids = []
        for name in self._caches:
            if name.startswith('schema_') and name != 'schema_global':
                try:
                    # Extract the connection ID from the cache name
                    conn_id = name.replace('schema_', '')
                    schema_cache_ids.append(conn_id)
                except (ValueError, IndexError):
                    pass
        return schema_cache_ids

    def clear_all(self):
        """Clear all managed caches

        Removes all entries from all caches managed by this instance.
        """
        with self._lock:
            for cache in self._caches.values():
                cache.clear()

    def clear_cache(self, name):
        """Clear a specific cache

        Args:
            name: Name of the cache to clear
        """
        with self._lock:
            if name in self._caches:
                self._caches[name].clear()

    def get_stats(self):
        """Get statistics about all managed caches

        Useful for monitoring and debugging cache usage and effectiveness.

        Returns
            dict: Dictionary with statistics for each cache including:
                - size: Current number of items
                - maxsize: Maximum capacity
                - ttl: Time-to-live in seconds
                - currsize: Current memory usage approximation
        """
        stats = {}
        for name, cache in self._caches.items():
            stats[name] = {
                'size': len(cache),
                'maxsize': cache.maxsize,
                'ttl': cache.ttl,
                'currsize': getattr(cache, 'currsize', len(cache)),
            }
        return stats

    def get_detailed_stats(self):
        """Get detailed statistics about all cache systems.

        This method provides a comprehensive view of cache usage across both the
        strategy and schema caching systems. It consolidates statistics for easier
        monitoring and performance analysis.

        Returns
            dict: Detailed statistics organized by cache systems with the following structure:
                {
                    'all_caches': {overall cache statistics},
                    'strategy_caches': {statistics for each strategy cache},
                    'schema_caches': {statistics for each schema cache},
                    'system_summary': {summary statistics for entire caching system}
                }
        """
        all_caches_stats = self.get_stats()

        # Get strategy cache statistics
        strategy_caches = self.get_strategy_caches()
        strategy_stats = {
            name: {
                'size': len(cache),
                'maxsize': cache.maxsize,
                'ttl': cache.ttl,
                'utilization': len(cache) / cache.maxsize if cache.maxsize > 0 else 1.0,
                'key_count': len(cache),
                'cache_type': 'strategy'
            }
            for name, cache in strategy_caches.items()
        }

        # Get schema cache statistics
        schema_stats = {}
        for conn_id in self.get_schema_cache_ids():
            conn_cache = self.get_schema_cache(conn_id)
            cache_name = f'schema_{conn_id}'

            # Analyze entry count per table
            table_counts = {}
            for key in conn_cache:
                # Schema cache keys are typically table names
                table_name = key.split(':')[0] if ':' in key else key
                table_counts[table_name] = table_counts.get(table_name, 0) + 1

            schema_stats[cache_name] = {
                'size': len(conn_cache),
                'maxsize': conn_cache.maxsize,
                'ttl': conn_cache.ttl,
                'utilization': len(conn_cache) / conn_cache.maxsize if conn_cache.maxsize > 0 else 1.0,
                'tables_cached': len(table_counts),
                'table_entries': table_counts,
                'cache_type': 'schema'
            }

        # Add global schema cache
        global_cache = self.get_schema_cache(None)
        if len(global_cache) > 0:
            schema_stats['schema_global'] = {
                'size': len(global_cache),
                'maxsize': global_cache.maxsize,
                'ttl': global_cache.ttl,
                'utilization': len(global_cache) / global_cache.maxsize if global_cache.maxsize > 0 else 1.0,
                'cache_type': 'schema_global'
            }

        # Calculate system summary
        total_entries = sum(len(cache) for cache in self._caches.values())
        total_capacity = sum(cache.maxsize for cache in self._caches.values())
        overall_utilization = total_entries / total_capacity if total_capacity > 0 else 0

        system_summary = {
            'total_caches': len(self._caches),
            'total_strategy_caches': len(strategy_caches),
            'total_schema_caches': len(schema_stats),
            'total_entries': total_entries,
            'total_capacity': total_capacity,
            'overall_utilization': overall_utilization,
            'strategy_entries': sum(stats['size'] for stats in strategy_stats.values()),
            'schema_entries': sum(stats['size'] for stats in schema_stats.values()),
        }

        return {
            'all_caches': all_caches_stats,
            'strategy_caches': strategy_stats,
            'schema_caches': schema_stats,
            'system_summary': system_summary
        }

    def get_strategy_caches(self):
        """Get all strategy-related caches

        This is useful for clearing or introspecting just the strategy caches.

        Returns
            dict: Dictionary mapping cache names to cache objects
        """
        strategy_caches = {}
        strategy_prefixes = ['primary_keys_', 'table_columns_',
                             'sequence_columns_', 'sequence_column_finder_']
        strategy_classes = ['PostgresStrategy', 'SQLiteStrategy']

        for name, cache in self._caches.items():
            # Check for known prefixes
            if any(name.startswith(prefix) for prefix in strategy_prefixes):
                strategy_caches[name] = cache
            # Or if it contains a strategy class name
            elif any(strategy_class in name for strategy_class in strategy_classes):
                strategy_caches[name] = cache
            # Or if it's a strategy cache based on naming pattern
            elif '_get_columns' in name or '_get_primary_keys' in name:
                strategy_caches[name] = cache
        return strategy_caches

    def clear_strategy_caches(self):
        """Clear all strategy-related caches

        This is useful when schema changes are detected and cached table
        information needs to be refreshed.
        """
        with self._lock:
            for name, cache in self.get_strategy_caches().items():
                logger.debug(f'Clearing strategy cache: {name}')
                cache.clear()

    def clear_strategy_caches_for_table(self, table_name: str):
        """Clear strategy cache entries for a specific table

        This allows targeted cache clearing when a specific table's
        schema has changed, without affecting cache entries for other tables.

        Args:
            table_name: Name of the table to clear cache entries for
        """
        table_name_lower = table_name.lower()
        with self._lock:
            for cache_name, cache in self.get_strategy_caches().items():
                # Strategy cache keys start with the table name
                keys_to_clear = [key for key in list(cache.keys()) if key.lower().startswith(table_name_lower)]

                for key in keys_to_clear:
                    if key in cache:
                        del cache[key]
                        logger.debug(f'Cleared strategy cache entry {key} for table {table_name} from {cache_name}')

    def clear_caches_for_table(self, table_name: str):
        """Clear all cache entries (both strategy and schema) for a specific table.

        This provides a unified method to clear both strategy caches and schema caches
        for a given table, ensuring consistency across the caching system when table
        structures change.

        Args:
            table_name: Name of the table to clear cache entries for
        """
        # First clear strategy caches
        self.clear_strategy_caches_for_table(table_name)

        # Then clear schema caches
        table_name_lower = table_name.lower()
        with self._lock:
            for conn_id in self.get_schema_cache_ids():
                conn_cache = self.get_schema_cache(conn_id)
                keys_to_clear = [key for key in list(conn_cache.keys()) if table_name_lower in key.lower()]

                for key in keys_to_clear:
                    if key in conn_cache:
                        del conn_cache[key]
                        logger.debug(f'Cleared schema cache entry {key} for table {table_name} from connection {conn_id}')
