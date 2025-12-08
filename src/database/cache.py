"""
Unified caching for database operations.

Provides a single, simple caching system for schema metadata and strategy results.
Uses cachetools TTLCache for automatic expiration.
"""
import functools
import logging
import threading

import cachetools

logger = logging.getLogger(__name__)


class Cache:
    """Unified cache manager for the database module.

    Thread-safe singleton that manages all TTL caches.
    """

    _instance = None
    _caches: dict[str, cachetools.TTLCache] = {}
    _lock = threading.RLock()

    @classmethod
    def get_instance(cls) -> 'Cache':
        """Get singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_cache(self, name: str, maxsize: int = 100, ttl: int = 300) -> cachetools.TTLCache:
        """Get or create a TTL cache with the given name.

        Args:
            name: Name of the cache
            maxsize: Maximum cache size
            ttl: Time-to-live in seconds

        Returns
            TTLCache instance
        """
        if name not in self._caches:
            with self._lock:
                if name not in self._caches:
                    self._caches[name] = cachetools.TTLCache(maxsize=maxsize, ttl=ttl)
        return self._caches[name]

    def clear_all(self) -> None:
        """Clear all managed caches."""
        with self._lock:
            for cache in self._caches.values():
                cache.clear()

    def clear_cache(self, name: str) -> None:
        """Clear a specific cache by name."""
        with self._lock:
            if name in self._caches:
                self._caches[name].clear()

    def clear_for_table(self, table_name: str) -> None:
        """Clear all cache entries related to a specific table.

        Args:
            table_name: Name of the table to clear cache entries for
        """
        table_lower = table_name.lower()
        with self._lock:
            for cache in self._caches.values():
                keys_to_clear = [
                    key for key in list(cache.keys())
                    if table_lower in str(key).lower()
                ]
                for key in keys_to_clear:
                    if key in cache:
                        del cache[key]
                        logger.debug(f'Cleared cache entry {key} for table {table_name}')

    # Alias for backwards compatibility
    clear_caches_for_table = clear_for_table

    def get_strategy_caches(self) -> dict[str, cachetools.TTLCache]:
        """Get all strategy-related caches.

        Returns
            Dict mapping cache names to TTLCache instances
        """
        strategy_prefixes = ('primary_keys_', 'table_columns_',
                             'sequence_columns_', 'sequence_column_finder_')
        return {
            name: cache for name, cache in self._caches.items()
            if any(name.startswith(prefix) for prefix in strategy_prefixes)
        }

    def clear_strategy_caches(self) -> None:
        """Clear all strategy-related caches."""
        with self._lock:
            for cache in self.get_strategy_caches().values():
                cache.clear()

    def get_schema_cache(self, connection_id: int | None = None) -> cachetools.TTLCache:
        """Get schema cache for a connection.

        Args:
            connection_id: Connection identifier, or None for global cache

        Returns
            TTLCache for schema metadata
        """
        cache_name = f'schema_{connection_id}' if connection_id else 'schema_global'
        return self.get_cache(cache_name, maxsize=50, ttl=600)


def _create_cache_key(table_name: str, method_args: tuple, method_kwargs: dict) -> str:
    """Create a deterministic cache key from arguments.

    Excludes connection objects (detected by cursor/driver_connection attributes).
    """
    args_str = ':'.join(
        repr(arg) for arg in method_args[1:]
        if not hasattr(arg, 'cursor') and not hasattr(arg, 'driver_connection')
    )

    kwargs_str = ':'.join(
        f'{k}={repr(v)}' for k, v in sorted(method_kwargs.items())
        if k != 'bypass_cache'
        and not hasattr(v, 'cursor')
        and not hasattr(v, 'driver_connection')
    )

    return f'{table_name}:{args_str}:{kwargs_str}'.lower()


def cacheable_strategy(cache_name: str, ttl: int = 300, maxsize: int = 50):
    """Decorator for caching strategy method results.

    Caches results keyed by table name and method arguments.
    Respects bypass_cache parameter to skip cache lookup.

    Args:
        cache_name: Base name for the cache
        ttl: Time-to-live in seconds
        maxsize: Maximum cache size
    """
    def decorator(method):
        @functools.wraps(method)
        def wrapper(self, cn, table, *args, bypass_cache=False, **kwargs):
            if bypass_cache:
                logger.debug(f'Bypassing cache for {method.__name__}({table})')
                return method(self, cn, table, *args, **kwargs)

            try:
                # Create cache name specific to strategy class and method
                strategy_class = self.__class__.__name__
                specific_cache_name = f'{cache_name}_{strategy_class}_{method.__name__}'

                cache = Cache.get_instance().get_cache(specific_cache_name, ttl=ttl, maxsize=maxsize)
                cache_key = _create_cache_key(table, args, kwargs)

                if cache_key in cache:
                    logger.debug(f'Cache hit for {method.__name__}({table})')
                    return cache[cache_key]

                logger.debug(f'Cache miss for {method.__name__}({table})')
                result = method(self, cn, table, *args, **kwargs)
                cache[cache_key] = result
                return result

            except Exception as e:
                logger.error(f'Cache error in {method.__name__}({table}): {e}')
                return method(self, cn, table, *args, **kwargs)

        return wrapper
    return decorator


def get_schema_cache(connection_id: int | None = None) -> cachetools.TTLCache:
    """Get schema cache for a connection.

    Args:
        connection_id: Connection identifier, or None for global cache

    Returns
        TTLCache for schema metadata
    """
    cache_name = f'schema_{connection_id}' if connection_id else 'schema_global'
    return Cache.get_instance().get_cache(cache_name, maxsize=50, ttl=600)
