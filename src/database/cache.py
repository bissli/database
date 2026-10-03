"""Process-wide TTL caches for schema metadata and strategy lookups.
"""
import functools
import logging
import threading
from collections.abc import Callable
from typing import Any

import cachetools

logger = logging.getLogger(__name__)


class Cache:
    """Process-wide registry of named TTL caches.

    Creating and clearing caches take a lock. Reading and writing an
    entry does not.
    """

    _instance = None
    _caches: dict[str, cachetools.TTLCache] = {}
    _lock = threading.RLock()

    @classmethod
    def get_instance(cls) -> 'Cache':
        """The one shared Cache, created on first call.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_cache(self, name: str, maxsize: int = 100,
                  ttl: int = 300) -> cachetools.TTLCache:
        """The TTL cache registered under name, created on first request.

        Parameters
        ----------
        name : str
            Cache name.
        maxsize : int, default 100
            Entry limit; applies only when this call creates the cache.
        ttl : int, default 300
            Entry lifetime in seconds; applies only when this call creates
            the cache.

        Returns
        -------
        cachetools.TTLCache
            The same object on every call for a name.
        """
        if name not in self._caches:
            with self._lock:
                if name not in self._caches:
                    self._caches[name] = cachetools.TTLCache(maxsize=maxsize, ttl=ttl)
        return self._caches[name]

    def clear_all(self) -> None:
        """Empty every cache, keeping each object a caller may hold.
        """
        with self._lock:
            for cache in self._caches.values():
                cache.clear()

    def clear_cache(self, name: str) -> None:
        """Empty the named cache; an unknown name does nothing.
        """
        with self._lock:
            if name in self._caches:
                self._caches[name].clear()

    def clear_for_table(self, table_name: str) -> None:
        """Drop every entry, in every cache, whose key mentions a table.

        Parameters
        ----------
        table_name : str
            Matched case-insensitively as a substring of str(key), so
            'orders' also drops the entries of 'orders_archive'. An empty
            name logs a warning and drops nothing.
        """
        if not table_name:
            logger.warning('clear_for_table called with an empty table name; '
                           'ignoring rather than clearing every cache')
            return

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
                        logger.debug(
                            f'Cleared cache entry {key} for table {table_name}')

    clear_caches_for_table = clear_for_table

    def get_strategy_caches(self) -> dict[str, cachetools.TTLCache]:
        """Caches named for the cache_name prefixes the shipped strategies use.

        Returns
        -------
        dict[str, cachetools.TTLCache]
            Live caches keyed by name.
        """
        strategy_prefixes = ('primary_keys_', 'table_columns_',
                             'sequence_columns_', 'sequence_column_finder_')
        return {
            name: cache for name, cache in self._caches.items()
            if any(name.startswith(prefix) for prefix in strategy_prefixes)
        }

    def clear_strategy_caches(self) -> None:
        """Empty the caches get_strategy_caches selects; schema caches stay.
        """
        with self._lock:
            for cache in self.get_strategy_caches().values():
                cache.clear()

    def get_schema_cache(self, connection_id: int | None = None) -> cachetools.TTLCache:
        """Schema metadata cache for one connection.

        Parameters
        ----------
        connection_id : int or None, default None
            None selects the shared 'schema_global' cache. Any other
            value, 0 included, gets its own cache named
            'schema_{connection_id}'.

        Returns
        -------
        cachetools.TTLCache
            50 entries, 600 seconds each.
        """
        cache_name = ('schema_global' if connection_id is None
                      else f'schema_{connection_id}')
        return self.get_cache(cache_name, maxsize=50, ttl=600)


_MISS = object()


def _create_cache_key(table_name: str, method_args: tuple, method_kwargs: dict) -> str:
    """Cache key for one call of a cacheable_strategy method.

    Parameters
    ----------
    table_name : str
        Table the call is for; clear_for_table matches on it.
    method_args : tuple
        Positional arguments after cn and table. An argument with a cursor
        or driver_connection attribute counts as a connection and is left
        out.
    method_kwargs : dict
        Keyword arguments; bypass_cache is left out.

    Returns
    -------
    str
        'table:arg:...:name=value:...' built from repr() of each value,
        with kwargs sorted by name, the whole key lower-cased.
    """
    args_str = ':'.join(
        repr(arg) for arg in method_args
        if not hasattr(arg, 'cursor') and not hasattr(arg, 'driver_connection')
    )

    kwargs_str = ':'.join(
        f'{k}={repr(v)}' for k, v in sorted(method_kwargs.items())
        if k != 'bypass_cache'
        and not hasattr(v, 'cursor')
        and not hasattr(v, 'driver_connection')
    )

    return f'{table_name}:{args_str}:{kwargs_str}'.lower()


def cacheable_strategy(cache_name: str, ttl: int = 300,
                       maxsize: int = 50) -> Callable[[Callable], Callable]:
    """Decorator that caches a strategy method's result per table.

    The method must take (self, cn, table, ...).

    Parameters
    ----------
    cache_name : str
        Prefix of the cache name. Each class and method gets its own
        cache, '{cache_name}_{ClassName}_{method name}'.
    ttl : int, default 300
        Entry lifetime in seconds.
    maxsize : int, default 50
        Entry limit per cache.

    Returns
    -------
    Callable
        Decorator that wraps the method. bypass_cache=True, passed by
        keyword, runs the method and neither reads nor writes the cache. A
        hit returns the stored object itself, so mutating it changes the
        cached value.
    """
    def decorator(method: Callable) -> Callable:
        @functools.wraps(method)
        def wrapper(self: Any, cn: Any, table: str, *args: Any,
                    bypass_cache: bool = False, **kwargs: Any) -> Any:
            if bypass_cache:
                logger.debug(f'Bypassing cache for {method.__name__}({table})')
                return method(self, cn, table, *args, bypass_cache=True, **kwargs)

            try:
                strategy_class = self.__class__.__name__
                specific_cache_name = f'{cache_name}_{strategy_class}_{method.__name__}'
                cache = Cache.get_instance().get_cache(
                    specific_cache_name, ttl=ttl, maxsize=maxsize)
                cache_key = _create_cache_key(table, args, kwargs)
                cached = cache.get(cache_key, _MISS)
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(f'Cache error in {method.__name__}({table}): {e}')
                return method(self, cn, table, *args, **kwargs)

            if cached is not _MISS:
                logger.debug(f'Cache hit for {method.__name__}({table})')
                return cached

            logger.debug(f'Cache miss for {method.__name__}({table})')
            result = method(self, cn, table, *args, **kwargs)
            try:
                cache[cache_key] = result
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(f'Could not store cache entry for '
                               f'{method.__name__}({table}): {e}')
            return result

        return wrapper
    return decorator


def get_schema_cache(connection_id: int | None = None) -> cachetools.TTLCache:
    """Cache.get_schema_cache on the shared Cache instance.
    """
    return Cache.get_instance().get_schema_cache(connection_id)
