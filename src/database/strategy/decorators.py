"""
Decorators for database strategy classes.

This module provides decorators that enhance database strategy classes with
additional capabilities like caching, logging, and error handling. These decorators
maintain separation of concerns by keeping cross-cutting functionality outside
the strategy implementations themselves.

The caching decorators here focus on high-level strategy results like lists of columns
or primary keys, not detailed column metadata which is handled by SchemaCache.
These two caching systems work together but serve different purposes:
- Strategy caching (this module): Caches higher-level schema information such as
  lists of column names, primary key names, etc.
- SchemaCache: Stores detailed per-column metadata from database system catalogs
  including type information, nullability, precision, scale, etc.

Both caching systems use the shared CacheManager for consistent cache behavior and
clearing capabilities.
"""
import functools
import logging
from collections.abc import Callable
from typing import TypeVar

from database.utils.cache import CacheManager

logger = logging.getLogger(__name__)

T = TypeVar('T')


# Create a deterministic cache key from the arguments
def create_cache_key(table_name, method_args, method_kwargs):
    """Create a deterministic cache key from the arguments.

    Args:
        table_name: The name of the table (main part of the key)
        method_args: Positional arguments to the method
        method_kwargs: Keyword arguments to the method

    Returns
        str: A deterministic string key for caching
    """
    # Skip self/cls argument by starting from index 1
    args_str = ':'.join(repr(arg) for arg in method_args[1:]
                        if not hasattr(arg, 'cursor') and
                        not hasattr(arg, 'driver_connection'))  # Skip connection objects

    # Sort kwargs for deterministic ordering and skip bypass_cache and connection-like objects
    kwargs_str = ':'.join(f'{k}={repr(v)}' for k, v in sorted(method_kwargs.items())
                          if k != 'bypass_cache' and
                          not hasattr(v, 'cursor') and
                          not hasattr(v, 'driver_connection'))

    return f'{table_name}:{args_str}:{kwargs_str}'.lower()


def cacheable_strategy(cache_name: str, ttl: int = 300, maxsize: int = 50) -> Callable:
    """Decorator for making strategy methods cacheable

    This decorator enables caching for strategy methods that retrieve schema
    information. It respects the strategy pattern by keeping cache logic
    separate from strategy implementations.

    Note on inheritance: This decorator uses the actual class name at runtime
    to determine the cache name, which means that if a subclass inherits a
    decorated method without overriding it, the cache will still be specific
    to that subclass.

    Args:
        cache_name: Base name for the cache
        ttl: Time-to-live for cache entries in seconds
        maxsize: Maximum size of the cache

    Returns
        Decorator function that wraps strategy methods with caching
    """
    def decorator(method: Callable) -> Callable:
        @functools.wraps(method)
        def wrapper(self, cn, table, *args, bypass_cache=False, **kwargs):
            # Skip cache if explicitly requested
            if bypass_cache:
                logger.debug(f'Bypassing cache for {method.__name__}({table})')
                return method(self, cn, table, *args, **kwargs)

            try:
                # Get cache with proper name based on decorated method
                strategy_class = self.__class__.__name__
                # Cache naming scheme: {cache_name}_{strategy_class}_{method_name}
                # This ensures unique caches per method even within the same strategy class
                specific_cache_name = f'{cache_name}_{strategy_class}_{method.__name__}'
                cache_manager = CacheManager.get_instance()
                cache = cache_manager.get_cache(specific_cache_name, ttl=ttl, maxsize=maxsize)

                # Use the cache key function to create a deterministic key for this operation
                # This ensures that identical calls with the same arguments get the same cache entry
                cache_key = create_cache_key(table, args, kwargs)

                # Check cache before executing the strategy method
                if not bypass_cache and cache_key in cache:
                    logger.debug(f'Cache hit for {method.__name__}({table})')
                    return cache[cache_key]

                # Cache miss or bypass_cache: Execute the original strategy method
                logger.debug(f'Cache miss or bypass for {method.__name__}({table})')
                result = method(self, cn, table, *args, **kwargs)

                # Only cache the result if we're not bypassing the cache
                if not bypass_cache:
                    cache[cache_key] = result

                return result

            except Exception as e:
                # If any cache operations fail, log and call the original method
                logger.error(f'Cache error in {method.__name__}({table}): {str(e)}')
                return method(self, cn, table, *args, **kwargs)

        # Add method for cache inspection
        wrapper.get_cache_info = lambda: {
            'cache_name_pattern': f'{cache_name}_<strategy_class>_{method.__name__}',
            'ttl': ttl,
            'maxsize': maxsize
        }

        return wrapper
    return decorator
