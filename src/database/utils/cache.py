"""
Backwards compatibility module.

The cache module has been consolidated into database.cache.
This module re-exports from the new location for backwards compatibility.
"""
from database.cache import Cache
from database.cache import \
    Cache as CacheManager  # Backwards compatibility alias
from database.cache import _create_cache_key, cacheable_strategy
from database.cache import get_schema_cache

__all__ = ['Cache', 'CacheManager', 'cacheable_strategy', '_create_cache_key', 'get_schema_cache']
