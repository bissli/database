"""
Caching utilities for database operations.
"""
import logging

import cachetools

logger = logging.getLogger(__name__)


def ignore_first_argument_cache_key(cls, *args, **kwargs):
    """Cache key function that ignores the first argument (usually self/cls)"""
    return cachetools.keys.hashkey(*args, **kwargs)


class TTLCacheManager:
    """Manages TTL caches with monitoring capabilities"""

    _caches = {}

    @classmethod
    def get_cache(cls, name, maxsize=100, ttl=300):
        """Get or create a TTL cache with the given name"""
        if name not in cls._caches:
            cls._caches[name] = cachetools.TTLCache(maxsize=maxsize, ttl=ttl)
        return cls._caches[name]

    @classmethod
    def clear_all(cls):
        """Clear all managed caches"""
        for cache in cls._caches.values():
            cache.clear()

    @classmethod
    def get_stats(cls):
        """Get statistics about all managed caches"""
        stats = {}
        for name, cache in cls._caches.items():
            stats[name] = {
                'size': len(cache),
                'maxsize': cache.maxsize,
                'ttl': cache.ttl,
                'currsize': cache.currsize,
            }
        return stats
