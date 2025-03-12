"""
Caching utilities for database operations.
"""
import logging
import threading

import cachetools

logger = logging.getLogger(__name__)


def ignore_first_argument_cache_key(cls, *args, **kwargs):
    """Cache key function that ignores the first argument (usually self/cls)"""
    return cachetools.keys.hashkey(*args, **kwargs)


class CacheManager:
    """Unified cache manager for the database module"""

    _instance = None
    _caches = {}
    _lock = threading.RLock()

    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_cache(self, name, maxsize=100, ttl=300):
        """Get or create a TTL cache with the given name"""
        if name not in self._caches:
            with self._lock:
                if name not in self._caches:
                    self._caches[name] = cachetools.TTLCache(maxsize=maxsize, ttl=ttl)
        return self._caches[name]

    def get_schema_cache(self, connection_id=None):
        """Get schema cache for a connection"""
        if connection_id:
            cache_name = f'schema_{connection_id}'
        else:
            cache_name = 'schema_global'
        return self.get_cache(cache_name, maxsize=50, ttl=600)  # 10 minute TTL for schema

    def clear_all(self):
        """Clear all managed caches"""
        with self._lock:
            for cache in self._caches.values():
                cache.clear()

    def clear_cache(self, name):
        """Clear a specific cache"""
        with self._lock:
            if name in self._caches:
                self._caches[name].clear()

    def get_stats(self):
        """Get statistics about all managed caches"""
        stats = {}
        for name, cache in self._caches.items():
            stats[name] = {
                'size': len(cache),
                'maxsize': cache.maxsize,
                'ttl': cache.ttl,
                'currsize': getattr(cache, 'currsize', len(cache)),
            }
        return stats
