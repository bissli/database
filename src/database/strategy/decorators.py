"""
Backwards compatibility module.

The decorators have been consolidated into database.cache.
This module re-exports from the new location for backwards compatibility.
"""
from database.cache import _create_cache_key as create_cache_key
from database.cache import cacheable_strategy

__all__ = ['cacheable_strategy', 'create_cache_key']
