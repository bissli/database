"""
Backwards compatibility module.

The execute function has been moved to database.query.
This module re-exports from the new location.
"""
from database.query import execute

__all__ = ['execute']
