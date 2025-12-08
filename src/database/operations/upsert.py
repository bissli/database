"""
Backwards compatibility module.

The upsert operations have been moved to database.upsert.
This module re-exports from the new location.
"""
from database.upsert import upsert_rows

__all__ = ['upsert_rows']
