"""
Backwards compatibility module.

The query operations have been moved to database.query.
This module re-exports from the new location.
"""
from database.query import execute, select, select_column, select_row
from database.query import select_row_or_none, select_scalar
from database.query import select_scalar_or_none

__all__ = [
    'execute',
    'select',
    'select_column',
    'select_row',
    'select_row_or_none',
    'select_scalar',
    'select_scalar_or_none',
]
