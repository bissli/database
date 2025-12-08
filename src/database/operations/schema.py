"""
Backwards compatibility module.

The schema operations have been moved to database.schema.
This module re-exports from the new location.
"""
from database.schema import cluster_table, find_sequence_column
from database.schema import get_sequence_columns, get_table_columns
from database.schema import get_table_primary_keys, reindex_table
from database.schema import reset_table_sequence, table_fields, vacuum_table

__all__ = [
    'cluster_table',
    'find_sequence_column',
    'get_sequence_columns',
    'get_table_columns',
    'get_table_primary_keys',
    'reindex_table',
    'reset_table_sequence',
    'table_fields',
    'vacuum_table',
]
