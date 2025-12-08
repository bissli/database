"""
Backwards compatibility module.

Operations have been moved to top-level modules:
- database.query
- database.data
- database.schema
- database.upsert

This module provides lazy re-exports for backwards compatibility.
"""


# Use lazy imports to avoid circular dependencies
def __getattr__(name):
    """Lazy import for backwards compatibility."""
    # Query operations
    if name in {'select', 'select_column', 'select_row', 'select_row_or_none',
                'select_scalar', 'select_scalar_or_none', 'execute'}:
        return locals()[name]

    # Data operations
    if name in {'delete', 'insert', 'update', 'insert_row', 'insert_rows',
                'update_or_insert', 'update_row', 'update_row_sql',
                'filter_table_columns', 'table_data'}:
        return locals()[name]

    # Schema operations
    if name in {'cluster_table', 'find_sequence_column', 'get_sequence_columns',
                'get_table_columns', 'get_table_primary_keys', 'reindex_table',
                'reset_table_sequence', 'table_fields', 'vacuum_table'}:
        return locals()[name]

    # Upsert operations
    if name == 'upsert_rows':
        from database.upsert import upsert_rows
        return upsert_rows

    raise AttributeError(f"module 'database.operations' has no attribute {name!r}")


__all__ = [
    # Query operations
    'select', 'select_column', 'select_row', 'select_row_or_none',
    'select_scalar', 'select_scalar_or_none', 'execute',
    # Data operations
    'delete', 'insert', 'update', 'insert_row', 'insert_rows',
    'update_or_insert', 'update_row', 'update_row_sql',
    'filter_table_columns', 'table_data',
    # Schema operations
    'cluster_table', 'find_sequence_column', 'get_sequence_columns',
    'get_table_columns', 'get_table_primary_keys', 'reindex_table',
    'reset_table_sequence', 'table_fields', 'vacuum_table',
    # Upsert operations
    'upsert_rows',
]
