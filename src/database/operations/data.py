"""
Backwards compatibility module.

The data operations have been moved to database.data.
This module re-exports from the new location.
"""
from database.data import delete, filter_table_columns, insert, insert_row
from database.data import insert_rows, table_data, update, update_or_insert
from database.data import update_row, update_row_sql

__all__ = [
    'delete',
    'filter_table_columns',
    'insert',
    'insert_row',
    'insert_rows',
    'table_data',
    'update',
    'update_or_insert',
    'update_row',
    'update_row_sql',
]
