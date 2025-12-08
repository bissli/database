"""
Backwards compatibility module.

The cursor module has been moved to database.cursor.
This module re-exports from the new location.
"""
from database.cursor import Cursor
from database.cursor import Cursor as AbstractCursor  # Backwards compatibility
from database.cursor import DictRowFactory, FakeCursor, IterChunk
from database.cursor import PostgresqlCursor, SqliteCursor, get_dict_cursor


# Re-export decorators for backwards compatibility
def batch_execute(func):
    """Deprecated - batching is now handled inline."""
    return func


def convert_params(func):
    """Deprecated - conversion is now handled inline."""
    return func


def dumpsql(func):
    """Deprecated - logging is now handled inline."""
    return func


__all__ = [
    'Cursor', 'AbstractCursor',
    'PostgresqlCursor', 'SqliteCursor', 'FakeCursor',
    'IterChunk', 'DictRowFactory', 'get_dict_cursor',
    'batch_execute', 'convert_params', 'dumpsql',
]
