"""
Backwards compatibility module.

The cursor module has been moved to database.cursor.
This module re-exports from the new location.
"""
from database.cursor import Cursor  # Backwards compatibility
from database.cursor import Cursor as AbstractCursor
from database.cursor import DictRowFactory, FakeCursor, IterChunk
from database.cursor import PostgresqlCursor, SqliteCursor, get_dict_cursor

__all__ = [
    'Cursor', 'AbstractCursor',
    'PostgresqlCursor', 'SqliteCursor', 'FakeCursor',
    'IterChunk', 'DictRowFactory', 'get_dict_cursor',
]
