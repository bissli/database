"""
Database strategy factory for database-specific operations.
"""
from functools import lru_cache

from database.connection import get_dialect_name
from database.strategy.base import DatabaseStrategy as DatabaseStrategy
from database.strategy.postgres import PostgresStrategy
from database.strategy.sqlite import SQLiteStrategy


@lru_cache(maxsize=4)
def _get_strategy(dialect: str) -> DatabaseStrategy:
    """Get cached strategy instance for a dialect."""
    if dialect == 'postgresql':
        return PostgresStrategy()
    if dialect == 'sqlite':
        return SQLiteStrategy()
    raise ValueError(f'Unsupported dialect: {dialect}')


def get_db_strategy(cn) -> DatabaseStrategy:
    """Get database strategy for the connection."""
    dialect = get_dialect_name(cn)
    return _get_strategy(dialect)
