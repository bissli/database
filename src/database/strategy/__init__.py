"""
Database strategy factory for database-specific operations.
"""
from database.strategy.base import DatabaseStrategy
from database.strategy.postgres import PostgresStrategy
from database.strategy.sqlite import SQLiteStrategy
from database.strategy.sqlserver import SQLServerStrategy
from database.utils.connection_utils import get_dialect_name


def get_db_strategy(cn):
    """Get database strategy for the connection"""

    dialect = get_dialect_name(cn)
    if dialect == 'postgresql':
        return PostgresStrategy()
    if dialect == 'sqlite':
        return SQLiteStrategy()
    if dialect == 'mssql':
        return SQLServerStrategy()
    raise ValueError(f'Unsupported connection type: {type(cn)}')
