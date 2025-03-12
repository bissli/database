"""
Database strategy factory for database-specific operations.
"""
from database.strategy.base import DatabaseStrategy
from database.strategy.postgres import PostgresStrategy
from database.strategy.sqlite import SQLiteStrategy
from database.strategy.sqlserver import SQLServerStrategy


def get_db_strategy(cn):
    """Get database strategy for the connection"""
    from database.utils.connection_utils import is_psycopg_connection
    from database.utils.connection_utils import is_pyodbc_connection
    from database.utils.connection_utils import is_sqlite3_connection

    if is_psycopg_connection(cn):
        return PostgresStrategy()
    if is_sqlite3_connection(cn):
        return SQLiteStrategy()
    if is_pyodbc_connection(cn):
        return SQLServerStrategy()
    raise ValueError(f'Unsupported connection type: {type(cn)}')
