"""
Database access module with support for PostgreSQL, SQLite, and SQL Server.
"""


def initialize_adapters(isolated=False):
    """Initialize database adapters, called automatically on first use"""
    if not hasattr(initialize_adapters, '_initialized'):
        from database.adapters.type_adapters import register_adapters
        register_adapters(isolated=isolated)
        initialize_adapters._initialized = True
    return initialize_adapters._initialized


initialize_adapters()


# Core functionality
from database.core.connection import connect
# Exception classes
from database.core.exceptions import ConnectionError, DatabaseError
from database.core.exceptions import DbConnectionError, IntegrityError
from database.core.exceptions import IntegrityViolationError, OperationalError
from database.core.exceptions import ProgrammingError, QueryError
from database.core.exceptions import TypeConversionError, UniqueViolation
from database.core.query import execute
from database.core.transaction import Transaction as transaction
# Data operations
from database.operations.data import delete, insert, insert_row, insert_rows
from database.operations.data import update, update_or_insert, update_row
# Query operations
from database.operations.query import callproc, select, select_column
from database.operations.query import select_column_unique, select_row
from database.operations.query import select_row_or_none, select_scalar
from database.operations.query import select_scalar_or_none
# Schema operations
from database.operations.schema import cluster_table, reindex_table
from database.operations.schema import reset_table_sequence, vacuum_table
# Upsert operations
from database.operations.upsert import upsert_rows
# Options
from database.options import DatabaseOptions
# Connection utilities
from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pymssql_connection
from database.utils.connection_utils import is_sqlite3_connection
from database.utils.connection_utils import isconnection
# Column information
from database.adapters.column_info import Column

# Define public exports
__all__ = [
    # Core functionality
    'transaction',
    'connect',
    'execute',
    # Data operations
    'delete',
    'insert',
    'update',
    'insert_row',
    'insert_rows',
    'update_or_insert',
    'update_row',
    'upsert_rows',
    # Query operations
    'callproc',
    'select',
    'select_column',
    'select_column_unique',
    'select_row',
    'select_row_or_none',
    'select_scalar',
    'select_scalar_or_none',
    # Schema operations
    'reset_table_sequence',
    'vacuum_table',
    'reindex_table',
    'cluster_table',
    # Connection utilities
    'isconnection',
    'is_psycopg_connection',
    'is_pymssql_connection',
    'is_sqlite3_connection',
    # Options
    'DatabaseOptions',
    # Column information
    'Column',
    # Exceptions
    'IntegrityError',
    'ProgrammingError',
    'OperationalError',
    'UniqueViolation',
    'DbConnectionError',
    'ConnectionError',
    'DatabaseError',
    'IntegrityViolationError',
    'QueryError',
    'TypeConversionError',
]
