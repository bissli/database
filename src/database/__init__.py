"""
Database access module with support for PostgreSQL, SQLite, and SQL Server.
"""
from database.adapters.type_conversion import get_adapter_registry

adapter_registry = get_adapter_registry()

from database.adapters.column_info import Column
from database.core.connection import connect
from database.core.exceptions import ConnectionError, DatabaseError
from database.core.exceptions import DbConnectionError, IntegrityError
from database.core.exceptions import IntegrityViolationError, OperationalError
from database.core.exceptions import ProgrammingError, QueryError
from database.core.exceptions import TypeConversionError, UniqueViolation
from database.core.transaction import Transaction as transaction
from database.data import delete, insert, insert_row, insert_rows, update
from database.data import update_or_insert, update_row
from database.options import DatabaseOptions
from database.query import execute, select, select_column, select_row
from database.query import select_row_or_none, select_scalar
from database.query import select_scalar_or_none
from database.schema import cluster_table, reindex_table, reset_table_sequence
from database.schema import vacuum_table
from database.upsert import upsert_rows

__all__ = [
    'transaction',
    'connect',
    'execute',
    'delete',
    'insert',
    'update',
    'insert_row',
    'insert_rows',
    'update_or_insert',
    'update_row',
    'upsert_rows',
    'select',
    'select_column',
    'select_row',
    'select_row_or_none',
    'select_scalar',
    'select_scalar_or_none',
    'reset_table_sequence',
    'vacuum_table',
    'reindex_table',
    'cluster_table',
    'DatabaseOptions',
    'Column',
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
