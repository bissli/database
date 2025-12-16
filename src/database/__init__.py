"""
Database access module with support for PostgreSQL, SQLite, and SQL Server.

All query/data operations can be called either as:
- Module functions: db.select(cn, sql, *args)
- ConnectionWrapper methods: cn.select(sql, *args)

The module functions are facades for backwards compatibility.
"""
__version__ = '0.1.1'

from typing import Any

from database.connection import ConnectionWrapper, connect
from database.exceptions import ConnectionFailure, DatabaseError, ValidationError
from database.exceptions import DbConnectionError, IntegrityError
from database.exceptions import IntegrityViolationError, OperationalError
from database.exceptions import ProgrammingError, QueryError
from database.exceptions import TypeConversionError, UniqueViolation
from database.options import DatabaseOptions
from database.transaction import Transaction as transaction
from database.types import Column, get_adapter_registry

adapter_registry = get_adapter_registry()


def execute(cn: ConnectionWrapper, sql: str, *args: Any) -> int:
    """Execute a SQL query and return affected row count.
    """
    return cn.execute(sql, *args)


delete = execute
insert = execute
update = execute


def select(cn: ConnectionWrapper, sql: str, *args: Any, **kwargs: Any) -> Any:
    """Execute a SELECT query or stored procedure.
    """
    return cn.select(sql, *args, **kwargs)


def select_column(cn: ConnectionWrapper, sql: str, *args: Any) -> list[Any]:
    """Execute a query and return a single column as a list.
    """
    return cn.select_column(sql, *args)


def select_row(cn: ConnectionWrapper, sql: str, *args: Any) -> Any:
    """Execute a query and return a single row.

    Raises ValidationError if the query returns zero or multiple rows.
    """
    return cn.select_row(sql, *args)


def select_row_or_none(cn: ConnectionWrapper, sql: str, *args: Any) -> Any | None:
    """Execute a query and return a single row or None if no rows found.
    """
    return cn.select_row_or_none(sql, *args)


def select_scalar(cn: ConnectionWrapper, sql: str, *args: Any) -> Any:
    """Execute a query and return a single scalar value.

    Raises ValidationError if the query returns zero or multiple rows.
    """
    return cn.select_scalar(sql, *args)


def select_scalar_or_none(cn: ConnectionWrapper, sql: str, *args: Any) -> Any | None:
    """Execute a query and return a single scalar value or None if no rows found.
    """
    return cn.select_scalar_or_none(sql, *args)


def insert_row(cn: ConnectionWrapper, table: str, fields: list[str],
               values: list[Any]) -> int:
    """Insert a row into a table using the supplied list of fields and values.
    """
    return cn.insert_row(table, fields, values)


def insert_rows(cn: ConnectionWrapper, table: str,
                rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> int:
    """Insert multiple rows into a table.
    """
    return cn.insert_rows(table, rows)


def update_row(cn: ConnectionWrapper, table: str, keyfields: list[str],
               keyvalues: list[Any], datafields: list[str],
               datavalues: list[Any]) -> int:
    """Update the specified datafields in a table row identified by keyfields.
    """
    return cn.update_row(table, keyfields, keyvalues, datafields, datavalues)


def update_or_insert(cn: ConnectionWrapper, update_sql: str, insert_sql: str,
                     *args: Any) -> int:
    """Try to update first; if no rows are updated, then insert.
    """
    return cn.update_or_insert(update_sql, insert_sql, *args)


def upsert_rows(
    cn: ConnectionWrapper,
    table: str,
    rows: tuple[dict[str, Any], ...],
    constraint_name: str | None = None,
    update_cols_always: list[str] | None = None,
    update_cols_ifnull: list[str] | None = None,
    reset_sequence: bool = False,
    batch_size: int = 500,
    use_primary_key: bool = False,
    **kw: Any
) -> int:
    """Perform an UPSERT operation (INSERT or UPDATE) for multiple rows.
    """
    return cn.upsert_rows(
        table=table,
        rows=rows,
        constraint_name=constraint_name,
        update_cols_always=update_cols_always,
        update_cols_ifnull=update_cols_ifnull,
        reset_sequence=reset_sequence,
        batch_size=batch_size,
        use_primary_key=use_primary_key,
        **kw)


def reset_table_sequence(cn: ConnectionWrapper, table: str,
                         identity: str | None = None) -> None:
    """Reset a table's sequence/identity column to the max value + 1.
    """
    cn.reset_table_sequence(table, identity)


def vacuum_table(cn: ConnectionWrapper, table: str) -> None:
    """Optimize a table by reclaiming space.
    """
    cn.vacuum_table(table)


def reindex_table(cn: ConnectionWrapper, table: str) -> None:
    """Rebuild indexes for a table.
    """
    cn.reindex_table(table)


def cluster_table(cn: ConnectionWrapper, table: str,
                  index: str | None = None) -> None:
    """Order table data according to an index.
    """
    cn.cluster_table(table, index)


__all__ = [
    'connect',
    'ConnectionWrapper',
    'transaction',
    'DatabaseOptions',
    'execute',
    'delete',
    'insert',
    'update',
    'select',
    'select_column',
    'select_row',
    'select_row_or_none',
    'select_scalar',
    'select_scalar_or_none',
    'insert_row',
    'insert_rows',
    'update_row',
    'update_or_insert',
    'upsert_rows',
    'reset_table_sequence',
    'vacuum_table',
    'reindex_table',
    'cluster_table',
    'Column',
    'IntegrityError',
    'ProgrammingError',
    'OperationalError',
    'UniqueViolation',
    'DbConnectionError',
    'ConnectionFailure',
    'ValidationError',
    'DatabaseError',
    'IntegrityViolationError',
    'QueryError',
    'TypeConversionError',
]
