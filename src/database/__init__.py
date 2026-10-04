"""Database access for PostgreSQL and SQLite.
"""
__version__ = '0.6.4'

from typing import Any, TextIO

from database.connection import ConnectionWrapper, connect
from database.exceptions import ConnectionFailure, DatabaseError
from database.exceptions import DbConnectionError, IntegrityError
from database.exceptions import IntegrityViolationError, OperationalError
from database.exceptions import ProgrammingError, QueryError, ReadOnlyError
from database.exceptions import TypeConversionError, UniqueViolation
from database.exceptions import ValidationError
from database.options import DatabaseOptions
from database.transaction import Transaction as transaction
from database.types import Column, ColumnInfo, get_adapter_registry

adapter_registry = get_adapter_registry()


def execute(cn: ConnectionWrapper, sql: str, *args: Any) -> int:
    """Rows the statement affected, committed unless cn is in a transaction.
    """
    return cn.execute(sql, *args)


delete = execute
insert = execute
update = execute


def select(cn: ConnectionWrapper, sql: str, *args: Any, **kwargs: Any) -> Any:
    """Query rows in the form cn.options.data_loader builds.

    See ConnectionWrapper.select for the keyword arguments.
    """
    return cn.select(sql, *args, **kwargs)


def select_column(cn: ConnectionWrapper, sql: str, *args: Any) -> list[Any]:
    """First-column value of each row the query returns.
    """
    return cn.select_column(sql, *args)


def select_row(cn: ConnectionWrapper, sql: str, *args: Any) -> Any:
    """The query's one row, as an attrdict.

    Raises
    ------
    ValidationError
        When the query returns zero rows or more than one.
    """
    return cn.select_row(sql, *args)


def select_row_or_none(cn: ConnectionWrapper, sql: str, *args: Any) -> Any | None:
    """The query's one row, as an attrdict, or None when it returns none.

    Raises
    ------
    ValidationError
        When the query returns more than one row.
    """
    return cn.select_row_or_none(sql, *args)


def select_scalar(cn: ConnectionWrapper, sql: str, *args: Any) -> Any:
    """First column of the query's one row.

    Raises
    ------
    ValidationError
        When the query returns zero rows or more than one.
    """
    return cn.select_scalar(sql, *args)


def select_scalar_or_none(cn: ConnectionWrapper, sql: str, *args: Any) -> Any | None:
    """First column of the query's one row, or None when it returns none.

    Raises
    ------
    ValidationError
        When the query returns more than one row.
    """
    return cn.select_scalar_or_none(sql, *args)


def insert_row(cn: ConnectionWrapper, table: str, fields: list[str],
               values: list[Any]) -> int:
    """Insert values into the paired fields of table; rows inserted.

    Raises
    ------
    ValidationError
        When fields and values differ in length.
    """
    return cn.insert_row(table, fields, values)


def insert_rows(cn: ConnectionWrapper, table: str,
                rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> int:
    """Insert dict rows into table, dropping keys it has no column for.

    Returns the driver's rowcount, or 0 for no rows.
    """
    return cn.insert_rows(table, rows)


def update_row(cn: ConnectionWrapper, table: str, keyfields: list[str],
               keyvalues: list[Any], datafields: list[str],
               datavalues: list[Any]) -> int:
    """Set datafields to datavalues in the rows keyfields match.

    Parameters
    ----------
    cn : ConnectionWrapper
        Writer connection.
    table : str
        Table name, optionally schema-qualified.
    keyfields : list[str]
        Columns a row must match, all of them.
    keyvalues : list[Any]
        Values for keyfields, in order.
    datafields : list[str]
        Columns to set.
    datavalues : list[Any]
        Values for datafields, in order.

    Returns
    -------
    int
        Rows updated.

    Raises
    ------
    ValidationError
        When a field list and its value list differ in length, or a
        keyfield is also a datafield.
    """
    return cn.update_row(table, keyfields, keyvalues, datafields, datavalues)


def update_or_insert(cn: ConnectionWrapper, update_sql: str, insert_sql: str,
                     *args: Any) -> int:
    """Run update_sql, and insert_sql only when it changes no row.

    Both statements take the same args and run in one transaction.
    Returns the rowcount of the last statement run.
    """
    return cn.update_or_insert(update_sql, insert_sql, *args)


def upsert_rows(
    cn: ConnectionWrapper,
    table: str,
    rows: tuple[dict[str, Any], ...],
    constraint_name: str | None = None,
    conflict_columns: list[str] | None = None,
    update_cols_always: list[str] | None = None,
    update_cols_ifnull: list[str] | None = None,
    reset_sequence: bool = False,
    batch_size: int = 500,
    use_primary_key: bool = False,
) -> int:
    """Insert rows, resolving a conflict on the key by update or skip.

    Parameters
    ----------
    cn : ConnectionWrapper
        Writer connection.
    table : str
        Table name, optionally schema-qualified.
    rows : tuple[dict[str, Any], ...]
        Rows as dicts. Keys with no column in table are dropped. Empty
        rows return 0.
    constraint_name : str | None, default None
        PostgreSQL only: the conflict target, by constraint or unique
        index name. Ignored on SQLite.
    conflict_columns : list[str] | None, default None
        Conflict target columns, covered by a unique constraint or index.
        With neither this nor constraint_name, the primary key is the
        target. With no usable target the call is a plain insert_rows.
    update_cols_always : list[str] | None, default None
        Columns overwritten on conflict.
    update_cols_ifnull : list[str] | None, default None
        Columns written on conflict only where the stored value is null.
        A conflicting row is left as is when neither list names a
        non-key column the rows carry.
    reset_sequence : bool, default False
        Reset the table's sequence after the write.
    batch_size : int, default 500
        Rows per executemany batch.
    use_primary_key : bool, default False
        SQLite only: when the rows lack the primary key, skip the search
        for a unique constraint they cover.

    Returns
    -------
    int
        Rows the driver reports affected.

    Raises
    ------
    ValidationError
        When both constraint_name and conflict_columns are given.
    """
    return cn.upsert_rows(
        table=table,
        rows=rows,
        constraint_name=constraint_name,
        conflict_columns=conflict_columns,
        update_cols_always=update_cols_always,
        update_cols_ifnull=update_cols_ifnull,
        reset_sequence=reset_sequence,
        batch_size=batch_size,
        use_primary_key=use_primary_key)


def reset_table_sequence(cn: ConnectionWrapper, table: str,
                         identity: str | None = None) -> None:
    """Set the table's sequence so the next value is the column max + 1.

    identity names the column; None finds the table's sequence column.
    SQLite has no sequence and changes nothing.
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


def copy_from(cn: ConnectionWrapper, table: str, file: TextIO,
              columns: list[str] | None = None) -> int:
    """Bulk-load CSV text from file into table; rows loaded.

    PostgreSQL only: SQLite logs a warning and returns 0.
    """
    return cn.copy_from(table, file, columns)


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
    'copy_from',
    'Column',
    'ColumnInfo',
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
    'ReadOnlyError',
    'TypeConversionError',
]
