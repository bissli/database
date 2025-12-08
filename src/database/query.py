"""
Query operations for database access.

This module provides a comprehensive set of functions for executing
queries against various database backends with consistent behavior.
"""
import logging
from typing import Any, TypeVar, Union

import pandas as pd
from database.connection import ConnectionWrapper, check_connection
from database.options import use_iterdict_data_loader
from database.sql import prepare_query
from database.transaction import extract_column_info, load_data
from database.transaction import process_multiple_result_sets
from database.types import RowAdapter

from libb import attrdict, is_null

SqlParams = Union[list[Any], tuple[Any, ...]]
DataRow = dict[str, Any]
ResultSet = list[DataRow]
T = TypeVar('T')

logger = logging.getLogger(__name__)


@check_connection
def execute(cn: Any, sql: str, *args: Any) -> int:
    """Execute a SQL query with the given parameters and return affected row count.

    Handles various SQL dialects and parameter styles.

    >>> import sqlite3
    >>> conn = sqlite3.connect(':memory:')
    >>> cursor = conn.cursor()
    >>> cursor.execute('CREATE TABLE test (id INTEGER, name TEXT)')
    <sqlite3.Cursor object at ...>
    >>> execute(conn, 'INSERT INTO test VALUES (?, ?)', 1, 'test')
    1
    """
    cursor = cn.cursor()
    try:
        processed_sql, processed_args = prepare_query(sql, args, cn.dialect)

        cursor.execute(processed_sql, processed_args)
        logger.debug(f'Executed query with {len(processed_args) if processed_args else 0} parameters: {processed_sql[:60]}...')

        rowcount = cursor.rowcount
        cn.commit()
        return rowcount
    except Exception as e:
        try:
            cn.rollback()
        except:
            pass
        raise


@check_connection
def select(cn: ConnectionWrapper, sql: str, *args: Any, **kwargs: Any) -> ResultSet | pd.DataFrame | list[pd.DataFrame]:
    """Execute a SELECT query or stored procedure.

    This function handles both regular SELECT queries and stored procedures.
    For stored procedures, it can process multiple result sets.
    """
    processed_sql, processed_args = prepare_query(sql, args, cn.dialect)

    cursor = cn.cursor()

    cursor.execute(processed_sql, processed_args)

    is_procedure = any(kw in processed_sql.upper() for kw in ['EXEC ', 'CALL ', 'EXECUTE '])

    return_all = kwargs.pop('return_all', False)
    prefer_first = kwargs.pop('prefer_first', False)

    if not is_procedure and not return_all:
        columns = extract_column_info(cursor)
        result = load_data(cursor, columns=columns, **kwargs)
        logger.debug(f"Select query returned {len(result) if hasattr(result, '__len__') else 'scalar'} result")
        return result
    result = process_multiple_result_sets(cursor, return_all, prefer_first, **kwargs)
    logger.debug(f"Procedure returned {len(result) if isinstance(result, list) else 'single'} result set(s)")
    return result


@use_iterdict_data_loader
def select_column(cn: ConnectionWrapper, sql: str, *args: Any) -> list[Any]:
    """Execute a query and return a single column as a list.

    Extracts the first column from each row and returns its values in a list.
    This is useful when you need just a simple list of values from a query.

    Args:
        cn: Database connection
        sql: SQL query that returns at least one column
        *args: Query parameters

    Returns
        List containing values from the first column of each row
    """
    data = select(cn, sql, *args)
    return [RowAdapter.create(cn, row).get_value() for row in data]


@use_iterdict_data_loader
def select_row(cn: ConnectionWrapper, sql: str, *args: Any) -> attrdict:
    """Execute a query and return a single row as an attribute dictionary.

    Use this when you expect exactly one row to be returned.

    Args:
        cn: Database connection
        sql: SQL query that should return exactly one row
        *args: Query parameters

    Returns
        An attribute dictionary representing the row

    Raises
        AssertionError: If the query returns zero or multiple rows
    """
    data = select(cn, sql, *args)
    assert len(data) == 1, f'Expected one row, got {len(data)}'
    return RowAdapter.create(cn, data[0]).to_attrdict()


@use_iterdict_data_loader
def select_row_or_none(cn: ConnectionWrapper, sql: str, *args: Any) -> attrdict | None:
    """Execute a query and return a single row or None if no rows found.

    This is a safer version of select_row that returns None instead of raising
    an error when no rows are found.

    Args:
        cn: Database connection
        sql: SQL query that should return at most one row
        *args: Query parameters

    Returns
        An attribute dictionary representing the row, or None if no rows
    """
    data = select(cn, sql, *args)
    if len(data) == 1:
        return RowAdapter.create(cn, data[0]).to_attrdict()
    return None


@use_iterdict_data_loader
def select_scalar(cn: ConnectionWrapper, sql: str, *args: Any) -> Any:
    """Execute a query and return a single scalar value.

    Use this when you expect exactly one row with one column to be returned.

    Args:
        cn: Database connection
        sql: SQL query that should return exactly one value
        *args: Query parameters

    Returns
        The scalar value from the first column of the first row

    Raises
        AssertionError: If the query returns zero or multiple rows
    """
    data = select(cn, sql, *args)
    assert len(data) == 1, f'Expected one row, got {len(data)}'
    result = RowAdapter.create(cn, data[0]).get_value()
    logger.debug(f'Scalar query returned value of type {type(result).__name__}')
    return result


def select_scalar_or_none(cn: ConnectionWrapper, sql: str, *args: Any) -> Any | None:
    """Execute a query and return a single scalar value or None if no rows found.

    This is a safer version of select_scalar that returns None instead of raising
    an error when no rows are found or when the value is NULL.

    Args:
        cn: Database connection
        sql: SQL query that should return at most one value
        *args: Query parameters

    Returns
        The scalar value from the first column of the first row, or None
    """
    try:
        val = select_scalar(cn, sql, *args)
        if not is_null(val):
            return val
        return None
    except AssertionError:
        return None


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
