"""
Query operations for database access.
"""
import logging
from functools import wraps

import pandas as pd
from database.adapters.row_adapter import DatabaseRowAdapter
from database.core.exceptions import QueryError
from database.core.query import dumpsql
from database.core.transaction import check_connection
from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pymssql_connection
from database.utils.connection_utils import is_sqlite3_connection
from database.utils.sql import handle_query_params

from libb import attrdict, is_null

logger = logging.getLogger(__name__)


@check_connection
@handle_query_params
@dumpsql
def select(cn, sql, *args, **kwargs) -> pd.DataFrame:
    """Execute a SELECT query

    Args:
        cn: Database connection
        sql: SQL query string
        *args: Query parameters
        **kwargs: Additional options passed to data loader

    Returns
        Result data as a pandas DataFrame
    """
    from database.core.cursor import get_dict_cursor
    from database.utils.sql import prepare_sql_params_for_execution
    cursor = get_dict_cursor(cn)

    # Process parameters one final time right before execution
    processed_sql, processed_args = prepare_sql_params_for_execution(sql, args)

    # Execute with the processed SQL and args
    cursor.execute(processed_sql, processed_args)

    # Immediately capture column information after execution
    columns = extract_column_info(cursor)

    return load_data(cursor, columns=columns, **kwargs)


@check_connection
@handle_query_params
@dumpsql
def callproc(cn, sql, *args, **kwargs) -> pd.DataFrame:
    """Execute a stored procedure that may return multiple result sets.

    This function handles stored procedures which often return multiple
    resultsets, particularly when NOCOUNT is OFF in SQL Server. It processes
    each result set and returns the one with the most rows by default.

    Args:
        cn: Database connection
        sql: SQL query string or stored procedure call
        *args: Query parameters
        **kwargs: Additional options:
            - return_all: If True, returns a list of all result sets instead of just the largest one
            - prefer_first: If True, returns the first result set instead of the largest

    Returns
        Result data as a pandas DataFrame (default: largest result set)
        or a list of DataFrames if return_all=True
    """
    from database.adapters.column_info import Column
    from database.core.cursor import get_dict_cursor
    from database.utils.sql import prepare_sql_params_for_execution

    cursor = get_dict_cursor(cn)

    # Process parameters one final time right before execution
    processed_sql, processed_args = prepare_sql_params_for_execution(sql, args)

    # Execute with the processed SQL and args
    cursor.execute(processed_sql, processed_args)

    # Get options from kwargs
    return_all = kwargs.pop('return_all', False)
    prefer_first = kwargs.pop('prefer_first', False)

    # Process all result sets
    result_sets = []
    columns_sets = []  # Cache for column information
    largest_result = None
    largest_size = 0
    largest_cols_index = 0

    # First result set is already available
    columns = extract_column_info(cursor)
    columns_sets.append(columns)

    result = load_data(cursor, columns=columns, **kwargs)
    if result is not None:
        result_sets.append(result)
        largest_result = result
        largest_size = len(result)

    # Process any additional result sets
    while cursor.nextset():
        columns = extract_column_info(cursor)
        columns_sets.append(columns)

        result = load_data(cursor, columns=columns, **kwargs)
        if result is not None:
            result_sets.append(result)
            if len(result) > largest_size:
                largest_result = result
                largest_size = len(result)
                largest_cols_index = len(columns_sets) - 1

    # Return based on options
    if return_all:
        # For empty results, return list of empty DataFrames with columns preserved
        if not result_sets and columns_sets:
            empty_results = []
            for cols in columns_sets:
                if len(cols) > 0:
                    df = pd.DataFrame(columns=Column.get_names(cols))
                    # Save column type information
                    df.attrs['column_types'] = Column.get_column_types_dict(cols)
                    empty_results.append(df)
            return empty_results
        return result_sets or []

    if prefer_first and result_sets:
        return result_sets[0]

    # Handle no results case
    if not result_sets:
        # Get the richest column set if available (most columns)
        if columns_sets:
            best_columns = max(columns_sets, key=len)
            df = pd.DataFrame(columns=Column.get_names(best_columns))
            # Save column type information
            df.attrs['column_types'] = Column.get_column_types_dict(best_columns)
            return df
        return pd.DataFrame()

    return largest_result


def execute_with_context(cn, sql, *args, **kwargs):
    """Execute SQL with enhanced error context"""
    try:
        from database.core.query import execute
        return execute(cn, sql, *args, **kwargs)
    except Exception as e:
        # Capture original exception for context
        raise QueryError(f'Query execution failed: {e}\nSQL: {sql}') from e


def extract_column_info(cursor):
    """Extract column information from cursor description based on database type"""
    from database.adapters.column_info import columns_from_cursor_description

    if cursor.description is None:
        return []

    connection_type = 'unknown'
    if is_psycopg_connection(cursor.connwrapper):
        connection_type = 'postgres'
    elif is_pymssql_connection(cursor.connwrapper):
        connection_type = 'sqlserver'
    elif is_sqlite3_connection(cursor.connwrapper):
        connection_type = 'sqlite'

    # Create Column objects directly from cursor description
    return columns_from_cursor_description(cursor, connection_type)


def _extract_columns(cursor):
    """Extract column names from cursor description based on database type"""
    from database.adapters.column_info import Column
    columns = extract_column_info(cursor)
    return Column.get_names(columns)


def load_data(cursor, columns=None, **kwargs):
    """Data loader callable (IE into DataFrame)
    """
    if columns is None:
        columns = extract_column_info(cursor)

    data = cursor.fetchall()  # Get raw data

    # Convert all database-specific row types to a consistent format
    adapted_data = []
    for row in data:
        adapter = DatabaseRowAdapter.create(cursor.connwrapper, row)
        adapted_data.append(adapter.to_dict())
    data = adapted_data

    # Use the configured data loader
    data_loader = cursor.connwrapper.options.data_loader
    return data_loader(data, columns, **kwargs)


def use_iterdict_data_loader(func):
    """Temporarily use default dict loader over user-specified loader"""

    @wraps(func)
    def inner(*args, **kwargs):
        cn = args[0]
        from database.options import iterdict_data_loader
        original_data_loader = cn.options.data_loader
        cn.options.data_loader = iterdict_data_loader
        try:
            return func(*args, **kwargs)
        finally:
            cn.options.data_loader = original_data_loader
    return inner


@use_iterdict_data_loader
def select_column(cn, sql, *args) -> list:
    """Execute a query and return a single column as a list

    Extracts the first column from each row.
    """
    data = select(cn, sql, *args)
    return [DatabaseRowAdapter.create(cn, row).get_value() for row in data]


def select_column_unique(cn, sql, *args) -> set:
    """Execute a query and return a set of unique values from the first column"""
    return set(select_column(cn, sql, *args))


@use_iterdict_data_loader
def select_row(cn, sql, *args) -> attrdict:
    """Execute a query and return a single row

    Raises an assertion error if more than one row is returned.
    """
    data = select(cn, sql, *args)
    assert len(data) == 1, f'Expected one row, got {len(data)}'
    return DatabaseRowAdapter.create(cn, data[0]).to_attrdict()


@use_iterdict_data_loader
def select_row_or_none(cn, sql, *args) -> attrdict | None:
    """Execute a query and return a single row or None if no rows found"""
    data = select(cn, sql, *args)
    if len(data) == 1:
        return DatabaseRowAdapter.create(cn, data[0]).to_attrdict()
    return None


@use_iterdict_data_loader
def select_scalar(cn, sql, *args):
    """Execute a query and return a single scalar value

    Raises an assertion error if more than one row is returned.
    """
    data = select(cn, sql, *args)
    assert len(data) == 1, f'Expected one row, got {len(data)}'
    return DatabaseRowAdapter.create(cn, data[0]).get_value()


def select_scalar_or_none(cn, sql, *args):
    """Execute a query and return a single scalar value or None if no rows found"""
    try:
        val = select_scalar(cn, sql, *args)
        if not is_null(val):
            return val
        return None
    except AssertionError:
        return None
