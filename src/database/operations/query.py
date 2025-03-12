"""
Query operations for database access.
"""
import logging

import pandas as pd
from database.adapters.structure import RowStructureAdapter
from database.core.exceptions import QueryError
from database.core.query import dumpsql
from database.options import use_iterdict_data_loader
from database.utils.connection_utils import check_connection
from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pyodbc_connection
from database.utils.connection_utils import is_sqlite3_connection
from database.utils.sql import handle_query_params

from libb import attrdict, is_null

logger = logging.getLogger(__name__)


@check_connection
@handle_query_params
@dumpsql
def select(cn, sql, *args, **kwargs) -> list | pd.DataFrame:
    """Execute a SELECT query or stored procedure

    This function handles both regular SELECT queries and stored procedures.
    For stored procedures, it can process multiple result sets.

    Args:
        cn: Database connection
        sql: SQL query string or stored procedure call
        *args: Query parameters
        **kwargs: Additional options:
            - return_all: If True, returns a list of all result sets (for procedures)
            - prefer_first: If True, returns the first result set instead of the largest
            - Other options are passed to the data loader

    Returns
        Result data as a pandas DataFrame (or list of DataFrames if return_all=True)
    """
    from database.core.cursor import get_dict_cursor
    from database.utils.sql import prepare_sql_params_for_execution
    cursor = get_dict_cursor(cn)

    # Process parameters one final time right before execution
    processed_sql, processed_args = prepare_sql_params_for_execution(sql, args, cn)

    # Execute with the processed SQL and args
    cursor.execute(processed_sql, processed_args)

    # Check for procedure execution (EXEC/CALL) or multiple result sets
    is_procedure = any(kw in processed_sql.upper() for kw in ['EXEC ', 'CALL ', 'EXECUTE '])

    # Extract options for result set handling from kwargs
    return_all = kwargs.pop('return_all', False)
    prefer_first = kwargs.pop('prefer_first', False)

    # If it's a simple query (not a procedure) and we're not expecting multiple result sets
    if not is_procedure and not return_all:
        # Immediately capture column information after execution
        columns = extract_column_info(cursor)
        return load_data(cursor, columns=columns, **kwargs)

    # Handle procedure or multiple result sets
    # Process all result sets
    return process_multiple_result_sets(cursor, return_all, prefer_first, **kwargs)


def execute_with_context(cn, sql, *args, **kwargs):
    """Execute SQL with enhanced error context"""
    try:
        from database.core.query import execute
        return execute(cn, sql, *args, **kwargs)
    except Exception as e:
        # Capture original exception for context
        raise QueryError(f'Query execution failed: {e}\nSQL: {sql}') from e


def process_multiple_result_sets(cursor, return_all=False, prefer_first=False, **kwargs):
    """
    Process multiple result sets from a query or stored procedure.

    This is a common function used by both the Transaction.select and the
    select function to handle multiple result sets consistently.

    Args:
        cursor: Database cursor with results
        return_all: If True, returns all result sets as a list
        prefer_first: If True, returns the first result set instead of the largest
        **kwargs: Additional options for the data loader

    Returns
        List of result sets or the largest/first result set based on options
    """
    result_sets = []
    columns_sets = []  # Cache for column information
    largest_result = None
    largest_size = 0

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

    # Return based on options
    if return_all:
        # For empty results, return list of dictionaries or empty list
        if not result_sets:
            return []
        return result_sets

    if prefer_first and result_sets:
        return result_sets[0]

    # Handle no results case
    if not result_sets:
        return []

    return largest_result


def extract_column_info(cursor, table_name=None):
    """Extract column information from cursor description based on database type"""
    from database.adapters.column_info import columns_from_cursor_description

    if cursor.description is None:
        return []

    # Determine connection type
    connection_type = 'unknown'
    if is_psycopg_connection(cursor.connwrapper):
        connection_type = 'postgresql'
    elif is_pyodbc_connection(cursor.connwrapper):
        connection_type = 'mssql'
    elif is_sqlite3_connection(cursor.connwrapper):
        connection_type = 'sqlite'

    connection = cursor.connwrapper

    # Create column info objects - With ODBC Driver 18+, column names are preserved
    columns = columns_from_cursor_description(
        cursor,
        connection_type,
        table_name,
        connection
    )

    # Store the columns on the cursor for use by row adapters
    cursor.columns = columns

    return columns


def _extract_columns(cursor):
    """Extract column names from cursor description based on database type"""
    from database.adapters.column_info import Column
    columns = extract_column_info(cursor)
    return Column.get_names(columns)


def load_data(cursor, columns=None, **kwargs):
    """Data loader callable (IE into DataFrame)
    """
    from database.utils.connection_utils import is_pyodbc_connection

    if columns is None:
        columns = extract_column_info(cursor)

    # For SQL Server, use specialized result processing
    if is_pyodbc_connection(cursor.connwrapper):
        from database.utils.sqlserver_utils import process_sqlserver_result
        adapted_data = process_sqlserver_result(cursor, columns)
        data_loader = cursor.connwrapper.options.data_loader
        return data_loader(adapted_data, columns, **kwargs)

    data = cursor.fetchall()  # Get raw data

    if not data:
        data_loader = cursor.connwrapper.options.data_loader
        return data_loader([], columns, **kwargs)

    # Convert all database-specific row types to a consistent format
    adapted_data = []
    for row in data:
        adapter = RowStructureAdapter.create(cursor.connwrapper, row)
        # Pass the cursor to the adapter if needed
        if hasattr(adapter, 'cursor'):
            adapter.cursor = cursor
        adapted_data.append(adapter.to_dict())
    data = adapted_data

    # Use the configured data loader
    data_loader = cursor.connwrapper.options.data_loader
    return data_loader(data, columns, **kwargs)


@use_iterdict_data_loader
def select_column(cn, sql, *args) -> list:
    """Execute a query and return a single column as a list

    TODO: Should this always be an interdict? Could we return a pandas Series

    Extracts the first column from each row.
    """
    data = select(cn, sql, *args)
    return [RowStructureAdapter.create(cn, row).get_value() for row in data]


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
    return RowStructureAdapter.create(cn, data[0]).to_attrdict()


@use_iterdict_data_loader
def select_row_or_none(cn, sql, *args) -> attrdict | None:
    """Execute a query and return a single row or None if no rows found"""
    data = select(cn, sql, *args)
    if len(data) == 1:
        return RowStructureAdapter.create(cn, data[0]).to_attrdict()
    return None


@use_iterdict_data_loader
def select_scalar(cn, sql, *args):
    """Execute a query and return a single scalar value

    Raises an assertion error if more than one row is returned.
    """
    # Use consistent approach for all database types

    # Normal flow for other cases
    data = select(cn, sql, *args)
    assert len(data) == 1, f'Expected one row, got {len(data)}'
    return RowStructureAdapter.create(cn, data[0]).get_value()


def select_scalar_or_none(cn, sql, *args):
    """Execute a query and return a single scalar value or None if no rows found"""
    try:
        val = select_scalar(cn, sql, *args)
        if not is_null(val):
            return val
        return None
    except AssertionError:
        return None
