"""
Common query utilities used by both transaction and query operations.
"""
import logging
from typing import Any

from database.types import RowStructureAdapter, columns_from_cursor_description
from database.utils.connection_utils import get_dialect_name

logger = logging.getLogger(__name__)


def extract_column_info(cursor: any, table_name: str = None) -> list:
    """Extract column information from cursor description based on database type.

    Gets column metadata from cursor and converts it to a standardized format.

    Args:
        cursor: Database cursor with active result set
        table_name: Optional table name to associate with columns

    Returns
        List of column information objects
    """
    if cursor.description is None:
        return []

    connection_type = get_dialect_name(cursor.connwrapper) or 'unknown'
    connection = cursor.connwrapper

    columns = columns_from_cursor_description(
        cursor,
        connection_type,
        table_name,
        connection
    )

    cursor.columns = columns

    return columns


def load_data(cursor: any, columns: list = None, **kwargs: dict) -> any:
    """Data loader callable that processes cursor results into the configured format.

    Converts raw database rows to a standardized format using the connection's
    configured data loader (typically DataFrame or dict list).

    Args:
        cursor: Database cursor with active result set
        columns: Column metadata list (will be extracted if None)
        **kwargs: Additional options passed to the data loader

    Returns
        Loaded data in the format specified by the connection's data_loader
    """
    if columns is None:
        columns = extract_column_info(cursor)

    data = cursor.fetchall()

    if not data:
        data_loader = cursor.connwrapper.options.data_loader
        return data_loader([], columns, **kwargs)

    adapted_data = []
    for row in data:
        adapter = RowStructureAdapter.create(cursor.connwrapper, row)
        if hasattr(adapter, 'cursor'):
            adapter.cursor = cursor
        adapted_data.append(adapter.to_dict())
    data = adapted_data

    data_loader = cursor.connwrapper.options.data_loader
    return data_loader(data, columns, **kwargs)


def process_multiple_result_sets(cursor: any, return_all: bool = False,
                                 prefer_first: bool = False, **kwargs: dict) -> list | Any:
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
    columns_sets = []
    largest_result = None
    largest_size = 0

    columns = extract_column_info(cursor)
    columns_sets.append(columns)

    result = load_data(cursor, columns=columns, **kwargs)
    if result is not None:
        result_sets.append(result)
        largest_result = result
        largest_size = len(result)

    while cursor.nextset():
        columns = extract_column_info(cursor)
        columns_sets.append(columns)

        result = load_data(cursor, columns=columns, **kwargs)
        if result is not None:
            result_sets.append(result)
            if len(result) > largest_size:
                largest_result = result
                largest_size = len(result)

    if return_all:
        if not result_sets:
            return []
        return result_sets

    if prefer_first and result_sets:
        return result_sets[0]

    if not result_sets:
        return []

    return largest_result
