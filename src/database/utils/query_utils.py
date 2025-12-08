"""
Common query utilities used by both transaction and query operations.
"""
import logging
from typing import TYPE_CHECKING, Any

from database.types import RowStructureAdapter, columns_from_cursor_description

if TYPE_CHECKING:
    from database.adapters.column_info import Column
    from database.core.cursor import AbstractCursor

logger = logging.getLogger(__name__)


def extract_column_info(cursor: 'AbstractCursor',
                        table_name: str | None = None) -> list['Column']:
    """Extract column information from cursor description based on database type.
    """
    if cursor.description is None:
        return []

    connection_type = cursor.connwrapper.dialect
    connection = cursor.connwrapper

    columns = columns_from_cursor_description(
        cursor,
        connection_type,
        table_name,
        connection
    )

    cursor.columns = columns

    return columns


def load_data(cursor: 'AbstractCursor', columns: list['Column'] | None = None,
              **kwargs: Any) -> Any:
    """Data loader callable that processes cursor results into the configured format.

    Converts raw database rows to a standardized format using the connection's
    configured data loader (typically DataFrame or dict list).
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


def process_multiple_result_sets(cursor: 'AbstractCursor', return_all: bool = False,
                                 prefer_first: bool = False, **kwargs: Any) -> list[Any] | Any:
    """Process multiple result sets from a query or stored procedure.

    This is a common function used by both the Transaction.select and the
    select function to handle multiple result sets consistently.
    """
    result_sets: list[Any] = []
    columns_sets: list[list[Column]] = []
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
