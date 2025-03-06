"""
Transaction handling for database operations.
"""
import logging
from functools import wraps

import pandas as pd
from database.utils.sql import handle_query_params

from libb import attrdict, isiterable

logger = logging.getLogger(__name__)


class Transaction:
    """Context manager for running multiple commands in a transaction.

    with Transaction(cn) as tx:
        tx.execute('delete from ...', args)
        tx.execute('update from ...', args)
    """

    def __init__(self, cn):
        self.connection = cn

    @property
    def cursor(self):
        """Lazy cursor wrapped with timing and error handling"""
        from database.core.cursor import get_dict_cursor
        return get_dict_cursor(self.connection)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, value, traceback):
        if exc_type is not None:
            self.connection.rollback()
            logger.warning('Rolling back the current transaction')
        else:
            self.connection.commit()
            logger.debug('Committed transaction.')

    @handle_query_params
    def execute(self, sql, *args, returnid=None):
        """Execute SQL within transaction context"""
        cursor = self.cursor

        # Use existing parameter handling function from utils.sql
        from database.utils.sql import prepare_sql_params_for_execution
        processed_sql, processed_args = prepare_sql_params_for_execution(sql, args)

        cursor.execute(processed_sql, processed_args)
        rc = cursor.rowcount

        if not returnid:
            return rc

        # Try to get result if returnid specified
        result = None
        try:
            result = cursor.fetchone()
        except Exception as e:
            logger.debug(f'No results to return: {e}')
        finally:
            if not result:
                return

        # Return specific fields or whole row
        if isiterable(returnid):
            return [result[r] for r in returnid]
        else:
            return result[returnid]

    @handle_query_params
    def select(self, sql, *args, **kwargs) -> pd.DataFrame:
        """Execute SELECT query within transaction context"""
        cursor = self.cursor

        # Process parameters one final time right before execution
        from database.utils.sql import prepare_sql_params_for_execution
        processed_sql, processed_args = prepare_sql_params_for_execution(sql, args)

        # Execute with the processed SQL and args
        cursor.execute(processed_sql, processed_args)

        from database.operations.query import extract_column_info, load_data
        columns = extract_column_info(cursor)
        return load_data(cursor, columns=columns, **kwargs)

    def select_column(self, sql, *args) -> list:
        """Execute a query and return a single column as a list

        Extracts the first column from each row.
        """
        from database.adapters.row_adapter import DatabaseRowAdapter
        data = self.select(sql, *args)
        return [DatabaseRowAdapter.create(self.connection, row).get_value() for row in data]

    def select_row(self, sql, *args) -> 'attrdict':
        """Execute a query and return a single row

        Raises an assertion error if more than one row is returned.
        """
        data = self.select(sql, *args)
        assert len(data) == 1, f'Expected one row, got {len(data)}'
        from database.adapters.row_adapter import DatabaseRowAdapter

        # Convert the first row to a dictionary explicitly since pandas DataFrame
        # doesn't allow integer indexing in the same way
        row_dict = data.iloc[0].to_dict()
        return DatabaseRowAdapter.create(self.connection, row_dict).to_attrdict()

    def select_row_or_none(self, sql, *args) -> 'attrdict | None':
        """Execute a query and return a single row or None if no rows found"""
        data = self.select(sql, *args)
        if not data or len(data) == 0:
            return None

        from database.adapters.row_adapter import DatabaseRowAdapter

        # Convert the first row to a dictionary explicitly
        row_dict = data.iloc[0].to_dict()
        return DatabaseRowAdapter.create(self.connection, row_dict).to_attrdict()

    def select_scalar(self, sql, *args):
        """Execute a query and return a single scalar value

        Raises an assertion error if more than one row is returned.
        """
        data = self.select(sql, *args)
        assert len(data) == 1, f'Expected one row, got {len(data)}'
        from database.adapters.row_adapter import DatabaseRowAdapter

        # Convert the first row to a dictionary explicitly
        row_dict = data.iloc[0].to_dict()
        return DatabaseRowAdapter.create(self.connection, row_dict).get_value()


def check_connection(func=None, *, max_retries=3, retry_delay=1,
                     retry_errors=None, retry_backoff=1.5):
    """Enhanced connection retry decorator with backoff"""
    def decorator(f):
        @wraps(f)
        def inner(*args, **kwargs):
            # Get the appropriate exception types
            if retry_errors is None:
                # Import here to avoid circular imports
                from database import DbConnectionError
                retry_err_types = DbConnectionError
            else:
                retry_err_types = retry_errors

            tries = 0
            delay = retry_delay
            while tries < max_retries:
                try:
                    return f(*args, **kwargs)
                except retry_err_types as err:
                    tries += 1
                    if tries >= max_retries:
                        logger.error(f'Maximum retries ({max_retries}) exceeded: {err}')
                        raise

                    logger.warning(f'Connection error (attempt {tries}/{max_retries}): {err}')
                    conn = args[0]
                    if hasattr(conn, 'options') and conn.options.check_connection:
                        try:
                            conn.connection.close()
                        except:
                            pass  # Ignore errors when closing broken connection

                        # Reconnect
                        from database.core.connection import connect
                        conn.connection = connect(conn.options).connection

                    # Wait before retry with exponential backoff
                    import time
                    time.sleep(delay)
                    delay *= retry_backoff

        return inner

    # Allow both @check_connection and @check_connection() syntax
    if func is None:
        return decorator
    return decorator(func)
