"""
Transaction handling for database operations.
"""
import logging
import threading

import pandas as pd
from database.core.query import dumpsql
from database.options import use_iterdict_data_loader
from database.utils.sql import handle_query_params

from libb import attrdict, isiterable

logger = logging.getLogger(__name__)


# Thread-local storage for tracking active transactions
_local = threading.local()


class Transaction:
    """Context manager for running multiple commands in a transaction.

    This implementation uses thread-local storage to track transaction state,
    making it safe to use in multi-threaded environments. Each thread can have
    its own transaction for the same connection, but nested transactions within
    the same thread are not supported.

    Examples
        with Transaction(cn) as tx:
            tx.execute('delete from ...', args)
            tx.execute('update from ...', args)
    """

    def __init__(self, cn):
        self.connection = cn

        self.sa_connection = getattr(cn, 'sa_connection', None)

        # Initialize thread-local storage if needed
        if not hasattr(_local, 'active_transactions'):
            _local.active_transactions = {}

        # Check for nested transactions in the current thread
        connection_id = id(cn)
        if connection_id in _local.active_transactions:
            raise RuntimeError('Nested transactions are not supported')

    @property
    def cursor(self):
        """Lazy cursor wrapped with timing and error handling"""
        from database.core.cursor import get_dict_cursor
        return get_dict_cursor(self.connection)

    def __enter__(self):
        # Mark this connection as having an active transaction in this thread
        _local.active_transactions[id(self.connection)] = True
        return self

    def __exit__(self, exc_type, value, traceback):
        try:
            cn = getattr(self.connection, 'connection', self.connection)

            if exc_type is not None:
                cn.rollback()
                logger.warning('Rolling back the current transaction')
            else:
                cn.commit()
                logger.debug('Committed transaction.')
        finally:
            # Always remove from active transactions, even if an exception occurred
            _local.active_transactions.pop(id(self.connection), None)

    @dumpsql
    @handle_query_params
    def execute(self, sql, *args, returnid=None):
        """Execute SQL within transaction context"""
        cursor = self.cursor

        # Use existing parameter handling function from utils.sql
        from database.utils.sql import prepare_sql_params_for_execution
        processed_sql, processed_args = prepare_sql_params_for_execution(sql, args, self.connection)

        cursor.execute(processed_sql, processed_args)
        rc = cursor.rowcount

        if not returnid:
            return rc

        # Try to get results if returnid specified
        results = None
        try:
            results = cursor.fetchall()  # Get all returned rows
        except Exception as e:
            logger.debug(f'No results to return: {e}')
        finally:
            if not results:
                return

        # If there's only one result, return it in the original format
        if len(results) == 1:
            result = results[0]
            if isiterable(returnid):
                return [result[r] for r in returnid]
            else:
                return result[returnid]
        # For multiple results, return a list of extracted values
        else:
            if isiterable(returnid):
                # Return a list of lists when returnid specifies multiple fields
                return [[row[r] for r in returnid] for row in results]
            else:
                # Return a list of values when returnid is a single field
                return [row[returnid] for row in results]

    @dumpsql
    @handle_query_params
    def select(self, sql, *args, **kwargs) -> pd.DataFrame:
        """Execute SELECT query or procedure within transaction context"""
        cursor = self.cursor

        # Process parameters one final time right before execution
        from database.utils.sql import prepare_sql_params_for_execution
        processed_sql, processed_args = prepare_sql_params_for_execution(sql, args, self.connection)

        # Execute with the processed SQL and args
        cursor.execute(processed_sql, processed_args)

        from database.operations.query import extract_column_info, load_data

        # Check for procedure execution (EXEC/CALL) or multiple result sets
        is_procedure = any(kw in processed_sql.upper() for kw in ['EXEC ', 'CALL ', 'EXECUTE '])

        # Extract options for result set handling from kwargs
        return_all = kwargs.pop('return_all', False)
        prefer_first = kwargs.pop('prefer_first', False)

        # If it's a simple query (not a procedure) and we're not expecting multiple result sets
        if not is_procedure and not return_all and not cursor.nextset():
            columns = extract_column_info(cursor)
            return load_data(cursor, columns=columns, **kwargs)

        # Handle procedure or multiple result sets
        # Process all result sets
        from database.operations.query import process_multiple_result_sets
        return process_multiple_result_sets(cursor, return_all, prefer_first, **kwargs)

    @dumpsql
    def select_column(self, sql, *args) -> list:
        """Execute a query and return a single column as a list

        Extracts the first column from each row.
        """
        from database.adapters.structure import RowStructureAdapter
        data = self.select(sql, *args)
        return [RowStructureAdapter.create(self.connection, row).get_value() for row in data]

    @dumpsql
    @use_iterdict_data_loader
    def select_row(self, sql, *args) -> attrdict:
        """Execute a query and return a single row

        Raises an assertion error if more than one row is returned.
        """
        data = self.select(sql, *args)
        assert len(data) == 1, f'Expected one row, got {len(data)}'
        from database.adapters.structure import RowStructureAdapter
        return RowStructureAdapter.create(self.connection, data[0]).to_attrdict()

    @use_iterdict_data_loader
    def select_row_or_none(self, sql, *args) -> attrdict | None:
        """Execute a query and return a single row or None if no rows found"""
        data = self.select(sql, *args)
        if not data or len(data) == 0:
            return None

        from database.adapters.structure import RowStructureAdapter
        return RowStructureAdapter.create(self.connection, data[0]).to_attrdict()

    @use_iterdict_data_loader
    def select_scalar(self, sql, *args):
        """Execute a query and return a single scalar value

        Raises an assertion error if more than one row is returned.
        """
        # Special case for SQL Server to handle unnamed columns in scalar queries
        from database.utils.connection_utils import is_pyodbc_connection

        if is_pyodbc_connection(self.connection) and ('COUNT(' in sql.upper() or 'SELECT 1 ' in sql):
            # For SQL Server COUNT queries, execute directly with the cursor
            cursor = self.cursor
            cursor.execute(sql, *args)
            result = cursor.fetchone()
            if result:
                return result[0]
            return None

        # Normal flow for other cases
        data = self.select(sql, *args)
        assert len(data) == 1, f'Expected one row, got {len(data)}'
        from database.adapters.structure import RowStructureAdapter

        return RowStructureAdapter.create(self.connection, data[0]).get_value()
