"""
Transaction handling for database operations.
"""
import logging
import threading
from typing import Any

import pandas as pd
from database.cursor import get_dict_cursor
from database.options import use_iterdict_data_loader
from database.sql import prepare_query
from database.types import RowAdapter
from database.utils.auto_commit import disable_auto_commit, enable_auto_commit
from database.utils.connection_utils import check_connection
from database.utils.query_utils import extract_column_info, load_data
from database.utils.query_utils import process_multiple_result_sets

from libb import attrdict, isiterable

logger = logging.getLogger(__name__)


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

    def __init__(self, cn: Any) -> None:
        self.connection = cn
        self.sa_connection = getattr(cn, 'sa_connection', None)

        if not hasattr(_local, 'active_transactions'):
            _local.active_transactions = {}

        connection_id = id(cn)
        if connection_id in _local.active_transactions:
            raise RuntimeError('Nested transactions are not supported')

        if hasattr(cn, 'in_transaction'):
            cn.in_transaction = True

    @property
    def cursor(self) -> Any:
        """Lazy cursor wrapped with timing and error handling.
        """
        return get_dict_cursor(self.connection)

    def __enter__(self):
        _local.active_transactions[id(self.connection)] = True

        disable_auto_commit(self.connection)
        logger.debug(f'Started transaction for connection {id(self.connection)}')

        return self

    def __exit__(self, exc_type: type | None, value: Exception | None, traceback: Any | None) -> None:
        try:
            cn = getattr(self.connection, 'connection', self.connection)

            if exc_type is not None:
                cn.rollback()
                logger.warning('Rolling back the current transaction')
            else:
                cn.commit()
                logger.debug(f'Committed transaction for connection {id(self.connection)}')
        finally:
            _local.active_transactions.pop(id(self.connection), None)
            enable_auto_commit(self.connection)

            if hasattr(self.connection, 'in_transaction'):
                self.connection.in_transaction = False

            logger.debug(f'Transaction cleanup complete for connection {id(self.connection)}')

    @check_connection
    def execute(self, sql: str, *args, returnid: str | list[str] | None = None) -> Any:
        """Execute SQL within transaction context with connection retry"""
        cursor = self.cursor

        processed_sql, processed_args = prepare_query(sql, args, self.connection.dialect)
        cursor.execute(processed_sql, processed_args)
        rc = cursor.rowcount

        if not returnid:
            return rc

        results = None
        try:
            results = cursor.fetchall()
        except Exception as e:
            logger.debug(f'No results to return: {e}')
        finally:
            if not results:
                return

        if len(results) == 1:
            result = results[0]
            if isiterable(returnid):
                return [result[r] for r in returnid]
            else:
                return result[returnid]
        else:
            if isiterable(returnid):
                return [[row[r] for r in returnid] for row in results]
            else:
                return [row[returnid] for row in results]

    def select(self, sql: str, *args, **kwargs) -> pd.DataFrame:
        """Execute SELECT query or procedure within transaction context"""
        cursor = self.cursor

        processed_sql, processed_args = prepare_query(sql, args, self.connection.dialect)
        cursor.execute(processed_sql, processed_args)
        logger.debug(f'Executed query with {cursor.rowcount} rows affected')

        is_procedure = any(kw in processed_sql.upper() for kw in ['EXEC ', 'CALL ', 'EXECUTE '])
        return_all = kwargs.pop('return_all', False)
        prefer_first = kwargs.pop('prefer_first', False)

        if not is_procedure and not return_all and not cursor.nextset():
            columns = extract_column_info(cursor)
            result = load_data(cursor, columns=columns, **kwargs)
            logger.debug(f'Query returned {len(result)} rows in single result set')
            return result
        return process_multiple_result_sets(cursor, return_all, prefer_first, **kwargs)

    def select_column(self, sql: str, *args) -> list[Any]:
        """Execute a query and return a single column as a list

        Extracts the first column from each row.
        """
        data = self.select(sql, *args)
        return [RowAdapter.create(self.connection, row).get_value() for row in data]

    @use_iterdict_data_loader
    def select_row(self, sql: str, *args) -> attrdict:
        """Execute a query and return a single row

        Raises an assertion error if more than one row is returned.
        """
        data = self.select(sql, *args)
        assert len(data) == 1, f'Expected one row, got {len(data)}'
        return RowAdapter.create(self.connection, data[0]).to_attrdict()

    @use_iterdict_data_loader
    def select_row_or_none(self, sql: str, *args) -> attrdict | None:
        """Execute a query and return a single row or None if no rows found"""
        data = self.select(sql, *args)
        if not data or len(data) == 0:
            return None

        return RowAdapter.create(self.connection, data[0]).to_attrdict()

    @use_iterdict_data_loader
    def select_scalar(self, sql: str, *args) -> Any:
        """Execute a query and return a single scalar value

        Raises an assertion error if more than one row is returned.
        """
        data = self.select(sql, *args)
        assert len(data) == 1, f'Expected one row, got {len(data)}'
        return RowAdapter.create(self.connection, data[0]).get_value()
