"""
Transaction handling and auto-commit management for database operations.
"""
import logging
import threading
from typing import Any

import pandas as pd
from database.cursor import get_dict_cursor
from database.options import use_iterdict_data_loader
from database.sql import prepare_query
from database.types import RowAdapter

from libb import attrdict, isiterable

logger = logging.getLogger(__name__)


_local = threading.local()


def _get_raw_connection(connection: Any) -> Any:
    """Extract the raw DBAPI connection from a wrapper."""
    raw_conn = connection
    if hasattr(connection, 'driver_connection'):
        raw_conn = connection.driver_connection
    return raw_conn


def _try_strategy_autocommit(connection: Any, enable: bool) -> bool:
    """Try to use strategy for autocommit. Returns True if successful."""
    try:
        from database.strategy import get_db_strategy
        if hasattr(connection, 'dialect'):
            strategy = get_db_strategy(connection)
            raw_conn = _get_raw_connection(connection)
            if enable:
                strategy.enable_autocommit(raw_conn)
            else:
                strategy.disable_autocommit(raw_conn)
            return True
    except Exception as e:
        logger.debug(f'Could not use strategy for autocommit: {e}')
    return False


def enable_auto_commit(connection: Any) -> None:
    """Enable auto-commit mode for any database connection.

    Works with:
    - SQLAlchemy connections
    - psycopg (PostgreSQL)
    - sqlite3
    - Raw DBAPI connections
    """
    if hasattr(connection, 'execution_options') and callable(connection.execution_options):
        try:
            connection.execution_options(isolation_level='AUTOCOMMIT')
        except Exception as e:
            logger.debug(f'Could not set SQLAlchemy execution_options: {e}')

    if _try_strategy_autocommit(connection, enable=True):
        return

    raw_conn = _get_raw_connection(connection)

    if hasattr(raw_conn, 'autocommit'):
        try:
            raw_conn.autocommit = True
            return
        except Exception as e:
            logger.debug(f'Could not set autocommit: {e}')

    if hasattr(raw_conn, 'isolation_level'):
        try:
            raw_conn.isolation_level = None
        except Exception as e:
            logger.debug(f'Could not set isolation_level: {e}')


def disable_auto_commit(connection: Any) -> None:
    """Disable auto-commit mode for any database connection.
    """
    if hasattr(connection, 'execution_options') and callable(connection.execution_options):
        try:
            connection.execution_options(isolation_level='READ COMMITTED')
        except Exception as e:
            logger.debug(f'Could not reset SQLAlchemy execution_options: {e}')

    if _try_strategy_autocommit(connection, enable=False):
        return

    raw_conn = _get_raw_connection(connection)

    if hasattr(raw_conn, 'autocommit'):
        try:
            raw_conn.autocommit = False
            return
        except Exception as e:
            logger.debug(f'Could not set autocommit: {e}')

    if hasattr(raw_conn, 'isolation_level'):
        try:
            raw_conn.isolation_level = 'DEFERRED'
        except Exception as e:
            logger.debug(f'Could not set isolation_level: {e}')


def diagnose_connection(conn: Any) -> dict[str, Any]:
    """Diagnose connection state for debugging.
    """
    info: dict[str, Any] = {
        'type': 'unknown',
        'auto_commit': None,
        'in_transaction': False,
        'closed': False,
    }

    if hasattr(conn, 'dialect'):
        info['type'] = conn.dialect

    info['is_sqlalchemy'] = hasattr(conn, 'sa_connection')

    raw_conn = _get_raw_connection(conn)

    info['closed'] = getattr(conn, 'closed', False)

    info['auto_commit'] = getattr(raw_conn, 'autocommit', None)
    if info['auto_commit'] is None and hasattr(raw_conn, 'isolation_level'):
        info['auto_commit'] = raw_conn.isolation_level is None

    if hasattr(conn, 'in_transaction'):
        info['in_transaction'] = conn.in_transaction

    return info


def extract_column_info(cursor: 'Any', table_name: str | None = None) -> list['Any']:
    """Extract column information from cursor description based on database type.
    """
    from database.types import columns_from_cursor_description

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


def load_data(cursor: 'Any', columns: list['Any'] | None = None,
              **kwargs: Any) -> Any:
    """Data loader callable that processes cursor results into the configured format.
    """
    if columns is None:
        columns = extract_column_info(cursor)

    data = cursor.fetchall()

    if not data:
        data_loader = cursor.connwrapper.options.data_loader
        return data_loader([], columns, **kwargs)

    adapted_data = []
    for row in data:
        adapter = RowAdapter.create(cursor.connwrapper, row)
        if hasattr(adapter, 'cursor'):
            adapter.cursor = cursor
        adapted_data.append(adapter.to_dict())
    data = adapted_data

    data_loader = cursor.connwrapper.options.data_loader
    return data_loader(data, columns, **kwargs)


def process_multiple_result_sets(cursor: 'Any', return_all: bool = False,
                                 prefer_first: bool = False, **kwargs: Any) -> list[Any] | Any:
    """Process multiple result sets from a query or stored procedure.
    """
    result_sets: list[Any] = []
    columns_sets: list[list[Any]] = []
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
        self.cn = cn
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

    def execute(self, sql: str, *args, returnid: str | list[str] | None = None) -> Any:
        """Execute SQL within transaction context"""
        from database.connection import check_connection

        @check_connection
        def _execute(sql: str, *args, returnid: str | list[str] | None = None) -> Any:
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

        return _execute(sql, *args, returnid=returnid)

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
        """
        data = self.select(sql, *args)
        return [RowAdapter.create(self.connection, row).get_value() for row in data]

    @use_iterdict_data_loader
    def select_row(self, sql: str, *args) -> attrdict:
        """Execute a query and return a single row
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
        """
        data = self.select(sql, *args)
        assert len(data) == 1, f'Expected one row, got {len(data)}'
        return RowAdapter.create(self.connection, data[0]).get_value()
