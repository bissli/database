"""
Transaction handling and auto-commit management for database operations.
"""
import logging
import threading
from typing import Any

import pandas as pd
from database.cursor import get_dict_cursor
from database.sql import prepare_query
from database.strategy import get_db_strategy
from database.utils import get_raw_connection

from libb import attrdict, isiterable

logger = logging.getLogger(__name__)


_local = threading.local()


def _try_strategy_autocommit(connection: Any, enable: bool) -> bool:
    """Try to use strategy for autocommit. Returns True if successful."""
    try:
        if hasattr(connection, 'dialect'):
            strategy = get_db_strategy(connection)
            raw_conn = get_raw_connection(connection)
            if enable:
                strategy.enable_autocommit(raw_conn)
            else:
                strategy.disable_autocommit(raw_conn)
            return True
    except Exception as e:
        logger.debug(f'Could not use strategy for autocommit: {e}')
    return False


def _set_autocommit(connection: Any, enable: bool) -> None:
    """Set auto-commit mode for any database connection.

    Works with SQLAlchemy connections, psycopg, sqlite3, and raw DBAPI connections.
    """
    # SQLAlchemy execution_options
    if hasattr(connection, 'execution_options') and callable(connection.execution_options):
        isolation = 'AUTOCOMMIT' if enable else 'READ COMMITTED'
        try:
            connection.execution_options(isolation_level=isolation)
        except Exception as e:
            logger.debug(f'Could not set SQLAlchemy execution_options: {e}')

    # Strategy-based autocommit
    if _try_strategy_autocommit(connection, enable=enable):
        return

    raw_conn = get_raw_connection(connection)

    # Direct autocommit property
    if hasattr(raw_conn, 'autocommit'):
        try:
            raw_conn.autocommit = enable
            return
        except Exception as e:
            logger.debug(f'Could not set autocommit: {e}')

    # SQLite isolation_level
    if hasattr(raw_conn, 'isolation_level'):
        level = None if enable else 'DEFERRED'
        try:
            raw_conn.isolation_level = level
        except Exception as e:
            logger.debug(f'Could not set isolation_level: {e}')


def enable_auto_commit(connection: Any) -> None:
    """Enable auto-commit mode for any database connection."""
    _set_autocommit(connection, enable=True)


def disable_auto_commit(connection: Any) -> None:
    """Disable auto-commit mode for any database connection."""
    _set_autocommit(connection, enable=False)


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

    raw_conn = get_raw_connection(conn)

    info['closed'] = getattr(conn, 'closed', False)

    info['auto_commit'] = getattr(raw_conn, 'autocommit', None)
    if info['auto_commit'] is None and hasattr(raw_conn, 'isolation_level'):
        info['auto_commit'] = raw_conn.isolation_level is None

    if hasattr(conn, 'in_transaction'):
        info['in_transaction'] = conn.in_transaction

    return info


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
        """Execute SQL within transaction context.
        """
        if not returnid:
            return self.connection.execute(sql, *args)

        cursor = self.cursor
        processed_sql, processed_args = prepare_query(sql, args, self.connection.dialect)
        cursor.execute(processed_sql, processed_args)

        results = None
        try:
            results = cursor.fetchall()
        except Exception as e:
            logger.debug(f'No results to return: {e}')

        if not results:
            return None

        if len(results) == 1:
            result = results[0]
            if isiterable(returnid):
                return [result[r] for r in returnid]
            return result[returnid]

        if isiterable(returnid):
            return [[row[r] for r in returnid] for row in results]
        return [row[returnid] for row in results]

    def select(self, sql: str, *args, **kwargs) -> pd.DataFrame:
        """Execute SELECT query within transaction context.
        """
        return self.connection.select(sql, *args, **kwargs)

    def select_column(self, sql: str, *args) -> list[Any]:
        """Execute a query and return a single column as a list.
        """
        return self.connection.select_column(sql, *args)

    def select_row(self, sql: str, *args) -> attrdict:
        """Execute a query and return a single row.
        """
        return self.connection.select_row(sql, *args)

    def select_row_or_none(self, sql: str, *args) -> attrdict | None:
        """Execute a query and return a single row or None if no rows found.
        """
        return self.connection.select_row_or_none(sql, *args)

    def select_scalar(self, sql: str, *args) -> Any:
        """Execute a query and return a single scalar value.
        """
        return self.connection.select_scalar(sql, *args)
