"""Transaction context manager and auto-commit switching.
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


def _set_autocommit(connection: Any, enable: bool) -> None:
    """Switch auto-commit on or off by every means the connection offers.

    Parameters
    ----------
    connection : Any
        ConnectionWrapper, SQLAlchemy connection, or raw DBAPI connection.
    enable : bool
        True turns auto-commit on.
    """
    if (hasattr(connection, 'execution_options')
        and callable(connection.execution_options)):
        isolation = 'AUTOCOMMIT' if enable else 'READ COMMITTED'
        try:
            connection.execution_options(isolation_level=isolation)
        except Exception as e:
            logger.debug(f'Could not set SQLAlchemy execution_options: {e}')

    try:
        if hasattr(connection, 'dialect'):
            strategy = get_db_strategy(connection)
            raw_conn = get_raw_connection(connection)
            if enable:
                strategy.enable_autocommit(raw_conn)
            else:
                strategy.disable_autocommit(raw_conn)
            return
    except Exception as e:
        logger.debug(f'Could not use strategy for autocommit: {e}')

    raw_conn = get_raw_connection(connection)

    if hasattr(raw_conn, 'autocommit'):
        try:
            raw_conn.autocommit = enable
            return
        except Exception as e:
            logger.debug(f'Could not set autocommit: {e}')

    if hasattr(raw_conn, 'isolation_level'):
        level = None if enable else 'DEFERRED'
        try:
            raw_conn.isolation_level = level
        except Exception as e:
            logger.debug(f'Could not set isolation_level: {e}')


def enable_auto_commit(connection: Any) -> None:
    """Turn auto-commit on; a failure is logged at DEBUG and swallowed.
    """
    _set_autocommit(connection, enable=True)


def disable_auto_commit(connection: Any) -> None:
    """Turn auto-commit off; a failure is logged at DEBUG and swallowed.
    """
    _set_autocommit(connection, enable=False)


def diagnose_connection(conn: Any) -> dict[str, Any]:
    """Report a connection's transaction state, for debugging.

    Parameters
    ----------
    conn : Any
        ConnectionWrapper or raw DBAPI connection.

    Returns
    -------
    dict[str, Any]
        type : dialect name, or 'unknown'.
        is_sqlalchemy : whether conn carries an sa_connection.
        closed : conn.closed, or False.
        auto_commit : the raw connection's autocommit; failing that,
        whether its isolation_level is None (sqlite3); else None.
        in_transaction : conn.in_transaction, or False.
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
    """Run several statements as one commit, rolled back on an exception.

    Parameters
    ----------
    cn : Any
        Connection to run on, usually a ConnectionWrapper.

    Attributes
    ----------
    cn : Any
        The connection as passed; strategy code unwraps a Transaction by it.
    connection : Any
        The same connection; every method runs on it.

    Raises
    ------
    RuntimeError
        When this thread already holds an open Transaction on cn.

    Notes
    -----
    - Auto-commit is on after the block, even where it was off before it.

    Examples
    --------
    >>> with Transaction(cn) as tx:
    ...     tx.execute('delete from ...', args)
    ...     tx.execute('update ...', args)
    """

    def __init__(self, cn: Any) -> None:
        """Refuse a second open Transaction on cn in this thread.
        """
        self.cn = cn
        self.connection = cn

        if not hasattr(_local, 'active_transactions'):
            _local.active_transactions = {}

        connection_id = id(cn)
        if connection_id in _local.active_transactions:
            raise RuntimeError('Nested transactions are not supported')

    @property
    def cursor(self) -> Any:
        """A new dict cursor on the connection at each access.
        """
        return get_dict_cursor(self.connection)

    @property
    def readonly(self) -> bool:
        """The connection's readonly flag, for guards handed a Transaction.
        """
        return getattr(self.connection, 'readonly', False)

    @property
    def dialect(self) -> str:
        """Dialect of the underlying connection.
        """
        return self.connection.dialect

    def __enter__(self) -> 'Transaction':
        """Mark the connection in a transaction and turn auto-commit off.
        """
        _local.active_transactions[id(self.connection)] = True

        if hasattr(self.connection, 'in_transaction'):
            self.connection.in_transaction = True

        disable_auto_commit(self.connection)
        logger.debug(f'Started transaction for connection {id(self.connection)}')

        return self

    def __exit__(self, exc_type: type | None, value: Exception | None,
                 traceback: Any | None) -> None:
        """Commit, or roll back on an exception, then restore auto-commit.
        """
        try:
            dbapi_conn = getattr(self.connection, 'connection', self.connection)

            if exc_type is not None:
                dbapi_conn.rollback()
                logger.warning('Rolling back the current transaction')
            else:
                dbapi_conn.commit()
                logger.debug(
                    f'Committed transaction for connection {id(self.connection)}')
        finally:
            _local.active_transactions.pop(id(self.connection), None)
            enable_auto_commit(self.connection)

            if hasattr(self.connection, 'in_transaction'):
                self.connection.in_transaction = False

            logger.debug(
                'Transaction cleanup complete for connection '
                f'{id(self.connection)}')

    def execute(self, sql: str, *args: Any,
                returnid: str | list[str] | None = None) -> Any:
        """Run SQL inside the transaction.

        Parameters
        ----------
        sql : str
            Statement text, with placeholders as connection.execute
            takes them.
        *args : Any
            Parameter values.
        returnid : str | list[str] | None, default None
            Column name, or names, to read from the rows the statement
            returns, as with a returning clause.

        Returns
        -------
        Any
            Without returnid, the rowcount from connection.execute. With
            it, for one row: the named value, or a list of values for a
            list of names; for several rows, a list of those per row;
            None when the statement returns no rows.
        """
        if not returnid:
            return self.connection.execute(sql, *args)

        cursor = self.cursor
        processed_sql, processed_args = prepare_query(
            sql, args, self.connection.dialect)
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

    def select(self, sql: str, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Result of connection.select, run inside the transaction.
        """
        return self.connection.select(sql, *args, **kwargs)

    def select_column(self, sql: str, *args: Any) -> list[Any]:
        """Result of connection.select_column, run inside the transaction.
        """
        return self.connection.select_column(sql, *args)

    def select_row(self, sql: str, *args: Any) -> attrdict:
        """Result of connection.select_row, run inside the transaction.
        """
        return self.connection.select_row(sql, *args)

    def select_row_or_none(self, sql: str, *args: Any) -> attrdict | None:
        """Result of connection.select_row_or_none, run in the transaction.
        """
        return self.connection.select_row_or_none(sql, *args)

    def select_scalar(self, sql: str, *args: Any) -> Any:
        """Result of connection.select_scalar, run inside the transaction.
        """
        return self.connection.select_scalar(sql, *args)
