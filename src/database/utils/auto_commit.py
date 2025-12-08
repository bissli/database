"""
Helper functions for managing auto-commit across different database drivers.
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)


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


def ensure_commit(connection: Any) -> None:
    """Force a commit on any database connection if it's not in auto-commit mode.

    Works safely even if the connection is already in auto-commit mode.
    """
    if hasattr(connection, 'commit'):
        try:
            connection.commit()
            return
        except Exception as e:
            logger.debug(f'Could not commit transaction: {e}')

    if hasattr(connection, 'driver_connection') and hasattr(connection.driver_connection, 'commit'):
        try:
            connection.driver_connection.commit()
            return
        except Exception as e:
            logger.debug(f'Could not commit driver_connection transaction: {e}')


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
