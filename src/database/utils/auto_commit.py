"""
Helper functions for managing auto-commit across different database drivers.
"""
import logging
from typing import Any

from database.utils.connection_utils import get_dialect_name

logger = logging.getLogger(__name__)


def enable_auto_commit(connection: Any) -> None:
    """
    Enable auto-commit mode for any database connection.

    Works with:
    - SQLAlchemy connections
    - psycopg (PostgreSQL)
    - sqlite3
    - Raw DBAPI connections

    Args:
        connection: Any database connection object
    """
    # First handle SQLAlchemy connection
    if hasattr(connection, 'execution_options') and callable(connection.execution_options):
        try:
            connection.execution_options(isolation_level='AUTOCOMMIT')
        except Exception as e:
            logger.debug(f'Could not set SQLAlchemy execution_options: {e}')

    # Get raw DBAPI connection if needed
    raw_conn = connection
    if hasattr(connection, 'driver_connection'):
        raw_conn = connection.driver_connection

    # Get dialect name
    dialect = get_dialect_name(connection)

    # PostgreSQL
    if dialect == 'postgresql' or hasattr(raw_conn, 'autocommit'):
        try:
            raw_conn.autocommit = True
        except Exception as e:
            logger.debug(f'Could not set PostgreSQL autocommit: {e}')

    # SQLite
    elif dialect == 'sqlite' or hasattr(raw_conn, 'isolation_level'):
        try:
            raw_conn.isolation_level = None
        except Exception as e:
            logger.debug(f'Could not set SQLite isolation_level: {e}')

    # Generic DBAPI
    elif hasattr(raw_conn, 'autocommit'):
        try:
            raw_conn.autocommit = True
        except Exception as e:
            logger.debug(f'Could not set generic autocommit: {e}')


def disable_auto_commit(connection: Any) -> None:
    """
    Disable auto-commit mode for any database connection.

    Args:
        connection: Any database connection object
    """
    # First handle SQLAlchemy connection
    if hasattr(connection, 'execution_options') and callable(connection.execution_options):
        try:
            connection.execution_options(isolation_level='READ COMMITTED')
        except Exception as e:
            logger.debug(f'Could not reset SQLAlchemy execution_options: {e}')

    # Get raw DBAPI connection if needed
    raw_conn = connection
    if hasattr(connection, 'driver_connection'):
        raw_conn = connection.driver_connection

    dialect = get_dialect_name(connection)

    if dialect == 'postgresql' or hasattr(raw_conn, 'autocommit'):
        try:
            raw_conn.autocommit = False
        except Exception as e:
            logger.debug(f'Could not set PostgreSQL autocommit: {e}')

    elif dialect == 'sqlite' or hasattr(raw_conn, 'isolation_level'):
        try:
            raw_conn.isolation_level = 'DEFERRED'
        except Exception as e:
            logger.debug(f'Could not set SQLite isolation_level: {e}')

    # Generic DBAPI
    elif hasattr(raw_conn, 'autocommit'):
        try:
            raw_conn.autocommit = False
        except Exception as e:
            logger.debug(f'Could not set generic autocommit: {e}')


def ensure_commit(connection: Any) -> None:
    """
    Force a commit on any database connection if it's not in auto-commit mode.
    Works safely even if the connection is already in auto-commit mode.

    Args:
        connection: Any database connection object
    """
    # Try SQLAlchemy connection first
    if hasattr(connection, 'commit'):
        try:
            connection.commit()
            return
        except Exception as e:
            logger.debug(f'Could not commit SQLAlchemy transaction: {e}')

    # Try raw connection if available
    if hasattr(connection, 'driver_connection') and hasattr(connection.driver_connection, 'commit'):
        try:
            connection.driver_connection.commit()
            return
        except Exception as e:
            logger.debug(f'Could not commit driver_connection transaction: {e}')


def diagnose_connection(conn):
    """
    Diagnose connection state for debugging.

    Args:
        conn: Any database connection object

    Returns
        dict: Connection state information
    """
    info = {
        'type': 'unknown',
        'auto_commit': None,
        'in_transaction': False,
        'closed': False
    }

    # Identify connection type
    dialect = get_dialect_name(conn)
    if dialect == 'postgresql':
        info['type'] = 'postgresql'
    elif dialect == 'sqlite':
        info['type'] = 'sqlite'

    # Get SQLAlchemy/raw status
    info['is_sqlalchemy'] = hasattr(conn, 'sa_connection')

    # Get raw connection
    raw_conn = conn
    if hasattr(conn, 'driver_connection'):
        raw_conn = conn.driver_connection

    # Check closed state
    info['closed'] = getattr(conn, 'closed', False)

    # Check auto-commit state
    info['auto_commit'] = getattr(raw_conn, 'autocommit', None)
    if info['auto_commit'] is None and hasattr(raw_conn, 'isolation_level'):
        info['auto_commit'] = raw_conn.isolation_level is None

    # Check transaction state
    if hasattr(conn, 'in_transaction'):
        info['in_transaction'] = conn.in_transaction

    return info
