"""
Helper functions for managing auto-commit across different database drivers.
"""
import logging
from typing import Any

from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pyodbc_connection
from database.utils.connection_utils import is_sqlite3_connection

logger = logging.getLogger(__name__)


def enable_auto_commit(connection: Any) -> None:
    """
    Enable auto-commit mode for any database connection.

    Works with:
    - SQLAlchemy connections
    - psycopg (PostgreSQL)
    - sqlite3
    - pyodbc (SQL Server)
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

    # PostgreSQL
    if is_psycopg_connection(connection) or hasattr(raw_conn, 'autocommit'):
        try:
            raw_conn.autocommit = True
        except Exception as e:
            logger.debug(f'Could not set PostgreSQL autocommit: {e}')

    # SQLite
    elif is_sqlite3_connection(connection) or hasattr(raw_conn, 'isolation_level'):
        try:
            raw_conn.isolation_level = None
        except Exception as e:
            logger.debug(f'Could not set SQLite isolation_level: {e}')

    # SQL Server via ODBC
    elif is_pyodbc_connection(connection):
        try:
            # First try the standard way
            if hasattr(raw_conn, 'autocommit'):
                raw_conn.autocommit = True
            # Then try ODBC-specific method
            elif hasattr(raw_conn, 'set_attr'):
                import pyodbc
                raw_conn.set_attr(pyodbc.SQL_ATTR_AUTOCOMMIT, pyodbc.SQL_AUTOCOMMIT_ON)
        except Exception as e:
            logger.debug(f'Could not set SQL Server autocommit: {e}')

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

    # PostgreSQL
    if is_psycopg_connection(connection) or hasattr(raw_conn, 'autocommit'):
        try:
            raw_conn.autocommit = False
        except Exception as e:
            logger.debug(f'Could not set PostgreSQL autocommit: {e}')

    # SQLite
    elif is_sqlite3_connection(connection) or hasattr(raw_conn, 'isolation_level'):
        try:
            raw_conn.isolation_level = 'DEFERRED'
        except Exception as e:
            logger.debug(f'Could not set SQLite isolation_level: {e}')

    # SQL Server via ODBC
    elif is_pyodbc_connection(connection):
        try:
            # First try the standard way
            if hasattr(raw_conn, 'autocommit'):
                raw_conn.autocommit = False
            # Then try ODBC-specific method
            elif hasattr(raw_conn, 'set_attr'):
                import pyodbc
                raw_conn.set_attr(pyodbc.SQL_ATTR_AUTOCOMMIT, pyodbc.SQL_AUTOCOMMIT_OFF)
        except Exception as e:
            logger.debug(f'Could not set SQL Server autocommit: {e}')

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
    if is_psycopg_connection(conn):
        info['type'] = 'postgresql'
    elif is_sqlite3_connection(conn):
        info['type'] = 'sqlite'
    elif is_pyodbc_connection(conn):
        info['type'] = 'sqlserver'

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
