"""Low-level connection utilities with no internal dependencies.

These utilities work with any database connection type (ConnectionWrapper,
SQLAlchemy connections, raw DBAPI connections) and have no imports from
other database modules, making them safe to import without circular
dependency concerns.
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)


def get_dialect_name(obj: Any) -> str:
    """Get dialect name for a database connection or engine.
    """
    if hasattr(obj, 'dialect'):
        dialect = obj.dialect
        if isinstance(dialect, str):
            return dialect.lower()
        return str(dialect.name).lower()

    if hasattr(obj, 'engine') and hasattr(obj.engine, 'dialect'):
        return str(obj.engine.dialect.name).lower()

    if hasattr(obj, 'sa_connection') and hasattr(obj.sa_connection, 'engine'):
        return str(obj.sa_connection.engine.dialect.name).lower()

    if hasattr(obj, 'dbapi_connection'):
        return get_dialect_name(obj.dbapi_connection)

    type_name = f'{type(obj).__module__}.{type(obj).__name__}'
    if 'psycopg' in type_name:
        return 'postgresql'
    if 'sqlite3' in type_name:
        return 'sqlite'

    raise AttributeError(f'Cannot determine dialect for {type(obj)}')


def get_raw_connection(connection: Any) -> Any:
    """Extract the raw DBAPI connection from a wrapper."""
    raw_conn = connection
    if hasattr(connection, 'driver_connection'):
        raw_conn = connection.driver_connection
    return raw_conn


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
