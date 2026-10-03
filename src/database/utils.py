"""Helpers taking a ConnectionWrapper, SQLAlchemy or raw DBAPI connection.
"""
import logging
import sqlite3
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

_COMMIT_ERRORS = (
    psycopg.ProgrammingError,
    psycopg.InterfaceError,
    psycopg.OperationalError,
    sqlite3.ProgrammingError,
    sqlite3.InterfaceError,
    sqlite3.OperationalError,
    )


def get_dialect_name(obj: Any) -> str:
    """Lower-case dialect name, such as 'postgresql' or 'sqlite', of obj.

    Parameters
    ----------
    obj : Any
        A wrapper, engine, SQLAlchemy or bare DBAPI connection.

    Returns
    -------
    str
        Dialect name.

    Raises
    ------
    AttributeError
        When no attribute or driver module names a dialect.
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
    """connection.driver_connection when present, else connection.
    """
    return getattr(connection, 'driver_connection', connection)


def ensure_commit(connection: Any) -> None:
    """Commit on connection, else on connection.driver_connection.

    A psycopg or sqlite3 ProgrammingError, InterfaceError or
    OperationalError from either commit is logged and swallowed, so a
    failed commit returns normally.

    Parameters
    ----------
    connection : Any
        Any object; one with no commit() at either level is left alone.
    """
    if hasattr(connection, 'commit'):
        try:
            connection.commit()
            return
        except _COMMIT_ERRORS as e:
            logger.debug(f'Could not commit transaction: {e}')

    if (hasattr(connection, 'driver_connection')
        and hasattr(connection.driver_connection, 'commit')):
        try:
            connection.driver_connection.commit()
        except _COMMIT_ERRORS as e:
            logger.debug(f'Could not commit driver_connection transaction: {e}')
