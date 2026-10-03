"""Helpers taking a ConnectionWrapper, SQLAlchemy or raw DBAPI connection.
"""
from typing import Any


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
