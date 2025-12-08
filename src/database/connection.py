"""
Database connection handling with SQLAlchemy.

This module provides:
1. The `connect()` function for creating new database connections
2. The `ConnectionWrapper` class that wraps SQLAlchemy connections
3. Engine creation and management through a thread-safe registry
4. Connection type detection and dialect utilities
"""
import atexit
import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import fields
from functools import wraps
from typing import Any, Self, TypeVar

import sqlalchemy as sa
from database.options import DatabaseOptions
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from libb import load_options

__all__ = [
    'ConnectionWrapper',
    'connect',
    'configure_connection',
    'check_connection',
    'create_url_from_options',
    'get_engine_for_options',
    'dispose_all_engines',
    'get_dialect_name',
]

logger = logging.getLogger(__name__)

T = TypeVar('T')
_engine_registry: dict[str, Engine] = {}
_engine_registry_lock = threading.RLock()


def create_url_from_options(options: DatabaseOptions,
                            url_creator: Callable[..., sa.URL] = sa.URL.create) -> sa.URL:
    """Convert DatabaseOptions to SQLAlchemy URL.
    """
    if options.drivername == 'sqlite':
        return url_creator(
            drivername='sqlite',
            database=options.database
        )

    elif options.drivername == 'postgresql':
        query = {}
        if options.timeout:
            query['connect_timeout'] = str(options.timeout)

        return url_creator(
            drivername='postgresql+psycopg',
            username=options.username,
            password=options.password,
            host=options.hostname,
            port=options.port,
            database=options.database,
            query=query
        )

    raise ValueError(f'Unsupported database type: {options.drivername}')


def check_connection(func: Callable[..., T] | None = None, *, max_retries: int = 3,
                     retry_delay: float = 1, retry_errors: type | tuple[type, ...] | None = None,
                     retry_backoff: float = 1.5,
                     sleep_func: Callable[[float], None] = time.sleep) -> Callable[..., T]:
    """Connection retry decorator with backoff.

    Decorator that handles connection errors by automatically retrying the operation.
    It has configurable retry parameters and supports exponential backoff.

    Supports both @check_connection and @check_connection() syntax.
    """
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        @wraps(f)
        def inner(*args: Any, **kwargs: Any) -> T:
            if retry_errors is None:
                from database import DbConnectionError
                error_types = DbConnectionError
            else:
                error_types = retry_errors

            tries = 0
            delay = retry_delay
            while tries < max_retries:
                try:
                    return f(*args, **kwargs)
                except error_types as err:
                    tries += 1
                    if tries >= max_retries:
                        logger.error(f'Maximum retries ({max_retries}) exceeded: {err}')
                        raise
                    logger.warning(f'Connection error (attempt {tries}/{max_retries}): {err}')
                    sleep_func(delay)
                    delay *= retry_backoff

        return inner

    if func is None:
        return decorator
    return decorator(func)


def get_engine_for_options(options: DatabaseOptions, use_pool: bool = False,
                           pool_size: int = 5, pool_recycle: int = 300,
                           pool_timeout: int = 30,
                           engine_factory: Callable[..., Engine] = sa.create_engine,
                           **kwargs: Any) -> Engine:
    """Get or create a SQLAlchemy engine for the given options.
    """
    key = f'{str(options)}_{use_pool}_{pool_size}_{pool_recycle}_{pool_timeout}'

    with _engine_registry_lock:
        if key in _engine_registry:
            logger.debug(f'Using existing engine for {options.drivername}')
            return _engine_registry[key]

        url = create_url_from_options(options)

        engine_kwargs: dict[str, Any] = {'echo': False}

        if options.drivername == 'sqlite':
            engine_kwargs['connect_args'] = {
                'detect_types': sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            }

        if not use_pool:
            engine_kwargs['poolclass'] = NullPool
        else:
            engine_kwargs['pool_size'] = pool_size
            engine_kwargs['pool_recycle'] = pool_recycle
            engine_kwargs['pool_timeout'] = pool_timeout
            engine_kwargs['max_overflow'] = 10
            engine_kwargs['pool_pre_ping'] = True
            engine_kwargs['pool_reset_on_return'] = 'rollback'

        engine_kwargs.update(kwargs)

        engine = engine_factory(url, **engine_kwargs)

        _engine_registry[key] = engine
        logger.debug(f'Created new engine for {options.drivername}')

        return engine


def dispose_all_engines() -> None:
    """Dispose all engines in the registry.
    """
    with _engine_registry_lock:
        for key, engine in list(_engine_registry.items()):
            engine.dispose()
        _engine_registry.clear()
        logger.debug('All database engines disposed')


atexit.register(dispose_all_engines)


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


def _get_raw_connection(connection: Any) -> Any:
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


class ConnectionWrapper:
    """Wraps a SQLAlchemy connection object to track calls and execution time

    This class provides a thin wrapper around SQLAlchemy connection objects that:
    1. Tracks query execution counts and timing
    2. Manages connection lifecycle with SQLAlchemy pooling
    3. Supports context manager protocol for explicit resource management
    4. Provides access to the underlying DBAPI connection via driver_connection
    5. Delegates attribute access to the SQLAlchemy connection object
    """

    def __init__(self, sa_connection: sa.engine.Connection | None = None,
                 options: 'DatabaseOptions | None' = None) -> None:
        """Initialize a connection wrapper
        """
        self.sa_connection = sa_connection
        self.engine = sa_connection.engine if sa_connection else None
        self.options = options
        self.dbapi_connection = sa_connection.connection if sa_connection else None
        self._dialect = get_dialect_name(sa_connection) if sa_connection else None
        self.calls = 0
        self.time = 0
        self.in_transaction = False

    def __enter__(self) -> Self:
        """Support for context manager protocol
        """
        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None,
                 exc_tb: Any | None) -> None:
        """Return the connection to the pool when exiting the context manager
        """
        try:
            self.close()
            logger.debug('Closed connection via context manager')
        except Exception as e:
            logger.debug(f'Error closing connection in __exit__: {e}')

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the SQLAlchemy connection or the raw connection.
        """
        if hasattr(self.sa_connection, name):
            return getattr(self.sa_connection, name)

        return getattr(self.dbapi_connection, name)

    def cursor(self) -> 'Cursor':
        """Get a wrapped cursor for this connection
        """
        from database.cursor import get_dict_cursor

        if getattr(self.sa_connection, 'closed', False):
            self.sa_connection = self.engine.connect()
            self.dbapi_connection = self.sa_connection.connection
            configure_connection(self.sa_connection)

        return get_dict_cursor(self)

    def addcall(self, elapsed: float) -> None:
        """Track execution statistics
        """
        self.time += elapsed
        self.calls += 1

    @property
    def is_pooled(self) -> bool:
        """Check if this connection is using SQLAlchemy's connection pooling
        """
        return not isinstance(self.engine.pool, sa.pool.NullPool)

    @property
    def dialect(self) -> str:
        """Return the dialect name ('postgresql' or 'sqlite')."""
        return self._dialect

    def commit(self) -> None:
        """Explicit commit that works regardless of auto-commit setting
        """
        self.sa_connection.commit()

    def close(self) -> None:
        """Close the SQLAlchemy connection, committing first if needed
        """
        if not getattr(self.sa_connection, 'closed', False):
            if not self.in_transaction:
                ensure_commit(self.sa_connection)

            if self.sa_connection and not self.sa_connection.closed:
                self.sa_connection.close()

            logger.debug(f'Connection closed: {self.calls} queries in {self.time:.2f}s (avg: {self.time/max(1,self.calls):.3f}s per query)')


def configure_connection(sa_connection: sa.engine.Connection) -> None:
    """Configure a SQLAlchemy connection with database-specific settings.
    """
    from database.strategy import get_db_strategy
    strategy = get_db_strategy(sa_connection)
    strategy.configure_connection(sa_connection.connection)

    from database import adapter_registry
    dialect_name = get_dialect_name(sa_connection)
    if dialect_name == 'sqlite':
        adapter_registry.sqlite(sa_connection.connection)


@load_options(cls=DatabaseOptions)
def connect(options: DatabaseOptions | dict[str, Any] | str,
            config: Any | None = None, **kw: Any) -> ConnectionWrapper:
    """Connect to a database using SQLAlchemy for connection management

    Args:
        options: Can be:
                - DatabaseOptions object
                - String path to configuration
                - Dictionary of options
                - Options specified as keyword arguments
        config: Configuration object (for loading from config files)
        **kw: Additional keyword arguments to override options

    Connection pooling options:
        use_pool: Whether to use connection pooling (default: False)
        pool_max_connections: Maximum connections in pool (default: 5)
        pool_max_idle_time: Maximum seconds a connection can be idle (default: 300)
        pool_wait_timeout: Maximum seconds to wait for a connection (default: 30)

    Returns
        ConnectionWrapper object for connecting to the database
    """
    if isinstance(options, DatabaseOptions):
        for field in fields(options):
            kw.pop(field.name, None)
    else:
        options_func = load_options(cls=DatabaseOptions)(lambda o, c: o)
        options = options_func(options, config, **kw)

    engine = get_engine_for_options(options, use_pool=options.use_pool,
                                    pool_size=options.pool_max_connections,
                                    pool_recycle=options.pool_max_idle_time,
                                    pool_timeout=options.pool_wait_timeout)

    sa_connection = engine.connect()
    configure_connection(sa_connection)

    return ConnectionWrapper(sa_connection, options)
