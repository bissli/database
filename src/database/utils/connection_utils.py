"""
Database connection type utilities with SQLAlchemy integration.

This module provides:
1. Functions for detecting connection types (SQLite, PostgreSQL)
2. SQLAlchemy URL generation from DatabaseOptions
3. Engine creation and management through a thread-safe registry
4. Connection management utilities for SQLAlchemy

The type detection functions have been enhanced to work with both:
- Direct DBAPI connections
- SQLAlchemy connections and engines

The module functions as a bridge between our existing database API
and SQLAlchemy's connection management system.
"""
import atexit
import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any, TypeVar

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

if TYPE_CHECKING:
    from database.options import DatabaseOptions

__all__ = [
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


def create_url_from_options(options: 'DatabaseOptions',
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


def get_engine_for_options(options: 'DatabaseOptions', use_pool: bool = False,
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
