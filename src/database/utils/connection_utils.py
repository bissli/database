"""
Database connection type utilities with SQLAlchemy integration.

This module provides:
1. Functions for detecting connection types (SQLite, PostgreSQL, SQL Server)
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
import threading
import time
from functools import wraps

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

__all__ = [
    'is_psycopg_connection',
    'is_pyodbc_connection',
    'is_sqlite3_connection',
    'isconnection',
    'check_connection',
    'create_url_from_options',
    'get_engine_for_options',
    'dispose_all_engines',
    'get_connection_from_engine',
    'close_sqlalchemy_connection',
    'get_dialect_name',
]

logger = logging.getLogger(__name__)

# Thread-safe engine registry
_engine_registry: dict[str, Engine] = {}
_engine_registry_lock = threading.RLock()


def _check_connection_features(obj, dialect_name=None, type_str=None, _seen=None):
    """Check connection object for specific features or type signature.

    This internal helper reduces duplication in connection type detection.

    Args:
        obj: Object to check
        dialect_name: SQLAlchemy dialect name to match (e.g., 'postgresql')
        type_str: Type string to look for (e.g., 'psycopg.Connection')
        _seen: Set of already seen object IDs (for cycle detection)

    Returns
        bool: True if connection matches criteria
    """
    if _seen is None:
        _seen = set()

    if id(obj) in _seen:
        return False

    _seen.add(id(obj))

    # Check dialect for SQLAlchemy objects
    if dialect_name and hasattr(obj, 'dialect'):
        if str(obj.dialect.name).lower() == dialect_name:
            return True

    # Check engine dialect for SQLAlchemy connections
    if dialect_name and hasattr(obj, 'engine') and hasattr(obj.engine, 'dialect'):
        if str(obj.engine.dialect.name).lower() == dialect_name:
            return True

    # Check string type for direct connections
    if type_str and type_str in str(type(obj)):
        return True

    # Check for SQLAlchemy 2.0 driver_connection attribute
    if hasattr(obj, 'driver_connection'):
        driver_conn = obj.driver_connection
        if driver_conn is not obj:  # Prevent infinite recursion
            return _check_connection_features(
                driver_conn, dialect_name, type_str, _seen
            )

    # Check ConnectionWrapper objects
    if hasattr(obj, 'connection'):
        conn = obj.connection
        if conn is not obj:  # Prevent infinite recursion
            return _check_connection_features(
                conn, dialect_name, type_str, _seen
            )

    return False


def is_psycopg_connection(obj):
    """Check if object is a psycopg connection or wrapper containing one.

    Args:
        obj: Object to check

    Returns
        bool: True if the object is or contains a psycopg connection
    """
    return _check_connection_features(
        obj,
        dialect_name='postgresql',
        type_str='psycopg.Connection'
    )


def is_pyodbc_connection(obj):
    """Check if object is a pyodbc connection or wrapper containing one.

    Args:
        obj: Object to check

    Returns
        bool: True if the object is or contains a pyodbc connection
    """
    return _check_connection_features(
        obj,
        dialect_name='mssql',
        type_str='pyodbc.Connection'
    )


def is_sqlite3_connection(obj):
    """Check if object is a sqlite3 connection or wrapper containing one.

    Args:
        obj: Object to check

    Returns
        bool: True if the object is or contains a sqlite3 connection
    """
    return _check_connection_features(
        obj,
        dialect_name='sqlite',
        type_str='sqlite3.Connection'
    )


def isconnection(obj):
    """Check if object is any supported database connection"""
    if hasattr(obj, 'driver_connection'):
        return True
    if is_psycopg_connection(obj):
        return True
    if is_pyodbc_connection(obj):
        return True
    return is_sqlite3_connection(obj)


def create_url_from_options(options, url_creator=sa.URL.create):
    """Convert DatabaseOptions to SQLAlchemy URL.

    Args:
        options: DatabaseOptions object with connection parameters
        url_creator: Function used to create URL objects (default: sqlalchemy.URL.create)

    Returns
        sqlalchemy.URL: SQLAlchemy URL object for database connection
    """
    if options.drivername == 'sqlite':
        # SQLite connection string is simple
        return url_creator(
            drivername='sqlite',
            database=options.database
        )

    elif options.drivername == 'postgresql':
        # PostgreSQL connection with psycopg driver (SQLAlchemy 2.0 preferred driver)
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

    elif options.drivername == 'mssql':
        # SQL Server connection with pyodbc driver
        query_params = {
            'driver': options.driver,
            'TrustServerCertificate': 'yes' if options.trust_server_certificate else 'no',
            'MARS_Connection': 'yes'
        }

        if options.timeout:
            query_params['connect_timeout'] = str(options.timeout)

        if options.appname:
            query_params['APP'] = options.appname

        return url_creator(
            drivername='mssql+pyodbc',
            username=options.username,
            password=options.password,
            host=options.hostname,
            port=options.port,
            database=options.database,
            query=query_params
        )

    raise ValueError(f'Unsupported database type: {options.drivername}')


def check_connection(func=None, *, max_retries=3, retry_delay=1,
                     retry_errors=None, retry_backoff=1.5, sleep_func=time.sleep):
    """Connection retry decorator with backoff

    Decorator that handles connection errors by automatically retrying the operation.
    It has configurable retry parameters and supports exponential backoff.

    Supports both @check_connection and @check_connection() syntax

    Args:
        func: The function to decorate
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries in seconds
        retry_errors: Exception types that trigger a retry (default: DbConnectionError)
        retry_backoff: Multiplier for delay between retries (exponential backoff)
        sleep_func: Function to use for delay between retries (default: time.sleep)

    Returns
        Decorated function with retry logic
    """
    def decorator(f):
        @wraps(f)
        def inner(*args, **kwargs):
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


def get_engine_for_options(options, use_pool=False, pool_size=5,
                           pool_recycle=300, pool_timeout=30,
                           engine_factory=sa.create_engine, **kwargs):
    """Get or create a SQLAlchemy engine for the given options.

    Args:
        options: DatabaseOptions object
        use_pool: Whether to use connection pooling
        pool_size: Maximum number of connections in the pool (default 5)
        pool_recycle: Number of seconds after which a connection is recycled
        pool_timeout: Number of seconds to wait for a connection from the pool
        engine_factory: Function to create engines (defaults to sqlalchemy.create_engine)
        **kwargs: Additional arguments passed to engine factory

    Returns
        sqlalchemy.engine.Engine: SQLAlchemy engine
    """
    # Create a key based on options and pool settings
    key = f'{str(options)}_{use_pool}_{pool_size}_{pool_recycle}_{pool_timeout}'

    with _engine_registry_lock:
        if key in _engine_registry:
            logger.debug(f'Using existing engine for {options.drivername}')
            return _engine_registry[key]

        # Create a new engine with provided settings
        url = create_url_from_options(options)

        # SQLAlchemy 2.0 recommended settings
        engine_kwargs = {
            'pool_pre_ping': True,
            'pool_reset_on_return': 'rollback',
            'future': True,  # SQLAlchemy 2.0 behavior
            'echo': False    # Set to True for SQL logging
        }

        # Configure pooling based on use_pool parameter
        if not use_pool:
            engine_kwargs['poolclass'] = NullPool
        else:
            engine_kwargs['pool_size'] = pool_size
            engine_kwargs['pool_recycle'] = pool_recycle
            engine_kwargs['pool_timeout'] = pool_timeout
            engine_kwargs['max_overflow'] = 10

        # Add any additional engine parameters
        engine_kwargs.update(kwargs)

        # Create the engine using the factory
        engine = engine_factory(url, **engine_kwargs)

        # Store in registry
        _engine_registry[key] = engine
        logger.debug(f'Created new engine for {options.drivername}')

        return engine


def dispose_engine(options):
    """Dispose of the engine associated with the given options.

    Args:
        options: DatabaseOptions object
    """
    with _engine_registry_lock:
        for key in list(_engine_registry.keys()):
            if str(options) in key:
                _engine_registry[key].dispose()
                del _engine_registry[key]
                logger.debug(f'Disposed engine for {options.drivername}')


def dispose_all_engines():
    """Dispose all engines in the registry."""
    with _engine_registry_lock:
        for key, engine in list(_engine_registry.items()):
            engine.dispose()
        _engine_registry.clear()
        logger.debug('All database engines disposed')


# Register cleanup function to run at program exit
atexit.register(dispose_all_engines)


def get_connection_from_engine(engine):
    """Get a connection from the engine.

    This function uses SQLAlchemy 2.0 recommended connection patterns.

    Args:
        engine: SQLAlchemy engine

    Returns
        sqlalchemy.engine.Connection: SQLAlchemy connection
    """
    return engine.connect()


def close_sqlalchemy_connection(connection):
    """Close a SQLAlchemy connection.

    Args:
        connection: SQLAlchemy connection
    """
    if connection and not connection.closed:
        connection.close()


def get_dialect_name(engine_or_conn):
    """Get the dialect name for an engine or connection.

    Args:
        engine_or_conn: SQLAlchemy engine or connection

    Returns
        str: Dialect name ('postgresql', 'mssql', or 'sqlite')
    """
    dialect = None

    if hasattr(engine_or_conn, 'dialect'):
        dialect = engine_or_conn.dialect
    elif hasattr(engine_or_conn, 'engine') and hasattr(engine_or_conn.engine, 'dialect'):
        dialect = engine_or_conn.engine.dialect

    if dialect:
        name = str(dialect.name).lower()
        return name

    return None
