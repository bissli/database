"""
Database connection handling.
"""
import atexit
import logging
import sqlite3
import threading
import time
from dataclasses import fields

import psycopg
import pymssql
from database.core.cursor import CursorWrapper
from database.options import DatabaseOptions
from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pymssql_connection
from database.utils.connection_utils import is_sqlite3_connection
from psycopg import ClientCursor

from libb import load_options

logger = logging.getLogger(__name__)


class ConnectionWrapper:
    """Wraps a connection object to track calls and execution time"""

    def __init__(self, connection, options):
        self.connection = connection
        self.options = options
        self.calls = 0
        self.time = 0
        self._driver_type = None  # Cached driver type
        if self.options.cleanup:
            atexit.register(self.cleanup)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.connection.close()

    def __getattr__(self, name):
        """Delegate any members to the underlying connection."""
        return getattr(self.connection, name)

    def cursor(self, *args, **kwargs):
        # We'll import CursorWrapper here to avoid circular imports
        return CursorWrapper(self.connection.cursor(*args, **kwargs), self)

    def addcall(self, elapsed):
        self.time += elapsed
        self.calls += 1

    def cleanup(self):
        try:
            self.connection.close()
            logger.debug(f'Database connection lasted {self.time or 0} seconds, {self.calls or 0} queries')
        except:
            pass

    def get_driver_type(self):
        """Get the database driver type for this connection"""
        if self._driver_type is None:
            if is_psycopg_connection(self):
                self._driver_type = 'postgres'
            elif is_sqlite3_connection(self):
                self._driver_type = 'sqlite'
            elif is_pymssql_connection(self):
                self._driver_type = 'sqlserver'
            else:
                self._driver_type = 'unknown'
        return self._driver_type


class LoggingCursor(ClientCursor):
    """Cursor that logs all SQL operations for PostgreSQL"""

    def execute(self, query, params=None, *args):
        formatted = self.mogrify(query, params)
        logger.debug('SQL:\n' + formatted)
        result = super().execute(query, params, *args)
        return result


class ConnectionPool:
    """Simple database connection pool implementation"""

    @load_options(cls=DatabaseOptions)
    def __init__(self, options, config=None, max_connections=5, max_idle_time=300):
        """Initialize a connection pool

        Args:
            options: DatabaseOptions for creating connections
            max_connections: Maximum number of connections in the pool
            max_idle_time: Maximum time in seconds a connection can be idle
        """
        self.options = options
        self.max_connections = max_connections
        self.max_idle_time = max_idle_time
        self._pool = []  # (connection, last_used_time)
        self._in_use = set()
        self._lock = threading.RLock()

    def get_connection(self):
        """Get a connection from the pool or create a new one"""
        with self._lock:
            # Clean up expired connections
            self._cleanup()

            # Try to get a connection from the pool
            if self._pool:
                conn_wrapper, last_used = self._pool.pop()
                self._in_use.add(conn_wrapper)
                return conn_wrapper

            # Create new connection if under limit
            if len(self._in_use) < self.max_connections:
                from database.core.connection import \
                    connect  # Avoid circular imports
                conn_wrapper = connect(self.options)
                self._in_use.add(conn_wrapper)
                return conn_wrapper

            # Pool exhausted
            raise RuntimeError('Connection pool exhausted')

    def release_connection(self, conn_wrapper):
        """Return a connection to the pool"""
        with self._lock:
            if conn_wrapper in self._in_use:
                self._in_use.remove(conn_wrapper)
                self._pool.append((conn_wrapper, time.time()))

    def _cleanup(self):
        """Remove expired connections from the pool"""
        now = time.time()
        valid_connections = []

        for conn_wrapper, last_used in self._pool:
            if now - last_used > self.max_idle_time:
                try:
                    conn_wrapper.connection.close()
                except:
                    pass  # Ignore errors when closing expired connections
            else:
                valid_connections.append((conn_wrapper, last_used))

        self._pool = valid_connections

    def close_all(self):
        """Close all connections in the pool"""
        with self._lock:
            for conn_wrapper, _ in self._pool:
                try:
                    conn_wrapper.connection.close()
                except:
                    pass

            self._pool = []


@load_options(cls=DatabaseOptions)
def connect(options, config=None, use_pool=False, pool_max_connections=5,
            pool_max_idle_time=300, isolated_adapters=False, **kw):
    """Connect to a database

    Args:
        options: DatabaseOptions object or string/dict to create one
        config: Configuration object (for loading from config files)
        use_pool: Whether to use a connection pool
        pool_max_connections: Maximum connections in the pool
        pool_max_idle_time: Maximum idle time for pooled connections
        isolated_adapters: Whether to use isolated type adapters

    Returns
        ConnectionWrapper object
    """

    # Handle options passed as DatabaseOptions object
    if isinstance(options, DatabaseOptions):
        for field in fields(options):
            kw.pop(field.name, None)
    # Handle options passed as string or dict
    else:
        options_func = load_options(cls=DatabaseOptions)(lambda o, c: o)
        options = options_func(options, config, **kw)

    # Use connection pool if requested
    if use_pool:
        # Create a singleton pool for each options combination
        pool_key = str(options)
        if not hasattr(connect, '_connection_pools'):
            connect._connection_pools = {}

        if pool_key not in connect._connection_pools:
            connect._connection_pools[pool_key] = ConnectionPool(
                options,
                max_connections=pool_max_connections,
                max_idle_time=pool_max_idle_time
            )

        return connect._connection_pools[pool_key].get_connection()

    # Optional isolated adapters (per-connection adapter registration)
    adapter_maps = None
    if isolated_adapters:
        from database.adapters.type_adapters import register_adapters
        adapter_maps = register_adapters(isolated=True)

    # Standard connection creation
    conn = None
    if options.drivername == 'sqlite':
        conn = sqlite3.connect(database=options.database)
        conn.row_factory = sqlite3.Row

        # Apply isolated adapters if requested
        if isolated_adapters and adapter_maps['sqlite']:
            adapter_maps['sqlite'](conn)

    if options.drivername == 'postgres':
        # Use isolated adapter context if requested
        postgres_args = {
            'dbname': options.database,
            'host': options.hostname,
            'user': options.username,
            'password': options.password,
            'port': options.port,
            'connect_timeout': options.timeout,
            'cursor_factory': LoggingCursor
        }

        # Apply isolated adapters if requested
        if isolated_adapters and adapter_maps['postgres']:
            postgres_args['adapters'] = adapter_maps['postgres']

        conn = psycopg.connect(**postgres_args)

    if options.drivername == 'sqlserver':
        conn = pymssql.connect(
            database=options.database,
            user=options.username,
            server=options.hostname,
            password=options.password,
            appname=options.appname,
            timeout=options.timeout,
            port=options.port,
        )

    if not conn:
        raise AttributeError(f'{options.drivername} is not supported, see Options docstring')

    # Apply database-specific configuration
    from database.strategy import get_db_strategy
    strategy = get_db_strategy(conn)
    strategy.configure_connection(conn)

    return ConnectionWrapper(conn, options)
