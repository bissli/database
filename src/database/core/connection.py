"""
Database connection handling with SQLAlchemy.

This module provides the primary interfaces for connecting to databases:
1. The `connect()` function for creating new database connections
2. The `ConnectionWrapper` class that wraps SQLAlchemy connections

SQLAlchemy is used exclusively for connection management and pooling.
The module maintains the same API as before while using SQLAlchemy under the hood:
- Raw DBAPI connections are still accessible via `connection` attribute
- All existing code that uses our database connection API continues to work
- ConnectionWrapper continues to provide timing and statistics tracking

"""
import logging
import re
from dataclasses import fields

import sqlalchemy as sa
from database.core.cursor import CursorWrapper
from database.options import DatabaseOptions
from database.utils.connection_utils import get_connection_from_engine
from database.utils.connection_utils import get_engine_for_options

from libb import load_options

logger = logging.getLogger(__name__)


class ConnectionWrapper:
    """Wraps a SQLAlchemy connection object to track calls and execution time

    This class provides a thin wrapper around SQLAlchemy connection objects that:
    1. Tracks query execution counts and timing
    2. Manages connection lifecycle with SQLAlchemy pooling
    3. Supports context manager protocol for explicit resource management
    4. Provides access to the underlying DBAPI connection via driver_connection
    5. Delegates attribute access to the SQLAlchemy connection object

    The wrapper uses SQLAlchemy for connection management while providing access
    to the raw DBAPI connection for direct query execution.
    """

    def __init__(self, sa_connection, engine, options):
        """Initialize a connection wrapper

        Args:
            sa_connection: SQLAlchemy connection object to wrap
            engine: SQLAlchemy engine for this connection
            options: The DatabaseOptions used to create this connection
        """
        self.sa_connection = sa_connection
        self.engine = engine
        self.options = options
        self.connection = sa_connection.connection  # Raw DBAPI connection
        self.calls = 0  # Count of queries executed
        self.time = 0   # Total execution time in seconds
        self.in_transaction = False  # Track if in an explicit transaction

    def __enter__(self):
        """Support for context manager protocol"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Return the connection to the pool when exiting the context manager"""
        try:
            self.close()
            logger.debug('Closed connection via context manager')
        except Exception as e:
            logger.debug(f'Error closing connection in __exit__: {e}')

    def __getattr__(self, name):
        """Delegate attribute access to the SQLAlchemy connection or the raw connection.

        This allows the wrapper to be used like the original connection
        for any attributes or methods not explicitly overridden.
        """
        # First try the SQLAlchemy connection
        if hasattr(self.sa_connection, name):
            return getattr(self.sa_connection, name)

        # Then try the raw DBAPI connection
        return getattr(self.connection, name)

    def cursor(self, *args, **kwargs):
        """Get a wrapped cursor for this connection

        Returns a CursorWrapper that tracks execution statistics
        for the parent connection. Uses the raw DBAPI connection's cursor.

        Args:
            *args: Arguments to pass to the connection's cursor method
            **kwargs: Keyword arguments to pass to the connection's cursor method

        Returns
            CursorWrapper: A wrapped cursor object
        """
        # Ensure the connection is still valid
        if getattr(self.sa_connection, 'closed', False):
            # Get a new connection if this one is closed
            self.sa_connection = self.engine.connect()
            self.connection = self.sa_connection.driver_connection

        # Get a cursor from the raw DBAPI connection
        return CursorWrapper(self.connection.cursor(*args, **kwargs), self)

    def addcall(self, elapsed):
        """Track execution statistics

        Called by CursorWrapper to update query statistics on this connection.

        Args:
            elapsed: The execution time in seconds for a query
        """
        self.time += elapsed
        self.calls += 1

    @property
    def is_pooled(self):
        """Check if this connection is using SQLAlchemy's connection pooling

        Returns
            bool: True if connection is using pooling, False for NullPool
        """
        return not isinstance(self.engine.pool, sa.pool.NullPool)

    def commit(self):
        """Explicit commit that works regardless of auto-commit setting"""
        # Try to commit on SQLAlchemy connection
        self.sa_connection.commit()

        # Also try to commit on raw connection if needed
        # This ensures both transaction layers are committed
        if not getattr(self.connection, 'autocommit', None):
            self.connection.commit()

    def close(self):
        """Close the SQLAlchemy connection, committing first if needed"""
        from database.utils.auto_commit import ensure_commit

        if not getattr(self.sa_connection, 'closed', False):
            # Ensure any pending changes are committed before closing
            if not self.in_transaction:
                ensure_commit(self.sa_connection)

            self.sa_connection.close()
            logger.debug(f'Connection closed: {self.calls} queries in {self.time:.2f}s')


@load_options(cls=DatabaseOptions)
def connect(options, config=None, **kw):
    """Connect to a database using SQLAlchemy for connection management

    Args:
        options: Can be:
                - DatabaseOptions object
                - String path to configuration
                - Dictionary of options
                - Options specified as keyword arguments
        config: Configuration object (for loading from config files)

    Connection pooling options:
        use_pool: Whether to use connection pooling (default: False)
        pool_max_connections: Maximum connections in pool (default: 5)
        pool_max_idle_time: Maximum seconds a connection can be idle (default: 300)
        pool_wait_timeout: Maximum seconds to wait for a connection (default: 30)

    Returns
        ConnectionWrapper object

    Note:
        SQLAlchemy manages all connection cleanup and validation, so the cleanup
        option in DatabaseOptions is not needed.
    """
    # Handle options passed as DatabaseOptions object
    if isinstance(options, DatabaseOptions):
        for field in fields(options):
            kw.pop(field.name, None)
    # Handle options passed as string or dict
    else:
        options_func = load_options(cls=DatabaseOptions)(lambda o, c: o)
        options = options_func(options, config, **kw)

    # Get SQLAlchemy engine with appropriate pooling config
    engine = get_engine_for_options(
        options,
        use_pool=options.use_pool,
        pool_size=options.pool_max_connections,
        pool_recycle=options.pool_max_idle_time,
        pool_timeout=options.pool_wait_timeout
    )

    # If SQL Server, verify driver version before creating connection
    if options.drivername == 'mssql':
        version_match = re.search(r'ODBC\s+Driver\s+(\d+)', options.driver or '', re.IGNORECASE)
        if not version_match or int(version_match.group(1)) < 18:
            engine.dispose()
            raise RuntimeError(f'ODBC Driver 18 or newer is required. Configured driver: {options.driver}. '
                               f'Please upgrade your driver: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server')

    # Create SQLAlchemy connection
    sa_connection = get_connection_from_engine(engine)

    # Apply database type-specific configurations to the raw connection
    from database.strategy import get_db_strategy
    strategy = get_db_strategy(sa_connection)
    strategy.configure_connection(sa_connection.connection)

    # Register appropriate adapters for the database type
    from database import adapter_registry
    if options.drivername == 'postgresql':
        sa_connection.connection.adapters = adapter_registry.postgres()
    elif options.drivername == 'sqlite':
        adapter_registry.sqlite(sa_connection.connection)
    elif options.drivername == 'mssql':
        adapter_registry.sqlserver(sa_connection.connection)

    # Return wrapped connection
    return ConnectionWrapper(sa_connection, engine, options)
