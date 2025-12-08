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
from dataclasses import fields
from typing import Any, Self

import sqlalchemy as sa
from database.cursor import Cursor, get_dict_cursor
from database.options import DatabaseOptions
from database.utils.auto_commit import ensure_commit
from database.utils.connection_utils import get_dialect_name
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

    The wrapper uses SQLAlchemy for connection management while maintaining
    backward compatibility with code that needs direct DBAPI access.
    """

    def __init__(self, sa_connection: sa.engine.Connection | None = None,
                 options: DatabaseOptions | None = None) -> None:
        """Initialize a connection wrapper

        Args:
            sa_connection: SQLAlchemy connection object to wrap
            options: The DatabaseOptions used to create this connection
        """
        self.sa_connection = sa_connection
        self.engine = sa_connection.engine if sa_connection else None
        self.options = options
        self.dbapi_connection = sa_connection.connection if sa_connection else None
        self.calls = 0
        self.time = 0
        self.in_transaction = False

    def __enter__(self) -> Self:
        """Support for context manager protocol

        Returns
            Self reference for use in context manager

        """
        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None,
                 exc_tb: Any | None) -> None:
        """Return the connection to the pool when exiting the context manager

        Args:
            exc_type: Exception type if an exception was raised in the context
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised

        The connection's close method is called when exiting the context.
        """
        try:
            self.close()
            logger.debug('Closed connection via context manager')
        except Exception as e:
            logger.debug(f'Error closing connection in __exit__: {e}')

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the SQLAlchemy connection or the raw connection.

        Args:
            name: The name of the attribute to access

        Returns
            The requested attribute from either the SQLAlchemy connection or raw connection

        Raises
            AttributeError: If the attribute is not found on either connection

        Attributes are checked first on the SQLAlchemy connection, then on the
        underlying DBAPI connection if not found.
        """
        if hasattr(self.sa_connection, name):
            return getattr(self.sa_connection, name)

        return getattr(self.dbapi_connection, name)

    def cursor(self) -> Cursor:
        """Get a wrapped cursor for this connection

        Returns a database-specific cursor implementation (PgCursor, MsCursor,
        or SlCursor) based on the connection type. The cursor tracks execution
        statistics for the parent connection.

        Returns
            AbstractCursor: A database-specific cursor implementation

        If the connection is closed, it will be reconnected automatically.
        """
        if getattr(self.sa_connection, 'closed', False):
            self.sa_connection = self.engine.connect()
            self.dbapi_connection = self.sa_connection.connection
            configure_connection(self.sa_connection)

        return get_dict_cursor(self)

    def addcall(self, elapsed: float) -> None:
        """Track execution statistics

        Args:
            elapsed: Time in seconds that the query took to execute

        """
        self.time += elapsed
        self.calls += 1

    @property
    def is_pooled(self) -> bool:
        """Check if this connection is using SQLAlchemy's connection pooling

        Returns
            True if this connection uses SQLAlchemy's connection pool, False otherwise

        SQLAlchemy's NullPool is considered non-pooled, while QueuePool and other
        pool implementations are considered pooled connections.
        """
        return not isinstance(self.engine.pool, sa.pool.NullPool)

    def commit(self) -> None:
        """Explicit commit that works regardless of auto-commit setting

        This method ensures a transaction is committed explicitly through
        the SQLAlchemy connection.
        """
        self.sa_connection.commit()

    def close(self) -> None:
        """Close the SQLAlchemy connection, committing first if needed

        Ensures any pending transactions are committed before closing the connection,
        unless explicitly within a transaction block.

        If not in a transaction, pending changes are automatically committed.
        After closing, logs statistics about query execution.
        """
        if not getattr(self.sa_connection, 'closed', False):
            if not self.in_transaction:
                ensure_commit(self.sa_connection)

            if self.sa_connection and not self.sa_connection.closed:
                self.sa_connection.close()

            logger.debug(f'Connection closed: {self.calls} queries in {self.time:.2f}s (avg: {self.time/max(1,self.calls):.3f}s per query)')


def configure_connection(sa_connection: sa.engine.Connection) -> None:
    """Configure a SQLAlchemy connection with database-specific settings.

    Applies database-specific strategy configuration and registers appropriate
    type adapters based on the connection dialect.

    Args:
        sa_connection: SQLAlchemy connection to configure
    """
    # Import here to avoid circular import dependencies
    from database.strategy import get_db_strategy
    strategy = get_db_strategy(sa_connection)
    strategy.configure_connection(sa_connection.connection)

    # Register appropriate adapters for the database type
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

    Note:
        SQLAlchemy manages all connection cleanup and validation, so the cleanup
        option in DatabaseOptions is not needed.
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

    # Return wrapped connection
    return ConnectionWrapper(sa_connection, options)
