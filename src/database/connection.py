"""
Database connection handling with SQLAlchemy.

This module provides:
1. The `connect()` function for creating new database connections
2. The `ConnectionWrapper` class that wraps SQLAlchemy connections with query methods
3. Engine creation and management through a thread-safe registry
4. Connection type detection and dialect utilities

The ConnectionWrapper is the primary database client, providing methods like:
- execute(sql, *args) - Execute SQL and return affected row count
- select(sql, *args) - Execute SELECT and return results
- select_row(sql, *args) - Execute SELECT expecting exactly 1 row
- insert_rows(table, rows) - Bulk insert multiple rows
- upsert_rows(table, rows, ...) - Insert or update rows
"""
import atexit
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from functools import wraps
from typing import Any, Self, TextIO, TypeVar

import pandas as pd
import sqlalchemy as sa
from database.cursor import extract_column_info, get_dict_cursor, load_data
from database.cursor import process_multiple_result_sets
from database.exceptions import DbConnectionError, ReadOnlyError
from database.exceptions import ValidationError, is_retryable_error
from database.options import DatabaseOptions, use_iterdict_data_loader
from database.sql import _split_qualified_identifier, make_placeholders
from database.sql import prepare_query, quote_identifier
from database.strategy import get_db_strategy, get_strategy
from database.transaction import Transaction
from database.types import RowAdapter
from database.utils import ensure_commit, get_dialect_name
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool, StaticPool

from libb import attrdict, is_null, peel

__all__ = [
    'ConnectionWrapper',
    'connect',
    'configure_connection',
    'check_connection',
    'create_url_from_options',
    'get_engine_for_options',
    'dispose_all_engines',
    'get_dialect_name',
    'ensure_commit',
]

logger = logging.getLogger(__name__)

T = TypeVar('T')
_engine_registry: dict[str, Engine] = {}
_engine_registry_lock = threading.RLock()
_CONNECTION_ROLES = frozenset({'writer', 'reader'})


def _split_schema_for_inspector(table: str) -> tuple[str | None, str]:
    """Split a possibly schema-qualified table into (schema, name) for use
    with SQLAlchemy's Inspector, which takes schema separately.
    """
    parts = _split_qualified_identifier(table)
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return None, parts[-1]


def _build_engine_registry_key(options: DatabaseOptions, use_pool: bool,
                               pool_size: int, pool_recycle: int,
                               pool_timeout: int,
                               readonly: bool = False) -> str:
    """Build a stable, password-free cache key for the engine registry.

    The key must identify an engine uniquely per (drivername, host, port,
    user, database, appname, timeout, pool config) - but never expose the
    password. Two options with the same credentials except different
    passwords still collide; that is acceptable because the registry is
    process-local and the engine pool will fail-fast on the real
    connection if the password is wrong.

    Notes
    -----
    - timeout belongs in the key because the strategies bake it into the
      engine URL; leaving it out hands the second caller the first
      caller's timeout.
    - Parts are joined through repr() so a '|' inside a username,
      database, or appname cannot shift the boundary between two fields
      and make unlike configurations share an engine.
    - readonly belongs in the key because the read-only session
      setting outlives a return to the pool; one shared engine would
      hand a writer a connection the server refuses to write on.
    """
    return '|'.join(repr(part) for part in (
        options.drivername, options.hostname, options.port,
        options.username, options.database, options.appname,
        options.timeout,
        use_pool, pool_size, pool_recycle, pool_timeout,
        readonly,
    ))


# Simple cache for schema info (cleared on bypass_cache=True)
_schema_cache: dict[tuple, list[str]] = {}
_schema_cache_lock = threading.RLock()


def create_url_from_options(options: DatabaseOptions,
                            url_creator: Callable[..., sa.URL] | None = None) -> sa.URL:
    """Convert DatabaseOptions to SQLAlchemy URL.

    Parameters
    ----------
    options : DatabaseOptions
        Connection settings; options.database is authoritative over
        whatever survives the URL round trip.
    url_creator : Callable[..., sa.URL] | None, default None
        Test seam. When given, the parsed parts are handed to it instead
        of the sa.URL that make_url produced.

    Returns
    -------
    sa.URL
        A SQLAlchemy URL ready for create_engine.

    Notes
    -----
    - The strategies percent-encode the database name so a '?' or '#' in
      it cannot inject libpq query parameters. make_url unquotes only
      username and password, so the raw name is put back here; without
      that, a database called 'my db' would be opened as 'my%20db'.
    """
    strategy = get_strategy(options.drivername)
    url_string = strategy.build_connection_url(options)

    if url_creator is not None:
        # For testing - parse and recreate using the provided factory
        parsed = sa.make_url(url_string)
        if options.database and parsed.database != options.database:
            parsed = parsed.set(database=options.database)
        return url_creator(
            drivername=parsed.drivername,
            username=parsed.username,
            password=parsed.password,
            host=parsed.host,
            port=parsed.port,
            database=parsed.database,
            query=dict(parsed.query) if parsed.query else {}
        )

    url = sa.make_url(url_string)
    if options.database and url.database != options.database:
        url = url.set(database=options.database)
    return url


def check_connection(func: Callable[..., T] | None = None, *, max_retries: int = 3,
                     retry_delay: float = 1, retry_errors: type | tuple[type, ...] | None = None,
                     retry_backoff: float = 1.5,
                     sleep_func: Callable[[float], None] = time.sleep,
                     check_retryable: bool = True) -> Callable[..., T]:
    """Connection retry decorator with backoff.

    Decorator that handles connection errors by automatically retrying the operation.
    It has configurable retry parameters and supports exponential backoff.

    Only retries for transient/recoverable errors (SSL, connection drops, timeouts)
    unless check_retryable=False. Permanent errors (syntax, type, constraint) fail
    immediately without retry.

    Supports both @check_connection and @check_connection() syntax.

    :param max_retries: Maximum number of retry attempts (default 3).
    :param retry_delay: Initial delay between retries in seconds (default 1).
    :param retry_errors: Exception types to catch (default DbConnectionError).
    :param retry_backoff: Multiplier for delay between retries (default 1.5).
    :param sleep_func: Function to use for sleeping (default time.sleep).
    :param check_retryable: If True, only retry for transient errors (default True).
    """
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        @wraps(f)
        def inner(*args: Any, **kwargs: Any) -> T:
            error_types = retry_errors if retry_errors is not None else DbConnectionError

            tries = 0
            delay = retry_delay
            while tries < max_retries:
                try:
                    return f(*args, **kwargs)
                except error_types as err:
                    if check_retryable and not is_retryable_error(err):
                        logger.debug(f'Non-retryable error, failing immediately: {err}')
                        raise

                    tries += 1
                    if tries >= max_retries:
                        logger.error(f'Maximum retries ({max_retries}) exceeded: {err}')
                        raise
                    conn = args[0] if args else None
                    if isinstance(conn, ConnectionWrapper) and not conn.in_transaction:
                        conn._invalidate()
                    logger.warning(f'Retryable error (attempt {tries}/{max_retries}): {err}')
                    sleep_func(delay)
                    delay *= retry_backoff

        return inner

    if func is None:
        return decorator
    return decorator(func)


def get_engine_for_options(options: DatabaseOptions, use_pool: bool = False,
                           pool_size: int = 5, pool_recycle: int = 300,
                           pool_timeout: int = 30, readonly: bool = False,
                           engine_factory: Callable[..., Engine] = sa.create_engine,
                           **kwargs: Any) -> Engine:
    """Get or create a SQLAlchemy engine for the given options.

    Parameters
    ----------
    options : DatabaseOptions
        Connection settings.
    use_pool : bool, default False
        Whether the engine pools connections.
    pool_size : int, default 5
        Hard ceiling on pooled connections.
    pool_recycle : int, default 300
        Seconds before a pooled connection is discarded.
    pool_timeout : int, default 30
        Seconds a caller waits for a pooled connection.
    readonly : bool, default False
        Whether connections from this engine carry the read-only
        session setting. Engines are registered separately per value,
        so a writer never checks out a read-only connection.
    engine_factory : Callable[..., Engine], default sa.create_engine
        Test seam for engine construction.
    **kwargs : Any
        Extra create_engine keyword arguments.

    Returns
    -------
    Engine
        A cached engine, or a newly built one.
    """
    is_memory_sqlite = (options.drivername == 'sqlite'
                        and options.database == ':memory:')
    key = _build_engine_registry_key(options, use_pool, pool_size,
                                     pool_recycle, pool_timeout, readonly)

    with _engine_registry_lock:
        if not is_memory_sqlite and key in _engine_registry:
            logger.debug(f'Using existing engine for {options.drivername}')
            return _engine_registry[key]

        url = create_url_from_options(options)
        strategy = get_strategy(options.drivername)

        engine_kwargs: dict[str, Any] = {'echo': False}

        # Get dialect-specific engine kwargs from strategy
        strategy_kwargs = strategy.get_engine_kwargs(options)
        engine_kwargs.update(strategy_kwargs)

        if is_memory_sqlite:
            engine_kwargs['poolclass'] = StaticPool
            connect_args = engine_kwargs.setdefault('connect_args', {})
            connect_args['check_same_thread'] = False
        elif not use_pool:
            engine_kwargs['poolclass'] = NullPool
        else:
            engine_kwargs['pool_size'] = pool_size
            engine_kwargs['pool_recycle'] = pool_recycle
            engine_kwargs['pool_timeout'] = pool_timeout

        engine_kwargs.update(kwargs)

        engine = engine_factory(url, **engine_kwargs)

        # ':memory:' engines own a private in-memory database via StaticPool;
        # caching them would leak that database across independent connect()
        # calls and break test isolation.
        if not is_memory_sqlite:
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
                 options: 'DatabaseOptions | None' = None,
                 readonly: bool = False) -> None:
        """Initialize a connection wrapper.

        Parameters
        ----------
        sa_connection : sa.engine.Connection | None, default None
            Live SQLAlchemy connection this wrapper tracks.
        options : DatabaseOptions | None, default None
            Settings the connection was opened with. For a reader,
            hostname and port already name the reader endpoint.
        readonly : bool, default False
            Whether writes are rejected before they leave the process.
            connect() sets it for role='reader'.
        """
        self.sa_connection = sa_connection
        self.engine = sa_connection.engine if sa_connection else None
        self.options = options
        self.readonly = readonly
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
        self._ensure_connection()
        return get_dict_cursor(self)

    def _ensure_connection(self) -> None:
        """Rebuild the connection if it is closed, invalidated, or discarded.

        A server-dropped connection leaves the closed flag False, so
        invalidated and the None sentinel also trigger a reconnect.
        """
        if (self.sa_connection is None
                or getattr(self.sa_connection, 'closed', False)
                or getattr(self.sa_connection, 'invalidated', False)):
            self.sa_connection = self.engine.connect()
            self.dbapi_connection = self.sa_connection.connection
            configure_connection(self.sa_connection, readonly=self.readonly)

    def _invalidate(self) -> None:
        """Discard a broken connection so the next cursor() rebuilds it.
        """
        try:
            if self.sa_connection is not None:
                self.sa_connection.invalidate()
        except Exception as e:
            logger.debug(f'Error invalidating broken connection: {e}')
        finally:
            self.sa_connection = None
            self.dbapi_connection = None

    def _addcall(self, elapsed: float) -> None:
        """Track execution statistics
        """
        self.time += elapsed
        self.calls += 1

    def _reject_if_readonly(self, operation: str) -> None:
        """Refuse an unconditional write on a read-only connection.

        Parameters
        ----------
        operation : str
            Name of the calling method, quoted back in the error.

        Raises
        ------
        ReadOnlyError
            When this connection was opened for reading only.

        Notes
        -----
        - For a method that writes whatever its arguments, so it needs
          no look at the SQL. Three reasons it is not redundant with
          the classifier: PostgreSQL's sequence reset runs 'select
          setval(...)', which classifies as a select; copy_from never
          builds a statement; and an empty row collection returns early
          before any SQL exists, which would otherwise report a clean
          zero on a reader.
        - update_or_insert takes both statements from the caller, so it
          is no stronger than execute() unless rejected here.
        """
        if self.readonly:
            raise ReadOnlyError(
                f'{operation} is not allowed on a read-only connection')

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
        """Close the SQLAlchemy connection, committing first if needed.
        """
        if not getattr(self.sa_connection, 'closed', False):
            try:
                if not self.in_transaction:
                    ensure_commit(self.sa_connection)
            except Exception as e:
                logger.warning(f'Error during pre-close commit: {e}')
            finally:
                try:
                    if self.sa_connection and not self.sa_connection.closed:
                        self.sa_connection.close()
                except Exception as e:
                    logger.warning(f'Error closing SA connection: {e}')
            logger.debug(f'Connection closed: {self.calls} queries in {self.time:.2f}s (avg: {self.time/max(1,self.calls):.3f}s per query)')

    @check_connection
    def execute(self, sql: str, *args: Any) -> int:
        """Execute a SQL query with the given parameters and return affected row count.
        """
        cursor = self.cursor()
        try:
            processed_sql, processed_args = prepare_query(sql, args, self.dialect)
            cursor.execute(processed_sql, processed_args)
            logger.debug(f'Executed query with {len(processed_args) if processed_args else 0} parameters')
            rowcount = cursor.rowcount
            if not self.in_transaction:
                self.commit()
            return rowcount
        except Exception:
            if not self.in_transaction:
                try:
                    self.rollback()
                except Exception:
                    pass
            raise

    @check_connection
    def select(self, sql: str, *args: Any, **kwargs: Any) -> list[dict[str, Any]] | pd.DataFrame | list[pd.DataFrame]:
        """Execute a SELECT query or stored procedure.
        """
        processed_sql, processed_args = prepare_query(sql, args, self.dialect)
        cursor = self.cursor()
        cursor.execute(processed_sql, processed_args)

        normalized_sql = processed_sql.strip().upper()
        is_procedure = (normalized_sql.startswith(('EXEC ', 'CALL ', 'EXECUTE ')))
        return_all = kwargs.pop('return_all', False)
        prefer_first = kwargs.pop('prefer_first', False)

        if not is_procedure and not return_all:
            columns = extract_column_info(cursor)
            result = load_data(cursor, columns=columns, **kwargs)
            logger.debug(f"Select query returned {len(result) if hasattr(result, '__len__') else 'scalar'} result")
            return result

        result = process_multiple_result_sets(cursor, return_all, prefer_first, **kwargs)
        logger.debug(f"Procedure returned {len(result) if isinstance(result, list) else 'single'} result set(s)")
        return result

    @use_iterdict_data_loader
    def select_column(self, sql: str, *args: Any) -> list[Any]:
        """Execute a query and return a single column as a list.
        """
        data = self.select(sql, *args)
        return [RowAdapter.create(self, row).get_value() for row in data]

    @use_iterdict_data_loader
    def select_row(self, sql: str, *args: Any) -> attrdict:
        """Execute a query and return a single row as an attribute dictionary.

        Raises ValidationError if the query returns zero or multiple rows.
        """
        data = self.select(sql, *args)
        if len(data) != 1:
            raise ValidationError(f'Expected one row, got {len(data)}')
        return RowAdapter.create(self, data[0]).to_attrdict()

    @use_iterdict_data_loader
    def select_row_or_none(self, sql: str, *args: Any) -> attrdict | None:
        """Execute a query and return a single row or None if no rows found.
        """
        data = self.select(sql, *args)
        if len(data) == 1:
            return RowAdapter.create(self, data[0]).to_attrdict()
        return None

    @use_iterdict_data_loader
    def select_scalar(self, sql: str, *args: Any) -> Any:
        """Execute a query and return a single scalar value.

        Raises ValidationError if the query returns zero or multiple rows.
        """
        data = self.select(sql, *args)
        if len(data) != 1:
            raise ValidationError(f'Expected one row, got {len(data)}')
        result = RowAdapter.create(self, data[0]).get_value()
        logger.debug(f'Scalar query returned value of type {type(result).__name__}')
        return result

    def select_scalar_or_none(self, sql: str, *args: Any) -> Any | None:
        """Execute a query and return a single scalar value or None if no rows found.
        """
        try:
            val = self.select_scalar(sql, *args)
            if not is_null(val):
                return val
            return None
        except ValidationError:
            return None

    def get_table_columns(self, table: str, bypass_cache: bool = False) -> list[str]:
        """Get all column names for a table using SQLAlchemy Inspector.
        """
        cache_key = ('columns', id(self.engine), table)
        with _schema_cache_lock:
            if not bypass_cache and cache_key in _schema_cache:
                return _schema_cache[cache_key]

        schema, name = _split_schema_for_inspector(table)
        self._ensure_connection()
        inspector = inspect(self.sa_connection)
        columns = [col['name'] for col in inspector.get_columns(name, schema=schema)]
        with _schema_cache_lock:
            _schema_cache[cache_key] = columns
        return columns

    def get_table_primary_keys(self, table: str, bypass_cache: bool = False) -> list[str]:
        """Get primary key columns for a table using SQLAlchemy Inspector.
        """
        cache_key = ('primary_keys', id(self.engine), table)
        with _schema_cache_lock:
            if not bypass_cache and cache_key in _schema_cache:
                return _schema_cache[cache_key]

        schema, name = _split_schema_for_inspector(table)
        self._ensure_connection()
        inspector = inspect(self.sa_connection)
        pk_constraint = inspector.get_pk_constraint(name, schema=schema)
        primary_keys = pk_constraint.get('constrained_columns', [])
        with _schema_cache_lock:
            _schema_cache[cache_key] = primary_keys
        return primary_keys

    def get_sequence_columns(self, table: str, bypass_cache: bool = False) -> list[str]:
        """Identify columns that are likely to be sequence/identity columns.
        """
        strategy = get_db_strategy(self)
        return strategy.get_sequence_columns(self, table, bypass_cache=bypass_cache)

    def find_sequence_column(self, table: str, bypass_cache: bool = False) -> str:
        """Find the best column to reset sequence for.
        """
        strategy = get_db_strategy(self)
        return strategy.find_sequence_column(self, table, bypass_cache=bypass_cache)

    def table_fields(self, table: str, bypass_cache: bool = False) -> list[str]:
        """Get all column names for a table ordered by their position.
        """
        return self.get_table_columns(table, bypass_cache=bypass_cache)

    def vacuum_table(self, table: str) -> None:
        """Optimize a table by reclaiming space.
        """
        self._reject_if_readonly('vacuum_table')
        strategy = get_db_strategy(self)
        strategy.vacuum_table(self, table)

    def reindex_table(self, table: str) -> None:
        """Rebuild indexes for a table.
        """
        self._reject_if_readonly('reindex_table')
        strategy = get_db_strategy(self)
        strategy.reindex_table(self, table)

    def cluster_table(self, table: str, index: str | None = None) -> None:
        """Order table data according to an index.
        """
        self._reject_if_readonly('cluster_table')
        strategy = get_db_strategy(self)
        strategy.cluster_table(self, table, index)

    def reset_table_sequence(self, table: str, identity: str | None = None) -> None:
        """Reset a table's sequence/identity column to the max value + 1.
        """
        self._reject_if_readonly('reset_table_sequence')
        strategy = get_db_strategy(self)
        strategy.reset_sequence(self, table, identity)

    def insert_row(self, table: str, fields: list[str], values: list[Any]) -> int:
        """Insert a row into a table using the supplied list of fields and values.
        """
        self._reject_if_readonly('insert_row')
        if len(fields) != len(values):
            raise ValidationError('fields must be same length as values')

        quoted_table = quote_identifier(table, self.dialect)
        quoted_columns = ', '.join(quote_identifier(col, self.dialect) for col in fields)
        placeholders = make_placeholders(len(fields), self.dialect)
        sql = f'INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})'

        return self.execute(sql, *values)

    def insert_rows(self, table: str, rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> int:
        """Insert multiple rows into a table.
        """
        self._reject_if_readonly('insert_rows')
        if not rows:
            logger.debug('Skipping insert of empty rows')
            return 0

        filtered_rows = self.filter_table_columns(table, rows)
        if not filtered_rows:
            logger.warning(f'No valid columns found for {table} after filtering')
            return 0
        rows = tuple(filtered_rows)

        cols = tuple(rows[0].keys())

        quoted_table = quote_identifier(table, self.dialect)
        quoted_cols = ','.join(quote_identifier(col, self.dialect) for col in cols)

        placeholders = make_placeholders(len(cols), self.dialect)
        sql = f'INSERT INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders})'

        all_params = [tuple(row.values()) for row in rows]

        cursor = self.cursor()
        return cursor.executemany(sql, all_params)

    def update_row(self, table: str, keyfields: list[str], keyvalues: list[Any],
                   datafields: list[str], datavalues: list[Any]) -> int:
        """Update the specified datafields to the supplied datavalues in a table row
        identified by the keyfields and keyvalues.
        """
        self._reject_if_readonly('update_row')
        if len(keyfields) != len(keyvalues):
            raise ValidationError('keyfields must be same length as keyvalues')
        if len(datafields) != len(datavalues):
            raise ValidationError('datafields must be same length as datavalues')

        for kf in keyfields:
            if kf in datafields:
                raise ValidationError(f'keyfield {kf} cannot be in datafields')

        quoted_table = quote_identifier(table, self.dialect)
        keycols = ' and '.join([f'{quote_identifier(f, self.dialect)}=%s' for f in keyfields])
        datacols = ','.join([f'{quote_identifier(f, self.dialect)}=%s' for f in datafields])
        sql = f'update {quoted_table} set {datacols} where {keycols}'

        values = tuple(datavalues) + tuple(keyvalues)
        return self.execute(sql, *values)

    def update_or_insert(self, update_sql: str, insert_sql: str, *args: Any) -> int:
        """Try to update first; if no rows are updated, then insert.
        """
        self._reject_if_readonly('update_or_insert')

        with Transaction(self) as tx:
            rc = tx.execute(update_sql, *args)
            if rc:
                return rc
            rc = tx.execute(insert_sql, *args)
            return rc

    def filter_table_columns(self, table: str,
                             row_dicts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter dictionaries to only include valid columns for the table
        and correct column name casing to match database schema.
        """
        if not row_dicts:
            return []

        table_cols = self.get_table_columns(table)
        case_map = {col.lower(): col for col in table_cols}

        filtered_rows = []
        removed_columns: set[str] = set()

        for row in row_dicts:
            filtered_row = {}
            for col, val in row.items():
                if col.lower() in case_map:
                    correct_col = case_map[col.lower()]
                    filtered_row[correct_col] = val
                else:
                    removed_columns.add(col)
            filtered_rows.append(filtered_row)

        for col in removed_columns:
            logger.debug(f'Removed column {col} not in {table}')

        return filtered_rows

    def table_data(self, table: str, columns: list[str] | None = None,
                   bypass_cache: bool = False) -> Any:
        """Get table data by columns.
        """
        if not columns:
            strategy = get_db_strategy(self)
            columns = strategy.get_default_columns(self, table, bypass_cache=bypass_cache)

        quoted_table = quote_identifier(table, self.dialect)
        quoted_columns = [
            f'{quote_identifier(col, self.dialect)} as {quote_identifier(alias, self.dialect)}'
            for col, alias in peel(columns)
        ]
        return self.select(f"select {','.join(quoted_columns)} from {quoted_table}")

    @check_connection
    def upsert_rows(
        self,
        table: str,
        rows: tuple[dict[str, Any], ...],
        constraint_name: str | None = None,
        conflict_columns: list[str] | None = None,
        update_cols_always: list[str] | None = None,
        update_cols_ifnull: list[str] | None = None,
        reset_sequence: bool = False,
        batch_size: int = 500,
        use_primary_key: bool = False,
    ) -> int:
        """Perform an UPSERT operation (INSERT or UPDATE) for multiple rows.

        Conflict-target precedence: constraint_name (postgres only, by index
        or constraint name) > conflict_columns (explicit column list, must be
        covered by a unique constraint or unique index) > primary-key
        auto-detect.
        """
        self._reject_if_readonly('upsert_rows')
        if not rows:
            logger.debug('Skipping upsert of empty rows')
            return 0

        if constraint_name is not None and conflict_columns is not None:
            raise ValidationError('constraint_name and conflict_columns are mutually exclusive')

        dialect = self.dialect

        if dialect != 'postgresql':
            constraint_name = None

        filtered_rows = self.filter_table_columns(table, list(rows))
        if not filtered_rows:
            logger.debug(f'No valid columns found for {table} after filtering')
            return 0
        rows = tuple(filtered_rows)

        table_columns = self.get_table_columns(table)
        case_map = {col.lower(): col for col in table_columns}

        provided_keys = {key for row in rows for key in row}
        columns = tuple(col for col in table_columns if col in provided_keys)

        if not columns:
            logger.warning(f'No valid columns provided for table {table}')
            return 0

        should_update = update_cols_always is not None or update_cols_ifnull is not None

        if conflict_columns is not None:
            key_cols = [case_map.get(c.lower(), c) for c in conflict_columns]
        else:
            key_cols = self.get_table_primary_keys(table)

        provided_cols_lower = {col.lower() for col in columns}
        key_cols_in_data = key_cols and all(k.lower() in provided_cols_lower for k in key_cols)

        if dialect == 'sqlite' and not use_primary_key and not key_cols_in_data and conflict_columns is None:
            strategy = get_db_strategy(self)
            if hasattr(strategy, 'get_unique_columns'):
                unique_constraints = strategy.get_unique_columns(self, table)
                for unique_cols in unique_constraints:
                    if all(u.lower() in provided_cols_lower for u in unique_cols):
                        logger.debug(f'Using UNIQUE columns {unique_cols} instead of primary key')
                        key_cols = unique_cols
                        key_cols_in_data = True
                        break

        if should_update and ((dialect != 'postgresql') or (dialect == 'postgresql' and not constraint_name)):
            if not key_cols:
                logger.debug(f'No primary keys found for {table}, falling back to INSERT')
                return self.insert_rows(table, rows)

        columns_lower = {col.lower() for col in columns}
        key_cols_lower = {k.lower() for k in key_cols} if key_cols else set()

        if update_cols_always:
            orig_update_cols_always = update_cols_always[:]
            valid_update_always = []
            for col in orig_update_cols_always:
                lower = col.lower()
                if lower in columns_lower and (constraint_name is not None or lower not in key_cols_lower):
                    valid_update_always.append(case_map[lower])
            update_cols_always = valid_update_always

        if update_cols_ifnull:
            orig_update_cols_ifnull = update_cols_ifnull[:]
            valid_update_ifnull = []
            uc_always_lower = {c.lower() for c in update_cols_always} if update_cols_always else set()
            for col in orig_update_cols_ifnull:
                lower = col.lower()
                if lower in columns_lower and (constraint_name is not None or lower not in key_cols_lower) and lower not in uc_always_lower:
                    valid_update_ifnull.append(case_map[lower])
            update_cols_ifnull = valid_update_ifnull

        if (not key_cols or not key_cols_in_data) and (dialect != 'postgresql' or not constraint_name):
            logger.debug(f'No usable constraint or key columns for {dialect} upsert, falling back to INSERT')
            return self.insert_rows(table, rows)

        strategy = get_db_strategy(self)

        constraint_expr = None
        if constraint_name and dialect == 'postgresql':
            constraint_expr = strategy.get_constraint_definition(self, table, constraint_name)

        sql = strategy.build_upsert_sql(
            table=table,
            columns=list(columns),
            key_columns=key_cols,
            constraint_expr=constraint_expr,
            update_cols_always=update_cols_always if should_update else None,
            update_cols_ifnull=update_cols_ifnull if should_update else None,
        )

        params = [[row.get(col) for col in columns] for row in rows]

        cursor = self.cursor()
        rc = cursor.executemany(sql, params, batch_size)

        total_affected = rc if isinstance(rc, int) else 0
        if isinstance(rc, int) and rc != len(rows):
            logger.debug(f'{len(rows) - rc} rows skipped')

        if reset_sequence:
            self.reset_table_sequence(table)

        return total_affected

    def copy_from(self, table: str, file: TextIO,
                  columns: list[str] | None = None) -> int:
        """Bulk load data from a file-like object using COPY.
        """
        self._reject_if_readonly('copy_from')
        strategy = get_db_strategy(self)
        return strategy.copy_from(self, table, file, columns)


def configure_connection(sa_connection: sa.engine.Connection,
                         readonly: bool = False) -> None:
    """Configure a SQLAlchemy connection with database-specific settings.

    Parameters
    ----------
    sa_connection : sa.engine.Connection
        Connection to configure.
    readonly : bool, default False
        Whether to put the session in read-only mode, so the server
        refuses a write the local guard cannot classify. A writer
        takes the dialect's writer-only settings instead.
    """
    strategy = get_db_strategy(sa_connection)
    strategy.configure_connection(sa_connection.connection)
    strategy.register_type_adapters(sa_connection.connection)
    if readonly:
        # Last, because configure_connection turns auto-commit on and
        # psycopg refuses to change auto-commit once a statement has
        # opened a transaction.
        strategy.set_session_readonly(sa_connection.connection)
    else:
        strategy.configure_writer_connection(sa_connection.connection)


def connect(options: DatabaseOptions | dict[str, Any] | str | None = None,
            config: Any | None = None, *, role: str = 'writer',
            **kw: Any) -> ConnectionWrapper:
    """Connect to a database using SQLAlchemy for connection management.

    Parameters
    ----------
    options : DatabaseOptions | dict[str, Any] | str | None, default None
        A DatabaseOptions object, a dotted config path, a dict of
        options, or None when the options arrive as keyword arguments.
    config : Any | None, default None
        Configuration object a dotted path is resolved against.
    role : str, default 'writer'
        Which cluster endpoint to open. Keyword only. 'writer' uses
        hostname and port. 'reader' uses reader_hostname and
        reader_port, falls back to the writer's own value for whichever
        of the two is unset, and rejects writes.
    **kw : Any
        Individual options, used when options is None.

    Returns
    -------
    ConnectionWrapper
        A live connection. Its 'readonly' attribute is True for a
        reader, and its 'options' are the ones passed in, unresolved,
        so handing them back to connect() reopens the same role.

    Raises
    ------
    ValidationError
        When role names neither 'writer' nor 'reader', when config is
        a string, or when a reader asks for an in-memory SQLite
        database.

    Notes
    -----
    - A reader rejects an insert, update, delete, DDL statement, row
      lock, or maintenance call in-process, before it reaches the
      replica, and its session is read-only on the server as well.
    - A reader and a writer to one endpoint hold separate engines, so
      the read-only session setting cannot reach a writer through the
      pool.
    - SQLite has no reader endpoint, so reader_hostname and reader_port
      are ignored there; role='reader' opens the same database and
      applies the guard.
    - Pooling comes from options: use_pool, pool_max_connections,
      pool_max_idle_time, pool_wait_timeout.
    """
    if role not in _CONNECTION_ROLES:
        raise ValidationError(
            f'role must be one of {sorted(_CONNECTION_ROLES)}, got {role!r}')
    if isinstance(config, str):
        # role is keyword only, so a caller writing connect(options,
        # 'reader') lands the role here, where it would otherwise be
        # dropped in silence and hand back a writer.
        raise ValidationError(
            f'config must be a configuration object, got the string '
            f"{config!r}; pass the role by keyword, as in "
            f"connect(options, role='reader')")

    if isinstance(options, str):
        options = DatabaseOptions.from_config(options, config=config)
    elif isinstance(options, dict):
        options = DatabaseOptions(**options)
    elif options is None:
        options = DatabaseOptions(**kw)

    readonly = role == 'reader'
    engine_options = options
    if readonly:
        if options.drivername == 'sqlite' and options.database == ':memory:':
            raise ValidationError(
                "role='reader' cannot open an in-memory SQLite database: "
                "each connection to ':memory:' owns a private, empty one")
        engine_options = replace(
            options,
            hostname=options.reader_hostname or options.hostname,
            port=options.reader_port or options.port)

    engine = get_engine_for_options(
        engine_options,
        use_pool=engine_options.use_pool,
        pool_size=engine_options.pool_max_connections,
        pool_recycle=engine_options.pool_max_idle_time,
        pool_timeout=engine_options.pool_wait_timeout,
        readonly=readonly)

    sa_connection = engine.connect()
    configure_connection(sa_connection, readonly=readonly)

    return ConnectionWrapper(sa_connection, options, readonly=readonly)
