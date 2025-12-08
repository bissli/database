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
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import fields
from functools import wraps
from typing import TYPE_CHECKING, Any, Self, TypeVar

import pandas as pd
import sqlalchemy as sa
from database.cursor import extract_column_info, get_dict_cursor, load_data
from database.cursor import process_multiple_result_sets
from database.exceptions import DbConnectionError
from database.options import DatabaseOptions, use_iterdict_data_loader
from database.sql import make_placeholders, prepare_query, quote_identifier
from database.strategy import get_db_strategy
from database.transaction import Transaction
from database.types import AdapterRegistry, RowAdapter
from database.utils import ensure_commit, get_dialect_name
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from libb import attrdict, is_null, load_options, peel

if TYPE_CHECKING:
    pass

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

# Simple cache for schema info (cleared on bypass_cache=True)
_schema_cache: dict[tuple, list[str]] = {}


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
            error_types = retry_errors if retry_errors is not None else DbConnectionError

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

        is_procedure = any(kw in processed_sql.upper() for kw in ['EXEC ', 'CALL ', 'EXECUTE '])
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

        Raises AssertionError if the query returns zero or multiple rows.
        """
        data = self.select(sql, *args)
        assert len(data) == 1, f'Expected one row, got {len(data)}'
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

        Raises AssertionError if the query returns zero or multiple rows.
        """
        data = self.select(sql, *args)
        assert len(data) == 1, f'Expected one row, got {len(data)}'
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
        except AssertionError:
            return None

    def get_table_columns(self, table: str, bypass_cache: bool = False) -> list[str]:
        """Get all column names for a table using SQLAlchemy Inspector.
        """
        cache_key = ('columns', id(self.engine), table)
        if not bypass_cache and cache_key in _schema_cache:
            return _schema_cache[cache_key]

        inspector = inspect(self.sa_connection)
        columns = [col['name'] for col in inspector.get_columns(table)]
        _schema_cache[cache_key] = columns
        return columns

    def get_table_primary_keys(self, table: str, bypass_cache: bool = False) -> list[str]:
        """Get primary key columns for a table using SQLAlchemy Inspector.
        """
        cache_key = ('primary_keys', id(self.engine), table)
        if not bypass_cache and cache_key in _schema_cache:
            return _schema_cache[cache_key]

        inspector = inspect(self.sa_connection)
        pk_constraint = inspector.get_pk_constraint(table)
        primary_keys = pk_constraint.get('constrained_columns', [])
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
        strategy = get_db_strategy(self)
        strategy.vacuum_table(self, table)

    def reindex_table(self, table: str) -> None:
        """Rebuild indexes for a table.
        """
        strategy = get_db_strategy(self)
        strategy.reindex_table(self, table)

    def cluster_table(self, table: str, index: str | None = None) -> None:
        """Order table data according to an index.
        """
        strategy = get_db_strategy(self)
        strategy.cluster_table(self, table, index)

    def reset_table_sequence(self, table: str, identity: str | None = None) -> None:
        """Reset a table's sequence/identity column to the max value + 1.
        """
        strategy = get_db_strategy(self)
        strategy.reset_sequence(self, table, identity)

    def insert_row(self, table: str, fields: list[str], values: list[Any]) -> int:
        """Insert a row into a table using the supplied list of fields and values.
        """
        assert len(fields) == len(values), 'fields must be same length as values'

        quoted_table = quote_identifier(table, self.dialect)
        quoted_columns = ', '.join(quote_identifier(col, self.dialect) for col in fields)
        placeholders = make_placeholders(len(fields), self.dialect)
        sql = f'INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})'

        return self.execute(sql, *values)

    def insert_rows(self, table: str, rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> int:
        """Insert multiple rows into a table.
        """
        if not rows:
            logger.debug('Skipping insert of empty rows')
            return 0

        filtered_rows = self.filter_table_columns(table, rows)
        if not filtered_rows:
            logger.warning(f'No valid columns found for {table} after filtering')
            return 0
        rows = tuple(filtered_rows)

        cols = tuple(rows[0].keys())

        try:
            quoted_table = quote_identifier(table, self.dialect)
            quoted_cols = ','.join(quote_identifier(col, self.dialect) for col in cols)
        except ValueError:
            quoted_table = table
            quoted_cols = ','.join(cols)

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
        assert len(keyfields) == len(keyvalues), 'keyfields must be same length as keyvalues'
        assert len(datafields) == len(datavalues), 'datafields must be same length as datavalues'

        for kf in keyfields:
            assert kf not in datafields, f'keyfield {kf} cannot be in datafields'

        quoted_table = quote_identifier(table, self.dialect)
        keycols = ' and '.join([f'{quote_identifier(f, self.dialect)}=%s' for f in keyfields])
        datacols = ','.join([f'{quote_identifier(f, self.dialect)}=%s' for f in datafields])
        sql = f'update {quoted_table} set {datacols} where {keycols}'

        values = tuple(datavalues) + tuple(keyvalues)
        return self.execute(sql, *values)

    def update_or_insert(self, update_sql: str, insert_sql: str, *args: Any) -> int:
        """Try to update first; if no rows are updated, then insert.
        """

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

    def upsert_rows(
        self,
        table: str,
        rows: tuple[dict[str, Any], ...],
        constraint_name: str | None = None,
        update_cols_always: list[str] | None = None,
        update_cols_ifnull: list[str] | None = None,
        reset_sequence: bool = False,
        batch_size: int = 500,
        use_primary_key: bool = False,
        **kw: Any
    ) -> int:
        """Perform an UPSERT operation (INSERT or UPDATE) for multiple rows.
        """
        if not rows:
            logger.debug('Skipping upsert of empty rows')
            return 0

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

        key_cols = self.get_table_primary_keys(table)

        provided_cols_lower = {col.lower() for col in columns}
        key_cols_in_data = key_cols and all(k.lower() in provided_cols_lower for k in key_cols)

        if dialect == 'sqlite' and not use_primary_key and not key_cols_in_data:
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

        params = [[row[col] for col in columns] for row in rows]

        cursor = self.cursor()
        rc = cursor.executemany(sql, params, batch_size)

        total_affected = rc if isinstance(rc, int) else 0
        if isinstance(rc, int) and rc != len(rows):
            logger.debug(f'{len(rows) - rc} rows skipped')

        if reset_sequence:
            self.reset_table_sequence(table)

        return total_affected


def configure_connection(sa_connection: sa.engine.Connection) -> None:
    """Configure a SQLAlchemy connection with database-specific settings.
    """
    strategy = get_db_strategy(sa_connection)
    strategy.configure_connection(sa_connection.connection)

    dialect_name = get_dialect_name(sa_connection)
    if dialect_name == 'sqlite':
        AdapterRegistry().sqlite(sa_connection.connection)


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
