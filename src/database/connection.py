"""Database connections over SQLAlchemy.
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
from database.cache import Cache, engine_cache_id
from database.cursor import Cursor, extract_column_info, get_dict_cursor
from database.cursor import load_data, process_multiple_result_sets
from database.exceptions import DbConnectionError, ReadOnlyError
from database.exceptions import ValidationError, is_retryable_error
from database.options import DatabaseOptions, use_iterdict_data_loader
from database.sql import _split_qualified_identifier, make_placeholders
from database.sql import prepare_query, quote_identifier
from database.strategy import get_db_strategy, get_strategy
from database.transaction import Transaction
from database.types import ColumnInfo, RowAdapter, null_special_string
from database.utils import get_dialect_name
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
]

logger = logging.getLogger(__name__)

T = TypeVar('T')
_engine_registry: dict[str, Engine] = {}
_engine_registry_lock = threading.RLock()
_CONNECTION_ROLES = frozenset({'writer', 'reader'})


def _split_schema_for_inspector(table: str) -> tuple[str | None, str]:
    """(schema, name) for SQLAlchemy's Inspector, schema None when unqualified.

    Parameters
    ----------
    table : str
        Table name. Only the last two dotted segments count.

    Returns
    -------
    tuple[str | None, str]
        Schema and table name.
    """
    parts = _split_qualified_identifier(table)
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return None, parts[-1]


def _build_engine_registry_key(options: DatabaseOptions, use_pool: bool,
                               pool_size: int, pool_recycle: int,
                               pool_timeout: int,
                               readonly: bool = False) -> str:
    """Engine registry key over every engine setting but the password.

    Parameters
    ----------
    options : DatabaseOptions
        Connection settings.
    use_pool : bool
        Whether the engine pools.
    pool_size : int
        Pool ceiling.
    pool_recycle : int
        Seconds before a pooled connection is discarded.
    pool_timeout : int
        Seconds a caller waits for a pooled connection.
    readonly : bool, default False
        Whether the engine serves the reader role.

    Returns
    -------
    str
        The registry key.
    """
    return '|'.join(repr(part) for part in (
        options.drivername, options.hostname, options.port,
        options.username, options.database, options.appname,
        options.timeout, options.open_mode,
        use_pool, pool_size, pool_recycle, pool_timeout,
        readonly,
    ))


def create_url_from_options(options: DatabaseOptions,
                            url_creator: Callable[..., sa.URL] | None = None) -> sa.URL:
    """SQLAlchemy URL the dialect's strategy builds from options.

    Parameters
    ----------
    options : DatabaseOptions
        Connection settings.
    url_creator : Callable[..., sa.URL] | None, default None
        Test seam. When given, it receives the URL's parts as keyword
        arguments (drivername, username, password, host, port,
        database, query) and its result is returned.

    Returns
    -------
    sa.URL
        A URL ready for create_engine.
    """
    strategy = get_strategy(options.drivername)
    url_string = strategy.build_connection_url(options)
    if options.drivername == 'sqlite' and options.open_mode is not None:
        database = url_string.removeprefix('sqlite:///').partition('?')[0]
    else:
        database = options.database

    url = sa.make_url(url_string)
    if database and url.database != database:
        url = url.set(database=database)
    if url_creator is None:
        return url
    return url_creator(
        drivername=url.drivername,
        username=url.username,
        password=url.password,
        host=url.host,
        port=url.port,
        database=url.database,
        query=dict(url.query))


def check_connection(
        func: Callable[..., T] | None = None, *, max_retries: int = 3,
        retry_delay: float = 1,
        retry_errors: type | tuple[type, ...] | None = None,
        retry_backoff: float = 1.5,
        sleep_func: Callable[[float], None] = time.sleep,
        check_retryable: bool = True) -> Callable[..., T]:
    """Decorator that retries a call on a connection error, with backoff.

    Parameters
    ----------
    func : Callable[..., T] | None, default None
        The decorated function in the bare form, None in the called form.
    max_retries : int, default 3
        Total attempts, the first included. Below 1 still makes one.
    retry_delay : float, default 1
        Seconds before the first retry.
    retry_errors : type | tuple[type, ...] | None, default None
        Exception types caught. None means DbConnectionError.
    retry_backoff : float, default 1.5
        Factor applied to the delay after each retry.
    sleep_func : Callable[[float], None], default time.sleep
        Test seam for the wait between attempts.
    check_retryable : bool, default True
        When True, a caught error that is_retryable_error rejects
        (syntax, type, constraint) raises at once.

    Returns
    -------
    Callable[..., T]
        The wrapped function, or a decorator in the called form. A
        retry of a ConnectionWrapper method outside a transaction runs
        on a fresh connection, so session state does not carry over.
    """
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        @wraps(f)
        def inner(*args: Any, **kwargs: Any) -> T:
            error_types = (retry_errors if retry_errors is not None
                           else DbConnectionError)

            tries = 0
            delay = retry_delay
            while True:
                try:
                    return f(*args, **kwargs)
                except error_types as err:
                    if check_retryable and not is_retryable_error(err):
                        logger.debug(
                            f'Non-retryable error, failing immediately: {err}')
                        raise

                    tries += 1
                    if tries >= max_retries:
                        logger.error(
                            f'Maximum retries ({max_retries}) exceeded: {err}')
                        raise
                    conn = args[0] if args else None
                    if (isinstance(conn, ConnectionWrapper)
                        and not conn.in_transaction):
                        conn._invalidate()
                    logger.warning(
                        f'Retryable error (attempt {tries}/{max_retries}): {err}')
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
    """The registry's engine for these settings, built on first use.

    Parameters
    ----------
    options : DatabaseOptions
        Connection settings. An in-memory SQLite database gets a fresh,
        unregistered engine on every call.
    use_pool : bool, default False
        Whether the engine pools. False means NullPool.
    pool_size : int, default 5
        Hard ceiling on pooled connections.
    pool_recycle : int, default 300
        Seconds before a pooled connection is discarded.
    pool_timeout : int, default 30
        Seconds a caller waits for a pooled connection.
    readonly : bool, default False
        Whether the engine serves the reader role.
    engine_factory : Callable[..., Engine], default sa.create_engine
        Test seam for engine construction.
    **kwargs : Any
        Extra create_engine keyword arguments. They override the
        defaults this function and the strategy set.

    Returns
    -------
    Engine
        A registered engine, or a newly built one.
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
        engine_kwargs: dict[str, Any] = {'echo': False}
        engine_kwargs.update(
            get_strategy(options.drivername).get_engine_kwargs(options))

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
        if not is_memory_sqlite:
            _engine_registry[key] = engine
        logger.debug(f'Created new engine for {options.drivername}')

        return engine


def dispose_all_engines() -> None:
    """Dispose every registered engine and empty the registry.
    """
    with _engine_registry_lock:
        for engine in _engine_registry.values():
            engine.dispose()
        _engine_registry.clear()
        logger.debug('All database engines disposed')


atexit.register(dispose_all_engines)


class ConnectionWrapper:
    """Query client over one SQLAlchemy connection.

    A closed, invalidated, or discarded connection is rebuilt from the
    engine on the next cursor(). Leaving a with block closes it.

    Attributes
    ----------
    calls : int
        Statements run through this wrapper's cursors.
    time : float
        Seconds those statements took, summed.
    in_transaction : bool
        True inside a Transaction. execute() and close() then leave the
        commit to it.
    """

    def __init__(self, sa_connection: sa.engine.Connection | None = None,
                 options: DatabaseOptions | None = None,
                 readonly: bool = False) -> None:
        """Wrap a SQLAlchemy connection.

        Parameters
        ----------
        sa_connection : sa.engine.Connection | None, default None
            Live SQLAlchemy connection.
        options : DatabaseOptions | None, default None
            Settings the connection was opened with.
        readonly : bool, default False
            Whether write methods raise ReadOnlyError.
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
        """This wrapper.
        """
        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None,
                 exc_tb: Any | None) -> None:
        """Close the connection, logging any error the close raises.
        """
        try:
            self.close()
            logger.debug('Closed connection via context manager')
        except Exception as e:
            logger.debug(f'Error closing connection in __exit__: {e}')

    def __getattr__(self, name: str) -> Any:
        """Name on the SQLAlchemy connection, else on the DBAPI connection.
        """
        if hasattr(self.sa_connection, name):
            return getattr(self.sa_connection, name)

        return getattr(self.dbapi_connection, name)

    def cursor(self) -> Cursor:
        """A dict-row cursor, reconnecting first if the connection is gone.
        """
        self._ensure_connection()
        return get_dict_cursor(self)

    def _ensure_connection(self) -> None:
        """Rebuild the connection if it is closed, invalidated, or discarded.

        Raises
        ------
        Exception
            Whatever configure_connection raises.
        """
        # A server-dropped connection leaves closed False.
        if (self.sa_connection is None
                or getattr(self.sa_connection, 'closed', False)
                or getattr(self.sa_connection, 'invalidated', False)):
            sa_connection = self.engine.connect()
            try:
                configure_connection(
                    sa_connection, readonly=self.readonly, options=self.options)
            except Exception:
                sa_connection.invalidate()
                raise
            self.sa_connection = sa_connection
            self.dbapi_connection = sa_connection.connection

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
        """Count one statement that took elapsed seconds.
        """
        self.time += elapsed
        self.calls += 1

    def _reject_if_readonly(self, operation: str) -> None:
        """Refuse a write method on a read-only connection.

        Parameters
        ----------
        operation : str
            Method name for the error.

        Raises
        ------
        ReadOnlyError
            When this connection was opened for reading only.
        """
        if self.readonly:
            raise ReadOnlyError(
                f'{operation} is not allowed on a read-only connection')

    @property
    def is_pooled(self) -> bool:
        """True unless the engine uses NullPool.
        """
        return not isinstance(self.engine.pool, sa.pool.NullPool)

    @property
    def dialect(self) -> str:
        """Dialect name, 'postgresql' or 'sqlite'.
        """
        return self._dialect

    def commit(self) -> None:
        """Commit, whatever the auto-commit setting.
        """
        # SQLAlchemy tracks no transaction for the raw cursor's
        # statements, so only the DBAPI commit reaches them.
        self.sa_connection.commit()
        if not self.sa_connection.closed:
            self.dbapi_connection.commit()

    def rollback(self) -> None:
        """Roll back work not yet committed, whatever the auto-commit setting.
        """
        self.sa_connection.rollback()
        if not self.sa_connection.closed:
            self.dbapi_connection.rollback()

    def close(self) -> None:
        """Commit unless in a transaction, then close the connection.

        An error from either step is logged at WARNING and never raised.
        """
        if not getattr(self.sa_connection, 'closed', False):
            try:
                if self.sa_connection is not None and not self.in_transaction:
                    self.commit()
            except Exception as e:
                logger.warning(f'Error during pre-close commit: {e}')
            finally:
                try:
                    if self.sa_connection and not self.sa_connection.closed:
                        self.sa_connection.close()
                except Exception as e:
                    logger.warning(f'Error closing SA connection: {e}')
            avg_seconds = self.time / max(1, self.calls)
            logger.debug(
                f'Connection closed: {self.calls} queries in {self.time:.2f}s'
                f' (avg: {avg_seconds:.3f}s per query)')

    @check_connection
    def execute(self, sql: str, *args: Any) -> int:
        """Run one statement, committing outside a transaction.

        Parameters
        ----------
        sql : str
            Statement text.
        *args : Any
            Statement parameters.

        Returns
        -------
        int
            The cursor's rowcount.
        """
        cursor = self.cursor()
        try:
            processed_sql, processed_args = prepare_query(
                sql, args, self.dialect)
            cursor.execute(processed_sql, processed_args)
            param_cnt = len(processed_args) if processed_args else 0
            logger.debug(f'Executed query with {param_cnt} parameters')
            rowcount = cursor.rowcount
            if not self.in_transaction:
                self.commit()
            return rowcount
        except Exception:
            if not self.in_transaction:
                # A failed rollback must not hide the statement's error.
                try:
                    self.rollback()
                except Exception:
                    pass
            raise

    @check_connection
    def select(self, sql: str, *args: Any, **kwargs: Any
               ) -> list[dict[str, Any]] | pd.DataFrame | list[pd.DataFrame]:
        """Rows of a query, in the form the options' data loader builds.

        Parameters
        ----------
        sql : str
            Query text.
        *args : Any
            Query parameters.
        **kwargs : Any
            return_all and prefer_first choose among the result sets of
            a procedure. The rest go to the data loader.

        Returns
        -------
        list[dict[str, Any]] | pd.DataFrame | list[pd.DataFrame]
            The loader's result. A statement opening with exec, call or
            execute in any case, or return_all=True, goes through
            process_multiple_result_sets instead. Any other
            multi-statement query, whatever prefer_first says, returns its
            first result set without args and its last with args, since
            each statement then runs on its own.
        """
        processed_sql, processed_args = prepare_query(sql, args, self.dialect)
        cursor = self.cursor()
        cursor.execute(processed_sql, processed_args)

        normalized_sql = processed_sql.strip().upper()
        is_procedure = normalized_sql.startswith(('EXEC ', 'CALL ', 'EXECUTE '))
        return_all = kwargs.pop('return_all', False)
        prefer_first = kwargs.pop('prefer_first', False)

        if not is_procedure and not return_all:
            columns = extract_column_info(cursor)
            result = load_data(cursor, columns=columns, **kwargs)
            row_cnt = len(result) if hasattr(result, '__len__') else 'scalar'
            logger.debug(f'Select query returned {row_cnt} result')
            return result

        result = process_multiple_result_sets(
            cursor, return_all, prefer_first, **kwargs)
        set_cnt = len(result) if isinstance(result, list) else 'single'
        logger.debug(f'Procedure returned {set_cnt} result set(s)')
        return result

    @use_iterdict_data_loader
    def select_column(self, sql: str, *args: Any) -> list[Any]:
        """First-column value of each row the query returns.
        """
        data = self.select(sql, *args)
        return [RowAdapter.create(self, row).get_value() for row in data]

    @use_iterdict_data_loader
    def select_row(self, sql: str, *args: Any) -> attrdict:
        """The query's one row.

        Parameters
        ----------
        sql : str
            Query text.
        *args : Any
            Query parameters.

        Returns
        -------
        attrdict
            The row.

        Raises
        ------
        ValidationError
            When the query returns zero rows or more than one.
        """
        data = self.select(sql, *args)
        if len(data) != 1:
            raise ValidationError(f'Expected one row, got {len(data)}')
        return RowAdapter.create(self, data[0]).to_attrdict()

    @use_iterdict_data_loader
    def select_row_or_none(self, sql: str, *args: Any) -> attrdict | None:
        """The query's one row, or None when it returns no row.

        Parameters
        ----------
        sql : str
            Query text.
        *args : Any
            Query parameters.

        Returns
        -------
        attrdict | None
            The row, or None for zero rows.

        Raises
        ------
        ValidationError
            When the query returns more than one row.
        """
        data = self.select(sql, *args)
        if len(data) > 1:
            raise ValidationError(f'Expected at most one row, got {len(data)}')
        if not data:
            return None
        return RowAdapter.create(self, data[0]).to_attrdict()

    @use_iterdict_data_loader
    def select_scalar(self, sql: str, *args: Any) -> Any:
        """The first column of the query's one row.

        Parameters
        ----------
        sql : str
            Query text.
        *args : Any
            Query parameters.

        Returns
        -------
        Any
            The value, null included.

        Raises
        ------
        ValidationError
            When the query returns zero rows or more than one.
        """
        data = self.select(sql, *args)
        if len(data) != 1:
            raise ValidationError(f'Expected one row, got {len(data)}')
        result = RowAdapter.create(self, data[0]).get_value()
        logger.debug(
            f'Scalar query returned value of type {type(result).__name__}')
        return result

    @use_iterdict_data_loader
    def select_scalar_or_none(self, sql: str, *args: Any) -> Any | None:
        """The first column of the query's one row, or None.

        Parameters
        ----------
        sql : str
            Query text.
        *args : Any
            Query parameters.

        Returns
        -------
        Any | None
            The value, or None for zero rows or a null value.

        Raises
        ------
        ValidationError
            When the query returns more than one row.
        """
        data = self.select(sql, *args)
        if len(data) > 1:
            raise ValidationError(f'Expected at most one row, got {len(data)}')
        if not data:
            return None
        val = RowAdapter.create(self, data[0]).get_value()
        if is_null(val):
            return None
        return val

    def get_table_columns(self, table: str,
                          bypass_cache: bool = False) -> list[str]:
        """Column names of a table in declaration order, cached per engine.

        Parameters
        ----------
        table : str
            Table name, optionally schema-qualified.
        bypass_cache : bool, default False
            Re-read the schema and replace the cached entry.

        Returns
        -------
        list[str]
            The cached list itself, so a caller must not mutate it. An
            entry lasts until Cache's schema TTL expires or
            Cache.clear_for_table(table) drops it.
        """
        cache_key = f'{table}:columns:{engine_cache_id(self)}'
        schema_cache = Cache.get_instance().get_schema_cache()
        # Cache.clear_for_table deletes entries under Cache._lock, so a
        # separate lock would let a clear run between the test and read.
        with Cache._lock:
            cached = None if bypass_cache else schema_cache.get(cache_key)
        if cached is not None:
            return cached

        schema, name = _split_schema_for_inspector(table)
        self._ensure_connection()
        inspector = inspect(self.sa_connection)
        columns = [col['name'] for col in inspector.get_columns(name, schema=schema)]
        with Cache._lock:
            schema_cache[cache_key] = columns
        return columns

    def get_table_primary_keys(self, table: str,
                               bypass_cache: bool = False) -> list[str]:
        """Primary key columns of a table, cached per engine.

        Parameters
        ----------
        table : str
            Table name, optionally schema-qualified.
        bypass_cache : bool, default False
            Re-read the schema and replace the cached entry.

        Returns
        -------
        list[str]
            Empty for a table without a primary key. The cached list
            itself, so a caller must not mutate it. An entry lasts until
            Cache's schema TTL expires or Cache.clear_for_table(table)
            drops it.
        """
        cache_key = f'{table}:primary_keys:{engine_cache_id(self)}'
        schema_cache = Cache.get_instance().get_schema_cache()
        with Cache._lock:
            cached = None if bypass_cache else schema_cache.get(cache_key)
        if cached is not None:
            return cached

        schema, name = _split_schema_for_inspector(table)
        self._ensure_connection()
        inspector = inspect(self.sa_connection)
        pk_constraint = inspector.get_pk_constraint(name, schema=schema)
        primary_keys = pk_constraint.get('constrained_columns', [])
        with Cache._lock:
            schema_cache[cache_key] = primary_keys
        return primary_keys

    def get_sequence_columns(self, table: str,
                             bypass_cache: bool = False) -> list[str]:
        """Columns the dialect's strategy takes for sequence or identity.
        """
        return get_db_strategy(self).get_sequence_columns(
            self, table, bypass_cache=bypass_cache)

    def find_sequence_column(self, table: str,
                             bypass_cache: bool = False) -> str:
        """The column reset_table_sequence resets when given no identity.
        """
        return get_db_strategy(self).find_sequence_column(
            self, table, bypass_cache=bypass_cache)

    def table_fields(self, table: str,
                     bypass_cache: bool = False) -> list[str]:
        """Same as get_table_columns.
        """
        return self.get_table_columns(table, bypass_cache=bypass_cache)

    def list_tables(self) -> list[str]:
        """User table names, ordered by name.
        """
        return get_db_strategy(self).list_tables(self)

    def table_exists(self, table: str) -> bool:
        """True when a user table of that name exists.
        """
        return get_db_strategy(self).table_exists(self, table)

    def describe_columns(self, table: str) -> list[ColumnInfo]:
        """Each column of a table as declared, in declaration order.

        Raises
        ------
        ValidationError
            When the table does not exist.
        """
        return get_db_strategy(self).describe_columns(self, table)

    def get_unique_indexes(self, table: str) -> list[list[str | None]]:
        """Columns of each unique index on a table, primary key included.
        """
        return get_db_strategy(self).get_unique_indexes(self, table)

    def table_ddl(self, table: str) -> str:
        """Text of the statement that creates a table.

        Raises
        ------
        ValidationError
            When the table does not exist.
        """
        return get_db_strategy(self).table_ddl(self, table)

    def vacuum_table(self, table: str) -> None:
        """Reclaim a table's dead space.

        Raises
        ------
        ReadOnlyError
            On a read-only connection.
        """
        self._reject_if_readonly('vacuum_table')
        get_db_strategy(self).vacuum_table(self, table)

    def reindex_table(self, table: str) -> None:
        """Rebuild a table's indexes.

        Raises
        ------
        ReadOnlyError
            On a read-only connection.
        """
        self._reject_if_readonly('reindex_table')
        get_db_strategy(self).reindex_table(self, table)

    def cluster_table(self, table: str, index: str | None = None) -> None:
        """Reorder a table's rows by an index.

        Raises
        ------
        ReadOnlyError
            On a read-only connection.
        """
        self._reject_if_readonly('cluster_table')
        get_db_strategy(self).cluster_table(self, table, index)

    def reset_table_sequence(self, table: str,
                             identity: str | None = None) -> None:
        """Set a table's sequence so the next value is the column max + 1.

        Parameters
        ----------
        table : str
            Table name.
        identity : str | None, default None
            Sequence column. None means find_sequence_column's pick.

        Raises
        ------
        ReadOnlyError
            On a read-only connection.
        """
        self._reject_if_readonly('reset_table_sequence')
        get_db_strategy(self).reset_sequence(self, table, identity)

    def insert_row(self, table: str, fields: list[str],
                   values: list[Any]) -> int:
        """Insert one row and return the row count.

        Parameters
        ----------
        table : str
            Table name, optionally schema-qualified.
        fields : list[str]
            Column names, used as given.
        values : list[Any]
            One value per field, in field order.

        Returns
        -------
        int
            Rows inserted.

        Raises
        ------
        ReadOnlyError
            On a read-only connection.
        ValidationError
            When fields and values differ in length.
        """
        self._reject_if_readonly('insert_row')
        if len(fields) != len(values):
            raise ValidationError('fields must be same length as values')

        quoted_table = quote_identifier(table, self.dialect)
        quoted_columns = ', '.join(
            quote_identifier(col, self.dialect) for col in fields)
        placeholders = make_placeholders(len(fields), self.dialect)
        sql = (f'insert into {quoted_table} ({quoted_columns})'
               f' values ({placeholders})')

        return self.execute(sql, *values)

    def insert_rows(
            self, table: str,
            rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> int:
        """Insert rows in one executemany and return the row count.

        Parameters
        ----------
        table : str
            Table name, optionally schema-qualified.
        rows : list[dict[str, Any]] | tuple[dict[str, Any], ...]
            Rows keyed by column name, matched without regard to case.
            Keys the table lacks are dropped. A row missing a key
            another row supplies binds null for it.

        Returns
        -------
        int
            Rows inserted, 0 for no rows or no key the table has.

        Raises
        ------
        ReadOnlyError
            On a read-only connection, even for no rows.
        """
        self._reject_if_readonly('insert_rows')
        if not rows:
            logger.debug('Skipping insert of empty rows')
            return 0

        filtered_rows = self.filter_table_columns(table, rows)
        if not any(filtered_rows):
            logger.warning(f'No valid columns found for {table} after filtering')
            return 0
        rows = tuple(filtered_rows)

        cols = tuple(dict.fromkeys(col for row in rows for col in row))

        quoted_table = quote_identifier(table, self.dialect)
        quoted_cols = ','.join(
            quote_identifier(col, self.dialect) for col in cols)

        placeholders = make_placeholders(len(cols), self.dialect)
        sql = (f'insert into {quoted_table} ({quoted_cols})'
               f' values ({placeholders})')

        all_params = [
            tuple(null_special_string(row.get(col)) for col in cols)
            for row in rows
            ]

        cursor = self.cursor()
        return cursor.executemany(sql, all_params)

    def update_row(self, table: str, keyfields: list[str],
                   keyvalues: list[Any], datafields: list[str],
                   datavalues: list[Any]) -> int:
        """Set datafields on the rows whose keyfields match, and count them.

        Parameters
        ----------
        table : str
            Table name, optionally schema-qualified.
        keyfields : list[str]
            Columns the where clause matches, joined by and.
        keyvalues : list[Any]
            One value per keyfield.
        datafields : list[str]
            Columns to set. None of them may be a keyfield.
        datavalues : list[Any]
            One value per datafield.

        Returns
        -------
        int
            Rows updated.

        Raises
        ------
        ReadOnlyError
            On a read-only connection.
        ValidationError
            When a field list and its value list differ in length, or a
            keyfield is also a datafield.
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
        keycols = ' and '.join(
            f'{quote_identifier(f, self.dialect)}=%s' for f in keyfields)
        datacols = ','.join(
            f'{quote_identifier(f, self.dialect)}=%s' for f in datafields)
        sql = f'update {quoted_table} set {datacols} where {keycols}'

        values = tuple(datavalues) + tuple(keyvalues)
        return self.execute(sql, *values)

    def update_or_insert(self, update_sql: str, insert_sql: str,
                         *args: Any) -> int:
        """Run update_sql, then insert_sql if it changed no row, atomically.

        Parameters
        ----------
        update_sql : str
            Update statement.
        insert_sql : str
            Insert statement.
        *args : Any
            Parameters, bound to both statements alike.

        Returns
        -------
        int
            Row count of the last statement run.

        Raises
        ------
        ReadOnlyError
            On a read-only connection.
        """
        self._reject_if_readonly('update_or_insert')

        with Transaction(self) as tx:
            rc = tx.execute(update_sql, *args)
            if rc:
                return rc
            return tx.execute(insert_sql, *args)

    def filter_table_columns(
            self, table: str,
            row_dicts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Copies of row_dicts holding only the table's columns.

        Parameters
        ----------
        table : str
            Table name, optionally schema-qualified.
        row_dicts : list[dict[str, Any]]
            Rows keyed by column name, matched to the table's columns
            without regard to case.

        Returns
        -------
        list[dict[str, Any]]
            One dict per input row, in input order, keyed by the
            table's own spelling of each name. A row with no known key
            becomes an empty dict.
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
                    filtered_row[case_map[col.lower()]] = val
                else:
                    removed_columns.add(col)
            filtered_rows.append(filtered_row)

        for col in removed_columns:
            logger.debug(f'Removed column {col} not in {table}')

        return filtered_rows

    def table_data(self, table: str, columns: list[str] | None = None,
                   bypass_cache: bool = False) -> Any:
        """Every row of a table, through select().

        Parameters
        ----------
        table : str
            Table name, optionally schema-qualified.
        columns : list[str] | None, default None
            Column names, or (column, alias) pairs. None or empty means
            the strategy's get_default_columns.
        bypass_cache : bool, default False
            Passed to get_default_columns.

        Returns
        -------
        Any
            What select() returns for the query.
        """
        if not columns:
            columns = get_db_strategy(self).get_default_columns(
                self, table, bypass_cache=bypass_cache)

        quoted_table = quote_identifier(table, self.dialect)
        quoted_columns = [
            f'{quote_identifier(col, self.dialect)}'
            f' as {quote_identifier(alias, self.dialect)}'
            for col, alias in peel(columns)
            ]
        return self.select(
            f"select {','.join(quoted_columns)} from {quoted_table}")

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
        """Insert rows, updating those that hit a conflict target.

        Parameters
        ----------
        table : str
            Table name, optionally schema-qualified.
        rows : tuple[dict[str, Any], ...]
            Rows keyed by column name, matched without regard to case.
            Keys the table lacks are dropped. A row missing a supplied
            column binds null for it.
        constraint_name : str | None, default None
            PostgreSQL only: a unique index or constraint whose
            definition becomes the conflict target. Ignored on SQLite.
        conflict_columns : list[str] | None, default None
            Conflict target columns, matched without regard to case. A
            unique constraint or index must cover them. None means the
            primary key. With no usable target the rows go to
            insert_rows.
        update_cols_always : list[str] | None, default None
            Columns set from the new row on conflict. Key columns are
            dropped, except under constraint_name. Both update lists
            None means do nothing on conflict.
        update_cols_ifnull : list[str] | None, default None
            Columns set from the new row on conflict only where the
            stored value is null. A column also in update_cols_always is
            set unconditionally, once.
        reset_sequence : bool, default False
            Whether to call reset_table_sequence after the write.
        batch_size : int, default 500
            Rows per executemany batch.
        use_primary_key : bool, default False
            SQLite only. When False and the rows omit a primary key
            column, the first unique index whose columns the rows all
            supply becomes the conflict target.

        Returns
        -------
        int
            The cursor's rowcount, or 0 when the driver reports none.

        Raises
        ------
        ReadOnlyError
            On a read-only connection, even for no rows.
        ValidationError
            When constraint_name and conflict_columns are both given.
        """
        self._reject_if_readonly('upsert_rows')
        if not rows:
            logger.debug('Skipping upsert of empty rows')
            return 0

        if constraint_name is not None and conflict_columns is not None:
            raise ValidationError(
                'constraint_name and conflict_columns are mutually exclusive')

        dialect = self.dialect

        if dialect != 'postgresql':
            constraint_name = None

        rows = tuple(self.filter_table_columns(table, list(rows)))

        table_columns = self.get_table_columns(table)
        case_map = {col.lower(): col for col in table_columns}

        provided_keys = {key for row in rows for key in row}
        columns = tuple(col for col in table_columns if col in provided_keys)

        if not columns:
            logger.warning(f'No valid columns provided for table {table}')
            return 0

        should_update = (update_cols_always is not None
                         or update_cols_ifnull is not None)

        if conflict_columns is not None:
            key_cols = [case_map.get(c.lower(), c) for c in conflict_columns]
        else:
            key_cols = self.get_table_primary_keys(table)

        columns_lower = {col.lower() for col in columns}
        key_cols_in_data = key_cols and all(
            k.lower() in columns_lower for k in key_cols)

        if (dialect == 'sqlite' and not use_primary_key
            and not key_cols_in_data and conflict_columns is None):
            strategy = get_db_strategy(self)
            if hasattr(strategy, 'get_unique_columns'):
                unique_constraints = strategy.get_unique_columns(self, table)
                for unique_cols in unique_constraints:
                    if all(u.lower() in columns_lower for u in unique_cols):
                        logger.debug(
                            f'Using UNIQUE columns {unique_cols}'
                            ' instead of primary key')
                        key_cols = unique_cols
                        key_cols_in_data = True
                        break

        key_cols_lower = {k.lower() for k in key_cols} if key_cols else set()
        if constraint_name is not None:
            updatable_lower = columns_lower
        else:
            updatable_lower = columns_lower - key_cols_lower

        if update_cols_always:
            update_cols_always = [
                case_map[col.lower()] for col in update_cols_always
                if col.lower() in updatable_lower]

        if update_cols_ifnull:
            always_lower = {c.lower() for c in update_cols_always or ()}
            ifnull_lower = updatable_lower - always_lower
            update_cols_ifnull = [
                case_map[col.lower()] for col in update_cols_ifnull
                if col.lower() in ifnull_lower]

        if (not key_cols or not key_cols_in_data) and not constraint_name:
            logger.debug(
                f'No usable constraint or key columns for {dialect} upsert,'
                ' falling back to INSERT')
            return self.insert_rows(table, rows)

        strategy = get_db_strategy(self)

        constraint_expr = None
        if constraint_name:
            constraint_expr = strategy.get_constraint_definition(
                self, table, constraint_name)

        sql = strategy.build_upsert_sql(
            table=table,
            columns=list(columns),
            key_columns=key_cols,
            constraint_expr=constraint_expr,
            update_cols_always=update_cols_always if should_update else None,
            update_cols_ifnull=update_cols_ifnull if should_update else None,
        )

        params = [
            [null_special_string(row.get(col)) for col in columns]
            for row in rows
            ]

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
        """Bulk load CSV text into a table with PostgreSQL's copy.

        Parameters
        ----------
        table : str
            Table name, optionally schema-qualified.
        file : TextIO
            CSV text with no header row.
        columns : list[str] | None, default None
            Columns the file fills, in file order. None means every
            column.

        Returns
        -------
        int
            Rows loaded. SQLite has no copy, so it loads nothing, logs
            a warning, and returns 0.

        Raises
        ------
        ReadOnlyError
            On a read-only connection.
        """
        self._reject_if_readonly('copy_from')
        return get_db_strategy(self).copy_from(self, table, file, columns)


def configure_connection(sa_connection: sa.engine.Connection,
                         readonly: bool = False, *,
                         options: DatabaseOptions) -> None:
    """Apply the dialect's session settings and type adapters.

    Parameters
    ----------
    sa_connection : sa.engine.Connection
        Connection to configure.
    readonly : bool, default False
        Whether to make the session read-only on the server. A writer
        takes the dialect's writer-only settings instead.
    options : DatabaseOptions
        Options the connection was opened with.
    """
    strategy = get_db_strategy(sa_connection)
    strategy.configure_connection(sa_connection.connection)
    strategy.register_type_adapters(sa_connection.connection)
    if readonly:
        strategy.set_session_readonly(sa_connection.connection)
    else:
        strategy.configure_writer_connection(sa_connection.connection, options)


def connect(options: DatabaseOptions | dict[str, Any] | str | None = None,
            config: Any | None = None, *, role: str = 'writer',
            **kw: Any) -> ConnectionWrapper:
    """Open a connection for the given options and role.

    Parameters
    ----------
    options : DatabaseOptions | dict[str, Any] | str | None, default None
        A DatabaseOptions object, a dotted config path, a dict of
        options, or None when the options arrive as keyword arguments.
    config : Any | None, default None
        Configuration object a dotted path is resolved against.
    role : str, default 'writer'
        Which cluster endpoint to open. 'writer' uses hostname and
        port. 'reader' uses reader_hostname and reader_port, each
        falling back to the writer's value, and rejects writes. On
        SQLite, 'reader' opens the same file read-only.
    **kw : Any
        Individual options, used when options is None.

    Returns
    -------
    ConnectionWrapper
        A live connection holding the options as passed in.

    Raises
    ------
    ValidationError
        When role names neither 'writer' nor 'reader', when config is
        a string, when a reader asks for an in-memory SQLite database,
        or when open_mode is set on a writer.
    """
    if role not in _CONNECTION_ROLES:
        raise ValidationError(
            f'role must be one of {sorted(_CONNECTION_ROLES)}, got {role!r}')
    if isinstance(config, str):
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
    if options.open_mode is not None and not readonly:
        raise ValidationError(
            f"open_mode={options.open_mode!r} opens the file read-only; "
            "pass role='reader'")
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
    try:
        configure_connection(sa_connection, readonly=readonly, options=options)
    except Exception:
        sa_connection.invalidate()
        raise

    return ConnectionWrapper(sa_connection, options, readonly=readonly)
