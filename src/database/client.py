import atexit
import datetime
import logging
import re
import sqlite3
import time
from collections.abc import Sequence
from dataclasses import fields
from functools import wraps
from numbers import Number
from typing import Any

import cachetools
import pandas as pd
import psycopg
import pymssql
from database.adapters import TypeConverter, register_adapters
from database.options import DatabaseOptions, iterdict_data_loader
from more_itertools import flatten
from psycopg import ClientCursor
from psycopg.postgres import types

from libb import attrdict, collapse, is_null, isiterable, load_options, peel

logger = logging.getLogger(__name__)

__all__ = [
    'transaction',
    'callproc',
    'connect',
    'execute',
    'delete',
    'insert',
    'update',
    'insert_row',
    'insert_rows',
    'upsert_rows',
    'select',
    'select_column',
    'select_column_unique',
    'select_row',
    'select_row_or_none',
    'select_scalar',
    'select_scalar_or_none',
    'update_or_insert',
    'update_row',
    'isconnection',
    'is_psycopg_connection',
    'is_pymssql_connection',
    'is_sqlite3_connection',
    'reset_table_sequence',
    'vacuum_table',
    'reindex_table',
    'IntegrityError',
    'ProgrammingError',
    'OperationalError',
    'UniqueViolation',
    'DbConnectionError',
    ]

register_adapters()


# == psycopg type mapping


oid = lambda x: types.get(x).oid
aoid = lambda x: types.get(x).array_oid

postgres_types = {}
for v in [
    oid('"char"'),
    oid('bpchar'),
    oid('character varying'),
    oid('character'),
    oid('json'),
    oid('name'),
    oid('text'),
    oid('uuid'),
    oid('varchar'),
]:
    postgres_types[v] = str
for v in [
    oid('bigint'),
    oid('int2'),
    oid('int4'),
    oid('int8'),
    oid('integer'),
]:
    postgres_types[v] = int
for v in [
    oid('float4'),
    oid('float8'),
    oid('double precision'),
    oid('numeric'),
]:
    postgres_types[v] = float
for v in [oid('date')]:
    postgres_types[v] = datetime.date
for v in [
    oid('time'),
    oid('time with time zone'),
    oid('time without time zone'),
    oid('timestamp with time zone'),
    oid('timestamp without time zone'),
    oid('timestamptz'),
    oid('timetz'),
    oid('timestamp'),
]:
    postgres_types[v] = datetime.datetime
for v in [oid('bool'), oid('boolean')]:
    postgres_types[v] = bool
for v in [oid('bytea'), oid('jsonb')]:
    postgres_types[v] = bytes
postgres_types[aoid('int2vector')] = tuple
for k in tuple(postgres_types):
    postgres_types[aoid(k)] = tuple


# == defined errors

DbConnectionError = (
    psycopg.OperationalError,    # Connection/timeout issues
    psycopg.InterfaceError,      # Connection interface issues
    pymssql.OperationalError,    # SQL Server connection issues
    pymssql.InterfaceError,      # SQL Server interface issues
    sqlite3.OperationalError,    # SQLite connection issues
    sqlite3.InterfaceError,      # SQLite interface issues
)
IntegrityError = (
    psycopg.IntegrityError,      # Postgres constraint violations
    pymssql.IntegrityError,      # SQL Server constraint violations
    sqlite3.IntegrityError,      # SQLite constraint violations
)
ProgrammingError = (
    psycopg.ProgrammingError,    # Postgres syntax/query errors
    psycopg.DatabaseError,       # Postgres general database errors
    pymssql.ProgrammingError,    # SQL Server syntax/query errors
    pymssql.DatabaseError,       # SQL Server general database errors
    sqlite3.ProgrammingError,    # SQLite syntax/query errors
    sqlite3.DatabaseError,       # SQLite general database errors
)
OperationalError = (
    psycopg.OperationalError,    # Postgres operational issues
    pymssql.OperationalError,    # SQL Server operational issues
    sqlite3.OperationalError,    # SQLite operational issues
)
UniqueViolation = (
    psycopg.errors.UniqueViolation,    # Postgres specific unique violation error
    sqlite3.IntegrityError,            # SQLite error for unique/primary key violations
    pymssql.IntegrityError,            # SQL Server unique constraint violations
)


def check_connection(func, x_times=1):
    """Reconnect on closed connection
    """
    @wraps(func)
    def inner(*args, **kwargs):
        tries = 0
        while tries <= x_times:
            try:
                return func(*args, **kwargs)
            except DbConnectionError as err:
                if tries > x_times:
                    raise err
                tries += 1
                logger.warning(err)
                conn = args[0]
                if conn.options.check_connection:
                    conn.connection.close()
                    conn.connection = connect(conn.options).connection
    return inner


def handle_query_params(func):
    """Decorator that standardizes SQL parameter handling:
    - Converts placeholders between ? and %s based on database type
    """
    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        # Standardize placeholders based on connection type
        if is_psycopg_connection(cn) or is_pymssql_connection(cn):
            sql = sql.replace('?', '%s')
        elif is_sqlite3_connection(cn):
            sql = sql.replace('%s', '?')

        # Convert parameters for pymssql if needed
        if is_pymssql_connection(cn) and args:
            args = TypeConverter.convert_params(args)

        # No parameters provided
        if not args:
            return func(cn, sql, **kwargs)

        return func(cn, sql, *args, **kwargs)

    return wrapper

# == main


def is_psycopg_connection(obj):
    """Check if object is a psycopg connection or wrapper containing one."""
    if hasattr(obj, 'connection'):
        obj = obj.connection
    return isinstance(obj, psycopg.Connection)


def is_pymssql_connection(obj):
    """Check if object is a pymssql connection or wrapper containing one."""
    if hasattr(obj, 'connection'):
        obj = obj.connection
    return isinstance(obj, pymssql.Connection)


def is_sqlite3_connection(obj):
    """Check if object is a sqlite3 connection or wrapper containing one."""
    if hasattr(obj, 'connection'):
        obj = obj.connection
    return isinstance(obj, sqlite3.Connection)


def isconnection(obj):
    """Connection type check."""
    return (is_psycopg_connection(obj) or
            is_pymssql_connection(obj) or
            is_sqlite3_connection(obj))


class ConnectionWrapper:
    """Wraps a connection object so we can keep track of the
    calls and execution time of any cursors used by this connection.
      haq from https://groups.google.com/forum/?fromgroups#!topic/pyodbc/BVIZBYGXNsk
    Can be used as a context manager ... with connect(...) as cn: pass
    """

    def __init__(self, connection, options):
        self.connection = connection
        self.options = options
        self.calls = 0
        self.time = 0
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


class CursorWrapper:
    """Wraps a cursor object so we can keep track of the
    execute calls and time used by this cursor.
    """

    def __init__(self, cursor, connwrapper):
        self.cursor = cursor
        self.connwrapper = connwrapper

    def __getattr__(self, name):
        """Delegate any members to the underlying cursor."""
        return getattr(self.cursor, name)

    def __iter__(self):
        return IterChunk(self.cursor)

    def execute(self, sql, *args, **kwargs):
        """Time the call and tell the connection wrapper that
        created this connection.
        """
        start = time.time()
        for arg in collapse(args):
            if isinstance(arg, dict):
                self.cursor.execute(sql, arg)
                break
        else:
            self.cursor.execute(sql, *args)
        if is_psycopg_connection(self.connwrapper):
            logger.debug(f'Query result: {self.cursor.statusmessage}')
        end = time.time()
        self.connwrapper.addcall(end - start)
        logger.debug('Query time: %f' % (end - start))
        return self.cursor.rowcount


def dumpsql(func):
    """This is a decorator for db module functions, for logging data flowing down to driver"""
    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        try:
            # For postgres, logging happens in LoggingCursor
            if is_pymssql_connection(cn) or is_sqlite3_connection(cn):
                logger.debug(f'SQL:\n{sql}\nargs: {args}')
            return func(cn, sql, *args, **kwargs)
        except:
            logger.error(f'Error with query:\nSQL:\n{sql}\nargs: {args}')
            raise
    return wrapper


def _page_mssql(sql, order_by, offset, limit):
    """Wrap a MSSQL stmt in sql server windowing notation, strip existing order by"""
    if isiterable(order_by):
        order_by = ','.join(order_by)
    match = re.search('order by', sql, re.IGNORECASE)
    if match:
        sql = sql[: match.start()]
    logger.info(f'Paged MSSQL statement with {order_by} {offset} {limit}')
    return f"""
{sql}
ORDER BY {order_by}
OFFSET {offset} ROWS
FETCH NEXT {limit} ROWS ONLY"""


def _page_pgsql(sql, order_by, offset, limit):
    """Wrap a Postgres SQL stmt in sql server windowing notation, strip existing order by"""
    if isiterable(order_by):
        order_by = ','.join(order_by)
    match = re.search('order by', sql, re.IGNORECASE)
    if match:
        sql = sql[: match.start()]
    logger.info(f'Paged Postgres statement with {order_by} {offset} {limit}')
    return f"""
{sql}
ORDER BY {order_by}
LIMIT {limit} OFFSET {offset}"""


class LoggingCursor(ClientCursor):
    """See https://github.com/psycopg/psycopg/discussions/153 if
    considering replacing raw connections with SQLAlchemy
    """
    def execute(self, query, params=None, *args):
        formatted = self.mogrify(query, params)
        logger.debug('SQL:\n' + formatted)
        result = super().execute(query, params, *args)
        return result


@load_options(cls=DatabaseOptions)
def connect(options: str | dict | DatabaseOptions | None, config=None, **kw):
    """Database connection wrapper

    Use config.py to specify database

    config.sql.<appname>.<environment>.<foo>
    ...

    cn = connect('sql.<appname>.<environment>', config=config)
    OR (legacy)
    cn = connect(database='foo', hostname='bar', ...)
    ...
    """
    if isinstance(options, DatabaseOptions):
        for field in fields(options):
            kw.pop(field.name, None)
    conn = None
    if options.drivername == 'sqlite':
        conn = sqlite3.connect(database=options.database)
        conn.row_factory = sqlite3.Row
    if options.drivername == 'postgres':
        conn = psycopg.connect(
            dbname=options.database,
            host=options.hostname,
            user=options.username,
            password=options.password,
            port=options.port,
            connect_timeout=options.timeout,
            cursor_factory=LoggingCursor
        )
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
    return ConnectionWrapper(conn, options)


def IterChunk(cursor, size=5000):
    """Alternative to builtin cursor generator
    breaks fetches into smaller chunks to avoid malloc problems
    for really large queries (index screen, etc.)
    """
    while True:
        try:
            chunked = cursor.fetchmany(size)
        except:
            chunked = []
        if not chunked:
            break
        yield from chunked


@check_connection
@handle_query_params
@dumpsql
def select(cn, sql, *args, **kwargs) -> pd.DataFrame:
    cursor = _dict_cur(cn)  # cn is already a ConnectionWrapper
    cursor.execute(sql, args)
    return load_data(cursor, **kwargs)


@handle_query_params
@dumpsql
def callproc(cn, sql, *args, **kwargs) -> pd.DataFrame:
    """Just like select above but used for stored procs which
    often return multiple resultsets because of nocount being
    off (each rowcount is a separate resultset). We walk through
    each resultset, saving and processing the one with the most
    rows.
    """
    cursor = _dict_cur(cn)  # cn is already a ConnectionWrapper
    cursor.execute(sql, args)
    return load_data(cursor, **kwargs)


class DictRowFactory:
    """Rough equivalent of psycopg2.extras.RealDictCursor
    """
    def __init__(self, cursor: psycopg.ClientCursor[Any]):
        self.fields = [(c.name, postgres_types.get(c.type_code)) for c in (cursor.description or [])]

    def __call__(self, values: Sequence[Any]) -> dict[str, Any]:
        return {name: cast(value)
                if isinstance(value, Number)
                else value
                for (name, cast), value in zip(self.fields, values)}


def _dict_cur(cn: ConnectionWrapper):
    """Get a cursor that returns rows as dictionaries for the given connection type
    """
    if is_psycopg_connection(cn):
        cursor = cn.cursor(row_factory=DictRowFactory)
    elif is_pymssql_connection(cn):
        cursor = cn.cursor(as_dict=True)
    elif is_sqlite3_connection(cn):
        cursor = cn.cursor()
    else:
        raise ValueError('Unknown connection type')

    return CursorWrapper(cursor, cn)


def load_data(cursor, **kwargs) -> pd.DataFrame:
    """Data loader callable (IE into DataFrame)
    """
    if is_psycopg_connection(cursor.connwrapper):
        cols = [c.name for c in (cursor.description or [])]
    if is_pymssql_connection(cursor.connwrapper) or is_sqlite3_connection(cursor.connwrapper):
        cols = [c[0] for c in (cursor.description or [])]
    data = cursor.fetchall()  # iterdict (dictcursor)
    data_loader = cursor.connwrapper.options.data_loader
    return data_loader(data, cols, **kwargs)


def use_iterdict_data_loader(func):
    """Temporarily use default Pandas loader over user-specified loader
    """
    @wraps(func)
    def inner(*args, **kwargs):
        cn = args[0]
        original_data_loader = cn.options.data_loader
        cn.options.data_loader = iterdict_data_loader
        try:
            return func(*args, **kwargs)
        finally:
            cn.options.data_loader = original_data_loader
    return inner


@use_iterdict_data_loader
def select_column(cn, sql, *args) -> list:
    data = select(cn, sql, *args)
    return [row[tuple(row.keys())[0]] for row in data]


def select_column_unique(cn, sql, *args) -> set:
    return set(select_column(cn, sql, *args))


@use_iterdict_data_loader
def select_row(cn, sql, *args) -> attrdict:
    data = select(cn, sql, *args)
    assert len(data) == 1, 'Expected one row, got %d' % len(data)
    return attrdict(data[0])


@use_iterdict_data_loader
def select_row_or_none(cn, sql, *args) -> attrdict | None:
    data = select(cn, sql, *args)
    if len(data) == 1:
        return attrdict(data[0])
    return None


@use_iterdict_data_loader
def select_scalar(cn, sql, *args):
    data = select(cn, sql, *args)
    assert len(data) == 1, 'Expected one col, got %d' % len(data)
    return tuple(data[0].values())[0]


def select_scalar_or_none(cn, sql, *args):
    val = select_scalar(cn, sql, *args)
    if not is_null(val):
        return val
    return None


@check_connection
@handle_query_params
@dumpsql
def execute(cn, sql, *args):
    cursor = cn.cursor()
    cursor.execute(sql, args)
    rowcount = cursor.rowcount
    cn.commit()
    return rowcount


insert = update = delete = execute


class transaction:
    """Context manager for running multiple commands in a transaction.

    with db.transaction(cn) as tx:
        tx.execute('delete from ...', args)
        tx.execute('update from ...', args)
    """

    def __init__(self, cn):
        self.connection = cn

    @property
    def cursor(self):
        """Lazy cursor wrapped with timing and error handling
        """
        return CursorWrapper(_dict_cur(self.connection), self.connection)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, value, traceback):
        if exc_type is not None:
            self.connection.rollback()
            logger.warning('Rolling back the current transaction')
        else:
            self.connection.commit()
            logger.debug('Committed transaction.')

    @check_connection
    @handle_query_params
    @dumpsql
    def execute(self, sql, *args, returnid=None):
        rc = self.cursor.execute(sql, args)

        if not returnid:
            return rc

        # may not work as cursor object may no longer exist
        # getting recreated on each call
        result = None
        try:
            result = self.cursor.fetchone()
        except:
            logger.debug('No results to return')
        finally:
            if not result:
                return

        if isiterable(returnid):
            return [result[r] for r in returnid]
        else:
            return result[returnid]

    @handle_query_params
    @dumpsql
    def select(self, sql, *args, **kwargs) -> pd.DataFrame:
        cursor = self.cursor
        cursor.execute(sql, args)
        return load_data(cursor, **kwargs)


@handle_query_params
@dumpsql
def insert_identity(cn, sql, *args):
    """Inject @@identity column into query for row by row unique id"""
    cursor=cn.cursor()
    cursor.execute(sql + '; select @@identity', args)
    # rowcount = cursor.rowcount
    cursor.nextset()
    identity=cursor.fetchone()[0]
    # must do the commit after retrieving data since commit closes cursor
    cn.commit()
    return identity


def update_or_insert(cn, update_sql, insert_sql, *args):
    """TODO: better way to do this query is with postgres on conflict do ..."""
    with transaction(cn) as tx:
        rc = tx.execute(update_sql, args)
        if rc:
            return rc
        rc = tx.execute(insert_sql, args)
        return rc


def _build_upsert_sql(
    table: str,
    columns: tuple[str],
    key_columns: list[str],
    update_always: list[str] = None,
    update_if_null: list[str] = None,
    driver: str = 'postgres',
) -> str:
    """Builds an UPSERT SQL statement for the specified database driver.

    Args:
        table: Target table name
        columns: All columns to insert
        key_columns: Columns that form conflict constraint (primary/unique keys)
        update_always: Columns that should always be updated on conflict
        update_if_null: Columns that should only be updated if target is null
        driver: Database driver ('postgres', 'sqlite', 'sqlserver')
    """
    # Validate inputs
    if not key_columns:
        return _build_insert_sql(table, columns)

    # PostgreSQL uses INSERT ... ON CONFLICT DO UPDATE
    if driver == 'postgres':
        # build the basic insert part
        insert_sql = _build_insert_sql(table, columns)

        # build the on conflict clause
        conflict_sql = f"on conflict ({','.join(key_columns)})"

        # If no updates requested, do nothing
        if not (update_always or update_if_null):
            return f'{insert_sql} {conflict_sql} do nothing'

        # Build update expressions
        update_exprs = []
        if update_always:
            update_exprs.extend(f'{col} = excluded.{col}'
                                for col in update_always)
        if update_if_null:
            update_exprs.extend(f'{col} = coalesce({table}.{col}, excluded.{col})'
                                for col in update_if_null)

        update_sql = f"do update set {', '.join(update_exprs)}"

        return f'{insert_sql} {conflict_sql} {update_sql}'

    # SQLite uses INSERT ... ON CONFLICT DO UPDATE (similar to PostgreSQL)
    elif driver == 'sqlite':
        # build the basic insert part
        insert_sql = _build_insert_sql(table, columns)

        # build the on conflict clause
        conflict_sql = f"on conflict ({','.join(key_columns)})"

        # If no updates requested, do nothing
        if not (update_always or update_if_null):
            return f'{insert_sql} {conflict_sql} do nothing'

        # Build update expressions
        update_exprs = []
        if update_always:
            update_exprs.extend(f'{col} = excluded.{col}'
                                for col in update_always)
        if update_if_null:
            update_exprs.extend(f'{col} = COALESCE({table}.{col}, excluded.{col})'
                                for col in update_if_null)

        update_sql = f"do update set {', '.join(update_exprs)}"

        return f'{insert_sql} {conflict_sql} {update_sql}'

    # SQL Server uses MERGE INTO
    elif driver == 'sqlserver':
        # For SQL Server we use MERGE statement
        placeholders = ','.join(['%s'] * len(columns))
        temp_alias = 'src'

        # Build conditions for matching keys
        match_conditions = ' AND '.join(
            f'target.{col} = {temp_alias}.{col}' for col in key_columns
        )

        # Build column value list for source
        source_values = ', '.join(f'%s as {col}' for col in columns)

        # Build UPDATE statements
        update_cols = []
        if update_always:
            update_cols.extend(update_always)
        if update_if_null:
            # For SQL Server, handle COALESCE in the driver logic instead
            update_cols.extend(update_if_null)

        update_clause = ''
        if update_cols:
            update_statements = ', '.join(
                f'target.{col} = {temp_alias}.{col}' for col in update_cols
            )
            update_clause = f'WHEN MATCHED THEN UPDATE SET {update_statements}'
        else:
            # If no updates but we have keys, just match without updating
            update_clause = 'WHEN MATCHED THEN DO NOTHING'

        # Build INSERT statements
        all_columns = ', '.join(columns)
        source_columns = ', '.join(f'{temp_alias}.{col}' for col in columns)

        # Full MERGE statement
        merge_sql = f"""
        MERGE INTO {table} AS target
        USING (SELECT {source_values}) AS {temp_alias}
        ON {match_conditions}
        {update_clause}
        WHEN NOT MATCHED THEN INSERT ({all_columns}) VALUES ({source_columns});
        """

        return merge_sql

    else:
        raise ValueError(f'Driver {driver} not supported for UPSERT operations')


def _build_insert_sql(table: str, columns: tuple[str]) -> str:
    """Builds the INSERT part of the SQL statement
    """
    placeholders = ','.join(['%s'] * len(columns))
    return f"insert into {table} ({','.join(columns)}) values ({placeholders})"


def _get_driver_type(cn):
    """Determine the database driver type from connection"""
    return 'postgres' if is_psycopg_connection(cn) else \
           'sqlite' if is_sqlite3_connection(cn) else \
           'sqlserver' if is_pymssql_connection(cn) else None


def _prepare_rows_for_upsert(cn, table, rows):
    """Prepare and validate rows for upsert operation"""
    # Include only columns that exist in the table
    filtered_rows = filter_table_columns(cn, table, rows)
    if not filtered_rows:
        logger.warning(f'No valid columns found for {table} after filtering')
        return None
    return tuple(filtered_rows)


def _filter_update_columns(columns, update_cols, key_cols):
    """Filter update columns to ensure they're valid"""
    if not update_cols:
        return []
    return [c for c in update_cols if c in columns and c not in key_cols]


def _execute_standard_upsert(cn, table, rows, columns, key_cols,
                             update_always, update_ifnull, driver):
    """Execute standard upsert operation for supported databases"""
    # Build the SQL statement
    sql = _build_upsert_sql(
        table=table,
        columns=columns,
        key_columns=key_cols,
        update_always=update_always,
        update_if_null=update_ifnull,
        driver=driver
    )

    rc = 0
    with transaction(cn) as tx:
        for row in rows:
            ordered_values = [row[col] for col in columns]
            rc += tx.execute(sql, *ordered_values)

    if rc != len(rows):
        logger.debug(f'{len(rows) - rc} rows were skipped due to existing constraints')
    return rc


def _fetch_existing_rows(cn, table, rows, key_cols):
    """Fetch existing rows for a set of key values"""
    existing_rows = {}

    # Group rows by key columns to minimize database queries
    key_groups = {}
    for row in rows:
        row_key = tuple(row[key] for key in key_cols)
        key_groups.setdefault(row_key, True)

    # Build query to fetch all needed rows at once if possible
    if len(key_groups) < 100:  # Arbitrary limit to avoid huge queries
        # Build WHERE clause for fetching all needed rows at once
        where_conditions = []
        params = []

        for row_key in key_groups:
            condition_parts = []
            for i, key_col in enumerate(key_cols):
                condition_parts.append(f'{key_col} = %s')
                params.append(row_key[i])

            where_conditions.append(f"({' AND '.join(condition_parts)})")

        if where_conditions:
            where_clause = ' OR '.join(where_conditions)
            sql = f'SELECT * FROM {table} WHERE {where_clause}'

            # Fetch all matching rows at once
            result = select(cn, sql, *params)
            for row in result:
                row_key = tuple(row[key] for key in key_cols)
                existing_rows[row_key] = row
    else:
        # Too many keys, fetch rows individually
        for row in rows:
            key_values = [row[key] for key in key_cols]
            key_conditions = ' AND '.join([f'{key} = %s' for key in key_cols])
            existing_row = select_row_or_none(cn, f'SELECT * FROM {table} WHERE {key_conditions}', *key_values)

            if existing_row:
                row_key = tuple(row[key] for key in key_cols)
                existing_rows[row_key] = existing_row

    return existing_rows


def _upsert_sqlserver_with_nulls(cn, table, rows, columns, key_cols,
                                 update_always, update_ifnull):
    """Special handling for SQL Server with NULL-preserving updates"""
    logger.warning('SQL Server MERGE with NULL preservation uses a specialized approach')

    with transaction(cn) as tx:
        # First retrieve existing rows to handle COALESCE logic
        existing_rows = _fetch_existing_rows(cn, table, rows, key_cols)

        # Apply NULL-preservation logic
        for row in rows:
            row_key = tuple(row[key] for key in key_cols)
            existing_row = existing_rows.get(row_key)

            if existing_row:
                # Apply update_cols_ifnull logic manually
                for col in update_ifnull:
                    if not is_null(existing_row.get(col)):
                        # Keep existing non-NULL value
                        row[col] = existing_row.get(col)

        # Build and execute the MERGE statement
        sql = _build_upsert_sql(
            table=table,
            columns=columns,
            key_columns=key_cols,
            update_always=update_always + update_ifnull,  # Handle all columns the same now
            update_if_null=[],  # No special NULL handling needed anymore
            driver='sqlserver'
        )

        ordered_values = []
        for row in rows:
            ordered_values.extend([row[col] for col in columns])

        return tx.execute(sql, *ordered_values)


def upsert_rows(
    cn,
    table: str,
    rows: tuple[dict],
    update_cols_key: list = None,
    update_cols_always: list = None,
    update_cols_ifnull: list = None,
    reset_sequence: bool = False,
    **kw
):
    """
    Performs an UPSERT operation for multiple rows with configurable update behavior.

    Args:
        cn: Database connection
        table: Target table name
        rows: Rows to insert/update
        update_cols_key: Columns that form the conflict constraint
        update_cols_always: Columns that should always be updated on conflict
        update_cols_ifnull: Columns that should only be updated if target is null
        reset_sequence: Whether to reset the table's sequence after operation
    """
    if not rows:
        logger.debug('Skipping upsert of empty rows')
        return 0

    # Get database driver type
    driver = _get_driver_type(cn)
    if not driver:
        raise ValueError('Unsupported database connection for upsert_rows')

    # SQLServer implementation notice
    if driver == 'sqlserver':
        logger.warning('SQL Server MERGE implementation is experimental and may have limitations')

    # Filter and validate rows and columns
    rows = _prepare_rows_for_upsert(cn, table, rows)
    if not rows:
        return 0

    columns = tuple(rows[0].keys())

    # Get primary keys if not specified
    update_cols_key = update_cols_key or get_table_primary_keys(cn, table, cn.options.drivername)
    if not update_cols_key:
        logger.warning(f'No primary keys found for {table}, falling back to INSERT')
        return insert_rows(cn, table, rows)

    # Filter update columns to only valid ones
    update_cols_always = _filter_update_columns(columns, update_cols_always, update_cols_key)
    update_cols_ifnull = _filter_update_columns(columns, update_cols_ifnull, update_cols_key)

    try:
        # Handle the specific database driver
        if driver == 'sqlserver' and update_cols_ifnull:
            rc = _upsert_sqlserver_with_nulls(cn, table, rows, columns,
                                              update_cols_key, update_cols_always,
                                              update_cols_ifnull)
        else:
            # Standard approach for PostgreSQL, SQLite and simple SQL Server cases
            rc = _execute_standard_upsert(cn, table, rows, columns,
                                          update_cols_key, update_cols_always,
                                          update_cols_ifnull, driver)
    finally:
        if reset_sequence:
            reset_table_sequence(cn, table)

    return rc


#
# SQL helpers
#

def insert_rows(cn, table, rows: tuple[dict]):
    """Insert multiple rows into a table
    """
    if not rows:
        logger.debug('Skipping insert of empty rows')
        return 0

    # Include only columns that exist in the table
    filtered_rows = filter_table_columns(cn, table, rows)
    if not filtered_rows:
        logger.warning(f'No valid columns found for {table} after filtering')
        return 0
    rows = tuple(filtered_rows)

    cols = tuple(rows[0].keys())
    vals = tuple(flatten([tuple(row.values()) for row in rows]))

    def genvals(cols, vals):
        this = ','.join(['%s']*len(cols))
        return ','.join([f'({this})']*int(len(vals)/len(cols)))

    sql = f'insert into {table} ({",".join(cols)}) values {genvals(cols, vals)}'
    return insert(cn, sql, *vals)


def insert_row(cn, table, fields, values):
    """Insert a row into a table using the supplied list of fields and values."""
    assert len(fields) == len(values), 'fields must be same length as values'
    return insert(cn, insert_row_sql(table, fields), *values)


def insert_row_sql(table, fields):
    """Generate the SQL to insert a row into a table using the supplied list
    of fields and values.
    """
    cols = ','.join(fields)
    vals = ','.join(['%s'] * len(fields))
    return f'insert into {table} ({cols}) values ({vals})'


def update_row(cn, table, keyfields, keyvalues, datafields, datavalues):
    """Update the specified datafields to the supplied datavalues in a table row
    identified by the keyfields and keyvalues.
    """
    assert len(keyfields) == len(keyvalues), 'keyfields must be same length as keyvalues'
    assert len(datafields) == len(datavalues), 'datafields must be same length as datavalues'
    values = tuple(datavalues) + tuple(keyvalues)
    return update(cn, update_row_sql(table, keyfields, datafields), *values)


def update_row_sql(table, keyfields, datafields):
    """Generate the SQL to update the specified datafields in a table row
    identified by the keyfields.
    """
    for kf in keyfields:
        assert kf not in datafields, f'keyfield {kf} cannot be in datafields'
    keycols = ' and '.join([f'{f}=%s' for f in keyfields])
    datacols = ','.join([f'{f}=%s' for f in datafields])
    return f'update {table} set {datacols} where {keycols}'


def find_sequence_column(cn, table):
    """Find the best column to reset sequence for.

    Intelligently determines the sequence column using these priorities:
    1. Columns that are both primary key and sequence columns
    2. Columns with 'id' in the name that are primary keys or sequence columns
    3. Any primary key or sequence column
    4. Default to 'id' as a last resort
    """
    sequence_cols = get_sequence_columns(cn, table)
    primary_keys = get_table_primary_keys(cn, table)

    # Find columns that are both PK and sequence columns
    pk_sequence_cols = [col for col in sequence_cols if col in primary_keys]

    if pk_sequence_cols:
        # Among PK sequence columns, prefer ones with 'id' in the name
        id_cols = [col for col in pk_sequence_cols if 'id' in col.lower()]
        if id_cols:
            return id_cols[0]
        return pk_sequence_cols[0]

    # If no PK sequence columns, try sequence columns
    if sequence_cols:
        # Among sequence columns, prefer ones with 'id' in the name
        id_cols = [col for col in sequence_cols if 'id' in col.lower()]
        if id_cols:
            return id_cols[0]
        return sequence_cols[0]

    # If no sequence columns, try primary keys
    if primary_keys:
        # Among primary keys, prefer ones with 'id' in the name
        id_cols = [col for col in primary_keys if 'id' in col.lower()]
        if id_cols:
            return id_cols[0]
        return primary_keys[0]

    # Default fallback
    return 'id'


def reset_table_sequence(cn, table, identity=None):
    # Auto-detect identity column if not provided
    if identity is None:
        identity = find_sequence_column(cn, table)

    if is_psycopg_connection(cn):
        sql = f"""
select
    setval(pg_get_serial_sequence('{table}', '{identity}'), coalesce(max({identity}),0)+1, false)
from
{table}
"""
    elif is_sqlite3_connection(cn):
        # SQLite doesn't need explicit sequence resetting
        return
    elif is_pymssql_connection(cn):
        # SQL Server IDENTITY reseed
        sql = f"""
DECLARE @max int;
SELECT @max = ISNULL(MAX({identity}), 0) FROM {table};
DBCC CHECKIDENT ('{table}', RESEED, @max);
"""
    else:
        logger.warning('Sequence reset not implemented for this database type')
        return

    if isinstance(cn, transaction):
        cn.execute(sql)
    else:
        execute(cn, sql)
    logger.debug(f'Reset sequence for {table=} using {identity=}')


def vacuum_table(cn, table):
    if is_psycopg_connection(cn):
        cn.connection.set_session(autocommit=True)
        execute(cn, f'vacuum (full, analyze) {table}')
        cn.connection.set_session(autocommit=False)
    else:
        logger.warning('Only postgres vacuum implemented')


def reindex_table(cn, table):
    if is_psycopg_connection(cn):
        cn.connection.set_session(autocommit=True)
        execute(cn, f'reindex table {table}')
        cn.connection.set_session(autocommit=False)
    else:
        logger.warning('Only postgres reindex implemented')


def cluster_table(cn, table, index: str = None):
    if is_psycopg_connection(cn):
        cn.connection.set_session(autocommit=True)
        if index is None:
            execute(cn, f'cluster {table}')
        else:
            execute(cn, f'cluster {table} using {index}')
        cn.connection.set_session(autocommit=False)
    else:
        logger.warning('Only postgres cluster implemented')


def get_table_columns(cn, table):
    """Get all column names for a table based on database type"""
    if is_psycopg_connection(cn):
        sql = f"""
select skeys(hstore(null::{table})) as column
    """
    elif is_sqlite3_connection(cn):
        sql = f"""
select name as column from pragma_table_info('{table}')
    """
    elif is_pymssql_connection(cn):
        sql = f"""
select c.name as column
from sys.columns c
join sys.tables t on c.object_id = t.object_id
where t.name = '{table}'
    """
    else:
        raise ValueError('Unsupported database type for get_table_columns')

    cols = select_column(cn, sql)
    return cols


def filter_table_columns(cn, table, row_dicts):
    """Filter dictionaries to only include valid columns for the table
    """
    if not row_dicts:
        return []

    # Get table columns (case insensitive comparison)
    table_cols = {c.lower() for c in get_table_columns(cn, table)}

    # Create new filtered dictionaries
    filtered_rows = []

    for row in row_dicts:
        filtered_row = {}
        for col, val in row.items():
            if col.lower() in table_cols:
                filtered_row[col] = val
            else:
                logger.debug(f'Removed column {col} not in {table}')
        filtered_rows.append(filtered_row)

    return filtered_rows


def ignore_first_argument_cache_key(cls, *args, **kwargs):
    return cachetools.keys.hashkey(*args, **kwargs)


@cachetools.cached(cache=cachetools.TTLCache(maxsize=10, ttl=60), key=ignore_first_argument_cache_key)
def get_table_primary_keys(cn, table, _=None):
    """Extra parameter for database switching. Pass in flag to bypass cache.
    """
    if cn.options.drivername == 'postgres':
        sql = """
select a.attname as column
from pg_index i
join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
where i.indrelid = %s::regclass and i.indisprimary
"""
    if cn.options.drivername == 'sqlite':
        sql = """
select l.name as column from pragma_table_info("%s") as l where l.pk <> 0
"""
    if cn.options.drivername == 'sqlserver':
        sql = """
SELECT c.name as column
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.is_primary_key = 1
AND OBJECT_NAME(i.object_id) = %s
"""
    cols = select_column(cn, sql, table)
    return cols


def get_sequence_columns(cn, table):
    """Identify columns that are likely to be sequence/identity columns based on database type.
    """
    if cn.options.drivername == 'postgres':
        # Find columns with sequences
        sql = """
        SELECT column_name as column
        FROM information_schema.columns
        WHERE table_name = %s
        AND column_default LIKE 'nextval%%'
        """
        return select_column(cn, sql, table)
    elif cn.options.drivername == 'sqlite':
        # Find autoincrement columns
        sql = """
        SELECT name as column
        FROM pragma_table_info('%s')
        WHERE pk = 1
        """
        return select_column(cn, sql, table)
    elif cn.options.drivername == 'sqlserver':
        # Find identity columns
        sql = """
        SELECT c.name as column
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        WHERE t.name = %s AND c.is_identity = 1
        """
        return select_column(cn, sql, table)
    return []


def table_fields(cn, table):
    """Get all column names for a table ordered by their position"""
    if is_psycopg_connection(cn):
        flds = select_column(cn, """
select
t.column_name
from information_schema.columns t
where
t.table_name = %s
order by
t.ordinal_position
""", table)
    elif is_sqlite3_connection(cn):
        flds = select_column(cn, """
select name from pragma_table_info('%s')
order by cid
""", table)
    elif is_pymssql_connection(cn):
        flds = select_column(cn, """
select c.name
from sys.columns c
join sys.tables t on c.object_id = t.object_id
where t.name = %s
order by c.column_id
""", table)
    else:
        raise ValueError('Unsupported database type for table_fields')

    return flds


def table_data(cn, table, columns=[]):
    """Get table data by columns
    """
    if not columns:
        if is_psycopg_connection(cn):
            columns = select_column(cn, """
select
t.column_name
from information_schema.columns t
where
t.table_name = %s
and t.data_type in ('character', 'character varying', 'boolean',
    'text', 'double precision', 'real', 'integer', 'date',
    'time without time zone', 'timestamp without time zone')
order by
t.ordinal_position
""", table)
        elif is_sqlite3_connection(cn):
            columns = select_column(cn, """
SELECT name FROM pragma_table_info('%s')
""", table)
        elif is_pymssql_connection(cn):
            columns = select_column(cn, """
SELECT c.name
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.types ty ON c.system_type_id = ty.system_type_id
WHERE t.name = %s
AND ty.name IN ('char', 'varchar', 'nvarchar', 'text', 'ntext', 'bit',
    'tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric',
    'float', 'real', 'date', 'time', 'datetime', 'datetime2')
ORDER BY c.column_id
""", table)
        else:
            raise ValueError('Unsupported database type for table_data')

    columns = [f'{col} as {alias}' for col, alias in peel(columns)]
    return select(cn, f"select {','.join(columns)} from {table}")
