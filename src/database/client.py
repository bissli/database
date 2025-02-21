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
from database.handler import handle_pg_error, QueryContext
from database.options import DatabaseOptions, iterdict_data_loader
from more_itertools import flatten
from psycopg import ClientCursor
from psycopg.postgres import types

from libb import attrdict, is_null, isiterable, load_options, peel

try:
    import psycopg2
except ImportError:
    psycopg2 = None

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
    'vacuum_table',
    'reindex_table',
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

DbConnectionError = (psycopg.OperationalError, psycopg.InterfaceError,
                     pymssql.OperationalError, sqlite3.InterfaceError)
DbIntegrityError = (psycopg.IntegrityError, pymssql.IntegrityError,
                    sqlite3.IntegrityError)
DbProgrammingError = (psycopg.ProgrammingError, pymssql.DatabaseError,
                      sqlite3.DatabaseError)
DbOperationalError = (psycopg.OperationalError, pymssql.OperationalError,
                      sqlite3.OperationalError)


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

# == main


def is_psycopg_connection(obj):
    """Check if object is a psycopg connection or wrapper containing one."""
    if hasattr(obj, 'connection'):
        obj = obj.connection
    return isinstance(obj, psycopg.Connection) or \
    (psycopg2 and isinstance(obj, psycopg2.extensions.connection))


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
        try:
            self.cursor.execute(sql, *args, **kwargs)
        except psycopg.Error as e:
            if is_psycopg_connection(self.connwrapper):
                error_info = handle_pg_error(e, QueryContext(sql=sql, args=args, kwargs=kwargs))
                raise type(e)(str(error_info)) from e
            raise
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
            if is_pymssql_connection(cn) or is_sqlite3_connection(cn):
                logger.debug(f'SQL:\n{sql}\nargs: {args}\nkwargs: {kwargs}')
            return func(cn, sql, *args, **kwargs)
        except:
            logger.error(f'Error with query:\nSQL:\n{sql}\nargs: {args}\nkwargs: {kwargs}')
            raise
    return wrapper


def placeholder(func):
    """Handle placeholder by connection type"""
    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        if is_psycopg_connection(cn):
            sql = sql.replace('?', '%s')
        if is_pymssql_connection(cn):
            sql = sql.replace('%s', '?')
        if is_sqlite3_connection(cn):
            sql = sql.replace('%s', '?')
        return func(cn, sql, *args, **kwargs)
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
    def execute(self, query, params=None, *args, **kwargs):
        formatted = self.mogrify(query, params)
        logger.debug('SQL:\n' + formatted)
        result = super().execute(query, params, *args, **kwargs)
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


@dumpsql
@check_connection
@placeholder
def select(cn, sql, *args, **kwargs) -> pd.DataFrame:
    cursor = _dict_cur(cn)  # cn is already a ConnectionWrapper
    cursor.execute(sql, args)
    return load_data(cursor, **kwargs)


@dumpsql
@placeholder
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


def load_data(cursor) -> pd.DataFrame:
    """Data loader callable (IE into DataFrame)
    """
    if is_psycopg_connection(cursor.connwrapper):
        cols = [c.name for c in (cursor.description or [])]
    if is_pymssql_connection(cursor.connwrapper) or is_sqlite3_connection(cursor.connwrapper):
        cols = [c[0] for c in (cursor.description or [])]
    data = cursor.fetchall()  # iterdict (dictcursor)
    data_loader = cursor.connwrapper.options.data_loader
    return data_loader(data, cols)


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


@dumpsql
@check_connection
@placeholder
def execute(cn, sql, *args, **kwargs):
    cursor = cn.cursor()
    if is_pymssql_connection(cn):
        args = TypeConverter.convert_params(args)
    try:
        cursor.execute(sql, args)
        rowcount = cursor.rowcount
        cn.commit()
        return rowcount
    except psycopg.Error as e:
        if is_psycopg_connection(cn):
            error_info = handle_pg_error(e, QueryContext(sql=sql, args=args, kwargs=kwargs))
            raise type(e)(str(error_info)) from e
        raise


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

    @dumpsql
    @check_connection
    @placeholder
    def execute(self, sql, *args, returnid=None):
        if is_pymssql_connection(self.connection):
            args = TypeConverter.convert_params(args)
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

    @dumpsql
    @placeholder
    def select(self, sql, *args, **kwargs) -> pd.DataFrame:
        cursor = self.cursor
        cursor.execute(sql, args)
        return load_data(cursor, **kwargs)


@dumpsql
@placeholder
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
    """One step database insert of iterdict
    """
    if not rows:
        logger.debug('Skipping upsert of empty rows')
        return 0

    cols = tuple(rows[0])  # assume consistency

    assert is_psycopg_connection(cn), '`upsert_rows` only supports postgres'

    if not update_cols_key:
        update_cols_key = get_table_primary_keys(cn, table, cn.options.drivername)

    if not update_cols_key:
        update_cols_always = update_cols_ifnull = update_cols = []
    else:
        update_cols_always = [c for c in (update_cols_always or []) if c in cols]
        update_cols_ifnull = [c for c in (update_cols_ifnull or []) if c in cols]
        update_cols_always = [c for c in update_cols_always if c not in update_cols_ifnull+update_cols_key]
        update_cols_ifnull = [c for c in update_cols_ifnull if c not in update_cols_always+update_cols_key]
        update_cols = update_cols_always+update_cols_ifnull

    if update_cols:
        coalesce=lambda t, c: \
            f'{c}=coalesce({t}.{c}, excluded.{c})' \
            if c in update_cols_ifnull \
            else f'{c}=excluded.{c}'
        conflict = f"""
on conflict ({','.join(update_cols_key)}
) do update set {','.join([coalesce(table, c) for c in update_cols])}
""".strip()
    else:
        conflict = 'on conflict do nothing'
    values = ','.join(['%s'] * len(cols))
    table_cols_sql = f"""
insert into {table} ({','.join(cols)})
values ({values})
{conflict}
    """.strip()
    rc = 0
    try:
        with transaction(cn) as tx:
            for row in rows:
                rc += tx.execute(table_cols_sql, *row.values())
    finally:
        if reset_sequence:
            id_name = kw.pop('id_name', 'id')
            reset_table_sequence(cn, table, id_name)

    if rc != len(rows):
        logger.debug(f'{len(rows) - rc} rows were skipped due to existing contraints')
    return rc


#
# SQL helpers
#

def insert_rows(cn, table, rows: tuple[dict]):
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


def reset_table_sequence(cn, table, identity='id'):
    sql = f"""
select
    setval(pg_get_serial_sequence('{table}', '{identity}'), coalesce(max({identity}),0)+1, false)
from
    {table}
    """
    if isinstance(cn, transaction):
        cn.execute(sql)
    else:
        execute(cn, sql)
    logger.debug(f'Reset sequence for {table=}')


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


# postgres


def get_table_columns(cn, table):
    sql = f"""
select skeys(hstore(null::{table})) as column
    """
    cols = select_column(cn, sql)
    return cols


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
    cols = select_column(cn, sql, table)
    return cols


def table_fields(cn, table):
    flds = select_column(cn, """
select
    t.column_name
from information_schema.columns t
where
    t.table_name = %s
order by
    t.ordinal_position
    """, table)
    return flds


def table_data(cn, table, columns=[]):
    """Get table data by columns
    """
    if not columns:
        columns = select_column(cn, """
select
    t.column_name
from information_schema.columns t
where
    t.table_name = %s
    and t.data_type in ('character', 'character varying', 'boolean',
        'text', 'double precision', 'real' 'integer', 'date',
        'time without time zone', 'timestamp without time zone')
order by
    t.ordinal_position
        """, table)
    columns = [f'{col} as {alias}' for col, alias in peel(columns)]
    return select(cn, f"select {','.join(columns)} from {table}")
