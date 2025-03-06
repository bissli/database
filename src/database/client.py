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
from database.adapter import DatabaseRowAdapter
from database.adapters import TypeConverter, register_adapters
from database.options import DatabaseOptions, iterdict_data_loader
from database.strategy import get_db_strategy
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
    'cluster_table',
    'IntegrityError',
    'ProgrammingError',
    'OperationalError',
    'UniqueViolation',
    'DbConnectionError',
    ]

register_adapters()


# == database type mappings

# PostgreSQL type mapping
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

# SQLite type mappings
sqlite_types = {
    'INTEGER': int,
    'REAL': float,
    'TEXT': str,
    'BLOB': bytes,
    'NUMERIC': float,
    'BOOLEAN': bool,
    'DATE': datetime.date,
    'DATETIME': datetime.datetime,
    'TIME': datetime.time,
}

# SQL Server type mappings
mssql_types = {
    'int': int,
    'bigint': int,
    'smallint': int,
    'tinyint': int,
    'bit': bool,
    'decimal': float,
    'numeric': float,
    'money': float,
    'smallmoney': float,
    'float': float,
    'real': float,
    'datetime': datetime.datetime,
    'datetime2': datetime.datetime,
    'smalldatetime': datetime.datetime,
    'date': datetime.date,
    'time': datetime.time,
    'datetimeoffset': datetime.datetime,
    'char': str,
    'varchar': str,
    'nchar': str,
    'nvarchar': str,
    'text': str,
    'ntext': str,
    'binary': bytes,
    'varbinary': bytes,
    'image': bytes,
    'uniqueidentifier': str,
    'xml': str,
}


# == custom exceptions

class DatabaseError(Exception):
    """Base class for all database module errors"""


class ConnectionError(DatabaseError):
    """Error establishing or maintaining database connection"""


class QueryError(DatabaseError):
    """Error in query syntax or execution"""


class TypeConversionError(DatabaseError):
    """Error converting types between Python and database"""


class IntegrityViolationError(DatabaseError):
    """Database constraint violation error"""


# == defined errors (for compatibility)

DbConnectionError = (
    psycopg.OperationalError,    # Connection/timeout issues
    psycopg.InterfaceError,      # Connection interface issues
    pymssql.OperationalError,    # SQL Server connection issues
    pymssql.InterfaceError,      # SQL Server interface issues
    sqlite3.OperationalError,    # SQLite connection issues
    sqlite3.InterfaceError,      # SQLite interface issues
    ConnectionError,             # Our custom exception
)
IntegrityError = (
    psycopg.IntegrityError,      # Postgres constraint violations
    pymssql.IntegrityError,      # SQL Server constraint violations
    sqlite3.IntegrityError,      # SQLite constraint violations
    IntegrityViolationError,     # Our custom exception
)
ProgrammingError = (
    psycopg.ProgrammingError,    # Postgres syntax/query errors
    psycopg.DatabaseError,       # Postgres general database errors
    pymssql.ProgrammingError,    # SQL Server syntax/query errors
    pymssql.DatabaseError,       # SQL Server general database errors
    sqlite3.ProgrammingError,    # SQLite syntax/query errors
    sqlite3.DatabaseError,       # SQLite general database errors
    QueryError,                  # Our custom exception
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
    IntegrityViolationError,           # Our custom exception
)


def check_connection(func=None, *, max_retries=3, retry_delay=1,
                     retry_errors=DbConnectionError, retry_backoff=1.5):
    """Enhanced connection retry decorator with backoff

    Args:
        func: Function to decorate
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries (seconds)
        retry_errors: Errors that should trigger a retry
        retry_backoff: Multiplier for delay after each retry
    """
    def decorator(f):
        @wraps(f)
        def inner(*args, **kwargs):
            tries = 0
            delay = retry_delay
            while tries < max_retries:
                try:
                    return f(*args, **kwargs)
                except retry_errors as err:
                    tries += 1
                    if tries >= max_retries:
                        logger.error(f'Maximum retries ({max_retries}) exceeded: {err}')
                        raise

                    logger.warning(f'Connection error (attempt {tries}/{max_retries}): {err}')
                    conn = args[0]
                    if hasattr(conn, 'options') and conn.options.check_connection:
                        try:
                            conn.connection.close()
                        except:
                            pass  # Ignore errors when closing broken connection

                        # Reconnect
                        conn.connection = connect(conn.options).connection

                    # Wait before retry with exponential backoff
                    time.sleep(delay)
                    delay *= retry_backoff

        return inner

    # Allow both @check_connection and @check_connection() syntax
    if func is None:
        return decorator
    return decorator(func)


def standardize_placeholders(cn, sql):
    """Standardize SQL placeholders between ? and %s based on database type

    Args:
        cn: Database connection
        sql: SQL query string with placeholders

    Returns
        SQL query with standardized placeholders
    """
    # Standardize placeholders based on connection type
    if is_psycopg_connection(cn) or is_pymssql_connection(cn):
        # Only replace ? that are actual parameter placeholders
        # This pattern finds ? that aren't part of regex operators or other contexts
        # Look for ? that:
        # - is surrounded by spaces, or
        # - is at the beginning of a clause, or
        # - is after a comma or parenthesis, or
        # - is at the end of a string, clause, or parenthesis
        sql = re.sub(r'(\s\?\s|\(\?\)|\s\?$|\s\?\)|\s\?,|\(\?,)',
                     lambda m: m.group().replace('?', '%s'),
                     ' ' + sql + ' ')
        # Remove the padding we added
        sql = sql.strip()
    elif is_sqlite3_connection(cn):
        # Same careful replacement for SQLite, converting %s to ?
        sql = re.sub(r'(\s%s\s|\(%s\)|\s%s$|\s%s\)|\s%s,|\(%s,)',
                     lambda m: m.group().replace('%s', '?'),
                     ' ' + sql + ' ')
        sql = sql.strip()

    return sql


def handle_in_clause_params(sql, args):
    """
    Expand list/tuple parameters for IN clauses across different database drivers

    For SQL like: "WHERE x IN %s" and args like [('A', 'B', 'C')]
    converts to: "WHERE x IN (%s, %s, %s)" and args ['A', 'B', 'C']
    """
    if not args or not sql or ' in ' not in sql.lower():
        return sql, args

    modified_sql = sql
    modified_args = list(args)

    # Find "IN %s" patterns in SQL
    pattern = re.compile(r'\bIN\s+(%s)\b', re.IGNORECASE)
    matches = list(pattern.finditer(sql))

    # Process matches in reverse to avoid position shifts
    for match in reversed(matches):
        placeholder_pos = match.start(1)
        param_index = sql[:placeholder_pos].count('%s')

        if param_index < len(args):
            param = args[param_index]

            # Check if parameter is a sequence (but not string/bytes)
            if isinstance(param, Sequence) and not isinstance(param, str | bytes):
                if not param:  # Empty sequence
                    # Replace with something that will always be false
                    repl = 'in (null)'
                    modified_sql = modified_sql[:match.start(0)] + repl + modified_sql[match.end(0):]
                    # Remove this parameter
                    modified_args.pop(param_index)
                else:
                    # Create the correct number of placeholders
                    placeholders = ', '.join(['%s'] * len(param))
                    repl = f'in ({placeholders})'
                    modified_sql = modified_sql[:match.start(0)] + repl + modified_sql[match.end(0):]

                    # Remove the original param and insert expanded values
                    modified_args.pop(param_index)
                    for i, val in enumerate(param):
                        modified_args.insert(param_index + i, val)

    return modified_sql, tuple(modified_args)


def handle_query_params(func):
    """Decorator that standardizes SQL parameter handling:
    - Converts placeholders between ? and %s based on database type
    - Handles IN clause parameters with lists/tuples
    """
    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        # Standardize placeholders
        sql = standardize_placeholders(cn, sql)

        if args:
            # Expand IN clause parameters
            sql, args = handle_in_clause_params(sql, args)

            # Convert parameters for pymssql if needed
            if is_pymssql_connection(cn):
                args = TypeConverter.convert_params(args)

        # No parameters provided
        if not args:
            return func(cn, sql, **kwargs)

        return func(cn, sql, *args, **kwargs)

    return wrapper

# == main


def is_psycopg_connection(obj):
    """Check if object is a psycopg connection or wrapper containing one.
    """
    if isinstance(obj, ConnectionWrapper) or (hasattr(obj, 'connection') and not isinstance(obj, transaction)):
        obj = obj.connection

    if isinstance(obj, transaction):
        obj = obj.connection.connection if hasattr(obj.connection, 'connection') else obj.connection

    if isinstance(obj, psycopg.Connection):
        return True

    return bool(hasattr(obj, '_spec_class') and obj._spec_class == psycopg.Connection)


def is_pymssql_connection(obj):
    """Check if object is a pymssql connection or wrapper containing one.
    """
    if isinstance(obj, ConnectionWrapper) or (hasattr(obj, 'connection') and not isinstance(obj, transaction)):
        obj = obj.connection

    if isinstance(obj, transaction):
        obj = obj.connection.connection if hasattr(obj.connection, 'connection') else obj.connection

    if isinstance(obj, pymssql.Connection):
        return True

    return bool(hasattr(obj, '_spec_class') and obj._spec_class == pymssql.Connection)


def is_sqlite3_connection(obj):
    """Check if object is a sqlite3 connection or wrapper containing one.
    """
    if isinstance(obj, ConnectionWrapper) or (hasattr(obj, 'connection') and not isinstance(obj, transaction)):
        obj = obj.connection

    if isinstance(obj, transaction):
        obj = obj.connection.connection if hasattr(obj.connection, 'connection') else obj.connection

    if isinstance(obj, sqlite3.Connection):
        return True

    return bool(hasattr(obj, '_spec_class') and obj._spec_class == sqlite3.Connection)


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


def sanitize_sql_for_logging(sql, args=None):
    """Remove sensitive information from SQL for logging

    Args:
        sql: SQL query string
        args: Query parameters

    Returns
        Sanitized SQL query and parameters
    """
    # List of keywords indicating potentially sensitive columns
    sensitive_patterns = [
        r'\b(password|passwd|secret|key|token|credential)\b',
        r'\bcredit_?card\b',
        r'\bcard_?number\b',
        r'\bssn\b',
        r'\bsocial_?security\b'
    ]

    # Copy the SQL for sanitization
    sanitized_sql = sql

    # Define sanitization helper function inside the main function
    def _sanitize_values_clause(match, sensitive_pattern):
        values_start = match.group(1)
        values_content = match.group(2)
        values_end = match.group(3)

        # Find column names from INSERT INTO ... (col1, col2, ...) part
        full_sql = sanitized_sql
        columns_match = re.search(r'INSERT\s+INTO\s+\w+\s*\(([^)]+)\)', full_sql, re.IGNORECASE)

        if columns_match:
            columns = [col.strip() for col in columns_match.group(1).split(',')]
            values = [val.strip() for val in values_content.split(',')]

            # If we have correct number of columns and values
            if len(columns) == len(values):
                for i, col in enumerate(columns):
                    # Check if column matches sensitive pattern
                    if re.search(sensitive_pattern, col, re.IGNORECASE) and i < len(values):
                        values[i] = "'***'"

                return values_start + ', '.join(values) + values_end

        return match.group(0)

    # Mask sensitive data in the SQL itself
    for pattern in sensitive_patterns:
        # Find values in INSERT statements - handles VALUES ('value1', 'sensitive_value')
        insert_pattern = r'(VALUES\s*\()([^)]+)(\))'
        sanitized_sql = re.sub(
            insert_pattern,
            lambda m: _sanitize_values_clause(m, pattern),
            sanitized_sql,
            flags=re.IGNORECASE
        )

        # Find value sections that match our sensitive patterns for other statements
        matches = re.finditer(pattern, sanitized_sql, re.IGNORECASE)
        for match in matches:
            # Look for patterns like "password = '...'" or "password = %s"
            value_pattern = rf"{match.group(0)}\s*=\s*('[^']*'|\$[^$]*\$|%s|:[a-zA-Z0-9_]+)"

            # Replace values with ***
            sanitized_sql = re.sub(
                value_pattern,
                lambda m: m.group(0).replace(m.group(1), "'***'"),
                sanitized_sql
            )

    def _sanitize_values_clause(match, sensitive_pattern):
        """Helper function to sanitize VALUES clause
        """
        values_start = match.group(1)
        values_content = match.group(2)
        values_end = match.group(3)

        # Find column names from INSERT INTO ... (col1, col2, ...) part
        full_sql = sanitized_sql
        columns_match = re.search(r'INSERT\s+INTO\s+\w+\s*\(([^)]+)\)', full_sql, re.IGNORECASE)

        if columns_match:
            columns = [col.strip() for col in columns_match.group(1).split(',')]
            values = [val.strip() for val in values_content.split(',')]

            # If we have correct number of columns and values
            if len(columns) == len(values):
                for i, col in enumerate(columns):
                    # Check if column matches sensitive pattern
                    if re.search(sensitive_pattern, col, re.IGNORECASE) and i < len(values):
                        values[i] = "'***'"

                return values_start + ', '.join(values) + values_end

        return match.group(0)

    # Sanitize arguments if provided
    sanitized_args = None
    if args:
        if isinstance(args, list | tuple):
            sanitized_args = list(args)
            # Find parameter positions that might be sensitive
            for pattern in sensitive_patterns:
                # For each column in INSERT statements
                columns_match = re.search(r'INSERT\s+INTO\s+\w+\s*\(([^)]+)\)', sql, re.IGNORECASE)
                if columns_match:
                    columns = [col.strip() for col in columns_match.group(1).split(',')]
                    # Find any columns that match sensitive patterns
                    for i, col in enumerate(columns):
                        if re.search(pattern, col, re.IGNORECASE) and i < len(sanitized_args):
                            if sanitized_args[i] is not None:
                                sanitized_args[i] = '***'

                # For standard parameter positions
                param_positions = []
                for match in re.finditer(pattern + r'\s*=\s*(%s)', sql, re.IGNORECASE):
                    # Count placeholders before this one
                    sql_before = sql[:match.start()]
                    param_count = sql_before.count('%s')
                    if param_count < len(sanitized_args):
                        param_positions.append(param_count)

                # Mask sensitive parameters
                for pos in param_positions:
                    if sanitized_args[pos] is not None:
                        sanitized_args[pos] = '***'
        elif isinstance(args, dict):
            # Handle dictionary parameters
            sanitized_args = args.copy()
            for key in sanitized_args:
                for pattern in sensitive_patterns:
                    if re.search(pattern, key, re.IGNORECASE):
                        if sanitized_args[key] is not None:
                            sanitized_args[key] = '***'

    return sanitized_sql, sanitized_args


def dumpsql(func):
    """This is a decorator for db module functions, for logging data flowing down to driver"""
    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        try:
            # For postgres, logging happens in LoggingCursor
            if is_pymssql_connection(cn) or is_sqlite3_connection(cn):
                sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql, args)
                logger.debug(f'SQL:\n{sanitized_sql}\nargs: {sanitized_args}')
            return func(cn, sql, *args, **kwargs)
        except:
            sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql, args)
            logger.error(f'Error with query:\nSQL:\n{sanitized_sql}\nargs: {sanitized_args}')
            raise
    return wrapper


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
class ConnectionPool:
    """Simple database connection pool implementation"""

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
        self._lock = __import__('threading').RLock()

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
            # We can't forcibly close connections in use


@load_options(cls=DatabaseOptions)
def connect(
    options: str | dict | DatabaseOptions | None,
    config=None,
    use_pool=False,
    pool_max_connections=5,
    pool_max_idle_time=300,
    isolated_adapters=False,
    **kw
):
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
    adapter_maps = register_adapters(isolated=isolated_adapters) if isolated_adapters else None

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
    strategy = get_db_strategy(conn)
    strategy.configure_connection(conn)

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
    """Execute a SELECT query

    Args:
        cn: Database connection
        sql: SQL query string
        *args: Query parameters
        **kwargs: Additional options passed to data loader

    Returns
        Result data as a pandas DataFrame
    """
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
    # Extract column names based on database type
    if is_psycopg_connection(cursor.connwrapper):
        cols = [c.name for c in (cursor.description or [])]
    elif is_pymssql_connection(cursor.connwrapper) or is_sqlite3_connection(cursor.connwrapper):
        cols = [c[0] for c in (cursor.description or [])]
    data = cursor.fetchall()  # Get raw data

    # Convert all database-specific row types to a consistent format
    adapted_data = []
    for row in data:
        adapter = DatabaseRowAdapter.create(cursor.connwrapper, row)
        adapted_data.append(adapter.to_dict())
    data = adapted_data

    # Use the configured data loader
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
    return [DatabaseRowAdapter.create(cn, row).get_value() for row in data]


def select_column_unique(cn, sql, *args) -> set:
    return set(select_column(cn, sql, *args))


@use_iterdict_data_loader
def select_row(cn, sql, *args) -> attrdict:
    data = select(cn, sql, *args)
    assert len(data) == 1, 'Expected one row, got %d' % len(data)
    return DatabaseRowAdapter.create(cn, data[0]).to_attrdict()


@use_iterdict_data_loader
def select_row_or_none(cn, sql, *args) -> attrdict | None:
    data = select(cn, sql, *args)
    if len(data) == 1:
        return DatabaseRowAdapter.create(cn, data[0]).to_attrdict()
    return None


@use_iterdict_data_loader
def select_scalar(cn, sql, *args):
    data = select(cn, sql, *args)
    assert len(data) == 1, 'Expected one col, got %d' % len(data)
    return DatabaseRowAdapter.create(cn, data[0]).get_value()


def select_scalar_or_none(cn, sql, *args):
    try:
        val = select_scalar(cn, sql, *args)
        if not is_null(val):
            return val
        return None
    except AssertionError:
        return None


def execute_with_context(cn, sql, *args, **kwargs):
    """Execute SQL with enhanced error context"""
    try:
        return execute(cn, sql, *args, **kwargs)
    except ProgrammingError as e:
        # Capture original exception for context
        raise QueryError(f'Query execution failed: {e}\nSQL: {sql}') from e
    except DbConnectionError as e:
        raise ConnectionError(f'Connection failed during query: {e}') from e


@check_connection
@handle_query_params
@dumpsql
def execute(cn, sql, *args):
    cursor = cn.cursor()
    try:
        cursor.execute(sql, args)
        rowcount = cursor.rowcount
        cn.commit()
        return rowcount
    except Exception as e:
        try:
            cn.rollback()
        except Exception:
            pass
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
    cn,
    table: str,
    columns: tuple[str],
    key_columns: list[str],
    update_always: list[str] = None,
    update_if_null: list[str] = None,
    driver: str = 'postgres',
) -> str:
    """Builds an UPSERT SQL statement for the specified database driver.

    Args:
        cn: Database connection
        table: Target table name
        columns: All columns to insert
        key_columns: Columns that form conflict constraint (primary/unique keys)
        update_always: Columns that should always be updated on conflict
        update_if_null: Columns that should only be updated if target is null
        driver: Database driver ('postgres', 'sqlite', 'sqlserver')
    """
    # Validate inputs
    if not key_columns:
        return _build_insert_sql(cn, table, columns)

    # Quote table name and all column names
    quoted_table = quote_identifier(cn, table)
    quoted_columns = [quote_identifier(cn, col) for col in columns]
    quoted_key_columns = [quote_identifier(cn, col) for col in key_columns]

    # PostgreSQL uses INSERT ... ON CONFLICT DO UPDATE
    if driver == 'postgres':
        # build the basic insert part
        insert_sql = _build_insert_sql(cn, table, columns)

        # build the on conflict clause
        conflict_sql = f"on conflict ({','.join(quoted_key_columns)})"

        # If no updates requested, do nothing
        if not (update_always or update_if_null):
            return f'{insert_sql} {conflict_sql} do nothing'

        # Build update expressions
        update_exprs = []
        if update_always:
            update_exprs.extend(f'{quote_identifier(cn, col)} = excluded.{quote_identifier(cn, col)}'
                                for col in update_always)
        if update_if_null:
            update_exprs.extend(f'{quote_identifier(cn, col)} = coalesce({quoted_table}.{quote_identifier(cn, col)}, excluded.{quote_identifier(cn, col)})'
                                for col in update_if_null)

        update_sql = f"do update set {', '.join(update_exprs)}"

        return f'{insert_sql} {conflict_sql} {update_sql}'

    # SQLite uses INSERT ... ON CONFLICT DO UPDATE (similar to PostgreSQL)
    elif driver == 'sqlite':
        # build the basic insert part
        insert_sql = _build_insert_sql(cn, table, columns)

        # build the on conflict clause
        conflict_sql = f"on conflict ({','.join(quoted_key_columns)})"

        # If no updates requested, do nothing
        if not (update_always or update_if_null):
            return f'{insert_sql} {conflict_sql} do nothing'

        # Build update expressions
        update_exprs = []
        if update_always:
            update_exprs.extend(f'{quote_identifier(cn, col)} = excluded.{quote_identifier(cn, col)}'
                                for col in update_always)
        if update_if_null:
            update_exprs.extend(f'{quote_identifier(cn, col)} = COALESCE({quoted_table}.{quote_identifier(cn, col)}, excluded.{quote_identifier(cn, col)})'
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
            f'target.{quote_identifier(cn, col)} = {temp_alias}.{quote_identifier(cn, col)}' for col in key_columns
        )

        # Build column value list for source
        source_values = ', '.join(f'%s as {quote_identifier(cn, col)}' for col in columns)

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
                f'target.{quote_identifier(cn, col)} = {temp_alias}.{quote_identifier(cn, col)}' for col in update_cols
            )
            update_clause = f'WHEN MATCHED THEN UPDATE SET {update_statements}'
        else:
            # If no updates but we have keys, just match without updating
            update_clause = 'WHEN MATCHED THEN DO NOTHING'

        # Build INSERT statements
        quoted_all_columns = ', '.join(quoted_columns)
        source_columns = ', '.join(f'{temp_alias}.{quote_identifier(cn, col)}' for col in columns)

        # Full MERGE statement
        merge_sql = f"""
        MERGE INTO {quoted_table} AS target
        USING (SELECT {source_values}) AS {temp_alias}
        ON {match_conditions}
        {update_clause}
        WHEN NOT MATCHED THEN INSERT ({quoted_all_columns}) VALUES ({source_columns});
        """

        return merge_sql

    else:
        raise ValueError(f'Driver {driver} not supported for UPSERT operations')


def _build_insert_sql(cn, table: str, columns: tuple[str]) -> str:
    """Builds the INSERT part of the SQL statement
    """
    placeholders = ','.join(['%s'] * len(columns))
    quoted_table = quote_identifier(cn, table)
    quoted_columns = ','.join(quote_identifier(cn, col) for col in columns)
    return f'insert into {quoted_table} ({quoted_columns}) values ({placeholders})'


def quote_identifier(cn, identifier):
    """Safely quote database identifiers based on connection type

    Args:
        cn: Database connection
        identifier: The identifier (table or column name) to quote

    Returns
        Properly quoted identifier safe for use in SQL
    """
    if is_psycopg_connection(cn):
        return '"' + identifier.replace('"', '""') + '"'
    elif is_sqlite3_connection(cn):
        return '"' + identifier.replace('"', '""') + '"'
    elif is_pymssql_connection(cn):
        return '[' + identifier.replace(']', ']]') + ']'
    else:
        raise ValueError(f'Unknown connection type: {type(cn)}')


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
        cn=cn,
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


def _fetch_existing_rows(tx_or_cn, table, rows, key_cols):
    """Fetch existing rows for a set of key values"""
    existing_rows = {}
    quoted_table = quote_identifier(tx_or_cn, table)

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
                quoted_key_col = quote_identifier(tx_or_cn, key_col)
                condition_parts.append(f'{quoted_key_col} = %s')
                params.append(row_key[i])

            where_conditions.append(f"({' AND '.join(condition_parts)})")

        if where_conditions:
            where_clause = ' OR '.join(where_conditions)
            sql = f'SELECT * FROM {quoted_table} WHERE {where_clause}'

            # Fetch all matching rows at once
            result = select(tx_or_cn, sql, *params)
            for row in result:
                adapter = DatabaseRowAdapter.create(tx_or_cn, row)
                row_key = tuple(adapter.get_value(key) for key in key_cols)
                existing_rows[row_key] = adapter.to_dict()
    else:
        # Too many keys, fetch rows individually
        for row in rows:
            key_values = [row[key] for key in key_cols]
            key_conditions = ' AND '.join([f'{quote_identifier(tx_or_cn, key)} = %s' for key in key_cols])
            existing_row = select_row_or_none(tx_or_cn, f'SELECT * FROM {quoted_table} WHERE {key_conditions}', *key_values)

            if existing_row:
                row_key = tuple(row[key] for key in key_cols)
                existing_rows[row_key] = existing_row

    return existing_rows


def _upsert_sqlserver_with_nulls(cn, table, rows, columns, key_cols,
                                 update_always, update_ifnull):
    """Special handling for SQL Server with NULL-preserving updates"""
    logger.warning('SQL Server MERGE with NULL preservation uses a specialized approach')

    driver = _get_driver_type(cn)

    with transaction(cn) as tx:
        # First retrieve existing rows to handle COALESCE logic, passing the transaction
        existing_rows = _fetch_existing_rows(tx, table, rows, key_cols)

        # Apply NULL-preservation logic
        for row in rows:
            row_key = tuple(row[key] for key in key_cols)
            existing_row = existing_rows.get(row_key)

            if existing_row:
                # Apply update_cols_ifnull logic manually
                for col in update_ifnull:
                    if col in existing_row and not is_null(existing_row.get(col)):
                        # Keep existing non-NULL value
                        row[col] = existing_row.get(col)

        # Build and execute the MERGE statement
        sql = _build_upsert_sql(
            cn=tx,
            table=table,
            columns=columns,
            key_columns=key_cols,
            update_always=update_always,  # Only handle "always update" columns
            update_if_null=[],  # No special NULL handling needed anymore since we modified the rows
            driver=driver
        )

        # For each row, convert values to ordered list for SQL parameters
        row_count = 0
        for row in rows:
            ordered_values = [row[col] for col in columns]
            row_count += tx.execute(sql, *ordered_values)

        return row_count


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
            # For SQL Server with NULL preservation, use specialized function
            return _upsert_sqlserver_with_nulls(cn, table, rows, columns,
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

    quoted_table = quote_identifier(cn, table)
    quoted_cols = ','.join(quote_identifier(cn, col) for col in cols)

    sql = f'insert into {quoted_table} ({quoted_cols}) values {genvals(cols, vals)}'
    return insert(cn, sql, *vals)


def insert_row(cn, table, fields, values):
    """Insert a row into a table using the supplied list of fields and values."""
    assert len(fields) == len(values), 'fields must be same length as values'
    quoted_table = quote_identifier(cn, table)
    quoted_columns = ', '.join(quote_identifier(cn, col) for col in fields)
    placeholders = ', '.join(['%s'] * len(fields))
    sql = f'insert into {quoted_table} ({quoted_columns}) values ({placeholders})'
    return insert(cn, sql, *values)


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
    """Reset a table's sequence/identity column to the max value + 1

    Args:
        cn: Database connection
        table: Table name
        identity: Identity column name (auto-detected if None)
    """
    strategy = get_db_strategy(cn)
    strategy.reset_sequence(cn, table, identity)


def vacuum_table(cn, table):
    """Optimize a table by reclaiming space

    This operation varies by database type:
    - PostgreSQL: VACUUM (FULL, ANALYZE)
    - SQLite: VACUUM (entire database)
    - SQL Server: Rebuilds all indexes
    """
    strategy = get_db_strategy(cn)
    strategy.vacuum_table(cn, table)


def reindex_table(cn, table):
    """Rebuild indexes for a table

    This operation varies by database type:
    - PostgreSQL: REINDEX TABLE
    - SQLite: REINDEX
    - SQL Server: ALTER INDEX ALL ... REBUILD
    """
    strategy = get_db_strategy(cn)
    strategy.reindex_table(cn, table)


def cluster_table(cn, table, index: str = None):
    """Order table data according to an index

    This operation is primarily for PostgreSQL:
    - PostgreSQL: CLUSTER table [USING index]
    - Other databases: Not supported (warning logged)
    """
    strategy = get_db_strategy(cn)
    strategy.cluster_table(cn, table, index)


def get_table_columns(cn, table):
    """Get all column names for a table based on database type"""
    quoted_table = quote_identifier(cn, table)

    if is_psycopg_connection(cn):
        sql = f"""
select skeys(hstore(null::{quoted_table})) as column
    """
    elif is_sqlite3_connection(cn):
        # SQLite pragma requires unquoted table name
        sql = f"""
select name as column from pragma_table_info('{table}')
    """
    elif is_pymssql_connection(cn):
        # SQL Server system queries use unquoted names for matching
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
    and correct column name casing to match database schema
    """
    if not row_dicts:
        return []

    # Get table columns with original case preserved
    table_cols = get_table_columns(cn, table)

    # Create case mapping dictionary (lowercase -> original case)
    case_map = {col.lower(): col for col in table_cols}

    # Create new filtered dictionaries with correct case
    filtered_rows = []
    # Track removed columns
    removed_columns = set()

    for row in row_dicts:
        filtered_row = {}
        for col, val in row.items():
            if col.lower() in case_map:
                # Use the correctly-cased column name from the database
                correct_col = case_map[col.lower()]
                filtered_row[correct_col] = val
            else:
                # Add to removed columns set
                removed_columns.add(col)
        filtered_rows.append(filtered_row)

    for col in removed_columns:
        logger.debug(f'Removed column {col} not in {table}')

    return filtered_rows


def ignore_first_argument_cache_key(cls, *args, **kwargs):
    return cachetools.keys.hashkey(*args, **kwargs)


@cachetools.cached(cache=cachetools.TTLCache(maxsize=10, ttl=60), key=ignore_first_argument_cache_key)
def get_table_primary_keys(cn, table, _=None):
    """Get primary key columns for a table

    Args:
        cn: Database connection
        table: Table name
        _: Extra parameter for database switching (to bypass cache)

    Returns
        List of primary key column names
    """
    strategy = get_db_strategy(cn)
    return strategy.get_primary_keys(cn, table)


def get_sequence_columns(cn, table):
    """Identify columns that are likely to be sequence/identity columns

    Args:
        cn: Database connection
        table: Table name

    Returns
        List of sequence/identity column names
    """
    strategy = get_db_strategy(cn)
    return strategy.get_sequence_columns(cn, table)


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
