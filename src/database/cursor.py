"""
Database cursor implementations for PostgreSQL and SQLite.

Implements Python DB-API 2.0 specification (PEP-249).
"""
import logging
import re
import sqlite3
import time
from collections.abc import Iterator, Sequence
from functools import wraps
from numbers import Number
from typing import Any

from database.sql import has_placeholders
from database.strategy import get_db_strategy
from database.types import RowAdapter, TypeConverter
from database.types import columns_from_cursor_description, postgres_types
from database.utils import ensure_commit

from libb import collapse

logger = logging.getLogger(__name__)


def dumpsql(func):
    """Decorator for logging SQL queries and parameters."""
    @wraps(func)
    def wrapper(self, operation: str, *args: Any, **kwargs: Any):
        start = time.time()
        logger.debug(f'SQL:\n{operation}\nargs: {args}')
        try:
            result = func(self, operation, *args, **kwargs)
            if hasattr(self.dbapi_cursor, 'statusmessage'):
                logger.debug(f'Query result: {self.dbapi_cursor.statusmessage}')
            return result
        except Exception:
            logger.error(f'Error with query:\nSQL:\n{operation}\nargs: {args}')
            raise
        finally:
            elapsed = time.time() - start
            self.connwrapper.addcall(elapsed)
            logger.debug(f'Query time: {elapsed:.4f}s')
    return wrapper


def dumpsql_many(func):
    """Decorator for logging executemany operations."""
    @wraps(func)
    def wrapper(self, operation: str, seq_of_parameters: Sequence, *args: Any, **kwargs: Any):
        start = time.time()
        logger.debug(f'SQL:\n{operation}\nparams: {len(seq_of_parameters)} rows')
        try:
            result = func(self, operation, seq_of_parameters, *args, **kwargs)
            if hasattr(self.dbapi_cursor, 'statusmessage'):
                logger.debug(f'Executemany result: {self.dbapi_cursor.statusmessage}')
            return result
        except Exception:
            logger.error(f'Error with executemany:\nSQL:\n{operation}')
            raise
        finally:
            elapsed = time.time() - start
            self.connwrapper.addcall(elapsed)
            logger.debug(f'Executemany time: {elapsed:.4f}s')
    return wrapper


class Cursor:
    """Unified cursor class implementing DB-API 2.0 for all database types.

    Uses the strategy pattern to handle dialect-specific behaviors like
    placeholder conversion (%s vs ?) automatically.
    """

    def __init__(self, cursor: Any, connection_wrapper: Any, strategy: Any = None) -> None:
        """Initialize cursor wrapper.

        Args:
            cursor: The underlying database cursor
            connection_wrapper: The connection wrapper that created this cursor
            strategy: Optional database strategy (auto-detected from connection if not provided)
        """
        self.dbapi_cursor = cursor
        self.connwrapper = connection_wrapper
        self._strategy = strategy
        self._arraysize: int = 1
        self._original_sql: str = ''

    @property
    def strategy(self) -> Any:
        """Get the database strategy, lazily initializing if needed."""
        if self._strategy is None:
            self._strategy = get_db_strategy(self.connwrapper)
        return self._strategy

    def __getattr__(self, name: str) -> Any:
        """Delegate members to underlying cursor."""
        return getattr(self.dbapi_cursor, name)

    def __iter__(self) -> Iterator:
        """Return iterator for cursor results."""
        return IterChunk(self.dbapi_cursor)

    @property
    def description(self) -> list[tuple] | None:
        """Column descriptions for last query."""
        return self.dbapi_cursor.description

    @property
    def rowcount(self) -> int:
        """Number of rows produced/affected by last operation."""
        return self.dbapi_cursor.rowcount

    @property
    def arraysize(self) -> int:
        """Number of rows fetched by fetchmany()."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._arraysize = value

    def close(self) -> None:
        """Close cursor."""
        self.dbapi_cursor.close()

    def fetchone(self) -> tuple | None:
        """Fetch next row."""
        return self.dbapi_cursor.fetchone()

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        """Fetch next set of rows."""
        if size is None:
            size = self.arraysize
        return self.dbapi_cursor.fetchmany(size)

    def fetchall(self) -> list[tuple]:
        """Fetch all remaining rows."""
        return self.dbapi_cursor.fetchall()

    def setinputsizes(self, sizes: Sequence) -> None:
        """Predefine memory areas for parameters."""

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """Set column buffer size for large columns."""

    def nextset(self) -> bool | None:
        """Move to next result set.

        Returns None for databases that don't support multiple result sets.
        """
        if hasattr(self.dbapi_cursor, 'nextset'):
            return self.dbapi_cursor.nextset()
        return None

    @dumpsql
    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        """Execute a database operation."""
        auto_commit = kwargs.pop('auto_commit', True)

        operation = self.strategy.standardize_sql(operation)
        self._original_sql = operation

        if args:
            args = tuple(TypeConverter.convert_params(arg) for arg in args)

        self._execute_query(operation, args)

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.dbapi_cursor.rowcount

    def _execute_query(self, sql: str, args: tuple) -> None:
        """Execute query with parameter handling."""
        if args and not has_placeholders(sql):
            self.dbapi_cursor.execute(sql)
            logger.debug('Executed query without placeholders (ignoring args)')
            return

        for arg in collapse(args):
            if isinstance(arg, dict):
                if self._is_multi_statement(sql):
                    self._execute_multi_statement_named(sql, arg)
                else:
                    self.dbapi_cursor.execute(sql, arg)
                return

        if self._is_multi_statement(sql) and args:
            self._execute_multi_statement(sql, args)
            return

        if args:
            if len(args) == 1 and isinstance(args[0], (list, tuple)):
                self.dbapi_cursor.execute(sql, args[0])
            else:
                self.dbapi_cursor.execute(sql, args)
        else:
            self.dbapi_cursor.execute(sql)

    def _is_multi_statement(self, sql: str) -> bool:
        """Check if SQL contains multiple statements."""
        return ';' in sql and len([s for s in sql.split(';') if s.strip()]) > 1

    def _execute_multi_statement(self, sql: str, args: tuple) -> None:
        """Execute multiple statements with positional parameters."""
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
        params = args[0] if len(args) == 1 and isinstance(args[0], (list, tuple)) else args

        placeholder = self.strategy.get_placeholder_style()
        placeholder_count = sum(stmt.count(placeholder) for stmt in statements)
        if len(params) != placeholder_count:
            raise ValueError(
                f'Parameter count mismatch: SQL needs {placeholder_count} '
                f'but {len(params)} were provided'
            )

        param_index = 0
        for stmt in statements:
            count = stmt.count(placeholder)
            if count > 0:
                stmt_params = params[param_index:param_index + count]
                param_index += count
                self.dbapi_cursor.execute(stmt, stmt_params)
            else:
                self.dbapi_cursor.execute(stmt)

    def _execute_multi_statement_named(self, sql: str, params_dict: dict) -> None:
        """Execute multiple statements with named parameters."""
        for stmt in (s.strip() for s in sql.split(';') if s.strip()):
            param_names = re.findall(r'%\(([^)]+)\)s', stmt)
            if param_names:
                stmt_params = {name: params_dict[name] for name in param_names if name in params_dict}
                self.dbapi_cursor.execute(stmt, stmt_params)
            else:
                self.dbapi_cursor.execute(stmt)

    @dumpsql_many
    def executemany(self, operation: str, seq_of_parameters: Sequence,
                    batch_size: int = 500, **kwargs: Any) -> int:
        """Execute against all parameter sequences."""
        if not seq_of_parameters:
            logger.warning('executemany called with no parameter sequences')
            return 0

        auto_commit = kwargs.pop('auto_commit', True)

        operation = self.strategy.standardize_sql(operation)

        seq_of_parameters = [TypeConverter.convert_params(p) for p in seq_of_parameters]

        total_rowcount = 0
        if len(seq_of_parameters) <= batch_size:
            self.dbapi_cursor.executemany(operation, seq_of_parameters)
            total_rowcount = self.dbapi_cursor.rowcount
        else:
            logger.debug(f'Batching {len(seq_of_parameters)} rows into chunks of {batch_size}')
            for i in range(0, len(seq_of_parameters), batch_size):
                chunk = seq_of_parameters[i:i + batch_size]
                self.dbapi_cursor.executemany(operation, chunk)
                total_rowcount += self.dbapi_cursor.rowcount

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return total_rowcount


def IterChunk(cursor: Any, size: int = 5000) -> Iterator[tuple]:
    """Iterate through cursor results in chunks."""
    while True:
        try:
            chunked = cursor.fetchmany(size)
        except Exception:
            chunked = []
        if not chunked:
            break
        yield from chunked


class DictRowFactory:
    """Row factory for psycopg that returns dictionary-like rows."""

    def __init__(self, cursor: Any) -> None:
        self.fields = [(c.name, postgres_types.get(c.type_code)) for c in (cursor.description or [])]

    def __call__(self, values: tuple) -> dict:
        return {
            name: cast(value) if isinstance(value, Number) and cast is not None else value
            for (name, cast), value in zip(self.fields, values)
        }


def get_dict_cursor(cn: Any) -> Cursor:
    """Get cursor that returns rows as dictionaries."""
    raw_conn = cn.connection if hasattr(cn, 'connection') else cn
    strategy = get_db_strategy(cn)

    if cn.dialect == 'postgresql':
        cursor = raw_conn.cursor(row_factory=DictRowFactory)
        return Cursor(cursor, cn, strategy)

    if cn.dialect == 'sqlite':
        sqlite_conn = raw_conn
        if hasattr(raw_conn, 'dbapi_connection'):
            sqlite_conn = raw_conn.dbapi_connection
        sqlite_conn.row_factory = sqlite3.Row
        cursor = sqlite_conn.cursor()
        return Cursor(cursor, cn, strategy)

    raise ValueError(f'Unknown connection type: {cn.dialect}')


def extract_column_info(cursor: 'Any', table_name: str | None = None) -> list['Any']:
    """Extract column information from cursor description based on database type."""
    if cursor.description is None:
        return []

    connection_type = cursor.connwrapper.dialect
    connection = cursor.connwrapper

    columns = columns_from_cursor_description(
        cursor,
        connection_type,
        table_name,
        connection
    )

    cursor.columns = columns

    return columns


def load_data(cursor: 'Any', columns: list['Any'] | None = None,
              **kwargs: Any) -> Any:
    """Data loader callable that processes cursor results into the configured format."""
    if columns is None:
        columns = extract_column_info(cursor)

    data = cursor.fetchall()

    if not data:
        data_loader = cursor.connwrapper.options.data_loader
        return data_loader([], columns, **kwargs)

    adapted_data = []
    for row in data:
        adapter = RowAdapter.create(cursor.connwrapper, row)
        if hasattr(adapter, 'cursor'):
            adapter.cursor = cursor
        adapted_data.append(adapter.to_dict())
    data = adapted_data

    data_loader = cursor.connwrapper.options.data_loader
    return data_loader(data, columns, **kwargs)


def process_multiple_result_sets(cursor: 'Any', return_all: bool = False,
                                 prefer_first: bool = False, **kwargs: Any) -> list[Any] | Any:
    """Process multiple result sets from a query or stored procedure."""
    result_sets: list[Any] = []
    columns_sets: list[list[Any]] = []
    largest_result = None
    largest_size = 0

    columns = extract_column_info(cursor)
    columns_sets.append(columns)

    result = load_data(cursor, columns=columns, **kwargs)
    if result is not None:
        result_sets.append(result)
        largest_result = result
        largest_size = len(result)

    while cursor.nextset():
        columns = extract_column_info(cursor)
        columns_sets.append(columns)

        result = load_data(cursor, columns=columns, **kwargs)
        if result is not None:
            result_sets.append(result)
            if len(result) > largest_size:
                largest_result = result
                largest_size = len(result)

    if return_all:
        if not result_sets:
            return []
        return result_sets

    if prefer_first and result_sets:
        return result_sets[0]

    if not result_sets:
        return []

    return largest_result
