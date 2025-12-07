"""
Database cursor abstractions that implement the Python DB-API 2.0 specification (PEP-249).
"""
import logging
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Sequence
from functools import wraps
from numbers import Number
from typing import Any, Generic, TypeVar

from database.adapters.type_conversion import TypeConverter
from database.adapters.type_mapping import postgres_types
from database.utils.auto_commit import ensure_commit
from database.utils.connection_utils import get_dialect_name, isconnection
from database.utils.sql import has_placeholders

from libb import collapse

logger = logging.getLogger(__name__)

# Type variables for connections and cursors
T = TypeVar('T')
ConnectionType = TypeVar('ConnectionType')
CursorType = TypeVar('CursorType')


def batch_execute(func):
    """Decorator to automatically batch parameter sets for executemany
    operations.

    This handles database-specific parameter limits by splitting large
    parameter sets into smaller batches to avoid exceeding DB engine
    constraints.

    Must be the outermost decorator (applied first in code, executed last).
    """
    @wraps(func)
    def wrapper(
        self,
        operation: str,
        seq_of_parameters: Sequence,
        batch_size: int = 500,
        **kwargs: Any
    ) -> int:
        if not seq_of_parameters:
            logger.warning('executemany called with no parameter sequences, skipping')
            return 0

        # If parameters fit within a single batch, just call the original function
        if len(seq_of_parameters) <= batch_size:
            return func(self, operation, seq_of_parameters, **kwargs)

        logger.debug(f'Batching {len(seq_of_parameters)} parameter sets into chunks of {batch_size}')

        # Execute in batches
        total_rows = 0
        for i in range(0, len(seq_of_parameters), batch_size):
            chunk = seq_of_parameters[i:i+batch_size]
            rows = func(self, operation, chunk, **kwargs)
            if rows >= 0:  # Some drivers might return -1 for unknown row count
                total_rows += rows

        return total_rows

    return wrapper


def convert_params(func):
    """
    Decorator to convert parameter types for database operations.

    Applies TypeConverter to all parameters to ensure proper conversion of
    special types like NumPy arrays, pandas Series, etc. to database-compatible formats.
    """
    @wraps(func)
    def wrapper(self, operation: str, *args: Any, **kwargs: Any) -> Any:
        if not args:
            return func(self, operation, *args, **kwargs)

        converted_args = tuple(TypeConverter.convert_params(arg) for arg in args)

        return func(self, operation, *converted_args, **kwargs)

    return wrapper


def dumpsql(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator for logging SQL queries and parameters.

    Wraps database functions to log SQL statements and their parameters
    before execution and log any errors that occur.

    >>> class MockConnection:
    ...     connnection = None
    >>> @dumpsql
    ... def example_query(cn, sql, *args):
    ...     return "executed"
    >>> result = example_query(MockConnection(), "SELECT * FROM table", 1, 2)
    >>> result
    'executed'
    """
    @wraps(func)
    def wrapper(cn: Any, sql: str, *args: Any, **kwargs: Any) -> Any:
        this_cn = isconnection(cn) and cn or cn.connnection
        try:
            logger.debug(f'SQL:\n{sql}\nargs: {args}')
            return func(cn, sql, *args, **kwargs)
        except Exception:
            logger.error(f'Error with query:\nSQL:\n{sql}\nargs: {args}')
            raise
    return wrapper


class AbstractCursor(ABC, Generic[ConnectionType]):
    """Base cursor class implementing the Python DB-API 2.0 specification.

    This class defines the interface for all database cursor implementations.
    Subclasses must implement the abstract methods for their specific database.
    """

    def __init__(self, cursor: Any, connection_wrapper: ConnectionType) -> None:
        """Initialize the cursor wrapper.

        Args:
            cursor: The underlying database cursor
            connection_wrapper: The connection wrapper that created this cursor
        """
        self.dbapi_cursor = cursor
        self.connwrapper = connection_wrapper
        self._arraysize: int = 1
        self._original_sql: str = ''
        self._sql: str = ''

    def __getattr__(self, name: str) -> Any:
        """Delegate any members to the underlying cursor."""
        return getattr(self.dbapi_cursor, name)

    def __iter__(self) -> Iterator:
        """Return an iterator for the cursor."""
        return IterChunk(self.dbapi_cursor)

    @property
    def description(self) -> list[tuple] | None:
        """Retrieve column descriptions for the last query.

        Returns a sequence of 7-item sequences:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)
        """
        return self.dbapi_cursor.description

    @property
    def rowcount(self) -> int:
        """Return the number of rows produced/affected by last operation."""
        return self.dbapi_cursor.rowcount

    @property
    def arraysize(self) -> int:
        """Control the number of rows fetched by fetchmany()."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        """Set the arraysize for fetchmany()."""
        self._arraysize = value

    def close(self) -> None:
        """Close the cursor, making it unusable for further operations."""
        self.dbapi_cursor.close()

    def fetchone(self) -> tuple | None:
        """Fetch the next row of a query result set."""
        return self.dbapi_cursor.fetchone()

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        """Fetch the next set of rows of a query result."""
        if size is None:
            size = self.arraysize
        return self.dbapi_cursor.fetchmany(size)

    def fetchall(self) -> list[tuple]:
        """Fetch all remaining rows of a query result."""
        return self.dbapi_cursor.fetchall()

    def setinputsizes(self, sizes: Sequence) -> None:
        """Predefine memory areas for parameters."""

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """Set a column buffer size for large columns."""

    @abstractmethod
    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        """Execute a database operation (query or command)."""

    @abstractmethod
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence,
        batch_size: int = 500,
        **kwargs: Any
    ) -> int:
        """Execute against all parameter sequences."""


class PostgresqlCursor(AbstractCursor):
    """PostgreSQL cursor implementation."""

    @convert_params
    @dumpsql
    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        """Execute a database operation for PostgreSQL."""
        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        try:
            self._execute_query(operation, args)
            logger.debug(f'Query result: {self.dbapi_cursor.statusmessage}')
        finally:
            end = time.time()
            self.connwrapper.addcall(end - start)
            logger.debug('Query time: %f' % (end - start))

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.dbapi_cursor.rowcount

    def _execute_query(self, sql: str, args: tuple) -> None:
        """Execute a PostgreSQL specific query.

        Handles special cases for PostgreSQL query execution including
        multi-statement queries and dictionary parameters.
        """
        self._original_sql = sql

        if args and not has_placeholders(sql):
            args_count = len(tuple(collapse(args)))
            self.dbapi_cursor.execute(sql)
            logger.debug(f'Executed query without placeholders (ignoring {args_count} args)')
            return

        for arg in collapse(args):
            if isinstance(arg, dict):
                is_multi_statement = ';' in sql and len([s for s in sql.split(';') if s.strip()]) > 1

                if is_multi_statement:
                    self._execute_multi_statement_named(sql, arg)
                    return

                self.dbapi_cursor.execute(sql, arg)
                return

        is_multi_statement = ';' in sql and len([s for s in sql.split(';') if s.strip()]) > 1

        if is_multi_statement and args:
            self._execute_multi_statement(sql, args)
            return

        self.dbapi_cursor.execute(sql, *args)

    def _execute_multi_statement(self, sql: str, args: tuple) -> None:
        """Execute multiple PostgreSQL statements with proper parameter handling.

        Splits a multi-statement SQL string into individual statements and
        distributes parameters appropriately to each statement.
        """
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]

        params = args[0] if len(args) == 1 and isinstance(args[0], (list, tuple)) else args

        placeholder_count = sum(stmt.count('%s') for stmt in statements)

        if len(params) != placeholder_count:
            raise ValueError(
                f'Parameter count mismatch: SQL needs {placeholder_count} parameters '
                f'but {len(params)} were provided'
            )

        param_index = 0
        for stmt in statements:
            placeholder_count = stmt.count('%s')

            if placeholder_count > 0:
                stmt_params = params[param_index:param_index + placeholder_count]
                param_index += placeholder_count
                self.dbapi_cursor.execute(stmt, stmt_params)
            else:
                self.dbapi_cursor.execute(stmt)

    def _execute_multi_statement_named(self, sql: str, params_dict: dict) -> None:
        """Execute multiple PostgreSQL statements with named parameter handling.

        Splits a multi-statement SQL string with named parameters into individual
        statements and executes each with the appropriate subset of parameters.
        """
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]

        for stmt in statements:
            if not stmt:
                continue

            param_names = re.findall(r'%\(([^)]+)\)s', stmt)

            if not param_names:
                self.dbapi_cursor.execute(stmt)
                continue

            stmt_params = {name: params_dict[name] for name in param_names if name in params_dict}

            self.dbapi_cursor.execute(stmt, stmt_params)

    @batch_execute
    @convert_params
    @dumpsql
    def executemany(self, sql: str, seq_of_parameters: Sequence, batch_size: int = 500,
                    **kwargs: Any) -> int:
        """Execute the SQL statement against all parameter sequences for PostgreSQL."""
        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        if not seq_of_parameters:
            logger.warning('executemany called with no parameter sequences, skipping')
            return 0

        try:
            self.dbapi_cursor.executemany(sql, seq_of_parameters)
            logger.debug(f'Executemany result: {self.dbapi_cursor.statusmessage}')
        finally:
            end = time.time()
            self.connwrapper.addcall(end - start)
            logger.debug('Executemany time: %f' % (end - start))

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.dbapi_cursor.rowcount


class SqliteCursor(AbstractCursor):
    """SQLite cursor implementation."""

    @convert_params
    @dumpsql
    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        """Execute a database operation for SQLite."""
        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        try:
            self._original_sql = operation

            if args and not has_placeholders(operation):
                self.dbapi_cursor.execute(operation)
                logger.debug('Executed query without placeholders (ignoring args)')
            else:
                for arg in collapse(args):
                    if isinstance(arg, dict):
                        self.dbapi_cursor.execute(operation, arg)
                        break
                else:
                    if args:
                        if len(args) == 1 and isinstance(args[0], (list, tuple)):
                            self.dbapi_cursor.execute(operation, args[0])
                        else:
                            self.dbapi_cursor.execute(operation, args)
                    else:
                        self.dbapi_cursor.execute(operation)
        finally:
            end = time.time()
            self.connwrapper.addcall(end - start)
            logger.debug('Query time: %f' % (end - start))

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.dbapi_cursor.rowcount

    @batch_execute
    @convert_params
    @dumpsql
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence,
        batch_size: int = 500,
        **kwargs: Any
    ) -> int:
        """Execute the SQL statement against all parameter sequences for SQLite."""
        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        if not seq_of_parameters:
            logger.warning('executemany called with no parameter sequences, skipping')
            return 0

        try:
            self.dbapi_cursor.executemany(operation, seq_of_parameters)
        finally:
            end = time.time()
            self.connwrapper.addcall(end - start)
            logger.debug('Executemany time: %f' % (end - start))

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.dbapi_cursor.rowcount


class FakeCursor(AbstractCursor):
    """Test cursor for mocking database operations."""

    def __init__(self, cursor: Any = None, connection_wrapper: Any = None) -> None:
        """Initialize with optional cursor and connection wrapper."""
        self.dbapi_cursor = cursor or object()
        self.connwrapper = connection_wrapper or object()
        self._description: list[tuple] | None = None
        self._rowcount: int = -1
        self._arraysize: int = 1
        self._rows: list[tuple] = []
        self._position: int = 0
        self._original_sql: str = ''

    @property
    def description(self) -> list[tuple] | None:
        """Return mock description."""
        return self._description

    @description.setter
    def description(self, value: list[tuple] | None) -> None:
        """Set mock description."""
        self._description = value

    @property
    def rowcount(self) -> int:
        """Return mock rowcount."""
        return self._rowcount

    @rowcount.setter
    def rowcount(self, value: int) -> None:
        """Set mock rowcount."""
        self._rowcount = value

    def close(self) -> None:
        """Close the cursor."""

    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        """Execute a database operation (mock)."""
        self._original_sql = operation
        return self._rowcount

    def executemany(self, operation: str, seq_of_parameters: Sequence, **kwargs: Any) -> int:
        """Execute against all parameter sequences (mock)."""
        self._original_sql = operation
        return self._rowcount

    def fetchone(self) -> tuple | None:
        """Fetch the next row (mock)."""
        if self._position >= len(self._rows):
            return None
        row = self._rows[self._position]
        self._position += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        """Fetch the next set of rows (mock)."""
        if size is None:
            size = self._arraysize
        start = self._position
        end = min(start + size, len(self._rows))
        self._position = end
        return self._rows[start:end]

    def fetchall(self) -> list[tuple]:
        """Fetch all remaining rows (mock)."""
        start = self._position
        self._position = len(self._rows)
        return self._rows[start:]

    def set_fake_data(self, rows: list[tuple], description: list[tuple] | None = None) -> None:
        """Set fake data for the cursor to return."""
        self._rows = rows
        self._position = 0
        self._rowcount = len(rows)
        self._description = description


def IterChunk(cursor: Any, size: int = 5000) -> Iterator[tuple]:
    """Iterate through cursor results in manageable chunks.

    Alternative to builtin cursor generator that breaks fetches into smaller
    chunks to avoid malloc problems for really large queries.
    """
    while True:
        try:
            chunked = cursor.fetchmany(size)
        except Exception:
            chunked = []
        if not chunked:
            break
        yield from chunked


class DictRowFactory:
    """Row factory for psycopg that returns dictionary-like rows.

    Converts tuple-based rows from the database cursor into dictionary objects
    with column names as keys, applying type conversions where appropriate.
    """

    def __init__(self, cursor: Any) -> None:
        self.fields = [(c.name, postgres_types.get(c.type_code)) for c in (cursor.description or [])]

    def __call__(self, values: tuple) -> dict:
        return {name: cast(value)
                if isinstance(value, Number) and cast is not None
                else value
                for (name, cast), value in zip(self.fields, values)}


def get_dict_cursor(cn: Any) -> AbstractCursor:
    """Get a cursor that returns rows as dictionaries for the given connection type.

    Creates and returns the appropriate cursor implementation based on the
    database dialect of the provided connection, with results formatted as
    dictionaries rather than tuples.
    """
    if hasattr(cn, 'connection'):
        raw_conn = cn.connection
    else:
        raw_conn = cn

    dialect_name = get_dialect_name(cn)
    if dialect_name == 'postgresql':
        cursor = raw_conn.cursor(row_factory=DictRowFactory)
        return PostgresqlCursor(cursor, cn)
    if dialect_name == 'sqlite':
        cursor = raw_conn.cursor()
        return SqliteCursor(cursor, cn)
    raise ValueError('Unknown connection type')


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
