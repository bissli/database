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

from database.utils.auto_commit import ensure_commit
from database.utils.connection_utils import get_dialect_name, isconnection
from database.utils.sql import has_placeholders, sanitize_sql_for_logging
from database.utils.sql import standardize_placeholders
from database.utils.sqlserver_utils import ensure_identity_column_named
from database.utils.sqlserver_utils import handle_unnamed_columns_error
from database.utils.sqlserver_utils import prepare_sqlserver_params

from libb import collapse

logger = logging.getLogger(__name__)

# Type variables for connections and cursors
T = TypeVar('T')
ConnectionType = TypeVar('ConnectionType')
CursorType = TypeVar('CursorType')


def dumpsql(func: Callable) -> Callable:
    """Decorator for logging SQL queries and parameters"""
    @wraps(func)
    def wrapper(cn: Any, sql: str, *args: Any, **kwargs: Any) -> Any:
        this_cn = isconnection(cn) and cn or cn.connnection
        sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql, args)
        try:
            logger.debug(f'SQL:\n{sanitized_sql}\nargs: {sanitized_args}')
            return func(cn, sql, *args, **kwargs)
        except Exception:
            logger.error(f'Error with query:\nSQL:\n{sanitized_sql}\nargs: {sanitized_args}')
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
        self.cursor = cursor
        self.connwrapper = connection_wrapper
        self._arraysize: int = 1
        self._original_sql: str = ''
        self._sql: str = ''

    def __getattr__(self, name: str) -> Any:
        """Delegate any members to the underlying cursor."""
        return getattr(self.cursor, name)

    def __iter__(self) -> Iterator:
        """Return an iterator for the cursor."""
        return IterChunk(self.cursor)

    @property
    def description(self) -> list[tuple] | None:
        """Retrieve column descriptions for the last query.

        Returns a sequence of 7-item sequences:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)
        """
        return self.cursor.description

    @property
    def rowcount(self) -> int:
        """Return the number of rows produced/affected by last operation."""
        return self.cursor.rowcount

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
        self.cursor.close()

    def fetchone(self) -> tuple | None:
        """Fetch the next row of a query result set."""
        return self.cursor.fetchone()

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        """Fetch the next set of rows of a query result."""
        if size is None:
            size = self.arraysize
        return self.cursor.fetchmany(size)

    def fetchall(self) -> list[tuple]:
        """Fetch all remaining rows of a query result."""
        return self.cursor.fetchall()

    def setinputsizes(self, sizes: Sequence) -> None:
        """Predefine memory areas for parameters."""

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """Set a column buffer size for large columns."""

    @abstractmethod
    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        """Execute a database operation (query or command)."""

    @abstractmethod
    def executemany(self, operation: str, seq_of_parameters: Sequence, **kwargs: Any) -> int:
        """Execute against all parameter sequences."""


class PgCursor(AbstractCursor):
    """PostgreSQL cursor implementation."""

    @dumpsql
    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        """Execute a database operation for PostgreSQL."""
        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        try:
            self._execute_query(operation, args)
            logger.debug(f'Query result: {self.cursor.statusmessage}')
        finally:
            end = time.time()
            self.connwrapper.addcall(end - start)
            logger.debug('Query time: %f' % (end - start))

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.cursor.rowcount

    def _execute_query(self, sql: str, args: tuple) -> None:
        """Execute a PostgreSQL specific query."""
        self._original_sql = sql

        # Check if SQL has placeholders - if not and we have args, ignore the args
        if args and not has_placeholders(sql):
            args_count = len(tuple(collapse(args)))
            self.cursor.execute(sql)
            return

        # Handle dictionary parameters (for named placeholders)
        for arg in collapse(args):
            if isinstance(arg, dict):
                # Check if we're dealing with multiple statements
                is_multi_statement = ';' in sql and len([s for s in sql.split(';') if s.strip()]) > 1

                if is_multi_statement:
                    self._execute_multi_statement_named(sql, arg)
                    return

                # Standard execution for single statements
                self.cursor.execute(sql, arg)
                return

        # Check if we're dealing with multiple statements
        is_multi_statement = ';' in sql and len([s for s in sql.split(';') if s.strip()]) > 1

        if is_multi_statement and args:
            # PostgreSQL doesn't support multi-statement prepared statements
            self._execute_multi_statement(sql, args)
            return

        # Standard execution for all other cases
        self.cursor.execute(sql, *args)

    def _execute_multi_statement(self, sql: str, args: tuple) -> None:
        """Execute multiple PostgreSQL statements with proper parameter handling."""
        # Split into individual statements, keeping only non-empty ones
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]

        # Unwrap nested parameters if needed
        params = args[0] if len(args) == 1 and isinstance(args[0], (list, tuple)) else args

        # Count total placeholders across all statements
        placeholder_count = sum(stmt.count('%s') for stmt in statements)

        # Validate parameter count
        if len(params) != placeholder_count:
            raise ValueError(
                f'Parameter count mismatch: SQL needs {placeholder_count} parameters '
                f'but {len(params)} were provided'
            )

        # Execute statements individually with their parameters
        param_index = 0
        for stmt in statements:
            placeholder_count = stmt.count('%s')

            if placeholder_count > 0:
                # Extract just the parameters needed for this statement
                stmt_params = params[param_index:param_index + placeholder_count]
                param_index += placeholder_count
                self.cursor.execute(stmt, stmt_params)
            else:
                # Execute without parameters
                self.cursor.execute(stmt)

    def _execute_multi_statement_named(self, sql: str, params_dict: dict) -> None:
        """Execute multiple PostgreSQL statements with named parameter handling."""
        # Split into individual statements, keeping only non-empty ones
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]

        # Execute each statement individually
        for stmt in statements:
            if not stmt:
                continue

            # Extract the parameter names used in this statement
            param_names = re.findall(r'%\(([^)]+)\)s', stmt)

            # If no parameters in this statement, execute it directly
            if not param_names:
                self.cursor.execute(stmt)
                continue

            # Create a filtered dictionary with only the parameters needed for this statement
            stmt_params = {name: params_dict[name] for name in param_names if name in params_dict}

            # Execute the statement with its parameters
            self.cursor.execute(stmt, stmt_params)

    @dumpsql
    def executemany(self, operation: str, seq_of_parameters: Sequence, **kwargs: Any) -> int:
        """Execute the SQL statement against all parameter sequences for PostgreSQL."""
        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        if not seq_of_parameters:
            logger.warning('executemany called with no parameter sequences, skipping')
            return 0

        try:
            self.cursor.executemany(operation, seq_of_parameters)
            logger.debug(f'Executemany result: {self.cursor.statusmessage}')
        finally:
            end = time.time()
            self.connwrapper.addcall(end - start)
            logger.debug('Executemany time: %f' % (end - start))

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.cursor.rowcount


class MsCursor(AbstractCursor):
    """Microsoft SQL Server cursor implementation."""

    @dumpsql
    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        """Execute a database operation for SQL Server."""
        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        try:
            self._execute_query(operation, args)
        except Exception as e:
            self._handle_sqlserver_error(e, operation, args)
        finally:
            end = time.time()
            self.connwrapper.addcall(end - start)
            logger.debug('Query time: %f' % (end - start))

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.cursor.rowcount

    def _execute_query(self, sql: str, args: tuple) -> None:
        """Execute a query specifically for SQL Server with appropriate handling."""
        # Only add explicit names for required expressions (@@identity, etc)
        sql = ensure_identity_column_named(sql)

        # Save the final SQL
        self._sql = sql
        self._original_sql = sql

        # Convert %s to ? for SQL Server
        sql = standardize_placeholders(self.connwrapper, sql)

        # Handle stored procedures with named parameters for SQL Server
        if args and 'EXEC ' in sql.upper() and '@' in sql:
            self._execute_stored_procedure(sql, args)
        else:
            # Standard execution for non-stored procedure queries
            if args:
                if len(args) == 1 and isinstance(args[0], (list, tuple)):
                    self.cursor.execute(sql, args[0])
                else:
                    self.cursor.execute(sql, args)
            else:
                self.cursor.execute(sql)

    def _execute_stored_procedure(self, sql: str, args: tuple) -> None:
        """Handle execution of SQL Server stored procedures with parameters."""
        # Convert parameters to positional placeholders (?)
        processed_sql, processed_args = prepare_sqlserver_params(sql, args)

        # Count placeholders in the SQL
        placeholder_count = processed_sql.count('?')
        param_count = len(processed_args) if processed_args else 0

        # Execute with the processed SQL and args
        if not processed_args:
            self.cursor.execute(processed_sql)
            return

        processed_args = self._adjust_parameter_count(placeholder_count, param_count, processed_args)
        self._execute_with_parameters(processed_sql, processed_args, placeholder_count)

    def _adjust_parameter_count(self, placeholder_count: int, param_count: int, processed_args: Sequence) -> Sequence:
        """Adjust parameter count to match placeholders."""
        if placeholder_count != param_count:
            logger.warning(f'Parameter count mismatch: {placeholder_count} placeholders, '
                           f'{param_count} parameters. Adjusting...')

            # Adjust parameters to match placeholders
            if placeholder_count > param_count:
                # Add None values if we need more parameters
                processed_args = list(processed_args)
                processed_args.extend([None] * (placeholder_count - param_count))
            else:
                # Truncate if we have too many parameters
                processed_args = processed_args[:placeholder_count]

        return processed_args

    def _execute_with_parameters(self, sql: str, params: Sequence, placeholder_count: int) -> None:
        """Execute SQL with parameters, handling single parameter case specially."""
        # Only use tuple unpacking for single parameters with single placeholders
        if placeholder_count == 1 and len(params) >= 1:
            # For a single placeholder, pass the first parameter directly
            self.cursor.execute(sql, params[0])
        else:
            # For multiple parameters, pass as a list
            self.cursor.execute(sql, params)

    def _handle_sqlserver_error(self, error: Exception, sql: str, args: tuple) -> None:
        """Handle SQL Server specific errors with automatic fixes."""
        modified_sql, should_retry = handle_unnamed_columns_error(error, sql, args)
        if should_retry:
            self.cursor.execute(modified_sql, *args)
        else:
            raise error

    @dumpsql
    def executemany(self, operation: str, seq_of_parameters: Sequence, **kwargs: Any) -> int:
        """Execute the SQL statement against all parameter sequences for SQL Server."""
        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        if not seq_of_parameters:
            logger.warning('executemany called with no parameter sequences, skipping')
            return 0

        try:
            modified_sql = standardize_placeholders(self.connwrapper, operation)
            modified_sql = ensure_identity_column_named(modified_sql)
            self.cursor.executemany(modified_sql, seq_of_parameters)
        finally:
            end = time.time()
            self.connwrapper.addcall(end - start)
            logger.debug('Executemany time: %f' % (end - start))

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.cursor.rowcount


class SlCursor(AbstractCursor):
    """SQLite cursor implementation."""

    @dumpsql
    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        """Execute a database operation for SQLite."""
        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        try:
            self._original_sql = operation

            # Check if SQL has placeholders - if not and we have args, ignore the args
            if args and not has_placeholders(operation):
                self.cursor.execute(operation)
            else:
                # Handle dictionary parameters
                for arg in collapse(args):
                    if isinstance(arg, dict):
                        self.cursor.execute(operation, arg)
                        break
                else:
                    # Standard execution
                    if args:
                        if len(args) == 1 and isinstance(args[0], (list, tuple)):
                            self.cursor.execute(operation, args[0])
                        else:
                            self.cursor.execute(operation, args)
                    else:
                        self.cursor.execute(operation)
        finally:
            end = time.time()
            self.connwrapper.addcall(end - start)
            logger.debug('Query time: %f' % (end - start))

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.cursor.rowcount

    @dumpsql
    def executemany(self, operation: str, seq_of_parameters: Sequence, **kwargs: Any) -> int:
        """Execute the SQL statement against all parameter sequences for SQLite."""
        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        if not seq_of_parameters:
            logger.warning('executemany called with no parameter sequences, skipping')
            return 0

        try:
            self.cursor.executemany(operation, seq_of_parameters)
        finally:
            end = time.time()
            self.connwrapper.addcall(end - start)
            logger.debug('Executemany time: %f' % (end - start))

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.cursor.rowcount


class FakeCursor(AbstractCursor):
    """Test cursor for mocking database operations."""

    def __init__(self, cursor: Any = None, connection_wrapper: Any = None) -> None:
        """Initialize with optional cursor and connection wrapper."""
        self.cursor = cursor or object()
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


def IterChunk(cursor: Any, size: int = 5000) -> Iterator:
    """Alternative to builtin cursor generator
    breaks fetches into smaller chunks to avoid malloc problems
    for really large queries
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
    """Row factory for psycopg that returns dictionary-like rows"""

    def __init__(self, cursor: Any) -> None:
        # Make sure to get both name and type conversion function
        self.fields = [(c.name, postgres_type_convert(c.type_code)) for c in (cursor.description or [])]

    def __call__(self, values: tuple) -> dict:
        return {name: cast(value)
                if isinstance(value, Number) and cast is not None
                else value
                for (name, cast), value in zip(self.fields, values)}


def postgres_type_convert(type_code: int) -> Callable | None:
    """Get type conversion function based on PostgreSQL type OID"""
    # Import here to avoid circular imports
    from database.adapters.type_mapping import postgres_types
    return postgres_types.get(type_code)


def get_dict_cursor(cn: Any) -> AbstractCursor:
    """Get a cursor that returns rows as dictionaries for the given connection type"""
    if hasattr(cn, 'connection'):
        raw_conn = cn.connection
    else:
        raw_conn = cn

    dialect_name = get_dialect_name(cn)
    if dialect_name == 'postgresql':
        cursor = raw_conn.cursor(row_factory=DictRowFactory)
        return PgCursor(cursor, cn)
    if dialect_name == 'mssql':
        cursor = raw_conn.cursor()
        return MsCursor(cursor, cn)
    if dialect_name == 'sqlite':
        cursor = raw_conn.cursor()
        return SlCursor(cursor, cn)
    raise ValueError('Unknown connection type')
