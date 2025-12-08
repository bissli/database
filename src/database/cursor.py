"""
Database cursor implementations for PostgreSQL and SQLite.

Implements Python DB-API 2.0 specification (PEP-249).
"""
import logging
import re
import time
from collections.abc import Iterator, Sequence
from numbers import Number
from typing import Any

from database.sql import has_placeholders, standardize_placeholders
from database.types import TypeConverter, postgres_types
from database.utils.auto_commit import ensure_commit

from libb import collapse

logger = logging.getLogger(__name__)


class Cursor:
    """Base cursor class with common DB-API 2.0 operations."""

    def __init__(self, cursor: Any, connection_wrapper: Any) -> None:
        """Initialize cursor wrapper.

        Args:
            cursor: The underlying database cursor
            connection_wrapper: The connection wrapper that created this cursor
        """
        self.dbapi_cursor = cursor
        self.connwrapper = connection_wrapper
        self._arraysize: int = 1
        self._original_sql: str = ''

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


class PostgresqlCursor(Cursor):
    """PostgreSQL cursor implementation."""

    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        """Execute a database operation."""
        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        # Convert parameters
        if args:
            args = tuple(TypeConverter.convert_params(arg) for arg in args)

        # Log query
        logger.debug(f'SQL:\n{operation}\nargs: {args}')

        try:
            self._execute_query(operation, args)
            logger.debug(f'Query result: {self.dbapi_cursor.statusmessage}')
        except Exception:
            logger.error(f'Error with query:\nSQL:\n{operation}\nargs: {args}')
            raise
        finally:
            elapsed = time.time() - start
            self.connwrapper.addcall(elapsed)
            logger.debug(f'Query time: {elapsed:.4f}s')

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.dbapi_cursor.rowcount

    def _execute_query(self, sql: str, args: tuple) -> None:
        """Execute PostgreSQL query with parameter handling."""
        self._original_sql = sql

        # No args or no placeholders
        if args and not has_placeholders(sql):
            self.dbapi_cursor.execute(sql)
            logger.debug('Executed query without placeholders (ignoring args)')
            return

        # Handle dict parameters
        for arg in collapse(args):
            if isinstance(arg, dict):
                if self._is_multi_statement(sql):
                    self._execute_multi_statement_named(sql, arg)
                else:
                    self.dbapi_cursor.execute(sql, arg)
                return

        # Handle multi-statement with positional params
        if self._is_multi_statement(sql) and args:
            self._execute_multi_statement(sql, args)
            return

        self.dbapi_cursor.execute(sql, *args)

    def _is_multi_statement(self, sql: str) -> bool:
        """Check if SQL contains multiple statements."""
        return ';' in sql and len([s for s in sql.split(';') if s.strip()]) > 1

    def _execute_multi_statement(self, sql: str, args: tuple) -> None:
        """Execute multiple statements with positional parameters."""
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
        params = args[0] if len(args) == 1 and isinstance(args[0], (list, tuple)) else args

        placeholder_count = sum(stmt.count('%s') for stmt in statements)
        if len(params) != placeholder_count:
            raise ValueError(
                f'Parameter count mismatch: SQL needs {placeholder_count} '
                f'but {len(params)} were provided'
            )

        param_index = 0
        for stmt in statements:
            count = stmt.count('%s')
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

    def executemany(self, operation: str, seq_of_parameters: Sequence,
                    batch_size: int = 500, **kwargs: Any) -> int:
        """Execute against all parameter sequences."""
        if not seq_of_parameters:
            logger.warning('executemany called with no parameter sequences')
            return 0

        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        # Convert parameters
        seq_of_parameters = [TypeConverter.convert_params(p) for p in seq_of_parameters]

        logger.debug(f'SQL:\n{operation}\nparams: {len(seq_of_parameters)} rows')

        total_rowcount = 0
        try:
            # Batch if needed
            if len(seq_of_parameters) <= batch_size:
                self.dbapi_cursor.executemany(operation, seq_of_parameters)
                total_rowcount = self.dbapi_cursor.rowcount
            else:
                logger.debug(f'Batching {len(seq_of_parameters)} rows into chunks of {batch_size}')
                for i in range(0, len(seq_of_parameters), batch_size):
                    chunk = seq_of_parameters[i:i + batch_size]
                    self.dbapi_cursor.executemany(operation, chunk)
                    total_rowcount += self.dbapi_cursor.rowcount

            logger.debug(f'Executemany result: {self.dbapi_cursor.statusmessage}')
        except Exception:
            logger.error(f'Error with executemany:\nSQL:\n{operation}')
            raise
        finally:
            elapsed = time.time() - start
            self.connwrapper.addcall(elapsed)
            logger.debug(f'Executemany time: {elapsed:.4f}s')

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return total_rowcount


class SqliteCursor(Cursor):
    """SQLite cursor implementation."""

    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        """Execute a database operation."""
        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        # Convert %s to ? for SQLite
        operation = standardize_placeholders(operation, dialect='sqlite')
        self._original_sql = operation

        # Convert parameters
        if args:
            args = tuple(TypeConverter.convert_params(arg) for arg in args)

        logger.debug(f'SQL:\n{operation}\nargs: {args}')

        try:
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
        except Exception:
            logger.error(f'Error with query:\nSQL:\n{operation}\nargs: {args}')
            raise
        finally:
            elapsed = time.time() - start
            self.connwrapper.addcall(elapsed)
            logger.debug(f'Query time: {elapsed:.4f}s')

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.dbapi_cursor.rowcount

    def executemany(self, operation: str, seq_of_parameters: Sequence,
                    batch_size: int = 500, **kwargs: Any) -> int:
        """Execute against all parameter sequences."""
        if not seq_of_parameters:
            logger.warning('executemany called with no parameter sequences')
            return 0

        start = time.time()
        auto_commit = kwargs.pop('auto_commit', True)

        # Convert %s to ? for SQLite
        operation = standardize_placeholders(operation, dialect='sqlite')

        # Convert parameters
        seq_of_parameters = [TypeConverter.convert_params(p) for p in seq_of_parameters]

        logger.debug(f'SQL:\n{operation}\nparams: {len(seq_of_parameters)} rows')

        total_rowcount = 0
        try:
            # Batch if needed
            if len(seq_of_parameters) <= batch_size:
                self.dbapi_cursor.executemany(operation, seq_of_parameters)
                total_rowcount = self.dbapi_cursor.rowcount
            else:
                logger.debug(f'Batching {len(seq_of_parameters)} rows into chunks of {batch_size}')
                for i in range(0, len(seq_of_parameters), batch_size):
                    chunk = seq_of_parameters[i:i + batch_size]
                    self.dbapi_cursor.executemany(operation, chunk)
                    total_rowcount += self.dbapi_cursor.rowcount
        except Exception:
            logger.error(f'Error with executemany:\nSQL:\n{operation}')
            raise
        finally:
            elapsed = time.time() - start
            self.connwrapper.addcall(elapsed)
            logger.debug(f'Executemany time: {elapsed:.4f}s')

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return total_rowcount

    def nextset(self) -> bool | None:
        """Move to next result set (SQLite doesn't support this)."""
        return None


class FakeCursor(Cursor):
    """Test cursor for mocking database operations."""

    def __init__(self, cursor: Any = None, connection_wrapper: Any = None) -> None:
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
        return self._description

    @description.setter
    def description(self, value: list[tuple] | None) -> None:
        self._description = value

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @rowcount.setter
    def rowcount(self, value: int) -> None:
        self._rowcount = value

    def close(self) -> None:
        pass

    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        self._original_sql = operation
        return self._rowcount

    def executemany(self, operation: str, seq_of_parameters: Sequence, **kwargs: Any) -> int:
        self._original_sql = operation
        return self._rowcount

    def fetchone(self) -> tuple | None:
        if self._position >= len(self._rows):
            return None
        row = self._rows[self._position]
        self._position += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        if size is None:
            size = self._arraysize
        start = self._position
        end = min(start + size, len(self._rows))
        self._position = end
        return self._rows[start:end]

    def fetchall(self) -> list[tuple]:
        start = self._position
        self._position = len(self._rows)
        return self._rows[start:]

    def set_fake_data(self, rows: list[tuple], description: list[tuple] | None = None) -> None:
        self._rows = rows
        self._position = 0
        self._rowcount = len(rows)
        self._description = description


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
    import sqlite3

    raw_conn = cn.connection if hasattr(cn, 'connection') else cn

    if cn.dialect == 'postgresql':
        cursor = raw_conn.cursor(row_factory=DictRowFactory)
        return PostgresqlCursor(cursor, cn)

    if cn.dialect == 'sqlite':
        sqlite_conn = raw_conn
        if hasattr(raw_conn, 'dbapi_connection'):
            sqlite_conn = raw_conn.dbapi_connection
        sqlite_conn.row_factory = sqlite3.Row
        cursor = sqlite_conn.cursor()
        return SqliteCursor(cursor, cn)

    raise ValueError(f'Unknown connection type: {cn.dialect}')


# Backwards compatibility
AbstractCursor = Cursor
