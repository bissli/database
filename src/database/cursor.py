"""DB-API 2.0 (PEP 249) cursor wrapper shared by PostgreSQL and SQLite.
"""
import logging
import re
import time
from collections.abc import Callable, Iterator, Sequence
from functools import wraps
from typing import Any

from database.exceptions import QueryError
from database.sql import has_named_placeholders, has_placeholders
from database.sql import mask_protected_text
from database.sql import raise_on_readonly_disarm, split_statements
from database.strategy import get_db_strategy
from database.types import RowAdapter, TypeConverter
from database.types import columns_from_cursor_description

from libb import collapse

logger = logging.getLogger(__name__)


def dumpsql(is_many: bool = False) -> Callable[[Callable], Callable]:
    """Decorator factory that logs a cursor call's SQL, parameters and time.

    Parameters
    ----------
    is_many : bool, default False
        True for executemany: log the row count in place of the rows.

    Returns
    -------
    Callable
        Decorator for a Cursor method that takes the operation first. A
        failed call logs at ERROR with its traceback, then re-raises.
    """
    def decorator(func: Callable) -> Callable:
        label = 'Executemany' if is_many else 'Query'

        @wraps(func)
        def wrapper(self: 'Cursor', operation: str, *args: Any,
                    **kwargs: Any) -> Any:
            start = time.perf_counter()
            if is_many:
                params = args[0] if args else kwargs.get('seq_of_parameters')
                row_count = len(params) if params else 0
                logger.debug('SQL:\n%s\nparams: %d rows', operation, row_count)
            else:
                logger.debug('SQL:\n%s\nargs: %s', operation, args)
            try:
                result = func(self, operation, *args, **kwargs)
                if hasattr(self.dbapi_cursor, 'statusmessage'):
                    logger.debug('%s result: %s',
                                 label, self.dbapi_cursor.statusmessage)
                return result
            except Exception:
                if is_many:
                    logger.error(
                        'Error with executemany:\nSQL:\n%s',
                        operation,
                        exc_info=True)
                else:
                    logger.error(
                        'Error with query:\nSQL:\n%s\nargs: %s',
                        operation,
                        args,
                        exc_info=True)
                raise
            finally:
                elapsed = time.perf_counter() - start
                self.connwrapper._addcall(elapsed)
                logger.debug('%s time: %.4fs', label, elapsed)
        return wrapper
    return decorator


class Cursor:
    """DB-API 2.0 cursor that adapts SQL and parameters to the dialect.

    Parameters
    ----------
    cursor : Any
        Driver cursor every call reaches in the end.
    connection_wrapper : Any
        ConnectionWrapper that created the cursor.
    strategy : Any, default None
        Dialect strategy. None looks it up from connection_wrapper on
        first use.
    """

    def __init__(self, cursor: Any, connection_wrapper: Any,
                 strategy: Any = None) -> None:
        """Store the driver cursor, its wrapper, and the strategy.
        """
        self.dbapi_cursor = cursor
        self.connwrapper = connection_wrapper
        self._strategy = strategy
        self._arraysize: int = 1

    @property
    def strategy(self) -> Any:
        """Dialect strategy, looked up from the connection on first use.
        """
        if self._strategy is None:
            self._strategy = get_db_strategy(self.connwrapper)
        return self._strategy

    def __getattr__(self, name: str) -> Any:
        """Any other member, read from the driver cursor.
        """
        return getattr(self.dbapi_cursor, name)

    def __iter__(self) -> Iterator:
        """Remaining rows, fetched in chunks; see iter_chunk.
        """
        return iter_chunk(self.dbapi_cursor)

    @property
    def description(self) -> list[tuple] | None:
        """Driver description of the last result set; None for none.
        """
        return self.dbapi_cursor.description

    @property
    def rowcount(self) -> int:
        """Driver rowcount of the last operation.
        """
        return self.dbapi_cursor.rowcount

    @property
    def arraysize(self) -> int:
        """Rows fetchmany() returns when called without a size.
        """
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._arraysize = value

    def close(self) -> None:
        """Close the driver cursor.
        """
        self.dbapi_cursor.close()

    def fetchone(self) -> tuple | None:
        """Next row, or None when no rows remain.
        """
        return self.dbapi_cursor.fetchone()

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        """Up to size rows; size None means arraysize.
        """
        if size is None:
            size = self.arraysize
        return self.dbapi_cursor.fetchmany(size)

    def fetchall(self) -> list[tuple]:
        """Every remaining row.
        """
        return self.dbapi_cursor.fetchall()

    def setinputsizes(self, sizes: Sequence) -> None:
        """Do nothing, as DB-API 2.0 allows.
        """

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """Do nothing, as DB-API 2.0 allows.
        """

    def nextset(self) -> bool | None:
        """Move to the next result set; None where the driver has none.
        """
        if hasattr(self.dbapi_cursor, 'nextset'):
            return self.dbapi_cursor.nextset()
        return None

    @dumpsql()
    def execute(self, operation: str, *args: Any, **kwargs: Any) -> int:
        """Run one operation, which may hold several statements.

        Parameters
        ----------
        operation : str
            SQL in either placeholder style; the strategy converts it.
        *args : Any
            Positional values, one sequence of them, or a dict of named
            values. Values are ignored when operation has no placeholder.
        auto_commit : bool, default True
            Keyword only. Commit afterwards unless the connection is
            inside a transaction.

        Returns
        -------
        int
            Driver rowcount after the last statement.

        Raises
        ------
        ReadOnlyError
            When operation would turn off a reader's read-only setting.
        QueryError
            When positional values across several statements do not
            match the placeholder count.
        """
        raise_on_readonly_disarm(self.connwrapper, operation)
        auto_commit = kwargs.pop('auto_commit', True)

        operation = self.strategy.standardize_sql(operation)

        if args:
            args = tuple(TypeConverter.convert_params(arg) for arg in args)

        self._execute_query(operation, args)

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            # SQLAlchemy tracks no transaction for the raw cursor's
            # statements, so sa_connection.commit() would not commit them.
            self.connwrapper.dbapi_connection.commit()

        return self.dbapi_cursor.rowcount

    def _execute_query(self, sql: str, args: tuple) -> None:
        """Send standardized SQL to the driver with the parameters it uses.

        Parameters
        ----------
        sql : str
            SQL already in the dialect's placeholder style.
        args : tuple
            Converted values as Cursor.execute received them. The first
            dict found anywhere in them is the named-parameter set.
        """
        dict_params = next(
            (arg for arg in collapse(args) if isinstance(arg, dict)), None)
        if dict_params is not None and has_named_placeholders(
                sql, getattr(self.connwrapper, 'dialect', 'postgresql')):
            self._execute_with_dict_params(sql, dict_params)
            return

        if args and not has_placeholders(sql):
            self._execute_without_params(sql)
            logger.debug('Executed query without placeholders (ignoring args)')
            return

        statements = self._statements(sql)
        if len(statements) > 1 and args:
            self._execute_multi_statement(statements, args)
            return

        self._execute_simple(sql, args)

    def _execute_with_dict_params(self, sql: str, params: dict) -> None:
        """Run SQL with named parameters, one statement at a time.
        """
        statements = self._statements(sql)
        if len(statements) > 1:
            self._execute_multi_statement_named(statements, params)
        else:
            self.dbapi_cursor.execute(sql, params)

    def _execute_simple(self, sql: str, args: tuple) -> None:
        """Run SQL in one driver call; a lone list or tuple arg is unwrapped.
        """
        if not args:
            self.dbapi_cursor.execute(sql)
        elif len(args) == 1 and isinstance(args[0], (list, tuple)):
            self.dbapi_cursor.execute(sql, args[0])
        else:
            self.dbapi_cursor.execute(sql, args)

    def _statements(self, sql: str) -> list[str]:
        """Statements in sql, cut at semicolons outside literals and comments.
        """
        return split_statements(
            sql, getattr(self.connwrapper, 'dialect', 'postgresql'))

    def _execute_multi_statement(self, statements: list[str],
                                 args: tuple) -> None:
        """Execute each statement, splitting the positional parameters.

        Parameters
        ----------
        statements : list[str]
            Statements in execution order.
        args : tuple
            Positional parameters for all statements, in text order.

        Raises
        ------
        QueryError
            When the parameter count differs from the placeholder count.
        """
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            params = args[0]
        else:
            params = args

        placeholder = self.strategy.get_placeholder_style()
        dialect = getattr(self.connwrapper, 'dialect', 'postgresql')
        counts = []
        for stmt in statements:
            masked = mask_protected_text(stmt, dialect)
            counts.append((stmt if masked is None else masked).count(placeholder))
        placeholder_count = sum(counts)
        if len(params) != placeholder_count:
            raise QueryError(
                f'Parameter count mismatch: SQL needs {placeholder_count} '
                f'but {len(params)} were provided')

        param_index = 0
        for stmt, count in zip(statements, counts):
            if count > 0:
                stmt_params = params[param_index:param_index + count]
                param_index += count
                self.dbapi_cursor.execute(stmt, stmt_params)
            else:
                self._execute_without_params(stmt)

    def _execute_multi_statement_named(self, statements: list[str],
                                       params_dict: dict) -> None:
        """Execute each statement with the named parameters it uses.

        Parameters
        ----------
        statements : list[str]
            Statements in execution order.
        params_dict : dict
            Named parameters for all statements.
        """
        if getattr(self.connwrapper, 'dialect', 'postgresql') == 'sqlite':
            for stmt in statements:
                self.dbapi_cursor.execute(stmt, params_dict)
            return
        for stmt in statements:
            param_names = re.findall(r'%\(([^)]+)\)s', stmt)
            if param_names:
                stmt_params = {
                    name: params_dict[name]
                    for name in param_names
                    if name in params_dict
                    }
                self.dbapi_cursor.execute(stmt, stmt_params)
            else:
                self._execute_without_params(stmt)

    def _execute_without_params(self, stmt: str) -> None:
        """Run one statement of a parameterized multi-statement call.

        Parameters
        ----------
        stmt : str
            Statement holding no placeholder.
        """
        if getattr(self.connwrapper, 'dialect', 'postgresql') == 'postgresql':
            stmt = stmt.replace('%%', '%')
        self.dbapi_cursor.execute(stmt)

    @dumpsql(is_many=True)
    def executemany(self, operation: str, seq_of_parameters: Sequence,
                    batch_size: int = 500, **kwargs: Any) -> int:
        """Run one statement once per parameter set, in driver batches.

        Parameters
        ----------
        operation : str
            One SQL statement in either placeholder style.
        seq_of_parameters : Sequence
            Parameter sets. Empty logs a warning and runs nothing.
        batch_size : int, default 500
            Most parameter sets per driver executemany call.
        auto_commit : bool, default True
            Keyword only. Commit afterwards unless the connection is
            inside a transaction.

        Returns
        -------
        int
            Rowcount summed across batches; 0 for no parameter sets.

        Raises
        ------
        ReadOnlyError
            When operation would turn off a reader's read-only setting.
        """
        if not seq_of_parameters:
            logger.warning('executemany called with no parameter sequences')
            return 0

        raise_on_readonly_disarm(self.connwrapper, operation)
        auto_commit = kwargs.pop('auto_commit', True)

        operation = self.strategy.standardize_sql(operation)

        seq_of_parameters = [
            TypeConverter.convert_params(p) for p in seq_of_parameters]

        total_rowcount = 0
        if len(seq_of_parameters) <= batch_size:
            self.dbapi_cursor.executemany(operation, seq_of_parameters)
            total_rowcount = self.dbapi_cursor.rowcount
        else:
            logger.debug('Batching %d rows into chunks of %d',
                         len(seq_of_parameters), batch_size)
            for i in range(0, len(seq_of_parameters), batch_size):
                chunk = seq_of_parameters[i:i + batch_size]
                self.dbapi_cursor.executemany(operation, chunk)
                total_rowcount += self.dbapi_cursor.rowcount

        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            # SQLAlchemy tracks no transaction for the raw cursor's
            # statements, so sa_connection.commit() would not commit them.
            self.connwrapper.dbapi_connection.commit()

        return total_rowcount


def iter_chunk(cursor: Any, size: int = 5000) -> Iterator[tuple]:
    """Yield a cursor's remaining rows, fetched size rows at a time.

    Parameters
    ----------
    cursor : Any
        Driver cursor.
    size : int, default 5000
        Rows per fetchmany call.

    Returns
    -------
    Iterator[tuple]
        Rows in fetch order. A fetchmany that raises, as on a statement
        with no result set, ends the iteration without an error.
    """
    while True:
        try:
            chunked = cursor.fetchmany(size)
        except Exception:
            chunked = []
        if not chunked:
            break
        yield from chunked


def get_dict_cursor(cn: Any) -> Cursor:
    """A new Cursor on cn whose rows read as dicts.
    """
    raw_conn = cn.connection if hasattr(cn, 'connection') else cn
    strategy = get_db_strategy(cn)
    cursor = strategy.create_dict_cursor(raw_conn)
    return Cursor(cursor, cn, strategy)


def extract_column_info(cursor: Any, table_name: str | None = None) -> list[Any]:
    """Column metadata for the cursor's current result set.

    Parameters
    ----------
    cursor : Any
        Cursor whose connwrapper names the dialect.
    table_name : str | None, default None
        Table the columns come from, where known.

    Returns
    -------
    list[Any]
        Column per description entry, also stored on cursor.columns.
        [] when the statement returned no result set, and then
        cursor.columns is left as it was.
    """
    if cursor.description is None:
        return []

    columns = columns_from_cursor_description(
        cursor,
        cursor.connwrapper.dialect,
        table_name,
        cursor.connwrapper)
    cursor.columns = columns
    return columns


def load_data(cursor: Any, columns: list[Any] | None = None,
              **kwargs: Any) -> Any:
    """The cursor's remaining rows, shaped by the connection's data loader.

    Parameters
    ----------
    cursor : Any
        Cursor positioned on a result set.
    columns : list[Any] | None, default None
        Column metadata. None reads it with extract_column_info.
    **kwargs : Any
        Passed through to options.data_loader.

    Returns
    -------
    Any
        Whatever options.data_loader builds from the rows as dicts and
        the columns.
    """
    if columns is None:
        columns = extract_column_info(cursor)

    data = []
    for row in cursor.fetchall() or []:
        adapter = RowAdapter.create(cursor.connwrapper, row)
        if hasattr(adapter, 'cursor'):
            adapter.cursor = cursor
        data.append(adapter.to_dict())

    data_loader = cursor.connwrapper.options.data_loader
    return data_loader(data, columns, **kwargs)


def process_multiple_result_sets(cursor: Any, return_all: bool = False,
                                 prefer_first: bool = False,
                                 **kwargs: Any) -> list[Any] | Any:
    """Load every result set of a statement and pick what to return.

    Parameters
    ----------
    cursor : Any
        Cursor positioned on the first result set.
    return_all : bool, default False
        Return every result set.
    prefer_first : bool, default False
        Return the first result set; ignored under return_all.
    **kwargs : Any
        Passed through to load_data.

    Returns
    -------
    list[Any] | Any
        Every result set the loader did not turn into None, in order,
        under return_all; else the first under prefer_first; else the
        one with the most rows, the earlier on a tie. [] when no result
        set loads.
    """
    result_sets: list[Any] = []
    largest_result = None
    largest_size = 0

    result = load_data(cursor, columns=extract_column_info(cursor), **kwargs)
    if result is not None:
        result_sets.append(result)
        largest_result = result
        largest_size = len(result)

    while cursor.nextset():
        result = load_data(
            cursor, columns=extract_column_info(cursor), **kwargs)
        if result is not None:
            result_sets.append(result)
            if len(result) > largest_size:
                largest_result = result
                largest_size = len(result)

    if not result_sets:
        return []
    if return_all:
        return result_sets
    if prefer_first:
        return result_sets[0]
    return largest_result
