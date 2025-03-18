"""
Database cursor wrappers.
"""
import logging
import re
import time
from functools import wraps
from numbers import Number

from database.utils.auto_commit import ensure_commit
from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pyodbc_connection
from database.utils.connection_utils import is_sqlite3_connection
from database.utils.connection_utils import isconnection
from database.utils.sql import has_placeholders, sanitize_sql_for_logging
from database.utils.sql import standardize_placeholders
from database.utils.sqlserver_utils import ensure_identity_column_named
from database.utils.sqlserver_utils import handle_unnamed_columns_error
from database.utils.sqlserver_utils import prepare_sqlserver_params

from libb import collapse

logger = logging.getLogger(__name__)


def dumpsql(func):
    """Decorator for logging SQL queries and parameters"""
    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        this_cn = isconnection(cn) and cn or cn.connnection
        sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql, args)
        try:
            logger.debug(f'SQL:\n{sanitized_sql}\nargs: {sanitized_args}')
            return func(cn, sql, *args, **kwargs)
        except:
            logger.error(f'Error with query:\nSQL:\n{sanitized_sql}\nargs: {sanitized_args}')
            raise
    return wrapper


class CursorWrapper:
    """Wraps a cursor object to track execution calls and time"""

    def __init__(self, cursor, connwrapper):
        self.cursor = cursor
        self.connwrapper = connwrapper

    def __getattr__(self, name):
        """Delegate any members to the underlying cursor."""
        return getattr(self.cursor, name)

    def __iter__(self):
        return IterChunk(self.cursor)

    @dumpsql
    def execute(self, sql, *args, **kwargs):
        """Time the call and tell the connection wrapper that created this connection."""
        start = time.time()

        # Extract custom arguments
        auto_commit = kwargs.pop('auto_commit', True)

        try:
            # Execute the SQL with appropriate parameter handling
            self._execute_sql(sql, args)

            # Log the result for PostgreSQL
            if is_psycopg_connection(self.connwrapper):
                logger.debug(f'Query result: {self.cursor.statusmessage}')

        except Exception as e:
            # Handle SQL Server specific errors
            if is_pyodbc_connection(self.connwrapper):
                self._handle_sqlserver_error(e, sql, args)
            else:
                raise
        finally:
            # Record timing information
            end = time.time()
            self.connwrapper.addcall(end - start)
            logger.debug('Query time: %f' % (end - start))

        # Commit automatically if needed and we're not in a transaction
        if auto_commit and not getattr(self.connwrapper, 'in_transaction', False):
            ensure_commit(self.connwrapper)

        return self.cursor.rowcount

    def _execute_sql(self, sql, args):
        """Execute SQL with parameter handling."""
        self._original_sql = sql

        # Check if SQL has placeholders - if not and we have args, ignore the args
        if args and not has_placeholders(sql):
            args_count = len(tuple(collapse(args)))
            self.cursor.execute(sql)
            return

        # Handle dictionary parameters (for named placeholders)
        for arg in collapse(args):
            if isinstance(arg, dict):
                # Check if we're dealing with multiple statements in PostgreSQL
                is_multi_statement = ';' in sql and len([s for s in sql.split(';') if s.strip()]) > 1

                if is_multi_statement and is_psycopg_connection(self.connwrapper):
                    # PostgreSQL doesn't support multi-statement prepared statements with named parameters
                    self._execute_multi_statement_postgresql_named(sql, arg)
                    return

                # Standard execution for single statements
                self.cursor.execute(sql, arg)
                return

        # Handle SQL Server with its own specific method
        if is_pyodbc_connection(self.connwrapper):
            self._execute_sqlserver_query(sql, args)
            return

        # Check if we're dealing with multiple statements in PostgreSQL
        is_multi_statement = ';' in sql and len([s for s in sql.split(';') if s.strip()]) > 1

        if is_multi_statement and is_psycopg_connection(self.connwrapper) and args:
            # PostgreSQL doesn't support multi-statement prepared statements
            self._execute_multi_statement_postgresql(sql, args)
            return

        # Standard execution for all other cases
        self.cursor.execute(sql, *args)

    def _execute_sqlserver_query(self, sql, args):
        """Execute a query specifically for SQL Server with appropriate handling."""
        # Only add explicit names for required expressions (@@identity, etc)
        sql = ensure_identity_column_named(sql)

        # Save the final SQL
        self._sql = sql

        # Convert %s to ? for SQL Server
        sql = standardize_placeholders(self.connwrapper, sql)

        # Handle stored procedures with named parameters for SQL Server
        if args and 'EXEC ' in sql.upper() and '@' in sql:
            self._execute_stored_procedure(sql, args)
        else:
            # Standard execution for non-stored procedure queries
            if args:
                if len(args) == 1 and isinstance(args[0], list | tuple):
                    self.cursor.execute(sql, args[0])
                else:
                    self.cursor.execute(sql, args)
            else:
                self.cursor.execute(sql)

    def _execute_stored_procedure(self, sql, args):
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

    def _adjust_parameter_count(self, placeholder_count, param_count, processed_args):
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

    def _execute_with_parameters(self, sql, params, placeholder_count):
        """Execute SQL with parameters, handling single parameter case specially."""
        # Only use tuple unpacking for single parameters with single placeholders
        if placeholder_count == 1 and len(params) >= 1:
            # For a single placeholder, pass the first parameter directly
            self.cursor.execute(sql, params[0])
        else:
            # For multiple parameters, pass as a list
            self.cursor.execute(sql, params)

    def _execute_multi_statement_postgresql(self, sql, args):
        """Execute multiple PostgreSQL statements with proper parameter handling."""
        # Split into individual statements, keeping only non-empty ones
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]

        # Unwrap nested parameters if needed
        params = args[0] if len(args) == 1 and isinstance(args[0], list | tuple) else args

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

    def _execute_multi_statement_postgresql_named(self, sql, params_dict):
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

    def _handle_sqlserver_error(self, error, sql, args):
        """Handle SQL Server specific errors with automatic fixes."""

        modified_sql, should_retry = handle_unnamed_columns_error(error, sql, args)
        if should_retry:
            self.cursor.execute(modified_sql, *args)
        else:
            raise


def IterChunk(cursor, size=5000):
    """Alternative to builtin cursor generator
    breaks fetches into smaller chunks to avoid malloc problems
    for really large queries
    """
    while True:
        try:
            chunked = cursor.fetchmany(size)
        except:
            chunked = []
        if not chunked:
            break
        yield from chunked


def get_dict_cursor(cn):
    """Get a cursor that returns rows as dictionaries for the given connection type"""
    if hasattr(cn, 'connection'):
        raw_conn = cn.connection
    else:
        raw_conn = cn

    # Enhanced type detection with better support for connection wrappers
    if is_psycopg_connection(cn):
        cursor = raw_conn.cursor(row_factory=DictRowFactory)
    elif is_pyodbc_connection(cn):
        cursor = raw_conn.cursor()
    elif is_sqlite3_connection(cn):
        cursor = raw_conn.cursor()
    else:
        raise ValueError('Unknown connection type')

    return CursorWrapper(cursor, cn)


class DictRowFactory:
    """Row factory for psycopg that returns dictionary-like rows"""

    def __init__(self, cursor):
        # Make sure to get both name and type conversion function
        self.fields = [(c.name, postgres_type_convert(c.type_code)) for c in (cursor.description or [])]

    def __call__(self, values):
        return {name: cast(value)
                if isinstance(value, Number) and cast is not None
                else value
                for (name, cast), value in zip(self.fields, values)}


def postgres_type_convert(type_code):
    """Get type conversion function based on PostgreSQL type OID"""
    # Import here to avoid circular imports
    from database.adapters.type_mapping import postgres_types
    return postgres_types.get(type_code)
