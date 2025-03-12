"""
Database cursor wrappers.
"""
import logging
import time
from functools import wraps
from numbers import Number

from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pyodbc_connection, isconnection

from libb import collapse

logger = logging.getLogger(__name__)


def dumpsql(func):
    """Decorator for logging SQL queries and parameters"""
    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        from database.utils.sql import sanitize_sql_for_logging
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
        from database.utils.connection_utils import is_pyodbc_connection
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
            from database.utils.auto_commit import ensure_commit
            ensure_commit(self.connwrapper)

        return self.cursor.rowcount

    def _execute_sql(self, sql, args):
        """Execute SQL with parameter handling."""
        # Store original SQL for debugging
        self._original_sql = sql

        # Handle dictionary parameters
        for arg in collapse(args):
            if isinstance(arg, dict):
                logger.debug('Executing with dictionary parameter')
                self.cursor.execute(sql, arg)
                return

        # Handle SQL Server
        if is_pyodbc_connection(self.connwrapper):
            self._execute_sqlserver_query(sql, args)
        else:
            # Execute with standard parameters for other drivers
            self.cursor.execute(sql, *args)

    def _execute_sqlserver_query(self, sql, args):
        """Execute a query specifically for SQL Server with appropriate handling."""
        # Only add explicit names for required expressions (@@identity, etc)
        from database.utils.sqlserver_utils import ensure_identity_column_named
        sql = ensure_identity_column_named(sql)

        # Save the final SQL
        self._sql = sql

        # Convert %s to ? for SQL Server
        from database.utils.sql import standardize_placeholders
        sql = standardize_placeholders(self.connwrapper, sql)

        # Execute directly with pyodbc - preserving any column name information
        try:
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
        except Exception as e:
            logger.error(f'Error executing SQL: {e}')
            logger.error(f'SQL: {sql}')
            logger.error(f'Args: {args}')
            raise

    def _execute_stored_procedure(self, sql, args):
        """Handle execution of SQL Server stored procedures with parameters."""
        from database.utils.sqlserver_utils import prepare_sqlserver_params

        # Convert parameters to positional placeholders (?)
        processed_sql, processed_args = prepare_sqlserver_params(sql, args)

        # Count placeholders in the SQL
        placeholder_count = processed_sql.count('?')
        param_count = len(processed_args) if processed_args else 0

        # Log the processed SQL and parameters for debugging
        logger.debug(f'Processed SQL: {processed_sql}')
        logger.debug(f'Processed args: {processed_args}')
        logger.debug(f'Placeholder count: {placeholder_count}')

        # Execute with the processed SQL and args
        if not processed_args:
            self.cursor.execute(processed_sql)
            return

        try:
            processed_args = self._adjust_parameter_count(placeholder_count, param_count, processed_args)
            self._execute_with_parameters(processed_sql, processed_args, placeholder_count)
        except Exception as e:
            # Add diagnostic information to help debug parameter mismatches
            logger.error(f'SQL parameter mismatch: SQL has {placeholder_count} placeholders, '
                         f'args has {param_count} parameters')
            logger.error(f'Processed SQL: {processed_sql}')
            logger.error(f'Processed args: {processed_args}')
            raise

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

    def _handle_sqlserver_error(self, error, sql, args):
        """Handle SQL Server specific errors with automatic fixes."""
        from database.utils.sqlserver_utils import handle_unnamed_columns_error

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
    from database.utils.connection_utils import is_psycopg_connection
    from database.utils.connection_utils import is_sqlite3_connection

    if hasattr(cn, 'connection'):
        raw_conn = cn.connection
    else:
        raw_conn = cn

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
