"""
Database cursor wrappers.
"""
import logging
import time
from numbers import Number

from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pymssql_connection
from database.utils.connection_utils import is_sqlite3_connection

from libb import collapse

logger = logging.getLogger(__name__)


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

    def execute(self, sql, *args, **kwargs):
        """Time the call and tell the connection wrapper that created this connection."""
        from database.utils.connection_utils import is_psycopg_connection
        from database.utils.connection_utils import is_pymssql_connection

        start = time.time()

        try:
            # Execute the SQL with appropriate parameter handling
            self._execute_sql(sql, args)

            # Log the result for PostgreSQL
            if is_psycopg_connection(self.connwrapper):
                logger.debug(f'Query result: {self.cursor.statusmessage}')

        except Exception as e:
            # Handle SQL Server specific errors
            if is_pymssql_connection(self.connwrapper):
                self._handle_sqlserver_error(e, sql, args)
            else:
                raise
        finally:
            # Record timing information
            end = time.time()
            self.connwrapper.addcall(end - start)
            logger.debug('Query time: %f' % (end - start))

        return self.cursor.rowcount

    def _execute_sql(self, sql, args):
        """Execute SQL with parameter handling."""
        from database.utils.connection_utils import is_pymssql_connection
        from database.utils.sqlserver_utils import ensure_identity_column_named

        # Handle dictionary parameters
        for arg in collapse(args):
            if isinstance(arg, dict):
                self.cursor.execute(sql, arg)
                return

        # Pre-process SQL for SQL Server to avoid common issues
        if is_pymssql_connection(self.connwrapper):
            sql = ensure_identity_column_named(sql)

        # Execute with standard parameters
        self.cursor.execute(sql, *args)

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
    if is_psycopg_connection(cn):
        cursor = cn.cursor(row_factory=DictRowFactory)
    elif is_pymssql_connection(cn):
        # For SQL Server, use a regular cursor for better handling of unnamed columns
        # We'll handle the dictionary conversion in the row adapter
        cursor = cn.cursor()
    elif is_sqlite3_connection(cn):
        cursor = cn.cursor()
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
    from database.adapters.type_adapters import postgres_types
    return postgres_types.get(type_code)
