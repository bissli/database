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
        """Time the call and tell the connection wrapper that created this connection.
        """
        from database.utils.connection_utils import is_psycopg_connection

        start = time.time()

        # Handle dictionary parameters
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
        cursor = cn.cursor(as_dict=True)
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
