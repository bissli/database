"""
Basic database query execution functions.
"""
import logging

from database.utils.connection_utils import check_connection, get_dialect_name
from database.utils.sql import handle_query_params
from database.utils.sql import prepare_sql_params_for_execution

logger = logging.getLogger(__name__)


@check_connection
@handle_query_params
def execute(cn: object, sql: str, *args: object) -> int:
    """Execute a SQL query with the given parameters and return affected row count.

    Handles various SQL dialects and parameter styles.

    Args:
        cn: Database connection object
        sql: SQL query string to execute
        *args: Query parameters to be bound to the SQL

    Returns
        Number of affected rows (for modification queries).

    >>> import sqlite3
    >>> conn = sqlite3.connect(':memory:')
    >>> cursor = conn.cursor()
    >>> cursor.execute('CREATE TABLE test (id INTEGER, name TEXT)')
    <sqlite3.Cursor object at ...>
    >>> execute(conn, 'INSERT INTO test VALUES (?, ?)', 1, 'test')
    1
    """
    cursor = cn.cursor()
    try:
        is_simple_select = sql.strip().upper().startswith('SELECT') and "'%" in sql and '%s' not in sql and not args

        if is_simple_select:
            cursor.execute(sql)
            logger.debug(f'Executed simple SELECT query directly: {sql[:60]}...')
        else:
            dialect = get_dialect_name(cn)
            processed_sql, processed_args = prepare_sql_params_for_execution(sql, args, dialect)

            cursor.execute(processed_sql, processed_args)
            logger.debug(f'Executed query with {len(processed_args) if processed_args else 0} parameters: {processed_sql[:60]}...')

        rowcount = cursor.rowcount
        cn.commit()  # cursor closed
        return rowcount
    except Exception as e:
        try:
            cn.rollback()
        except:
            pass
        raise


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
