"""
Basic database query execution functions.
"""
import logging
from functools import wraps

from database.core.transaction import check_connection
from database.utils.connection_utils import is_pymssql_connection
from database.utils.connection_utils import is_sqlite3_connection
from database.utils.sql import handle_query_params, sanitize_sql_for_logging

logger = logging.getLogger(__name__)


def dumpsql(func):
    """Decorator for logging SQL queries and parameters"""
    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        try:
            # For postgres, logging happens in LoggingCursor
            if is_pymssql_connection(cn) or is_sqlite3_connection(cn):
                sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql, args)
                logger.debug(f'SQL:\n{sanitized_sql}\nargs: {sanitized_args}')
            return func(cn, sql, *args, **kwargs)
        except:
            sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql, args)
            logger.error(f'Error with query:\nSQL:\n{sanitized_sql}\nargs: {sanitized_args}')
            raise
    return wrapper


@check_connection
@handle_query_params
@dumpsql
def execute(cn, sql, *args):
    """Execute a SQL query with the given parameters

    Args:
        cn: Database connection
        sql: SQL query string
        *args: Query parameters

    Returns
        Number of affected rows
    """
    cursor = cn.cursor()
    try:
        # Process parameters one final time right before execution
        from database.utils.sql import prepare_sql_params_for_execution
        processed_sql, processed_args = prepare_sql_params_for_execution(sql, args)

        # Execute with the processed SQL and args
        cursor.execute(processed_sql, processed_args)
        rowcount = cursor.rowcount
        cn.commit()
        return rowcount
    except Exception as e:
        try:
            cn.rollback()
        except Exception:
            pass
        raise
