"""
Basic database query execution functions.
"""
import logging

from database.utils.connection_utils import check_connection
from database.utils.sql import handle_query_params

logger = logging.getLogger(__name__)


@check_connection
@handle_query_params
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
        processed_sql, processed_args = prepare_sql_params_for_execution(sql, args, cn)

        # Apply SQL Server-specific parameter handling if needed
        from database.utils.connection_utils import is_pyodbc_connection
        if is_pyodbc_connection(cn):
            from database.utils.sqlserver_utils import prepare_sqlserver_params
            processed_sql, processed_args = prepare_sqlserver_params(processed_sql, processed_args)

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
