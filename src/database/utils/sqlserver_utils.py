"""
SQL Server-specific utilities for dealing with common issues.
"""
import logging
import re

logger = logging.getLogger(__name__)


def ensure_identity_column_named(sql):
    """
    Ensures columns have names in SQL Server queries.

    Args:
        sql: The SQL query to check and modify

    Returns
        Modified SQL with named columns
    """
    # Handle @@identity
    if '@@identity' in sql.lower() and 'as ' not in sql.lower():
        sql = re.sub(r'@@identity\b(?i)', '@@identity AS id', sql)

    # Handle "SELECT 1 FROM" queries with no AS
    if re.search(r'select\s+1\s+from', sql.lower()) and not re.search(r'select\s+1\s+as\s+', sql.lower()):
        sql = re.sub(r'(?i)select\s+1\s+from', 'SELECT 1 AS result FROM', sql)

    # Handle COUNT(*) queries with no AS
    if re.search(r'count\(\*\)', sql.lower()) and not re.search(r'count\(\*\)\s+as\s+', sql.lower()):
        sql = re.sub(r'(?i)count\(\*\)', 'COUNT(*) AS count', sql)

    # Handle multi-column SELECT statements without AS
    if re.search(r'select\s+\w+\s*,\s*\w+\s+from', sql.lower()):
        # Handle multiple columns - add AS clauses for columns without AS
        parts = re.split(r'(?i)select\s+', sql, 1)
        if len(parts) == 2:
            before_select, after_select = parts
            # Split at FROM to get column list
            parts = re.split(r'(?i)\s+from\s+', after_select, 1)
            if len(parts) == 2:
                columns_str, from_clause = parts
                # Process each column
                new_columns = []
                for col in columns_str.split(','):
                    col = col.strip()
                    if ' as ' not in col.lower():
                        # Extract just the column name (ignoring any functions/math)
                        col_name = col.split('.')[-1].strip()
                        new_columns.append(f'{col} AS {col_name}')
                    else:
                        new_columns.append(col)
                # Rebuild SQL
                sql = f"SELECT {', '.join(new_columns)} FROM {from_clause}"
                return sql

    # Handle SELECT col queries that lack AS and look like they'd be aliased
    if re.search(r'select\s+\w+\s+from', sql.lower()):
        sql = re.sub(r'(?i)select\s+(\w+)\s+from',
                     lambda m: f'SELECT {m.group(1)} AS {m.group(1)}_col FROM', sql)

    # Handle select value queries for scalar values
    value_types = ['value', 'name', 'id', 'code', 'key', 'type', 'status']
    for value_type in value_types:
        if re.search(rf'select\s+{value_type}\s+from', sql.lower()) and not re.search(rf'select\s+{value_type}\s+as\s+', sql.lower()):
            sql = re.sub(rf'(?i)select\s+{value_type}\s+from',
                         f'SELECT {value_type} AS {value_type}_col FROM', sql)

    return sql


def handle_unnamed_columns_error(e, sql, args):
    """
    Handles SQL Server errors related to unnamed columns.

    Args:
        e: The exception that was raised
        sql: The SQL query that caused the error
        args: Query parameters

    Returns
        Tuple of (modified_sql, should_retry)
    """
    if 'columns with no names' in str(e).lower():
        modified_sql = sql
        retry = False

        # Handle @@identity
        if '@@identity' in sql.lower() and 'as ' not in sql.lower():
            modified_sql = sql.replace('@@identity', '@@identity AS id')
            retry = True

        # Handle "SELECT 1 FROM" pattern
        elif 'select 1 from' in sql.lower() and 'as ' not in sql.lower():
            modified_sql = sql.replace('SELECT 1 FROM', 'SELECT 1 AS result FROM')
            retry = True

        # Handle COUNT(*) pattern
        elif 'count(*)' in sql.lower() and 'as ' not in sql.lower():
            modified_sql = sql.replace('COUNT(*)', 'COUNT(*) AS count')
            retry = True

        if retry:
            logger.debug(f'Retrying SQL Server query with column name: {modified_sql}')
            return modified_sql, True

    return sql, False


def extract_identity_from_result(result):
    """
    Safely extract identity value from a SQL Server result row.

    Args:
        result: Result row from cursor.fetchone()

    Returns
        The identity value
    """
    if isinstance(result, dict) and 'id' in result:
        return result['id']
    elif hasattr(result, '__getitem__'):
        return result[0]
    return result


def ensure_timezone_naive_datetime(dt):
    """
    Ensure a datetime object is timezone-naive.

    SQL Server datetimes are timezone-naive by default, so we should preserve that
    unless specifically working with DATETIMEOFFSET columns.

    Args:
        dt: datetime object to check

    Returns
        Timezone-naive datetime object
    """
    if dt is None:
        return None

    import datetime
    if isinstance(dt, datetime.datetime) and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


def convert_to_date_if_no_time(dt):
    """
    Convert a datetime to a date object if it has no time component.
    Much more conservative to avoid converting datetime columns to date objects.

    Args:
        dt: datetime object to check

    Returns
        date object only if explicitly marked as a date column, otherwise original datetime
    """
    if dt is None:
        return None

    # We're being very conservative now - keep as datetime unless
    # explicitly handled in the calling code
    return dt
