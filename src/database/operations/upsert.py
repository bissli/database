"""
Upsert operations for database tables.
"""
import logging

from database.adapters.row_adapter import DatabaseRowAdapter
from database.core.transaction import Transaction
from database.operations.data import filter_table_columns, insert_rows
from database.operations.query import select, select_row_or_none
from database.operations.schema import get_table_primary_keys
from database.operations.schema import reset_table_sequence
from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pymssql_connection
from database.utils.connection_utils import is_sqlite3_connection
from database.utils.sql import quote_identifier

logger = logging.getLogger(__name__)


def _get_driver_type(cn):
    """Determine the database driver type from connection
    """
    # If the connection has a get_driver_type method, use it
    if hasattr(cn, 'get_driver_type'):
        return cn.get_driver_type()

    # Check the connection type using the utility functions
    if is_psycopg_connection(cn):
        return 'postgres'
    elif is_sqlite3_connection(cn):
        return 'sqlite'
    elif is_pymssql_connection(cn):
        return 'sqlserver'
    return None


def _build_upsert_sql(
    cn,
    table: str,
    columns: tuple[str],
    key_columns: list[str],
    update_always: list[str] = None,
    update_if_null: list[str] = None,
    driver: str = 'postgres',
) -> str:
    """Builds an UPSERT SQL statement for the specified database driver.

    Args:
        cn: Database connection
        table: Target table name
        columns: All columns to insert
        key_columns: Columns that form conflict constraint (primary/unique keys)
        update_always: Columns that should always be updated on conflict
        update_if_null: Columns that should only be updated if target is null
        driver: Database driver ('postgres', 'sqlite', 'sqlserver')
    """
    # Validate inputs
    if not key_columns:
        return _build_insert_sql(cn, table, columns)

    # Quote table name and all column names
    quoted_table = quote_identifier(driver, table)
    quoted_columns = [quote_identifier(driver, col) for col in columns]
    quoted_key_columns = [quote_identifier(driver, col) for col in key_columns]

    # PostgreSQL uses INSERT ... ON CONFLICT DO UPDATE
    if driver == 'postgres':
        # build the basic insert part
        insert_sql = _build_insert_sql(cn, table, columns)

        # build the on conflict clause
        conflict_sql = f"on conflict ({','.join(quoted_key_columns)})"

        # If no updates requested, do nothing
        if not (update_always or update_if_null):
            return f'{insert_sql} {conflict_sql} do nothing'

        # Build update expressions
        update_exprs = []
        if update_always:
            update_exprs.extend(f'{quote_identifier(driver, col)} = excluded.{quote_identifier(driver, col)}'
                                for col in update_always)
        if update_if_null:
            update_exprs.extend(f'{quote_identifier(driver, col)} = coalesce({quoted_table}.{quote_identifier(driver, col)}, excluded.{quote_identifier(driver, col)})'
                                for col in update_if_null)

        update_sql = f"do update set {', '.join(update_exprs)}"

        return f'{insert_sql} {conflict_sql} {update_sql}'

    # SQLite uses INSERT ... ON CONFLICT DO UPDATE (similar to PostgreSQL)
    elif driver == 'sqlite':
        # build the basic insert part
        insert_sql = _build_insert_sql(cn, table, columns)

        # build the on conflict clause
        conflict_sql = f"on conflict ({','.join(quoted_key_columns)})"

        # If no updates requested, do nothing
        if not (update_always or update_if_null):
            return f'{insert_sql} {conflict_sql} do nothing'

        # Build update expressions
        update_exprs = []
        if update_always:
            update_exprs.extend(f'{quote_identifier(driver, col)} = excluded.{quote_identifier(driver, col)}'
                                for col in update_always)
        if update_if_null:
            update_exprs.extend(f'{quote_identifier(driver, col)} = COALESCE({quoted_table}.{quote_identifier(driver, col)}, excluded.{quote_identifier(driver, col)})'
                                for col in update_if_null)

        update_sql = f"do update set {', '.join(update_exprs)}"

        return f'{insert_sql} {conflict_sql} {update_sql}'

    # SQL Server uses MERGE INTO
    elif driver == 'sqlserver':
        # For SQL Server we use MERGE statement
        placeholders = ','.join(['%s'] * len(columns))
        temp_alias = 'src'

        # Build conditions for matching keys
        match_conditions = ' AND '.join(
            f'target.{quote_identifier(driver, col)} = {temp_alias}.{quote_identifier(driver, col)}' for col in key_columns
        )

        # Build column value list for source
        source_values = ', '.join(f'%s as {quote_identifier(driver, col)}' for col in columns)

        # Build UPDATE statements
        update_cols = []
        if update_always:
            update_cols.extend(update_always)
        if update_if_null:
            # For SQL Server, handle COALESCE in the driver logic instead
            update_cols.extend(update_if_null)

        update_clause = ''
        if update_cols:
            update_statements = ', '.join(
                f'target.{quote_identifier(driver, col)} = {temp_alias}.{quote_identifier(driver, col)}' for col in update_cols
            )
            update_clause = f'WHEN MATCHED THEN UPDATE SET {update_statements}'
        else:
            # If no updates but we have keys, just match without updating
            update_clause = 'WHEN MATCHED THEN DO NOTHING'

        # Build INSERT statements
        quoted_all_columns = ', '.join(quoted_columns)
        source_columns = ', '.join(f'{temp_alias}.{quote_identifier(driver, col)}' for col in columns)

        # Full MERGE statement
        merge_sql = f"""
        MERGE INTO {quoted_table} AS target
        USING (SELECT {source_values}) AS {temp_alias}
        ON {match_conditions}
        {update_clause}
        WHEN NOT MATCHED THEN INSERT ({quoted_all_columns}) VALUES ({source_columns});
        """

        return merge_sql

    else:
        raise ValueError(f'Driver {driver} not supported for UPSERT operations')


def _build_insert_sql(cn, table: str, columns: tuple[str]) -> str:
    """Builds the INSERT part of the SQL statement
    """
    driver = _get_driver_type(cn)
    placeholders = ','.join(['%s'] * len(columns))
    quoted_table = quote_identifier(driver, table)
    quoted_columns = ','.join(quote_identifier(driver, col) for col in columns)
    return f'insert into {quoted_table} ({quoted_columns}) values ({placeholders})'


def _prepare_rows_for_upsert(cn, table, rows):
    """Prepare and validate rows for upsert operation"""
    # Include only columns that exist in the table
    filtered_rows = filter_table_columns(cn, table, rows)
    if not filtered_rows:
        logger.warning(f'No valid columns found for {table} after filtering')
        return None
    return tuple(filtered_rows)


def _filter_update_columns(columns, update_cols, key_cols):
    """Filter update columns to ensure they're valid"""
    if not update_cols:
        return []
    return [c for c in update_cols if c in columns and c not in key_cols]


def _execute_standard_upsert(cn, table, rows, columns, key_cols,
                             update_always, update_ifnull, driver):
    """Execute standard upsert operation for supported databases"""
    # Build the SQL statement
    sql = _build_upsert_sql(
        cn=cn,
        table=table,
        columns=columns,
        key_columns=key_cols,
        update_always=update_always,
        update_if_null=update_ifnull,
        driver=driver
    )

    rc = 0
    with Transaction(cn) as tx:
        for row in rows:
            ordered_values = [row[col] for col in columns]
            rc += tx.execute(sql, *ordered_values)

    if rc != len(rows):
        logger.debug(f'{len(rows) - rc} rows were skipped due to existing constraints')
    return rc


def _fetch_existing_rows(tx_or_cn, table, rows, key_cols):
    """Fetch existing rows for a set of key values"""
    existing_rows = {}
    driver_type = _get_driver_type(tx_or_cn)
    quoted_table = quote_identifier(driver_type, table)

    # Group rows by key columns to minimize database queries
    key_groups = {}
    for row in rows:
        row_key = tuple(row[key] for key in key_cols)
        key_groups.setdefault(row_key, True)

    # Build query to fetch all needed rows at once if possible
    if len(key_groups) < 100:  # Arbitrary limit to avoid huge queries
        # Build WHERE clause for fetching all needed rows at once
        where_conditions = []
        params = []

        for row_key in key_groups:
            condition_parts = []
            for i, key_col in enumerate(key_cols):
                quoted_key_col = quote_identifier(driver_type, key_col)
                condition_parts.append(f'{quoted_key_col} = %s')
                params.append(row_key[i])

            where_conditions.append(f"({' AND '.join(condition_parts)})")

        if where_conditions:
            where_clause = ' OR '.join(where_conditions)
            sql = f'SELECT * FROM {quoted_table} WHERE {where_clause}'

            # Fetch all matching rows at once
            result = select(tx_or_cn, sql, *params)
            for row in result:
                adapter = DatabaseRowAdapter.create(tx_or_cn, row)
                row_key = tuple(adapter.get_value(key) for key in key_cols)
                existing_rows[row_key] = adapter.to_dict()
    else:
        # Too many keys, fetch rows individually
        for row in rows:
            key_values = [row[key] for key in key_cols]
            key_conditions = ' AND '.join([f'{quote_identifier(driver_type, key)} = %s' for key in key_cols])
            existing_row = select_row_or_none(tx_or_cn, f'SELECT * FROM {quoted_table} WHERE {key_conditions}', *key_values)

            if existing_row:
                row_key = tuple(row[key] for key in key_cols)
                existing_rows[row_key] = existing_row

    return existing_rows


def _upsert_sqlserver_with_nulls(cn, table, rows, columns, key_cols,
                                 update_always, update_ifnull):
    """Special handling for SQL Server with NULL-preserving updates"""
    logger.warning('SQL Server MERGE with NULL preservation uses a specialized approach')

    driver = _get_driver_type(cn)

    with Transaction(cn) as tx:
        # First retrieve existing rows to handle COALESCE logic, passing the transaction
        existing_rows = _fetch_existing_rows(tx, table, rows, key_cols)

        # Apply NULL-preservation logic
        for row in rows:
            row_key = tuple(row[key] for key in key_cols)
            existing_row = existing_rows.get(row_key)

            if existing_row:
                # Apply update_cols_ifnull logic manually
                for col in update_ifnull:
                    from libb import is_null
                    if col in existing_row and not is_null(existing_row.get(col)):
                        # Keep existing non-NULL value
                        row[col] = existing_row.get(col)

        # Build and execute the MERGE statement
        sql = _build_upsert_sql(
            cn=tx,
            table=table,
            columns=columns,
            key_columns=key_cols,
            update_always=update_always,  # Only handle "always update" columns
            update_if_null=[],  # No special NULL handling needed anymore since we modified the rows
            driver=driver
        )

        # For each row, convert values to ordered list for SQL parameters
        row_count = 0
        for row in rows:
            ordered_values = [row[col] for col in columns]
            row_count += tx.execute(sql, *ordered_values)

        return row_count


def upsert_rows(
    cn,
    table: str,
    rows: tuple[dict],
    update_cols_key: list = None,
    update_cols_always: list = None,
    update_cols_ifnull: list = None,
    reset_sequence: bool = False,
    **kw
):
    """
    Performs an UPSERT operation for multiple rows with configurable update behavior.

    Args:
        cn: Database connection
        table: Target table name
        rows: Rows to insert/update
        update_cols_key: Columns that form the conflict constraint
        update_cols_always: Columns that should always be updated on conflict
        update_cols_ifnull: Columns that should only be updated if target is null
        reset_sequence: Whether to reset the table's sequence after operation
    """
    if not rows:
        logger.debug('Skipping upsert of empty rows')
        return 0

    # Get database driver type
    driver = _get_driver_type(cn)
    if not driver:
        raise ValueError('Unsupported database connection for upsert_rows')

    # Warning for SQL Server implementation
    if driver == 'sqlserver':
        logger.warning('SQL Server MERGE implementation is experimental and may have limitations')

    # Filter and validate rows and columns
    rows = _prepare_rows_for_upsert(cn, table, rows)
    if not rows:
        return 0

    columns = tuple(rows[0].keys())

    # Get primary keys if not specified
    update_cols_key = update_cols_key or get_table_primary_keys(cn, table)
    if not update_cols_key:
        logger.warning(f'No primary keys found for {table}, falling back to INSERT')
        return insert_rows(cn, table, rows)

    # Filter update columns to only valid ones
    update_cols_always = _filter_update_columns(columns, update_cols_always, update_cols_key)
    update_cols_ifnull = _filter_update_columns(columns, update_cols_ifnull, update_cols_key)

    try:
        # Handle the specific database driver
        if driver == 'sqlserver' and update_cols_ifnull:
            # For SQL Server with NULL preservation, use specialized function
            return _upsert_sqlserver_with_nulls(cn, table, rows, columns,
                                                update_cols_key, update_cols_always,
                                                update_cols_ifnull)
        else:
            # Standard approach for PostgreSQL, SQLite and simple SQL Server cases
            rc = _execute_standard_upsert(cn, table, rows, columns,
                                          update_cols_key, update_cols_always,
                                          update_cols_ifnull, driver)
    finally:
        if reset_sequence:
            reset_table_sequence(cn, table)

    return rc
