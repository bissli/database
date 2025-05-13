"""
Upsert operations for database tables.

This module provides functionality to insert or update rows in database tables
based on constraint violations, with configurable update behavior for existing rows.
"""
import logging

from database.adapters.structure import RowStructureAdapter
from database.operations.data import insert_rows
from database.operations.query import select
from database.operations.schema import get_table_columns
from database.operations.schema import get_table_primary_keys
from database.operations.schema import reset_table_sequence
from database.strategy import get_db_strategy
from database.utils.connection_utils import get_dialect_name
from database.utils.sql import get_param_limit_for_db, quote_identifier
from database.utils.sql_generation import build_insert_sql

from libb import is_null

logger = logging.getLogger(__name__)


def filter_columns_by_table(cn, table: str, input_dict: dict) -> dict:
    """Filter dictionary keys to match valid table columns using case-insensitive matching.

    Takes a database connection, table name, and an input dictionary, and returns a new
    dictionary with only keys that match column names in the table (case-insensitive),
    with the keys converted to the database's exact column case.

    Parameters
        cn: Database connection
        table: Target table name
        input_dict: Dictionary with keys to filter

    Returns
        Dictionary with only valid keys, using database's exact column case
    """
    # Get table columns with exact case
    table_columns = get_table_columns(cn, table)

    # Create case mapping for case-insensitive lookup
    case_map = {col.lower(): col for col in table_columns}

    # Filter dictionary keys to match valid columns
    result = {}
    for key, val in input_dict.items():
        # Case-insensitive lookup of the column name
        exact_key = case_map.get(key.lower())
        if exact_key:
            result[exact_key] = val

    return result


def _build_upsert_sql(
    cn,
    table: str,
    columns: tuple[str],
    key_columns_or_constraint: list[str] | str = None,
    update_always: list[str] = None,
    update_if_null: list[str] = None,
    dialect: str = 'postgresql',
) -> str:
    """Build an UPSERT SQL statement for the specified database driver.

    Creates database-specific SQL for upsert operations with configurable conflict
    resolution rules, handling differences between PostgreSQL, SQLite, and SQL Server.

    Parameters
        cn: Database connection
        table: Target table name
        columns: All columns to insert
        key_columns_or_constraint: Columns that form conflict constraint or constraint name (PostgreSQL only)
        update_always: Columns that should always be updated on conflict
        update_if_null: Columns that should only be updated if target is null
        : Database type ('postgresql', 'sqlite', 'mssql')

    Returns
        SQL statement string for the upsert operation

    Raises
        ValueError: If using a constraint name with non-PostgreSQL database
                   or if update columns are not in column list
    """
    # Determine if we're using constraint name or columns
    constraint_name = None
    key_columns = None

    if isinstance(key_columns_or_constraint, str):
        constraint_name = key_columns_or_constraint
        if dialect != 'postgresql':
            raise ValueError(f'Constraint name is only supported for PostgreSQL, not {dialect}')
    else:
        key_columns = key_columns_or_constraint

    # Fall back to regular insert if no conflict detection is requested
    if not key_columns and not constraint_name:
        return _build_insert_sql(cn, table, columns)

    columns_lower = {col.lower() for col in columns}

    # Filter update columns to only include those that exist in the main columns list
    if update_always:
        update_always = [col for col in update_always if col.lower() in columns_lower]

    if update_if_null:
        update_if_null = [col for col in update_if_null if col.lower() in columns_lower]

    # Quote table name and all column names
    quoted_table = quote_identifier(table, dialect)
    quoted_columns = [quote_identifier(col, dialect) for col in columns]
    quoted_key_columns = [quote_identifier(col, dialect) for col in key_columns] if key_columns else []

    # PostgreSQL uses INSERT ... ON CONFLICT DO UPDATE
    if dialect == 'postgresql':
        insert_sql = _build_insert_sql(cn, table, columns)

        if constraint_name:
            expressions = get_db_strategy(cn).get_constraint_definition(cn, table, constraint_name)
            conflict_sql = f'on conflict {expressions}'
        else:
            conflict_sql = f"on conflict ({','.join(quoted_key_columns)})"

        # If no updates requested, do nothing
        if not (update_always or update_if_null):
            return f'{insert_sql} {conflict_sql} do nothing RETURNING *'

        # Build update expressions
        update_exprs = []
        if update_always:
            update_exprs.extend(f'{quote_identifier(col, dialect)} = excluded.{quote_identifier(col, dialect)}'
                                for col in update_always)
        if update_if_null:
            update_exprs.extend(f'{quote_identifier(col, dialect)} = coalesce({quoted_table}.{quote_identifier(col, dialect)}, excluded.{quote_identifier(col, dialect)})'
                                for col in update_if_null)

        update_sql = f"do update set {', '.join(update_exprs)}"

        return f'{insert_sql} {conflict_sql} {update_sql} RETURNING *'

    # SQLite uses INSERT ... ON CONFLICT DO UPDATE (similar to PostgreSQL)
    elif dialect == 'sqlite':
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
            update_exprs.extend(f'{quote_identifier(col, dialect)} = excluded.{quote_identifier(col, dialect)}'
                                for col in update_always)
        if update_if_null:
            update_exprs.extend(f'{quote_identifier(col, dialect)} = COALESCE({quoted_table}.{quote_identifier(col, dialect)}, excluded.{quote_identifier(col, dialect)})'
                                for col in update_if_null)

        update_sql = f"do update set {', '.join(update_exprs)}"

        return f'{insert_sql} {conflict_sql} {update_sql}'

    # SQL Server uses MERGE INTO
    elif dialect == 'mssql':
        # For SQL Server we use MERGE statement
        placeholders = ','.join(['?'] * len(columns))
        temp_alias = 'src'

        # Build conditions for matching keys
        match_conditions = ' AND '.join(
            f'target.{quote_identifier(col, dialect)} = {temp_alias}.{quote_identifier(col, dialect)}' for col in key_columns
        )

        # Build column value list for source
        source_values = ', '.join(f'? as {quote_identifier(col, dialect)}' for col in columns)

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
                f'target.{quote_identifier(col, dialect)} = {temp_alias}.{quote_identifier(col, dialect)}' for col in update_cols
            )
            update_clause = f'WHEN MATCHED THEN UPDATE SET {update_statements}'
        else:
            # If no updates but we have keys, just match without updating
            update_clause = 'WHEN MATCHED THEN DO NOTHING'

        # Build INSERT statements
        quoted_all_columns = ', '.join(quoted_columns)
        source_columns = ', '.join(f'{temp_alias}.{quote_identifier(col, dialect)}' for col in columns)

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
        raise ValueError(f'Database type {dialect} not supported for UPSERT operations')


def _build_insert_sql(cn, table: str, columns: tuple[str]) -> str:
    """Build the INSERT part of the SQL statement.

    Parameters
        cn: Database connection
        table: Target table name
        columns: Columns to include in the insert statement

    Returns
        SQL INSERT statement string
    """
    dialect = get_dialect_name(cn)
    return build_insert_sql(dialect, table, columns)


def _prepare_rows_for_upsert(cn, table, rows):
    """Prepare and validate rows for upsert operation with case-insensitive matching.

    Filters rows to include only valid columns that exist in the target table,
    correcting column case to match the database's exact column case.

    Parameters
        cn: Database connection
        table: Target table name
        rows: Collection of row dictionaries to process

    Returns
        Tuple of filtered row dictionaries with corrected column names, or None if no valid rows
    """
    if not rows:
        return None

    # Filter rows - keep only valid columns with database's exact column case
    filtered_rows = []
    for row in rows:
        corrected_row = filter_columns_by_table(cn, table, row)

        # Only include rows that have at least one valid column
        if corrected_row:
            filtered_rows.append(corrected_row)

    if not filtered_rows:
        logger.warning(f'No valid columns found for {table} after filtering')
        return None

    return tuple(filtered_rows)


def _execute_standard_upsert(
    cn,
    table,
    rows,
    columns,
    key_cols_or_constraint,
    update_always,
    update_ifnull,
    dialect,
):
    """Execute standard upsert operation for supported databases using executemany.

    Handles the execution of upsert operations for PostgreSQL, SQLite, and SQL Server,
    automatically batching rows to stay within database parameter limits.

    Parameters
        cn: Database connection
        table: Target table name
        rows: Rows to insert/update
        columns: Column names in correct case
        key_cols_or_constraint: Key columns list or constraint name
        update_always: Columns to always update on conflict
        update_ifnull: Columns to update only if target value is null
        dialect: Database type identifier

    Returns
        Number of rows affected by the operation
    """

    sql = _build_upsert_sql(
        cn=cn,
        table=table,
        columns=columns,
        key_columns_or_constraint=key_cols_or_constraint,
        update_always=update_always,
        update_if_null=update_ifnull,
        dialect=dialect
    )

    param_limit = get_param_limit_for_db(dialect)
    max_rows_per_batch = max(1, param_limit // max(1, len(columns)))

    if dialect == 'postgresql' and len(columns) > 10:
        max_rows_per_batch = min(max_rows_per_batch, 1000)

    logger.debug(f'Upserting {len(rows)} rows in batches of max {max_rows_per_batch} rows')

    # Execute in batches
    total_affected = 0
    cursor = cn.cursor()

    for batch_idx in range(0, len(rows), max_rows_per_batch):
        batch_rows = rows[batch_idx:batch_idx + max_rows_per_batch]
        params = [[row[col] for col in columns] for row in batch_rows]

        rc = cursor.executemany(sql, params)

        if isinstance(rc, int):
            total_affected += rc
            if rc != len(batch_rows):
                logger.debug(f'Batch {batch_idx//max_rows_per_batch+1}: {len(batch_rows) - rc} rows skipped')
        else:
            # Some drivers don't return row count
            logger.debug(f'Batch {batch_idx//max_rows_per_batch+1}: unknown count affected')

    return total_affected


def _fetch_existing_rows(tx_or_cn, table, rows, key_cols):
    """Fetch existing rows for a set of key values - optimized for batch fetching.

    Retrieves existing rows from the database using an efficient batching strategy
    to minimize database queries while staying within parameter limits.

    Parameters
        tx_or_cn: Database transaction or connection
        table: Target table name
        rows: Rows to check for existing matches
        key_cols: Key columns to use for matching

    Returns
        Dictionary mapping row keys to existing row data
    """
    existing_rows = {}

    dialect = get_dialect_name(tx_or_cn)
    quoted_table = quote_identifier(table, dialect)

    # Group rows by key columns to minimize database queries
    key_groups = {}
    for row in rows:
        row_key = tuple(row[key] for key in key_cols)
        key_groups.setdefault(row_key, True)

    # Handle empty key_cols case
    if not key_cols:
        logger.warning('No key columns specified for fetching existing rows')
        return {}

    # Calculate a reasonable parameter limit (accounting for complex queries)
    safe_param_limit = max(50, get_param_limit_for_db(dialect) // 2)

    # Each key will consume key_cols number of parameters
    max_keys_per_query = max(1, safe_param_limit // len(key_cols))

    # Handle cases where we can batch queries
    if len(key_groups) <= max_keys_per_query:
        # Build WHERE clause for fetching all needed rows at once
        where_conditions = []
        params = []

        for row_key in key_groups:
            condition_parts = []
            for j, key_col in enumerate(key_cols):
                quoted_key_col = quote_identifier(key_col, dialect)
                # Use correct placeholder based on database
                placeholder = '?' if dialect == 'mssql' else '%s'
                condition_parts.append(f'{quoted_key_col} = {placeholder}')
                params.append(row_key[j])

            where_conditions.append(f"({' AND '.join(condition_parts)})")

        if where_conditions:
            where_clause = ' OR '.join(where_conditions)
            sql = f'SELECT * FROM {quoted_table} WHERE {where_clause}'

            # Fetch all matching rows at once
            result = select(tx_or_cn, sql, *params)
            for row in result:
                adapter = RowStructureAdapter.create(tx_or_cn, row)
                row_key = tuple(adapter.get_value(key) for key in key_cols)
                existing_rows[row_key] = adapter.to_dict()
    else:
        # For very large datasets, process in batches to avoid parameter limits
        keys_list = list(key_groups.keys())
        for batch_idx in range(0, len(keys_list), max_keys_per_query):
            batch_keys = keys_list[batch_idx:batch_idx+max_keys_per_query]

            where_conditions = []
            params = []

            for row_key in batch_keys:
                condition_parts = []
                for j, key_col in enumerate(key_cols):
                    quoted_key_col = quote_identifier(key_col, dialect)
                    # Use correct placeholder based on database
                    placeholder = '?' if dialect == 'mssql' else '%s'
                    condition_parts.append(f'{quoted_key_col} = {placeholder}')
                    params.append(row_key[j])

                where_conditions.append(f"({' AND '.join(condition_parts)})")

            where_clause = ' OR '.join(where_conditions)
            sql = f'SELECT * FROM {quoted_table} WHERE {where_clause}'

            # Fetch batch of rows
            result = select(tx_or_cn, sql, *params)
            for row in result:
                adapter = RowStructureAdapter.create(tx_or_cn, row)
                row_key = tuple(adapter.get_value(key) for key in key_cols)
                existing_rows[row_key] = adapter.to_dict()

    return existing_rows


def _upsert_sqlserver_with_nulls(
    cn,
    table,
    rows,
    columns,
    key_cols,
    update_always,
    update_ifnull,
):
    """Special handling for SQL Server with NULL-preserving updates.

    Implements a specialized approach for SQL Server to preserve existing non-null values
    during upsert operations, which isn't directly supported by MERGE statements.

    Parameters
        cn: Database connection
        table: Target table name
        rows: Rows to insert/update
        columns: Column names in correct case
        key_cols: Key columns list
        update_always: Columns to always update on conflict
        update_ifnull: Columns to update only if target value is null

    Returns
        Number of rows affected by the operation
    """

    logger.warning('SQL Server MERGE with NULL preservation uses a specialized approach')
    dialect = 'mssql'

    # First retrieve existing rows to handle COALESCE logic
    existing_rows = _fetch_existing_rows(cn, table, rows, key_cols)

    # Apply NULL-preservation logic to each row
    for row in rows:
        row_key = tuple(row[key] for key in key_cols)
        existing_row = existing_rows.get(row_key)

        if existing_row:
            # Apply update_cols_ifnull logic manually
            for col in update_ifnull:
                if col in existing_row and not is_null(existing_row.get(col)):
                    # Keep existing non-NULL value
                    row[col] = existing_row.get(col)

    # Build MERGE statement with only update_always columns
    sql = _build_upsert_sql(
        cn=cn,
        table=table,
        columns=columns,
        key_columns=key_cols,
        update_always=update_always,  # Only handle "always update" columns
        update_if_null=[],  # No special NULL handling needed anymore since we modified the rows
        dialect=dialect
    )

    params = [[row[col] for col in columns] for row in rows]

    cursor = cn.cursor()
    rc = cursor.executemany(sql, params)


def upsert_rows(
    cn,
    table: str,
    rows: tuple[dict],
    update_cols_key: list | str = None,
    update_cols_always: list = None,
    update_cols_ifnull: list = None,
    reset_sequence: bool = False,
    **kw
):
    """Perform an UPSERT operation (INSERT or UPDATE) for multiple rows with configurable update behavior.

    This function handles the "upsert" pattern: INSERT new rows or UPDATE existing ones based on
    a unique constraint violation. The exact implementation varies by database type and is optimized
    for each supported database (PostgreSQL, SQLite, and SQL Server).

    Parameters
        cn: Database connection
            Active connection to the target database.

        table: Target table name in the database.

        rows: Collection of row dictionaries to insert/update. Each dictionary's keys should match
            column names in the target table. Empty input will result in no operation.

        update_cols_key: Either a list of columns that form the conflict constraint (typically primary
            or unique keys), or a string with the constraint name (PostgreSQL only).
            If omitted:
            - For all DBs: primary keys will be automatically determined from table schema
            - If no keys can be determined: falls back to plain INSERT operation
            When providing a constraint name (string), it is only supported in PostgreSQL and
            ignored for other database types.

        update_cols_always: Columns that should always be updated on conflict.
            If omitted, conflicting rows won't have any columns updated.
            Cannot include primary key columns.

        update_cols_ifnull: Columns that should only be updated if target value is null (preserves
            non-null values). If omitted, no conditional updates will be performed.
            Implemented differently by database:
            - PostgreSQL/SQLite: Uses SQL COALESCE function
            - SQL Server: Uses a specialized implementation to preserve existing non-null values

        reset_sequence: Whether to reset the table's auto-increment sequence after operation.
            Useful after bulk loads to ensure next generated ID is correct.

    Returns
        Number of rows affected by the operation.
        For PostgreSQL/SQLite, this should match the input row count.
        For SQL Server, may return -1 for some operations.

    Raises
        ValueError: If constraint name provided for non-PostgreSQL database
        ValueError: If using an unsupported database connection
    """
    if not rows:
        logger.debug('Skipping upsert of empty rows')
        return 0

    # Get database dialect
    dialect = get_dialect_name(cn)

    # Check if update_cols_key is a constraint name (string)
    constraint_name = None
    if isinstance(update_cols_key, str):
        constraint_name = update_cols_key
        update_cols_key = None
        # Check constraint name is only used with PostgreSQL
        if dialect != 'postgresql':
            raise ValueError(f'Constraint name is only supported for PostgreSQL, not {dialect}')

    # Warning for SQL Server implementation
    if dialect == 'mssql':
        logger.warning('SQL Server MERGE implementation is experimental and may have limitations')

    # Log information about batching for large datasets
    if len(rows) > 1000:
        logger.debug(f'Processing large dataset with {len(rows)} rows - will use batching to avoid SSL errors')

    # Get table columns to validate input
    table_columns = set(get_table_columns(cn, table))

    # Filter and validate rows
    rows = _prepare_rows_for_upsert(cn, table, rows)
    if not rows:
        return 0

    # Get columns from the first row
    input_columns = set(rows[0].keys())

    # Create case mapping for case-insensitive validation
    case_map = {col.lower(): col for col in table_columns}
    table_columns_lower = set(case_map.keys())

    # Find columns that don't exist in the table (case-insensitive check)
    invalid_columns = [col for col in input_columns if col.lower() not in table_columns_lower]
    if invalid_columns:
        logger.warning(f'Ignoring columns not present in the table schema: {invalid_columns}')

    # Filter to only valid columns with database's exact case, maintaining order from input
    columns = tuple(case_map.get(col.lower()) for col in rows[0]
                    if col.lower() in table_columns_lower)

    if not columns:
        logger.warning(f'No valid columns provided for table {table}')
        return 0

    # Get primary keys if not specified
    if not update_cols_key and not constraint_name:
        update_cols_key = get_table_primary_keys(cn, table)
        if not update_cols_key and dialect != 'postgresql':
            logger.warning(f'No primary keys found for {table}, falling back to INSERT')
            return insert_rows(cn, table, rows)

    try:
        # Handle the specific database type
        if dialect == 'mssql' and update_cols_ifnull:
            # For SQL Server with NULL preservation, use specialized function
            return _upsert_sqlserver_with_nulls(cn, table, rows, columns,
                                                update_cols_key, update_cols_always,
                                                update_cols_ifnull)
        else:
            # Standard approach for PostgreSQL, SQLite and simple SQL Server cases
            key_cols_or_constraint = constraint_name or update_cols_key
            rc = _execute_standard_upsert(cn, table, rows, columns,
                                          key_cols_or_constraint,
                                          update_cols_always, update_cols_ifnull,
                                          dialect)
    finally:
        if reset_sequence:
            reset_table_sequence(cn, table)

    return rc
