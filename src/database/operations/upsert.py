"""
Upsert operations for database tables.
"""
import logging

from database.adapters.structure import RowStructureAdapter
from database.operations.data import insert_rows
from database.operations.query import execute_many, select
from database.operations.schema import get_table_columns
from database.operations.schema import get_table_primary_keys
from database.operations.schema import reset_table_sequence
from database.strategy import get_db_strategy
from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pyodbc_connection
from database.utils.connection_utils import is_sqlite3_connection
from database.utils.sql import get_param_limit_for_db, quote_identifier
from database.utils.sql_generation import build_insert_sql

from libb import is_null

logger = logging.getLogger(__name__)


def _get_db_type_from_connection(cn):
    """Get database type from connection object"""

    if is_psycopg_connection(cn):
        return 'postgresql'
    elif is_pyodbc_connection(cn):
        return 'mssql'
    elif is_sqlite3_connection(cn):
        return 'sqlite'
    else:
        return 'unknown'


def _build_upsert_sql(
    cn,
    table: str,
    columns: tuple[str],
    key_columns: list[str] = None,
    constraint_name: str = None,
    update_always: list[str] = None,
    update_if_null: list[str] = None,
    db_type: str = 'postgresql',
) -> str:
    """Builds an UPSERT SQL statement for the specified database driver.

    Args:
        cn: Database connection
        table: Target table name
        columns: All columns to insert
        key_columns: Columns that form conflict constraint (primary/unique keys)
        constraint_name: Name of the constraint to use for conflict detection (PostgreSQL only)
        update_always: Columns that should always be updated on conflict
        update_if_null: Columns that should only be updated if target is null
        db_type: Database type ('postgresql', 'sqlite', 'mssql')
    """
    # Validate inputs for non-PostgreSQL databases
    if constraint_name and db_type != 'postgresql':
        raise ValueError(f'Constraint name is only supported for PostgreSQL, not {db_type}')

    # Fall back to regular insert if no conflict detection is requested
    if not key_columns and not constraint_name:
        return _build_insert_sql(cn, table, columns)

    if update_always and set(update_always)-set(columns) != set():
        raise ValueError('There are columns in `update_always` that are not in columns')

    if update_if_null and set(update_if_null)-set(columns) != set():
        raise ValueError('There are columns in `update_if_null` that are not in columns')

    # Quote table name and all column names
    quoted_table = quote_identifier(db_type, table)
    quoted_columns = [quote_identifier(db_type, col) for col in columns]
    quoted_key_columns = [quote_identifier(db_type, col) for col in key_columns] if key_columns else []

    # PostgreSQL uses INSERT ... ON CONFLICT DO UPDATE
    if db_type == 'postgresql':
        insert_sql = _build_insert_sql(cn, table, columns)

        if not constraint_name:
            conflict_sql = f"on conflict ({','.join(quoted_key_columns)})"
        else:
            expressions = get_db_strategy(cn).get_constraint_definition(cn, table, constraint_name)
            conflict_sql = f'on conflict {expressions}'

        # If no updates requested, do nothing
        if not (update_always or update_if_null):
            return f'{insert_sql} {conflict_sql} do nothing RETURNING *'

        # Build update expressions
        update_exprs = []
        if update_always:
            update_exprs.extend(f'{quote_identifier(db_type, col)} = excluded.{quote_identifier(db_type, col)}'
                                for col in update_always)
        if update_if_null:
            update_exprs.extend(f'{quote_identifier(db_type, col)} = coalesce({quoted_table}.{quote_identifier(db_type, col)}, excluded.{quote_identifier(db_type, col)})'
                                for col in update_if_null)

        update_sql = f"do update set {', '.join(update_exprs)}"

        return f'{insert_sql} {conflict_sql} {update_sql} RETURNING *'

    # SQLite uses INSERT ... ON CONFLICT DO UPDATE (similar to PostgreSQL)
    elif db_type == 'sqlite':
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
            update_exprs.extend(f'{quote_identifier(db_type, col)} = excluded.{quote_identifier(db_type, col)}'
                                for col in update_always)
        if update_if_null:
            update_exprs.extend(f'{quote_identifier(db_type, col)} = COALESCE({quoted_table}.{quote_identifier(db_type, col)}, excluded.{quote_identifier(db_type, col)})'
                                for col in update_if_null)

        update_sql = f"do update set {', '.join(update_exprs)}"

        return f'{insert_sql} {conflict_sql} {update_sql}'

    # SQL Server uses MERGE INTO
    elif db_type == 'mssql':
        # For SQL Server we use MERGE statement
        placeholders = ','.join(['?'] * len(columns))
        temp_alias = 'src'

        # Build conditions for matching keys
        match_conditions = ' AND '.join(
            f'target.{quote_identifier(db_type, col)} = {temp_alias}.{quote_identifier(db_type, col)}' for col in key_columns
        )

        # Build column value list for source
        source_values = ', '.join(f'? as {quote_identifier(db_type, col)}' for col in columns)

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
                f'target.{quote_identifier(db_type, col)} = {temp_alias}.{quote_identifier(db_type, col)}' for col in update_cols
            )
            update_clause = f'WHEN MATCHED THEN UPDATE SET {update_statements}'
        else:
            # If no updates but we have keys, just match without updating
            update_clause = 'WHEN MATCHED THEN DO NOTHING'

        # Build INSERT statements
        quoted_all_columns = ', '.join(quoted_columns)
        source_columns = ', '.join(f'{temp_alias}.{quote_identifier(db_type, col)}' for col in columns)

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
        raise ValueError(f'Database type {db_type} not supported for UPSERT operations')


def _build_insert_sql(cn, table: str, columns: tuple[str]) -> str:
    """Builds the INSERT part of the SQL statement
    """
    db_type = _get_db_type_from_connection(cn)

    return build_insert_sql(db_type, table, columns)


def _prepare_rows_for_upsert(cn, table, rows):
    """Prepare and validate rows for upsert operation with case-insensitive matching"""
    if not rows:
        return None

    # Get table columns with exact case
    table_columns = get_table_columns(cn, table)

    # Create case mapping for case-insensitive lookup
    case_map = {col.lower(): col for col in table_columns}

    # Filter rows - keep only valid columns with database's exact column case
    filtered_rows = []
    for row in rows:
        corrected_row = {}
        for col, val in row.items():
            # Case-insensitive lookup of the column name
            exact_col = case_map.get(col.lower())
            if exact_col:
                corrected_row[exact_col] = val

        # Only include rows that have at least one valid column
        if corrected_row:
            filtered_rows.append(corrected_row)

    if not filtered_rows:
        logger.warning(f'No valid columns found for {table} after filtering')
        return None

    return tuple(filtered_rows)


def _filter_update_columns(columns, update_cols, key_cols):
    """Filter update columns to ensure they're valid using case-insensitive matching"""
    if not update_cols:
        return []

    # Create case-insensitive lookup sets
    columns_lower = {col.lower() for col in columns}
    key_cols_lower = {col.lower() for col in key_cols}

    # Map for converting to database's exact column case
    case_map = {col.lower(): col for col in columns}

    # Filter columns, keeping only valid ones that aren't key columns
    # and convert to the exact case used in the database
    return [
        case_map[col.lower()]
        for col in update_cols
        if col.lower() in columns_lower and col.lower() not in key_cols_lower
    ]


def _execute_standard_upsert(
    cn,
    table,
    rows,
    columns,
    key_cols,
    constraint_name,
    update_always,
    update_ifnull,
    db_type,
):
    """Execute standard upsert operation for supported databases using execute_many"""

    sql = _build_upsert_sql(
        cn=cn,
        table=table,
        columns=columns,
        key_columns=key_cols,
        constraint_name=constraint_name,
        update_always=update_always,
        update_if_null=update_ifnull,
        db_type=db_type
    )

    params = [[row[col] for col in columns] for row in rows]
    rc = execute_many(cn, sql, params)

    if isinstance(rc, int) and rc != len(rows):
        logger.debug(f'{len(rows) - rc} rows were skipped due to existing constraints or errors')

    return rc


def _fetch_existing_rows(tx_or_cn, table, rows, key_cols):
    """Fetch existing rows for a set of key values - optimized for batch fetching"""
    existing_rows = {}
    db_type = _get_db_type_from_connection(tx_or_cn)

    quoted_table = quote_identifier(db_type, table)

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
    safe_param_limit = max(50, get_param_limit_for_db(db_type) // 2)

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
                quoted_key_col = quote_identifier(db_type, key_col)
                # Use correct placeholder based on database
                placeholder = '?' if db_type == 'mssql' else '%s'
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
                    quoted_key_col = quote_identifier(db_type, key_col)
                    # Use correct placeholder based on database
                    placeholder = '?' if db_type == 'mssql' else '%s'
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
    """Special handling for SQL Server with NULL-preserving updates"""

    logger.warning('SQL Server MERGE with NULL preservation uses a specialized approach')
    db_type = 'mssql'

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
        db_type=db_type
    )

    params = [[row[col] for col in columns] for row in rows]
    rc = execute_many(cn, sql, params)


def upsert_rows(
    cn,
    table: str,
    rows: tuple[dict],
    update_cols_key: list = None,
    constraint_name: str = None,
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
        constraint_name: Name of the constraint to use for conflict detection (PostgreSQL only)
        update_cols_always: Columns that should always be updated on conflict
        update_cols_ifnull: Columns that should only be updated if target is null
        reset_sequence: Whether to reset the table's sequence after operation
    """
    if not rows:
        logger.debug('Skipping upsert of empty rows')
        return 0

    # Get database type
    db_type = _get_db_type_from_connection(cn)
    if db_type == 'unknown':
        raise ValueError('Unsupported database connection for upsert_rows')

    # Check constraint name is only used with PostgreSQL
    if constraint_name and db_type != 'postgresql':
        raise ValueError(f'Constraint name is only supported for PostgreSQL, not {db_type}')

    # Warning for SQL Server implementation
    if db_type == 'mssql':
        logger.warning('SQL Server MERGE implementation is experimental and may have limitations')

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

    # Get primary keys if not specified and constraint name not used
    if not update_cols_key and not constraint_name:
        update_cols_key = get_table_primary_keys(cn, table)
        if not update_cols_key and db_type != 'postgresql':
            logger.warning(f'No primary keys found for {table}, falling back to INSERT')
            return insert_rows(cn, table, rows)

    try:
        # Handle the specific database type
        if db_type == 'mssql' and update_cols_ifnull:
            # For SQL Server with NULL preservation, use specialized function
            return _upsert_sqlserver_with_nulls(cn, table, rows, columns,
                                                update_cols_key, update_cols_always,
                                                update_cols_ifnull)
        else:
            # Standard approach for PostgreSQL, SQLite and simple SQL Server cases
            rc = _execute_standard_upsert(cn, table, rows, columns,
                                          update_cols_key, constraint_name,
                                          update_cols_always, update_cols_ifnull,
                                          db_type)
    finally:
        if reset_sequence:
            reset_table_sequence(cn, table)

    return rc
