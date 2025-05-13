"""Upsert operations for database tables.

This module provides functionality to insert or update rows in database tables
based on constraint violations, with configurable update behavior for existing rows.
"""
import logging
from typing import Any

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


def upsert_rows(
    cn: Any,
    table: str,
    rows: tuple[dict[str, Any], ...],
    constraint_name: str | None = None,
    update_cols_always: list[str] | None = None,
    update_cols_ifnull: list[str] | None = None,
    reset_sequence: bool = False,
    **kw: Any
) -> int:
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

        constraint_name: Name of a specific constraint to use for conflict detection.
            Only supported in PostgreSQL, ignored for other database types.
            If provided, this will be used instead of primary key columns.

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

    dialect = get_dialect_name(cn)

    if dialect != 'postgresql':
        constraint_name = None

    # Get table columns and create case mapping once
    table_columns = get_table_columns(cn, table)
    case_map = {col.lower(): col for col in table_columns}
    table_columns_lower = set(case_map.keys())

    # Filter and correct column cases for each row, preserving table column order
    filtered_rows = []
    for row in rows:
        row_lower = {k.lower(): v for k, v in row.items()}
        corrected_row = {col: row_lower[col.lower()] for col in table_columns if col.lower() in row_lower}
        if corrected_row:
            filtered_rows.append(corrected_row)

    if not filtered_rows:
        logger.debug(f'No valid columns found for {table} after filtering')
        return 0

    rows = tuple(filtered_rows)

    invalid_columns = [col for col in list(set().union(*(r.keys() for r in rows))) if
                       col.lower() not in table_columns_lower]
    if invalid_columns:
        logger.debug(f'Ignoring columns not present in the table schema: {invalid_columns}')

    provided_keys = {key for row in rows for key in row}
    columns = tuple(col for col in table_columns if col in provided_keys)

    if not columns:
        logger.warning(f'No valid columns provided for table {table}')
        return 0

    should_update = update_cols_always is not None or update_cols_ifnull is not None

    key_cols = get_table_primary_keys(cn, table)

    if should_update and ((dialect != 'postgresql') or (dialect == 'postgresql' and not constraint_name)):
        if not key_cols:
            logger.debug(f'No primary keys found for {table}, falling back to INSERT')
            return insert_rows(cn, table, rows)

    columns_lower = {col.lower() for col in columns}
    key_cols_lower = {k.lower() for k in key_cols} if key_cols else set()

    if update_cols_always:
        orig_update_cols_always = update_cols_always[:]
        valid_update_always = []
        for col in orig_update_cols_always:
            lower = col.lower()
            if lower in columns_lower and (constraint_name is not None or lower not in key_cols_lower):
                valid_update_always.append(case_map[lower])
        update_cols_always = valid_update_always
        invalid_always = [col for col in orig_update_cols_always if col.lower() not in {c.lower() for c in update_cols_always}]
        if invalid_always:
            logger.debug(f'Filtered invalid columns from update_cols_always: {invalid_always}')
    if update_cols_ifnull:
        orig_update_cols_ifnull = update_cols_ifnull[:]
        valid_update_ifnull = []
        uc_always_lower = {c.lower() for c in update_cols_always} if update_cols_always else set()
        for col in orig_update_cols_ifnull:
            lower = col.lower()
            if lower in columns_lower and (constraint_name is not None or lower not in key_cols_lower) and lower not in uc_always_lower:
                valid_update_ifnull.append(case_map[lower])
        update_cols_ifnull = valid_update_ifnull
        invalid_ifnull = [col for col in orig_update_cols_ifnull if col.lower() not in {c.lower() for c in update_cols_ifnull}]
        if invalid_ifnull:
            logger.debug(f'Filtered invalid columns from update_cols_ifnull: {invalid_ifnull}')

    if not key_cols and (dialect != 'postgresql' or not constraint_name):
        logger.debug(f'No constraint or key columns provided for {dialect} upsert, falling back to INSERT')
        return insert_rows(cn, table, rows)

    match dialect:
        case 'postgresql':
            return _upsert_postgresql(cn, table, rows, columns, constraint_name or key_cols,
                                      update_cols_always if should_update else None,
                                      update_cols_ifnull if should_update else None,
                                      reset_sequence)
        case 'sqlite':
            return _upsert_sqlite(cn, table, rows, columns, key_cols,
                                  update_cols_always if should_update else None,
                                  update_cols_ifnull if should_update else None,
                                  reset_sequence)
        case 'mssql':
            if not should_update:
                return insert_rows(cn, table, rows)
            return _upsert_mssql(cn, table, rows, columns, key_cols, update_cols_always,
                                 update_cols_ifnull, reset_sequence)


def batch_rows_for_execution(
    dialect: str,
    rows: tuple[dict[str, Any], ...],
    columns: tuple[str, ...],
    custom_batch_size: int | None = None
) -> list[tuple[list[list[Any]], int]]:
    """Create batches of rows for database operations to avoid parameter limits.

    This function divides rows into appropriately sized batches based on
    database parameter limits and returns data ready for execution.

    Parameters
        dialect: Database dialect name ('postgresql', 'sqlite', 'mssql')
            The database type being used for the operation
        rows: Collection of row dictionaries to batch
            Input rows that need to be batched
        columns: Column names in correct database case
            Column names with case matching the database schema
        custom_batch_size: Optional override for batch size calculation
            Custom batch size to use instead of calculated limit

    Returns
        List of tuples containing (params_list, batch_size) where:
            - params_list: List of parameter lists for executemany
            - batch_size: Size of this batch
    """
    if custom_batch_size:
        max_rows_per_batch = custom_batch_size
    else:
        param_limit = get_param_limit_for_db(dialect)
        max_rows_per_batch = max(1, param_limit // max(1, len(columns)))

        if dialect == 'postgresql' and len(columns) > 10:
            max_rows_per_batch = min(max_rows_per_batch, 1000)

    logger.debug(f'{dialect} batching {len(rows)} rows in batches of max {max_rows_per_batch} rows')

    batches = []

    for batch_idx in range(0, len(rows), max_rows_per_batch):
        batch_rows = rows[batch_idx:batch_idx + max_rows_per_batch]
        params = [[row[col] for col in columns] for row in batch_rows]
        batches.append((params, len(batch_rows)))

    return batches


def _fetch_existing_rows(tx_or_cn: Any, table: str, rows: tuple[dict[str, Any], ...], key_cols: list[str]) -> dict[tuple[Any, ...], dict[str, Any]]:
    """Fetch existing rows for a set of key values - optimized for batch fetching.

    Retrieves existing rows from the database using an efficient batching strategy
    to minimize database queries while staying within parameter limits.

    Parameters
        tx_or_cn: Database transaction or connection
            Active database transaction or connection
        table: Target table name
            Name of the table to query
        rows: Rows to check for existing matches
            Collection of row dictionaries to look up
        key_cols: Key columns to use for matching
            Column names that form the unique key for each row

    Returns
        Dictionary mapping row keys to existing row data
    """
    existing_rows = {}

    dialect = get_dialect_name(tx_or_cn)
    quoted_table = quote_identifier(table, dialect)

    key_groups = {}
    for row in rows:
        row_key = tuple(row[key] for key in key_cols)
        key_groups.setdefault(row_key, True)

    if not key_cols:
        logger.warning('No key columns specified for fetching existing rows')
        return {}

    safe_param_limit = max(50, get_param_limit_for_db(dialect) // 2)
    max_keys_per_query = max(1, safe_param_limit // len(key_cols))

    if len(key_groups) <= max_keys_per_query:
        where_conditions = []
        params = []

        for row_key in key_groups:
            condition_parts = []
            for j, key_col in enumerate(key_cols):
                quoted_key_col = quote_identifier(key_col, dialect)
                placeholder = '?' if dialect == 'mssql' else '%s'
                condition_parts.append(f'{quoted_key_col} = {placeholder}')
                params.append(row_key[j])

            where_conditions.append(f"({' AND '.join(condition_parts)})")

        if where_conditions:
            where_clause = ' OR '.join(where_conditions)
            sql = f'SELECT * FROM {quoted_table} WHERE {where_clause}'

            result = select(tx_or_cn, sql, *params)
            for row in result:
                adapter = RowStructureAdapter.create(tx_or_cn, row)
                row_key = tuple(adapter.get_value(key) for key in key_cols)
                existing_rows[row_key] = adapter.to_dict()
    else:
        keys_list = list(key_groups.keys())
        for batch_idx in range(0, len(keys_list), max_keys_per_query):
            batch_keys = keys_list[batch_idx:batch_idx+max_keys_per_query]

            where_conditions = []
            params = []

            for row_key in batch_keys:
                condition_parts = []
                for j, key_col in enumerate(key_cols):
                    quoted_key_col = quote_identifier(key_col, dialect)
                    placeholder = '?' if dialect == 'mssql' else '%s'
                    condition_parts.append(f'{quoted_key_col} = {placeholder}')
                    params.append(row_key[j])

                where_conditions.append(f"({' AND '.join(condition_parts)})")

            where_clause = ' OR '.join(where_conditions)
            sql = f'SELECT * FROM {quoted_table} WHERE {where_clause}'

            result = select(tx_or_cn, sql, *params)
            for row in result:
                adapter = RowStructureAdapter.create(tx_or_cn, row)
                row_key = tuple(adapter.get_value(key) for key in key_cols)
                existing_rows[row_key] = adapter.to_dict()

    return existing_rows


def _upsert_postgresql(
    cn: Any,
    table: str,
    rows: tuple[dict[str, Any], ...],
    columns: tuple[str, ...],
    key_cols_or_constraint: list[str] | str,
    update_cols_always: list[str] | None,
    update_cols_ifnull: list[str] | None,
    reset_sequence: bool
) -> int:
    """Perform an UPSERT operation using PostgreSQL-specific syntax.

    Uses INSERT ... ON CONFLICT ... DO UPDATE syntax for PostgreSQL.

    Parameters
        cn: Database connection
            Active connection to the PostgreSQL database
        table: Target table name
            Name of the table to upsert rows into
        rows: Prepared rows to upsert
            Collection of row dictionaries with correct column cases
        columns: Table columns in correct case
            Column names matching database schema case
        key_cols_or_constraint: Key columns or constraint name
            Either a list of key column names or a constraint name string
        update_cols_always: Columns to always update on conflict
            Column names to update when a conflict occurs
        update_cols_ifnull: Columns to update only if target is null
            Column names to update only when target value is NULL
        reset_sequence: Whether to reset table sequence after upsert
            Flag to determine if auto-increment sequences should be reset

    Returns
        Number of rows affected by the operation
    """
    dialect = 'postgresql'

    constraint_name = None
    key_columns = None

    if isinstance(key_cols_or_constraint, str):
        constraint_name = key_cols_or_constraint
    else:
        key_columns = key_cols_or_constraint

    quoted_table = quote_identifier(table, dialect)
    quoted_columns = [quote_identifier(col, dialect) for col in columns]
    quoted_key_columns = [quote_identifier(col, dialect) for col in key_columns] if key_columns else []

    insert_sql = build_insert_sql(dialect, table, columns)

    if constraint_name:
        expressions = get_db_strategy(cn).get_constraint_definition(cn, table, constraint_name)
        conflict_sql = f'on conflict {expressions}'
    else:
        conflict_sql = f"on conflict ({','.join(quoted_key_columns)})"

    if not (update_cols_always or update_cols_ifnull):
        sql = f'{insert_sql} {conflict_sql} do nothing RETURNING *'
    else:
        update_exprs = []
        if update_cols_always:
            update_exprs.extend(f'{quote_identifier(col, dialect)} = excluded.{quote_identifier(col, dialect)}'
                                for col in update_cols_always)
        if update_cols_ifnull:
            update_exprs.extend(f'{quote_identifier(col, dialect)} = coalesce({quoted_table}.{quote_identifier(col, dialect)}, excluded.{quote_identifier(col, dialect)})'
                                for col in update_cols_ifnull)

        update_sql = f"do update set {', '.join(update_exprs)}"
        sql = f'{insert_sql} {conflict_sql} {update_sql} RETURNING *'

    batches = batch_rows_for_execution(dialect, rows, columns)

    total_affected = 0
    cursor = cn.cursor()

    for batch_idx, (params, batch_size) in enumerate(batches):
        rc = cursor.executemany(sql, params)

        if isinstance(rc, int):
            total_affected += rc
            if rc != batch_size:
                logger.debug(f'Batch {batch_idx+1}: {batch_size - rc} rows skipped')
        else:
            logger.debug(f'Batch {batch_idx+1}: unknown count affected')

    if reset_sequence:
        reset_table_sequence(cn, table)

    return total_affected


def _upsert_sqlite(
    cn: Any,
    table: str,
    rows: tuple[dict[str, Any], ...],
    columns: tuple[str, ...],
    key_cols: list[str],
    update_cols_always: list[str] | None,
    update_cols_ifnull: list[str] | None,
    reset_sequence: bool
) -> int:
    """Perform an UPSERT operation using SQLite-specific syntax.

    Uses INSERT ... ON CONFLICT ... DO UPDATE syntax for SQLite.

    Parameters
        cn: Database connection
            Active connection to the SQLite database
        table: Target table name
            Name of the table to upsert rows into
        rows: Prepared rows to upsert
            Collection of row dictionaries with correct column cases
        columns: Table columns in correct case
            Column names matching database schema case
        key_cols: Key columns for conflict detection
            List of column names that form the unique key
        update_cols_always: Columns to always update on conflict
            Column names to update when a conflict occurs
        update_cols_ifnull: Columns to update only if target is null
            Column names to update only when target value is NULL
        reset_sequence: Whether to reset table sequence after upsert
            Flag to determine if auto-increment sequences should be reset

    Returns
        Number of rows affected by the operation
    """
    dialect = 'sqlite'
    quoted_table = quote_identifier(table, dialect)
    quoted_columns = [quote_identifier(col, dialect) for col in columns]
    quoted_key_columns = [quote_identifier(col, dialect) for col in key_cols]

    insert_sql = build_insert_sql(dialect, table, columns)

    conflict_sql = f"on conflict ({','.join(quoted_key_columns)})"

    if not (update_cols_always or update_cols_ifnull):
        sql = f'{insert_sql} {conflict_sql} do nothing'
    else:
        update_exprs = []
        if update_cols_always:
            update_exprs.extend(f'{quote_identifier(col, dialect)} = excluded.{quote_identifier(col, dialect)}'
                                for col in update_cols_always)
        if update_cols_ifnull:
            update_exprs.extend(f'{quote_identifier(col, dialect)} = COALESCE({quoted_table}.{quote_identifier(col, dialect)}, excluded.{quote_identifier(col, dialect)})'
                                for col in update_cols_ifnull)

        update_sql = f"do update set {', '.join(update_exprs)}"
        sql = f'{insert_sql} {conflict_sql} {update_sql}'

    batches = batch_rows_for_execution(dialect, rows, columns)

    total_affected = 0
    cursor = cn.cursor()

    for batch_idx, (params, batch_size) in enumerate(batches):
        rc = cursor.executemany(sql, params)

        if isinstance(rc, int):
            total_affected += rc
            if rc != batch_size:
                logger.debug(f'Batch {batch_idx+1}: {batch_size - rc} rows skipped')
        else:
            # Some drivers don't return row count
            logger.debug(f'Batch {batch_idx+1}: unknown count affected')

    if reset_sequence:
        reset_table_sequence(cn, table)

    return total_affected


def _upsert_mssql(
    cn: Any,
    table: str,
    rows: tuple[dict[str, Any], ...],
    columns: tuple[str, ...],
    key_cols: list[str],
    update_cols_always: list[str] | None,
    update_cols_ifnull: list[str] | None,
    reset_sequence: bool
) -> int:
    """Perform an UPSERT operation using SQL Server-specific syntax.

    Uses MERGE INTO syntax for SQL Server, with special handling for NULL values.

    Parameters
        cn: Database connection
            Active connection to the SQL Server database
        table: Target table name
            Name of the table to upsert rows into
        rows: Prepared rows to upsert
            Collection of row dictionaries with correct column cases
        columns: Table columns in correct case
            Column names matching database schema case
        key_cols: Key columns for matching
            List of column names that form the unique key
        update_cols_always: Columns to always update on match
            Column names to update when a match occurs
        update_cols_ifnull: Columns to update only if target is null
            Column names to update only when target value is NULL
        reset_sequence: Whether to reset table sequence after upsert
            Flag to determine if identity columns should be reset

    Returns
        Number of rows affected by the operation
    """
    dialect = 'mssql'
    logger.warning('SQL Server MERGE implementation is experimental and may have limitations')

    if update_cols_ifnull:
        logger.warning('SQL Server MERGE with NULL preservation uses a specialized approach')

        existing_rows = _fetch_existing_rows(cn, table, rows, key_cols)

        for row in rows:
            row_key = tuple(row[key] for key in key_cols)
            existing_row = existing_rows.get(row_key)

            if existing_row:
                for col in update_cols_ifnull:
                    if col in existing_row and not is_null(existing_row.get(col)):
                        row[col] = existing_row.get(col)

        update_cols = update_cols_always or []
    else:
        update_cols = []
        if update_cols_always:
            update_cols.extend(update_cols_always)

    quoted_table = quote_identifier(table, dialect)
    quoted_columns = [quote_identifier(col, dialect) for col in columns]

    temp_alias = 'src'

    match_conditions = ' AND '.join(
        f'target.{quote_identifier(col, dialect)} = {temp_alias}.{quote_identifier(col, dialect)}'
        for col in key_cols
    )

    source_values = ', '.join(f'? as {quote_identifier(col, dialect)}' for col in columns)

    if update_cols:
        update_statements = ', '.join(
            f'target.{quote_identifier(col, dialect)} = {temp_alias}.{quote_identifier(col, dialect)}'
            for col in update_cols
        )
        update_clause = f'WHEN MATCHED THEN UPDATE SET {update_statements}'
    else:
        update_clause = 'WHEN MATCHED THEN DO NOTHING'

    quoted_all_columns = ', '.join(quoted_columns)
    source_columns = ', '.join(f'{temp_alias}.{quote_identifier(col, dialect)}' for col in columns)

    sql = f"""
    MERGE INTO {quoted_table} AS target
    USING (SELECT {source_values}) AS {temp_alias}
    ON {match_conditions}
    {update_clause}
    WHEN NOT MATCHED THEN INSERT ({quoted_all_columns}) VALUES ({source_columns});
    """

    batches = batch_rows_for_execution(dialect, rows, columns)

    total_affected = 0
    cursor = cn.cursor()

    for batch_idx, (params, batch_size) in enumerate(batches):
        rc = cursor.executemany(sql, params)

        if isinstance(rc, int):
            total_affected += rc
        else:
            logger.debug(f'Batch {batch_idx+1}: unknown count affected')

    if reset_sequence:
        reset_table_sequence(cn, table)

    return total_affected
