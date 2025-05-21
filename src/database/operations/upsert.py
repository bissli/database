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
from database.utils.sql import quote_identifier
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
    batch_size: int = 500,
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

        batch_size: Maximum rows per batch to process.

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
                                      reset_sequence,
                                      batch_size)
        case 'sqlite':
            return _upsert_sqlite(cn, table, rows, columns, key_cols,
                                  update_cols_always if should_update else None,
                                  update_cols_ifnull if should_update else None,
                                  reset_sequence,
                                  batch_size)
        case 'mssql':
            if not should_update:
                return insert_rows(cn, table, rows)
            return _upsert_mssql(cn, table, rows, columns, key_cols, update_cols_always,
                                 update_cols_ifnull, reset_sequence)


def _fetch_existing_rows(
    tx_or_cn: Any,
    table: str,
    rows: tuple[dict[str, Any], ...],
    key_cols: list[str],
    batch_size: int,
) -> dict[tuple[Any, ...], dict[str, Any]]:
    """Fetch existing rows for a set of key values using batching.

    Parameters
        tx_or_cn: Database transaction or connection
            Active connection to the database.
        table: Target table name
            Name of the table to query.
        rows: Rows to check for existing matches
            Collection of row dictionaries.
        key_cols: Key columns to use for matching
            Column names that form the unique key.
        batch_size: Maximum rows per batch
            Maximum number of keys to query per batch.

    Returns
        Dictionary mapping row keys to existing row data.
    """
    existing_rows = {}
    dialect = get_dialect_name(tx_or_cn)
    quoted_table = quote_identifier(table, dialect)

    # Collect unique keys from rows
    keys_set = set()
    keys_list = []
    for row in rows:
        key_value = tuple(row[key] for key in key_cols)
        if key_value not in keys_set:
            keys_set.add(key_value)
            keys_list.append(key_value)

    if not key_cols:
        logger.warning('No key columns specified for fetching existing rows')
        return {}

    placeholder = '?' if dialect == 'mssql' else '%s'
    for batch_idx in range(0, len(keys_list), batch_size):
        batch_keys = keys_list[batch_idx: batch_idx + batch_size]
        where_conditions = []
        params = []
        for key_value in batch_keys:
            condition_parts = [f'{quote_identifier(key, dialect)} = {placeholder}' for key in key_cols]
            where_conditions.append(f"({' AND '.join(condition_parts)})")
            params.extend(key_value)
        where_clause = ' OR '.join(where_conditions)
        sql = f'SELECT * FROM {quoted_table} WHERE {where_clause}'
        result = select(tx_or_cn, sql, *params)
        for row in result:
            adapter = RowStructureAdapter.create(tx_or_cn, row)
            key_val = tuple(adapter.get_value(key) for key in key_cols)
            existing_rows[key_val] = adapter.to_dict()
    return existing_rows


def _upsert_postgresql(
    cn: Any,
    table: str,
    rows: tuple[dict[str, Any], ...],
    columns: tuple[str, ...],
    key_cols_or_constraint: list[str] | str,
    update_cols_always: list[str] | None,
    update_cols_ifnull: list[str] | None,
    reset_sequence: bool,
    batch_size: int
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

    params = [[row[col] for col in columns] for row in rows]

    cursor = cn.cursor()
    rc = cursor.executemany(sql, params, batch_size)

    total_affected = rc if isinstance(rc, int) else 0
    if isinstance(rc, int) and rc != len(rows):
        logger.debug(f'{len(rows) - rc} rows skipped')
    elif not isinstance(rc, int):
        logger.debug('Unknown count affected')

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
    reset_sequence: bool,
    batch_size: int
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

    params = [[row[col] for col in columns] for row in rows]

    cursor = cn.cursor()
    rc = cursor.executemany(sql, params, batch_size)

    total_affected = rc if isinstance(rc, int) else 0
    if isinstance(rc, int) and rc != len(rows):
        logger.debug(f'{len(rows) - rc} rows skipped')
    elif not isinstance(rc, int):
        # Some drivers don't return row count
        logger.debug('Unknown count affected')

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
    reset_sequence: bool,
    batch_size: int = 500,
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

    params = [[row[col] for col in columns] for row in rows]

    cursor = cn.cursor()
    rc = cursor.executemany(sql, params, batch_size)

    total_affected = rc if isinstance(rc, int) else 0
    if not isinstance(rc, int):
        logger.debug('Unknown count affected')

    if reset_sequence:
        reset_table_sequence(cn, table)

    return total_affected
