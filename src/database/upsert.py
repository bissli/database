"""Upsert operations for database tables.

This module provides functionality to insert or update rows in database tables
based on constraint violations, with configurable update behavior for existing rows.
"""
import logging
from typing import Any

from database.data import insert_rows
from database.schema import get_table_columns, get_table_primary_keys
from database.schema import reset_table_sequence
from database.strategy import get_db_strategy

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
    use_primary_key: bool = False,
    **kw: Any
) -> int:
    """Perform an UPSERT operation (INSERT or UPDATE) for multiple rows with configurable update behavior.

    This function handles the "upsert" pattern: INSERT new rows or UPDATE existing ones based on
    a unique constraint violation. The exact implementation varies by database type and is optimized
    for each supported database (PostgreSQL and SQLite).

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
            Uses SQL COALESCE function.

        reset_sequence: Whether to reset the table's auto-increment sequence after operation.
            Useful after bulk loads to ensure next generated ID is correct.

        batch_size: Maximum rows per batch to process.

        use_primary_key: Force using primary key columns for conflict detection.
            If False (default), uses UNIQUE columns that are in the provided data when
            primary key columns aren't in the data. Set to True to always use primary keys.

    Returns
        Number of rows affected by the operation.
        This should match the input row count.

    Raises
        ValueError: If constraint name provided for non-PostgreSQL database
        ValueError: If using an unsupported database connection
    """
    if not rows:
        logger.debug('Skipping upsert of empty rows')
        return 0

    dialect = cn.dialect

    if dialect != 'postgresql':
        constraint_name = None

    table_columns = get_table_columns(cn, table)
    case_map = {col.lower(): col for col in table_columns}
    table_columns_lower = set(case_map.keys())

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

    provided_cols_lower = {col.lower() for col in columns}
    key_cols_in_data = key_cols and all(k.lower() in provided_cols_lower for k in key_cols)

    if dialect == 'sqlite' and not use_primary_key and not key_cols_in_data:
        strategy = get_db_strategy(cn)
        if hasattr(strategy, 'get_unique_columns'):
            unique_constraints = strategy.get_unique_columns(cn, table)
            for unique_cols in unique_constraints:
                if all(u.lower() in provided_cols_lower for u in unique_cols):
                    logger.debug(f'Using UNIQUE columns {unique_cols} instead of primary key for conflict detection')
                    key_cols = unique_cols
                    key_cols_in_data = True
                    break

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

    if (not key_cols or not key_cols_in_data) and (dialect != 'postgresql' or not constraint_name):
        logger.debug(f'No usable constraint or key columns for {dialect} upsert, falling back to INSERT')
        return insert_rows(cn, table, rows)

    strategy = get_db_strategy(cn)

    constraint_expr = None
    if constraint_name and dialect == 'postgresql':
        constraint_expr = strategy.get_constraint_definition(cn, table, constraint_name)

    sql = strategy.build_upsert_sql(
        table=table,
        columns=list(columns),
        key_columns=key_cols,
        constraint_expr=constraint_expr,
        update_cols_always=update_cols_always if should_update else None,
        update_cols_ifnull=update_cols_ifnull if should_update else None,
    )

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
