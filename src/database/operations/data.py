"""
Data operations for database access (INSERT, UPDATE, DELETE).
"""
import logging

from database.core.query import execute
from database.core.transaction import Transaction
from database.utils.connection_utils import get_dialect_name
from database.utils.sql import quote_identifier

logger = logging.getLogger(__name__)


# Alias these operations for backward compatibility
insert = update = delete = execute


def update_or_insert(cn, update_sql, insert_sql, *args):
    """Try to update first; if no rows are updated, then insert.

    This is a simplified alternative to upsert operations.
    """
    with Transaction(cn) as tx:
        rc = tx.execute(update_sql, args)
        if rc:
            return rc
        rc = tx.execute(insert_sql, args)
        return rc


def insert_row(cn, table, fields, values):
    """Insert a row into a table using the supplied list of fields and values."""
    assert len(fields) == len(values), 'fields must be same length as values'
    from database.utils.sql_generation import build_insert_sql
    sql = build_insert_sql(cn.driver_type, table, fields)
    return insert(cn, sql, *values)


def insert_rows(cn, table, rows):
    """Insert multiple rows into a table"""
    if not rows:
        logger.debug('Skipping insert of empty rows')
        return 0

    # Filter to include only valid columns for the table
    filtered_rows = filter_table_columns(cn, table, rows)
    if not filtered_rows:
        logger.warning(f'No valid columns found for {table} after filtering')
        return 0
    rows = tuple(filtered_rows)

    # Determine database type for quoting
    dialect = get_dialect_name(cn)

    # Prepare the SQL INSERT statement
    cols = tuple(rows[0].keys())

    # Handle unknown database type by using a safe fallback (no quoting)
    try:
        quoted_table = quote_identifier(table, dialect)
        quoted_cols = ','.join(quote_identifier(col, dialect) for col in cols)
    except ValueError:
        # For unknown database types, use the identifiers without quoting
        quoted_table = table
        quoted_cols = ','.join(cols)

    # Create placeholders for the VALUES clause
    placeholders = ','.join(['%s'] * len(cols))
    sql = f'INSERT INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders})'

    # Extract the values from the row dictionaries
    all_params = [tuple(row.values()) for row in rows]

    # Use executemany with database-specific parameter limits - handles batching internally
    cursor = cn.cursor()
    return cursor.executemany(sql, all_params)


def update_row(cn, table, keyfields, keyvalues, datafields, datavalues):
    """Update the specified datafields to the supplied datavalues in a table row
    identified by the keyfields and keyvalues.
    """
    assert len(keyfields) == len(keyvalues), 'keyfields must be same length as keyvalues'
    assert len(datafields) == len(datavalues), 'datafields must be same length as datavalues'
    values = tuple(datavalues) + tuple(keyvalues)
    return update(cn, update_row_sql(table, keyfields, datafields), *values)


def update_row_sql(table, keyfields, datafields):
    """Generate the SQL to update the specified datafields in a table row
    identified by the keyfields.
    """
    for kf in keyfields:
        assert kf not in datafields, f'keyfield {kf} cannot be in datafields'
    keycols = ' and '.join([f'{f}=%s' for f in keyfields])
    datacols = ','.join([f'{f}=%s' for f in datafields])
    return f'update {table} set {datacols} where {keycols}'


def filter_table_columns(cn, table, row_dicts):
    """Filter dictionaries to only include valid columns for the table
    and correct column name casing to match database schema
    """
    if not row_dicts:
        return []

    # Get table columns with original case preserved
    from database.operations.schema import get_table_columns
    table_cols = get_table_columns(cn, table)

    # Create case mapping dictionary (lowercase -> original case)
    case_map = {col.lower(): col for col in table_cols}

    # Create new filtered dictionaries with correct case
    filtered_rows = []
    # Track removed columns
    removed_columns = set()

    for row in row_dicts:
        filtered_row = {}
        for col, val in row.items():
            if col.lower() in case_map:
                # Use the correctly-cased column name from the database
                correct_col = case_map[col.lower()]
                filtered_row[correct_col] = val
            else:
                # Add to removed columns set
                removed_columns.add(col)
        filtered_rows.append(filtered_row)

    for col in removed_columns:
        logger.debug(f'Removed column {col} not in {table}')

    return filtered_rows


def table_data(cn, table, columns=None, bypass_cache=False):
    """Get table data by columns

    Args:
        cn: Database connection
        table: Table name to get data from
        columns: List of columns to retrieve (if empty, auto-detects using strategy)
        bypass_cache: If True, bypass cache when auto-detecting columns, by default False

    Returns
        list or DataFrame: Table data for the specified columns
    """
    from database.operations.query import select
    from database.strategy import get_db_strategy

    from libb import peel

    if not columns:
        strategy = get_db_strategy(cn)
        columns = strategy.get_default_columns(cn, table, bypass_cache=bypass_cache)

    columns = [f'{col} as {alias}' for col, alias in peel(columns)]
    return select(cn, f"select {','.join(columns)} from {table}")
