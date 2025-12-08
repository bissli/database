"""
Data operations for database access (INSERT, UPDATE, DELETE).
"""
import logging
from typing import TYPE_CHECKING, Any

from database.core.transaction import Transaction
from database.query import execute, select
from database.schema import get_table_columns
from database.sql import quote_identifier
from database.strategy import get_db_strategy
from database.utils.sql_generation import build_insert_sql

from libb import peel

if TYPE_CHECKING:
    from database.core.connection import ConnectionWrapper

logger = logging.getLogger(__name__)

insert = update = delete = execute


def update_or_insert(cn: 'ConnectionWrapper', update_sql: str, insert_sql: str,
                     *args: Any) -> int:
    """Try to update first; if no rows are updated, then insert.

    This is a simplified alternative to upsert operations.
    """
    with Transaction(cn) as tx:
        rc = tx.execute(update_sql, args)
        if rc:
            return rc
        rc = tx.execute(insert_sql, args)
        return rc


def insert_row(cn: 'ConnectionWrapper', table: str, fields: list[str],
               values: list[Any]) -> int:
    """Insert a row into a table using the supplied list of fields and values.
    """
    assert len(fields) == len(values), 'fields must be same length as values'
    sql = build_insert_sql(cn.dialect, table, fields)
    return insert(cn, sql, *values)


def insert_rows(cn: 'ConnectionWrapper', table: str,
                rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> int:
    """Insert multiple rows into a table.
    """
    if not rows:
        logger.debug('Skipping insert of empty rows')
        return 0

    filtered_rows = filter_table_columns(cn, table, rows)
    if not filtered_rows:
        logger.warning(f'No valid columns found for {table} after filtering')
        return 0
    rows = tuple(filtered_rows)

    dialect = cn.dialect

    cols = tuple(rows[0].keys())

    try:
        quoted_table = quote_identifier(table, dialect)
        quoted_cols = ','.join(quote_identifier(col, dialect) for col in cols)
    except ValueError:
        quoted_table = table
        quoted_cols = ','.join(cols)

    placeholders = ','.join(['%s'] * len(cols))
    sql = f'INSERT INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders})'

    all_params = [tuple(row.values()) for row in rows]

    cursor = cn.cursor()
    return cursor.executemany(sql, all_params)


def update_row(cn: 'ConnectionWrapper', table: str, keyfields: list[str],
               keyvalues: list[Any], datafields: list[str],
               datavalues: list[Any]) -> int:
    """Update the specified datafields to the supplied datavalues in a table row
    identified by the keyfields and keyvalues.
    """
    assert len(keyfields) == len(keyvalues), 'keyfields must be same length as keyvalues'
    assert len(datafields) == len(datavalues), 'datafields must be same length as datavalues'
    values = tuple(datavalues) + tuple(keyvalues)
    return update(cn, update_row_sql(table, keyfields, datafields), *values)


def update_row_sql(table: str, keyfields: list[str], datafields: list[str]) -> str:
    """Generate the SQL to update the specified datafields in a table row
    identified by the keyfields.
    """
    for kf in keyfields:
        assert kf not in datafields, f'keyfield {kf} cannot be in datafields'
    keycols = ' and '.join([f'{f}=%s' for f in keyfields])
    datacols = ','.join([f'{f}=%s' for f in datafields])
    return f'update {table} set {datacols} where {keycols}'


def filter_table_columns(cn: 'ConnectionWrapper', table: str,
                         row_dicts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter dictionaries to only include valid columns for the table
    and correct column name casing to match database schema.
    """
    if not row_dicts:
        return []

    table_cols = get_table_columns(cn, table)

    case_map = {col.lower(): col for col in table_cols}

    filtered_rows = []
    removed_columns: set[str] = set()

    for row in row_dicts:
        filtered_row = {}
        for col, val in row.items():
            if col.lower() in case_map:
                correct_col = case_map[col.lower()]
                filtered_row[correct_col] = val
            else:
                removed_columns.add(col)
        filtered_rows.append(filtered_row)

    for col in removed_columns:
        logger.debug(f'Removed column {col} not in {table}')

    return filtered_rows


def table_data(cn: 'ConnectionWrapper', table: str, columns: list[str] | None = None,
               bypass_cache: bool = False) -> Any:
    """Get table data by columns.
    """
    if not columns:
        strategy = get_db_strategy(cn)
        columns = strategy.get_default_columns(cn, table, bypass_cache=bypass_cache)

    columns = [f'{col} as {alias}' for col, alias in peel(columns)]
    return select(cn, f"select {','.join(columns)} from {table}")
