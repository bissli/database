"""
Data operations for database access (INSERT, UPDATE, DELETE).
"""
import logging

from database.core.query import dumpsql, execute
from database.core.transaction import Transaction
from database.utils.sql import handle_query_params, quote_identifier
from more_itertools import flatten

logger = logging.getLogger(__name__)


# Alias these operations for backward compatibility
insert = update = delete = execute


@handle_query_params
@dumpsql
def insert_identity(cn, sql, *args):
    """Inject @@identity column into query for row by row unique id"""
    cursor = cn.cursor()
    cursor.execute(sql + '; select @@identity', args)
    cursor.nextset()
    identity = cursor.fetchone()[0]
    # must do the commit after retrieving data since commit closes cursor
    cn.commit()
    return identity


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
    quoted_table = quote_identifier(cn.get_driver_type(), table)
    quoted_columns = ', '.join(quote_identifier(cn.get_driver_type(), col) for col in fields)
    placeholders = ', '.join(['%s'] * len(fields))
    sql = f'insert into {quoted_table} ({quoted_columns}) values ({placeholders})'
    return insert(cn, sql, *values)


def insert_rows(cn, table, rows):
    """Insert multiple rows into a table"""
    if not rows:
        logger.debug('Skipping insert of empty rows')
        return 0

    # Include only columns that exist in the table
    filtered_rows = filter_table_columns(cn, table, rows)
    if not filtered_rows:
        logger.warning(f'No valid columns found for {table} after filtering')
        return 0
    rows = tuple(filtered_rows)

    cols = tuple(rows[0].keys())
    vals = tuple(flatten([tuple(row.values()) for row in rows]))

    def genvals(cols, vals):
        this = ','.join(['%s']*len(cols))
        return ','.join([f'({this})']*int(len(vals)/len(cols)))

    quoted_table = quote_identifier(cn.get_driver_type(), table)
    quoted_cols = ','.join(quote_identifier(cn.get_driver_type(), col) for col in cols)

    sql = f'insert into {quoted_table} ({quoted_cols}) values {genvals(cols, vals)}'
    return insert(cn, sql, *vals)


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
