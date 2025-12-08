"""
Utilities for SQL statement generation across different database backends.
"""
import logging

from database.sql import quote_identifier

logger = logging.getLogger(__name__)


def build_select_sql(table: str, dialect: str, columns: list[str] | None = None,
                     where: str | None = None, order_by: str | None = None,
                     limit: int | None = None) -> str:
    """Generate a SELECT statement for the specified database type.
    """
    quoted_table = quote_identifier(table, dialect)

    if columns:
        quoted_cols = ', '.join(quote_identifier(col, dialect) for col in columns)
        select_clause = f'SELECT {quoted_cols}'
    else:
        select_clause = 'SELECT *'

    sql = f'{select_clause} FROM {quoted_table}'

    if where:
        sql += f' WHERE {where}'

    if order_by:
        sql += f' ORDER BY {order_by}'

    if limit is not None:
        sql += f' LIMIT {limit}'

    return sql


def build_insert_sql(dialect: str, table: str, columns: list[str]) -> str:
    """Generate an INSERT statement.
    """
    quoted_table = quote_identifier(table, dialect)
    quoted_columns = ', '.join(quote_identifier(col, dialect) for col in columns)

    if dialect == 'postgresql':
        placeholders = ', '.join(['%s'] * len(columns))
    else:
        placeholders = ', '.join(['?'] * len(columns))

    return f'INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})'
