"""
Utilities for SQL statement generation across different database backends.
"""
import logging

from database.sql import quote_identifier

logger = logging.getLogger(__name__)


def build_select_sql(table, dialect, columns=None, where=None, order_by=None, limit=None):
    """Generate a SELECT statement for the specified database type.

    Args:
        db_type: Database type ('postgresql', 'sqlite')
        table: Table name
        columns: List of columns to select (None for *)
        where: WHERE clause (without 'WHERE' keyword)
        order_by: ORDER BY clause (without 'ORDER BY' keywords)
        limit: LIMIT value

    Returns
        SQL query string
    """
    quoted_table = quote_identifier(table, dialect)

    # Handle columns
    if columns:
        quoted_cols = ', '.join(quote_identifier(col, dialect) for col in columns)
        select_clause = f'SELECT {quoted_cols}'
    else:
        select_clause = 'SELECT *'

    # Build query
    sql = f'{select_clause} FROM {quoted_table}'

    # Add WHERE clause
    if where:
        sql += f' WHERE {where}'

    # Add ORDER BY
    if order_by:
        sql += f' ORDER BY {order_by}'

    # Add LIMIT for PostgreSQL and SQLite
    if limit is not None:
        sql += f' LIMIT {limit}'

    return sql


def build_insert_sql(dialect, table, columns):
    """Generate an INSERT statement.

    Args:
        db_type: Database type ('postgresql', 'sqlite')
        table: Table name
        columns: List of column names

    Returns
        SQL query string with placeholders
    """
    quoted_table = quote_identifier(table, dialect)
    quoted_columns = ', '.join(quote_identifier(col, dialect) for col in columns)

    # Create placeholders based on database type
    if dialect == 'postgresql':
        placeholders = ', '.join(['%s'] * len(columns))
    else:  # sqlite
        placeholders = ', '.join(['?'] * len(columns))

    return f'INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})'
