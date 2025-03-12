"""
Utilities for SQL statement generation across different database backends.
"""
import logging

from database.utils.sql import quote_identifier

logger = logging.getLogger(__name__)


def build_select_sql(db_type, table, columns=None, where=None, order_by=None, limit=None):
    """Generate a SELECT statement for the specified database type.

    Args:
        db_type: Database type ('postgresql', 'mssql', 'sqlite')
        table: Table name
        columns: List of columns to select (None for *)
        where: WHERE clause (without 'WHERE' keyword)
        order_by: ORDER BY clause (without 'ORDER BY' keywords)
        limit: LIMIT/TOP value

    Returns
        SQL query string
    """
    quoted_table = quote_identifier(db_type, table)

    # Handle columns
    if columns:
        quoted_cols = ', '.join(quote_identifier(db_type, col) for col in columns)
        select_clause = f'SELECT {quoted_cols}'
    else:
        select_clause = 'SELECT *'

    # Handle LIMIT/TOP
    if limit is not None and db_type == 'mssql':
        select_clause = f'SELECT TOP {limit} ' + select_clause[7:]
        # SQLite and PostgreSQL use LIMIT at the end

    # Build query
    sql = f'{select_clause} FROM {quoted_table}'

    # Add WHERE clause
    if where:
        sql += f' WHERE {where}'

    # Add ORDER BY
    if order_by:
        sql += f' ORDER BY {order_by}'

    # Add LIMIT for PostgreSQL and SQLite
    if limit is not None and db_type != 'mssql':
        sql += f' LIMIT {limit}'

    return sql


def build_insert_sql(db_type, table, columns):
    """Generate an INSERT statement.

    Args:
        db_type: Database type ('postgresql', 'mssql', 'sqlite')
        table: Table name
        columns: List of column names

    Returns
        SQL query string with placeholders
    """
    quoted_table = quote_identifier(db_type, table)
    quoted_columns = ', '.join(quote_identifier(db_type, col) for col in columns)

    # Create placeholders based on database type
    if db_type == 'postgresql':
        placeholders = ', '.join(['%s'] * len(columns))
    else:  # mssql, sqlite
        placeholders = ', '.join(['?'] * len(columns))

    return f'INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})'
