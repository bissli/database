from unittest.mock import MagicMock, patch

import pymssql
import pytest


@pytest.fixture
def mock_sqlserver_conn():
    """Create a mock SQL Server connection for testing"""
    conn = MagicMock()
    conn.connection = MagicMock(spec=pymssql.Connection)

    # Setup mock cursor
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    # Define behaviors for common operations
    def mock_execute(sql, args=None):
        # Store the SQL for inspection
        cursor.last_sql = sql
        cursor.last_args = args
        return 1  # Assume 1 row affected

    cursor.execute.side_effect = mock_execute

    # For is_pymssql_connection check
    is_pymssql_connection_patcher = patch('database.client.is_pymssql_connection',
                                          return_value=True)
    is_pymssql_connection_patcher.start()

    # Patch quote_identifier to return SQL Server style quoting
    quote_identifier_patcher = patch(
        'database.client.quote_identifier',
        side_effect=lambda conn, ident: f"[{ident.replace(']', ']]')}]"
    )
    quote_identifier_patcher.start()

    yield conn

    is_pymssql_connection_patcher.stop()
    quote_identifier_patcher.stop()
