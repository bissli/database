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

    # Mock DatabaseOptions with iterdict_data_loader for proper data conversion
    mock_options = MagicMock()
    mock_options.drivername = 'sqlserver'

    # Configure data_loader to properly handle test data
    def mock_data_loader(data, cols, **kwargs):
        # Create a simple DataFrame-like object that the tests can check
        result = []
        for row in data:
            if isinstance(row, dict):
                result.append(row)
            elif isinstance(row, tuple):
                # Convert tuple to dict using column names
                result.append({cols[i]: val for i, val in enumerate(row)})
        return result

    mock_options.data_loader = mock_data_loader
    conn.options = mock_options

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

    # Patch the load_data function to properly use our mock data
    load_data_patcher = patch(
        'database.client.load_data',
        side_effect=lambda cursor, **kwargs: mock_options.data_loader(
            cursor.fetchall(),
            [c[0] for c in cursor.description],
            **kwargs
        )
    )
    load_data_patcher.start()

    yield conn

    is_pymssql_connection_patcher.stop()
    quote_identifier_patcher.stop()
    load_data_patcher.stop()
