import datetime
from unittest.mock import MagicMock, patch

import database as db
import pymssql
import pytest
from database.adapters import TypeConverter

# Use fixture from fixtures directory


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
    is_pymssql_connection_patcher = patch('database.utils.connection_utils.is_pymssql_connection',
                                          return_value=True)
    is_pymssql_connection_patcher.start()

    # Patch quote_identifier to return SQL Server style quoting
    quote_identifier_patcher = patch(
        'database.utils.sql.quote_identifier',
        side_effect=lambda db_type, ident: f"[{ident.replace(']', ']]')}]"
    )
    quote_identifier_patcher.start()

    # Patch the load_data function to properly use our mock data
    load_data_patcher = patch(
        'database.operations.query.load_data',
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


def test_sqlserver_datetime_handling(mock_sqlserver_conn):
    """Test SQL Server date/time handling"""
    # Setup mock cursor with fetchall that returns date/time values
    cursor = mock_sqlserver_conn.cursor.return_value

    # Mock the cursor description and result for a DATETIME column
    cursor.description = [
        ('datetime_col', 61, None, None, None, None, None)  # 61 is DATETIME type in SQL Server
    ]

    test_date = datetime.datetime(2025, 3, 6, 12, 30, 45)
    cursor.fetchall.return_value = [(test_date,)]

    # Patch for SQLServer type mapping
    with patch('database.client.mssql_types', {'datetime': datetime.datetime}):
        # Execute a query that would return a datetime
        result = db.select(mock_sqlserver_conn, 'SELECT GETDATE() AS datetime_col')

        # Verify date was handled properly
        assert len(result) == 1
        assert isinstance(result[0]['datetime_col'], datetime.datetime)


def test_sqlserver_decimal_handling(mock_sqlserver_conn):
    """Test SQL Server decimal handling"""
    # Setup mock cursor with fetchall that returns decimal values
    cursor = mock_sqlserver_conn.cursor.return_value

    # Mock the cursor description and result for a DECIMAL column
    cursor.description = [
        ('decimal_col', 106, None, None, None, None, None)  # 106 is DECIMAL type in SQL Server
    ]

    cursor.fetchall.return_value = [(123.45,)]

    # Patch for SQLServer type mapping
    with patch('database.client.mssql_types', {'decimal': float}):
        # Execute a query that would return a decimal
        result = db.select(mock_sqlserver_conn, 'SELECT 123.45 AS decimal_col')

        # Verify decimal was handled properly
        assert len(result) == 1
        assert isinstance(result[0]['decimal_col'], float)
        assert result[0]['decimal_col'] == 123.45


def test_sqlserver_binary_handling(mock_sqlserver_conn):
    """Test SQL Server binary data handling"""
    # Setup mock cursor with fetchall that returns binary values
    cursor = mock_sqlserver_conn.cursor.return_value

    # Mock the cursor description and result for a VARBINARY column
    cursor.description = [
        ('binary_col', 173, None, None, None, None, None)  # 173 is VARBINARY type in SQL Server
    ]

    test_bytes = b'test binary data'
    cursor.fetchall.return_value = [(test_bytes,)]

    # Patch for SQLServer type mapping
    with patch('database.client.mssql_types', {'varbinary': bytes}):
        # Execute a query that would return binary data
        result = db.select(mock_sqlserver_conn, "SELECT CAST('test' AS VARBINARY) AS binary_col")

        # Verify binary data was handled properly
        assert len(result) == 1
        assert isinstance(result[0]['binary_col'], bytes)
        assert result[0]['binary_col'] == test_bytes


def test_typeconverter_with_sqlserver_types():
    """Test TypeConverter with SQL Server-specific types"""
    # Test SQL Server GUID (uniqueidentifier)
    guid_str = '01234567-89AB-CDEF-0123-456789ABCDEF'
    # In SQL Server a GUID would be represented as a string
    assert TypeConverter.convert_value(guid_str) == guid_str


if __name__ == '__main__':
    __import__('pytest').main([__file__])
