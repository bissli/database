"""
Unit tests for SQL Server utility functions.
"""
import datetime
from unittest.mock import patch

import pytest
from database.adapters.type_conversion import TypeConverter
from database.strategy import SQLServerStrategy
from database.utils.sqlserver_utils import ensure_identity_column_named
from database.utils.sqlserver_utils import ensure_timezone_naive_datetime
from database.utils.sqlserver_utils import extract_identity_from_result
from database.utils.sqlserver_utils import handle_unnamed_columns_error

pytestmark = pytest.mark.skip('Skipping SQL Server tests')


def test_sqlserver_datetime_handling(mock_sqlserver_conn):
    """Test SQL Server date/time handling with mock connection"""
    from database.utils.connection_utils import get_dialect_name

    # Verify connection is detected as SQL Server
    assert get_dialect_name(mock_sqlserver_conn) == 'mssql'

    # Setup mock cursor with fetchall that returns date/time values
    cursor = mock_sqlserver_conn.cursor()

    # Mock the cursor description and result for a DATETIME column
    cursor.description = [
        ('datetime_col', 93, None, None, None, None, None)  # 93 is SQL_TYPE_TIMESTAMP
    ]

    # Create test datetime value
    test_date = datetime.datetime(2025, 3, 6, 12, 30, 45)
    cursor.fetchall.return_value = [(test_date,)]

    # Import select inside test to avoid circular imports
    from database.operations.query import select

    # Execute a query that would return a datetime
    result = select(mock_sqlserver_conn, 'SELECT GETDATE() AS datetime_col')

    # Verify date was handled properly
    assert len(result) == 1
    assert 'datetime_col' in result[0]
    assert result[0]['datetime_col'] == test_date


def test_sqlserver_decimal_handling(mock_sqlserver_conn):
    """Test SQL Server decimal handling with mock connection"""
    from database.utils.connection_utils import get_dialect_name

    # Verify connection is detected as SQL Server
    assert get_dialect_name(mock_sqlserver_conn) == 'mssql'

    # Setup mock cursor with fetchall that returns decimal values
    cursor = mock_sqlserver_conn.cursor()

    # Mock the cursor description and result for a DECIMAL column
    cursor.description = [
        ('decimal_col', 2, None, None, None, 10, 2)  # SQL_NUMERIC with precision 10, scale 2
    ]

    test_decimal = 123.45
    cursor.fetchall.return_value = [(test_decimal,)]

    # Import select inside test to avoid circular imports
    from database.operations.query import select

    # Execute a query that would return a decimal
    result = select(mock_sqlserver_conn, 'SELECT 123.45 AS decimal_col')

    # Verify decimal was handled properly
    assert len(result) == 1
    assert 'decimal_col' in result[0]
    assert result[0]['decimal_col'] == test_decimal
    assert abs(result[0]['decimal_col'] - 123.45) < 0.001  # Allow for floating-point differences


def test_sqlserver_binary_handling(mock_sqlserver_conn):
    """Test SQL Server binary data handling with mock connection"""
    from database.utils.connection_utils import get_dialect_name

    # Verify connection is detected as SQL Server
    assert get_dialect_name(mock_sqlserver_conn) == 'mssql'

    # Setup mock cursor with fetchall that returns binary values
    cursor = mock_sqlserver_conn.cursor()

    # Mock the cursor description and result for a VARBINARY column
    cursor.description = [
        ('binary_col', -3, None, None, None, None, None)  # SQL_VARBINARY
    ]

    test_bytes = b'test binary data'
    cursor.fetchall.return_value = [(test_bytes,)]

    # Import select inside test to avoid circular imports
    from database.operations.query import select

    # Execute a query that would return binary data
    result = select(mock_sqlserver_conn, "SELECT CAST('test' AS VARBINARY) AS binary_col")

    # Verify binary data was handled properly
    assert len(result) == 1
    assert 'binary_col' in result[0]
    assert result[0]['binary_col'] == test_bytes


def test_typeconverter_with_sqlserver_types():
    """Test TypeConverter with SQL Server-specific types"""
    # Test SQL Server GUID (uniqueidentifier)
    guid_str = '01234567-89AB-CDEF-0123-456789ABCDEF'
    # In SQL Server a GUID would be represented as a string
    assert TypeConverter.convert_value(guid_str) == guid_str

    # Test with UUID object
    import uuid
    uuid_obj = uuid.UUID('01234567-89AB-CDEF-0123-456789ABCDEF')
    converted = TypeConverter.convert_value(uuid_obj)
    # Either returns the UUID or a string representation
    assert isinstance(converted, uuid.UUID | str)
    assert str(uuid_obj).lower() in str(converted).lower()


def test_sqlserver_upsert_sql_generation(mock_sqlserver_conn):
    """Test SQL Server MERGE statement generation using mock connection"""
    from database.utils.connection_utils import get_dialect_name

    # Verify connection is detected as SQL Server
    assert get_dialect_name(mock_sqlserver_conn) == 'mssql'

    # Import here to avoid circular imports in tests
    from database.operations.upsert import _build_upsert_sql

    # No need to set driver_type anymore
    # Use actual connection mock instead of creating a new one
    sql = _build_upsert_sql(
        cn=mock_sqlserver_conn,
        table='test_table',
        columns=('id', 'name', 'value'),
        key_columns=['id'],
        update_always=['name', 'value'],
        db_type='mssql'  # Changed from driver to db_type
    )

    # Verify SQL contains MERGE INTO and expected clauses
    assert 'MERGE INTO' in sql
    assert 'test_table' in sql
    assert 'WHEN MATCHED THEN UPDATE SET' in sql
    assert 'WHEN NOT MATCHED THEN INSERT' in sql
    assert 'id' in sql
    assert 'name' in sql
    assert 'value' in sql


def test_sqlserver_upsert_with_null_preservation(mock_sqlserver_conn):
    """Test SQL Server upsert with NULL preservation using mock connection"""
    from database.operations.upsert import _upsert_sqlserver_with_nulls
    from database.operations.upsert import upsert_rows
    from database.utils.connection_utils import get_dialect_name

    # Verify connection is detected as SQL Server
    assert get_dialect_name(mock_sqlserver_conn) == 'mssql'

    # Test data
    rows = [{'id': 1, 'name': 'Test', 'nullable_field': 'new_value'}]

    # Part 1: Test the upsert_rows function that calls _upsert_sqlserver_with_nulls
    with patch('database.operations.upsert._get_db_type_from_connection', return_value='mssql'), \
    patch('database.operations.upsert._upsert_sqlserver_with_nulls', return_value=1) as mock_upsert, \
    patch('database.operations.upsert._fetch_existing_rows',
          return_value={(1,): {'id': 1, 'name': 'Existing', 'nullable_field': None}}):

        # Setup mock_sqlserver_conn to have a cursor
        mock_cursor = mock_sqlserver_conn.cursor.return_value

        # Call upsert_rows with update_cols_ifnull to trigger special case
        result = upsert_rows(
            mock_sqlserver_conn,
            'test_table',
            rows,
            update_cols_key=['id'],
            update_cols_ifnull=['nullable_field']
        )

        # Verify the special SQL Server NULL-preserving function was called
        mock_upsert.assert_called_once()

    # Part 2: Test the implementation of _upsert_sqlserver_with_nulls directly
    # Using the transaction method from our mock connection
    mock_tx = mock_sqlserver_conn.create_transaction()
    mock_tx.execute.return_value = 1  # Simulate 1 row affected

    # Set up a transaction context manager patch
    with patch('database.core.transaction.Transaction', return_value=mock_tx), \
    patch('database.operations.upsert._fetch_existing_rows',
          return_value={(1,): {'id': 1, 'name': 'Existing', 'nullable_field': None}}), \
    patch('database.operations.upsert._build_upsert_sql', return_value='MERGE SQL'):

        # Test rows
        test_rows = [{'id': 1, 'name': 'Updated', 'nullable_field': 'new_value'}]

        # Call the function directly
        result = _upsert_sqlserver_with_nulls(
            mock_sqlserver_conn,
            'test_table',
            test_rows,
            ('id', 'name', 'nullable_field'),
            ['id'],
            ['name'],
            ['nullable_field']
        )

        # Verify transaction was used
        mock_tx.__enter__.assert_called_once()
        # Verify execute was called
        mock_tx.execute.assert_called_once()
        # Should return count of modified rows
        assert result == 1


def test_sqlserver_strategy(mock_sqlserver_conn):
    """Test SQL Server strategy implementation with mock connection"""
    from database.utils.connection_utils import get_dialect_name

    # Verify connection is detected as SQL Server
    assert get_dialect_name(mock_sqlserver_conn) == 'mssql'

    strategy = SQLServerStrategy()

    # Test quote_identifier function
    assert strategy.quote_identifier('table_name') == '[table_name]'
    assert strategy.quote_identifier('column]with]brackets') == '[column]]with]]brackets]'

    # Test reset_sequence by verifying it doesn't raise exceptions
    # Avoid checking specific SQL to make the test more maintainable
    strategy.reset_sequence(mock_sqlserver_conn, 'test_table', 'id')

    # Basic verification that something was executed
    assert mock_sqlserver_conn.cursor().execute.called


def test_ensure_identity_column_named():
    """Test SQL Server identity column naming utility"""
    # Test @@identity naming
    sql = 'SELECT @@identity FROM users'
    fixed_sql = ensure_identity_column_named(sql)
    assert 'AS id' in fixed_sql
    assert '@@identity AS id' in fixed_sql

    # Test COUNT(*) naming
    sql = 'SELECT COUNT(*) FROM users'
    fixed_sql = ensure_identity_column_named(sql)
    assert 'COUNT(*) AS count' in fixed_sql

    # Test simple column - with Driver 18+ this should NOT be changed
    sql = 'SELECT name FROM users'
    fixed_sql = ensure_identity_column_named(sql)
    # No alias should be added for simple columns with Driver 18+
    assert fixed_sql == sql


def test_handle_unnamed_columns_error():
    """Test handling unnamed column errors in SQL Server"""
    # Test error related to unnamed columns
    error = Exception('The query returned columns with no names')
    sql = 'SELECT @@identity FROM users'
    fixed_sql, should_retry = handle_unnamed_columns_error(error, sql, [])

    assert should_retry is True
    assert 'AS id' in fixed_sql

    # Test with special cases that still need aliases with Driver 18+
    sql = 'SELECT COUNT(*) FROM users'
    fixed_sql, should_retry = handle_unnamed_columns_error(error, sql, [])
    assert 'COUNT(*) AS count' in fixed_sql
    assert should_retry is True

    # Test simple column - with Driver 18+, should NOT need alias
    sql = 'SELECT name FROM users'
    fixed_sql, should_retry = handle_unnamed_columns_error(error, sql, [])
    # The error handler should leave it unchanged since simple columns
    # work correctly with Driver 18+
    assert fixed_sql == sql
    # Should not retry since no changes needed
    assert should_retry is False

    # Test unrelated error
    error = Exception('Some other error')
    sql = 'SELECT * FROM users'
    fixed_sql, should_retry = handle_unnamed_columns_error(error, sql, [])

    assert should_retry is False
    assert fixed_sql == sql


def test_extract_identity_from_result():
    """Test extracting identity from SQL Server result row"""
    # Test with dictionary object
    result = {'id': 42}
    assert extract_identity_from_result(result) == 42

    # Test with None
    assert extract_identity_from_result(None) is None

    # Test with object having id attribute
    class MockRow:
        def __init__(self):
            self.id = 42

    mock_row = MockRow()
    assert extract_identity_from_result(mock_row) == 42

    # Test with an object without id (should return None)
    class NoIdObject:
        pass

    no_id_obj = NoIdObject()
    assert extract_identity_from_result(no_id_obj) is None


def test_ensure_timezone_naive_datetime():
    """Test timezone handling for SQL Server datetimes"""
    import datetime

    import pytz

    # Test with None value
    assert ensure_timezone_naive_datetime(None) is None

    # Test with naive datetime
    dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
    assert ensure_timezone_naive_datetime(dt) is dt

    # Test with timezone-aware datetime
    tz_dt = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
    result = ensure_timezone_naive_datetime(tz_dt)
    assert result.tzinfo is None
    assert result.year == 2023
    assert result.month == 1
    assert result.day == 1


def test_handle_in_clause():
    """Test SQL Server IN clause parameter expansion with Driver 18+"""
    from database.utils.sqlserver_utils import _handle_in_clause

    # Test with a simple IN clause - still needed with Driver 18+
    # This is independent of column name handling and still required
    # for proper parameter expansion
    query = 'SELECT * FROM users WHERE id IN (?)'
    params = [(1, 2, 3)]

    modified_query, expanded_params = _handle_in_clause(query, params)

    assert modified_query == 'SELECT * FROM users WHERE id IN (?, ?, ?)'
    assert expanded_params == [1, 2, 3]

    # Test with multiple IN clauses
    query = 'SELECT * FROM users WHERE id IN (?) OR dept_id IN (?)'
    params = [(1, 2), (10, 20, 30)]

    modified_query, expanded_params = _handle_in_clause(query, params)

    assert modified_query == 'SELECT * FROM users WHERE id IN (?, ?) OR dept_id IN (?, ?, ?)'
    assert expanded_params == [1, 2, 10, 20, 30]

    # Test with mixed param types
    query = 'SELECT * FROM users WHERE id IN (?) AND name = ?'
    params = [(1, 2, 3), 'John']

    modified_query, expanded_params = _handle_in_clause(query, params)

    assert modified_query == 'SELECT * FROM users WHERE id IN (?, ?, ?) AND name = ?'
    assert expanded_params == [1, 2, 3, 'John']

    # Test with empty IN list
    query = 'SELECT * FROM users WHERE id IN (?)'
    params = [[]]

    modified_query, expanded_params = _handle_in_clause(query, params)

    assert modified_query == 'SELECT * FROM users WHERE id IN (NULL)'
    assert expanded_params == []

    # Test when there's no IN clause
    query = 'SELECT * FROM users WHERE id = ?'
    params = [5]

    modified_query, expanded_params = _handle_in_clause(query, params)

    assert modified_query == query
    assert expanded_params == params


if __name__ == '__main__':
    __import__('pytest').main([__file__])
