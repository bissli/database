"""
Tests for simplified row adapter behavior - verifying they only handle structure, not type conversion.
"""
import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

from database.adapters.structure import RowStructureAdapter
from database.adapters.structure import PostgreSQLRowAdapter
from database.adapters.structure import SQLiteRowAdapter
from database.adapters.structure import SQLServerRowAdapter


def test_sqlserver_row_adapter_no_conversion():
    """Test that SQLServerRowAdapter doesn't convert values"""
    # Create sample data with various types
    row_data = {
        'int_col': 123,
        'str_col': 'text value',
        'date_col': datetime.date(2023, 5, 15),
        'datetime_col': datetime.datetime(2023, 5, 15, 12, 30, 45),
        'decimal_col': Decimal('123.45'),
        'bool_col': True
    }

    # Mock connection for the adapter
    mock_conn = MagicMock()

    # Create adapter with the data
    adapter = SQLServerRowAdapter(row_data, mock_conn)

    # Test to_dict() doesn't modify values
    result = adapter.to_dict()
    assert result == row_data, 'to_dict() should return data without conversion'

    # Test get_value() doesn't modify values
    for col in row_data:
        assert adapter.get_value(col) is row_data[col], f'get_value({col}) should return original value'

    # Test specific types aren't modified
    assert isinstance(adapter.get_value('date_col'), datetime.date), 'Date type should be preserved'
    assert isinstance(adapter.get_value('decimal_col'), Decimal), 'Decimal type should be preserved'

    # Test first value access
    assert adapter.get_value() is row_data['int_col'], 'get_value() without key should return first value'


def test_postgresql_row_adapter_no_conversion():
    """Test that PostgreSQLRowAdapter doesn't convert values"""
    # Create sample data
    row_data = {
        'int_col': 123,
        'decimal_col': Decimal('123.45'),
        'date_col': datetime.date(2023, 5, 15),
    }

    # Create adapter
    adapter = PostgreSQLRowAdapter(row_data)

    # Test to_dict() doesn't modify values
    result = adapter.to_dict()
    assert result == row_data, 'to_dict() should return data without conversion'

    # Test get_value() doesn't modify values
    assert adapter.get_value('int_col') is row_data['int_col']
    assert adapter.get_value('decimal_col') is row_data['decimal_col']
    assert adapter.get_value('date_col') is row_data['date_col']

    # Make sure types are preserved
    assert isinstance(adapter.get_value('decimal_col'), Decimal), 'Decimal type should be preserved'


def test_sqlite_row_adapter_no_conversion():
    """Test that SQLiteRowAdapter doesn't convert values"""
    # Mock a SQLite Row object (which behaves like both a dict and a tuple)
    class MockSQLiteRow(dict):
        def __getitem__(self, key):
            if isinstance(key, int):
                return list(self.values())[key]
            return super().__getitem__(key)

        def keys(self):
            return super().keys()

    row_data = MockSQLiteRow({
        'int_col': 123,
        'decimal_col': Decimal('123.45'),
        'date_col': datetime.date(2023, 5, 15),
    })

    # Create adapter
    adapter = SQLiteRowAdapter(row_data)

    # Test to_dict() doesn't modify values
    result = adapter.to_dict()
    for key, value in row_data.items():
        assert result[key] is value, f'to_dict() should not convert {key}'

    # Test get_value() doesn't modify values
    for key, value in row_data.items():
        assert adapter.get_value(key) is value, f'get_value({key}) should not convert value'

    # Verify first value does not get converted
    assert adapter.get_value() is list(row_data.values())[0]


def test_database_row_adapter_factory(create_simple_mock_connection):
    """Test DatabaseRowAdapter.create() factory method with simplified adapters"""
    row_data = {'id': 1, 'name': 'test'}

    pg_conn = create_simple_mock_connection('postgresql')
    sqlite_conn = create_simple_mock_connection('sqlite')
    sql_conn = create_simple_mock_connection('mssql')

    # Test with PostgreSQL
    with patch('database.utils.connection_utils.is_psycopg_connection', return_value=True):
        with patch('database.utils.connection_utils.is_pyodbc_connection', return_value=False):
            with patch('database.utils.connection_utils.is_sqlite3_connection', return_value=False):
                adapter = RowStructureAdapter.create(pg_conn, row_data)
                assert isinstance(adapter, PostgreSQLRowAdapter)

    # Test with SQLite
    with patch('database.utils.connection_utils.is_psycopg_connection', return_value=False):
        with patch('database.utils.connection_utils.is_pyodbc_connection', return_value=False):
            with patch('database.utils.connection_utils.is_sqlite3_connection', return_value=True):
                adapter = RowStructureAdapter.create(sqlite_conn, row_data)
                assert isinstance(adapter, SQLiteRowAdapter)

    # Test with SQL Server
    with patch('database.utils.connection_utils.is_psycopg_connection', return_value=False):
        with patch('database.utils.connection_utils.is_pyodbc_connection', return_value=True):
            with patch('database.utils.connection_utils.is_sqlite3_connection', return_value=False):
                adapter = RowStructureAdapter.create(sql_conn, row_data)
                assert isinstance(adapter, SQLServerRowAdapter)


if __name__ == '__main__':
    __import__('pytest').main([__file__])
