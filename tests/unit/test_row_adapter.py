"""
Tests for simplified row adapter behavior - verifying they only handle structure, not type conversion.
"""
import datetime
from decimal import Decimal
from unittest.mock import patch

from database.types import RowAdapter


def test_row_adapter_postgresql_no_conversion():
    """Test that RowAdapter doesn't convert values for PostgreSQL rows"""
    # Create sample data (dict, like psycopg returns)
    row_data = {
        'int_col': 123,
        'decimal_col': Decimal('123.45'),
        'date_col': datetime.date(2023, 5, 15),
    }

    # Create adapter
    adapter = RowAdapter(row_data)

    # Test to_dict() doesn't modify values
    result = adapter.to_dict()
    assert result == row_data, 'to_dict() should return data without conversion'

    # Test get_value() doesn't modify values
    assert adapter.get_value('int_col') is row_data['int_col']
    assert adapter.get_value('decimal_col') is row_data['decimal_col']
    assert adapter.get_value('date_col') is row_data['date_col']

    # Make sure types are preserved
    assert isinstance(adapter.get_value('decimal_col'), Decimal), 'Decimal type should be preserved'


def test_row_adapter_sqlite_no_conversion():
    """Test that RowAdapter doesn't convert values for SQLite rows"""
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
    adapter = RowAdapter(row_data)

    # Test to_dict() doesn't modify values
    result = adapter.to_dict()
    for key, value in row_data.items():
        assert result[key] is value, f'to_dict() should not convert {key}'

    # Test get_value() doesn't modify values
    for key, value in row_data.items():
        assert adapter.get_value(key) is value, f'get_value({key}) should not convert value'

    # Verify first value does not get converted
    assert adapter.get_value() is list(row_data.values())[0]


def test_row_adapter_factory(create_simple_mock_connection):
    """Test RowAdapter.create() factory method"""
    row_data = {'id': 1, 'name': 'test'}

    pg_conn = create_simple_mock_connection('postgresql')
    sqlite_conn = create_simple_mock_connection('sqlite')

    # Test with PostgreSQL
    with patch('database.connection.get_dialect_name', return_value='postgresql'):
        adapter = RowAdapter.create(pg_conn, row_data)
        assert isinstance(adapter, RowAdapter)
        assert adapter.to_dict() == row_data

    # Test with SQLite
    with patch('database.connection.get_dialect_name', return_value='sqlite'):
        adapter = RowAdapter.create(sqlite_conn, row_data)
        assert isinstance(adapter, RowAdapter)
        assert adapter.to_dict() == row_data


if __name__ == '__main__':
    __import__('pytest').main([__file__])
