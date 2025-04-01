import pytest
from unittest.mock import patch
from database.adapters.column_info import Column
from database.adapters.column_info import columns_from_cursor_description


def test_column_get_names():
    """Test getting column names from a list of Columns"""
    columns = [
        Column(name='id', type_code=23, python_type=int),
        Column(name='name', type_code=25, python_type=str),
        Column(name='active', type_code=16, python_type=bool)
    ]

    names = Column.get_names(columns)
    assert names == ['id', 'name', 'active']


def test_column_get_column_by_name():
    """Test finding a column by name"""
    columns = [
        Column(name='id', type_code=23, python_type=int),
        Column(name='name', type_code=25, python_type=str),
        Column(name='active', type_code=16, python_type=bool)
    ]

    col = Column.get_column_by_name(columns, 'name')
    assert col is not None
    assert col.name == 'name'
    assert col.python_type == str

    # Column not found
    col = Column.get_column_by_name(columns, 'nonexistent')
    assert col is None


def test_column_get_column_types_dict():
    """Test getting column types dictionary"""
    columns = [
        Column(name='id', type_code=23, python_type=int),
        Column(name='name', type_code=25, python_type=str)
    ]

    types_dict = Column.get_column_types_dict(columns)
    assert 'id' in types_dict
    assert 'name' in types_dict
    assert types_dict['id']['python_type'] == 'int'
    assert types_dict['name']['python_type'] == 'str'


def test_column_create_empty_columns():
    """Test creating empty columns from names"""
    names = ['id', 'name', 'active']
    columns = Column.create_empty_columns(names)

    assert len(columns) == 3
    assert columns[0].name == 'id'
    assert columns[1].name == 'name'
    assert columns[2].name == 'active'
    assert columns[0].type_code is None
    assert columns[0].python_type is None


def test_columns_from_cursor_description():
    """Test creating columns from cursor description"""
    # Mock cursor with description
    class MockCursor:
        def __init__(self):
            self.description = [
                # Simplified format for test purposes
                ('id', 23, None, None, None, None, None),
                ('name', 25, None, None, None, None, None)
            ]

    cursor = MockCursor()
    # Mock the resolve_type function to avoid initialization errors
    with patch('database.adapters.column_info.resolve_type', return_value=str):
        columns = columns_from_cursor_description(cursor, 'sqlite')

    assert len(columns) == 2
    assert columns[0].name == 'id'
    assert columns[1].name == 'name'

    # Mock cursor with no description
    cursor.description = None
    columns = columns_from_cursor_description(cursor, 'sqlite')
    assert columns == []


def test_connection_type_mapping():
    """Test mapping from connection detection to database type string"""
    from unittest.mock import MagicMock
    from database.utils.connection_utils import get_dialect_name
    from tests.fixtures.mocks import _create_simple_mock_connection
    
    # Create mock connections
    pg_conn = _create_simple_mock_connection('postgresql')
    odbc_conn = _create_simple_mock_connection('mssql')
    sqlite_conn = _create_simple_mock_connection('sqlite')
    
    # Use get_dialect_name to determine the database type
    assert get_dialect_name(pg_conn) == 'postgresql'
    assert get_dialect_name(odbc_conn) == 'mssql'
    assert get_dialect_name(sqlite_conn) == 'sqlite'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
