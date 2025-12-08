"""
Tests for type resolution system to verify proper type identification behavior.
"""
import datetime

from database.types import resolve_type


def test_resolve_type_function():
    """Test the global resolve_type function"""
    # Test basic type resolution
    assert resolve_type('postgresql', 23) == int  # int4 OID
    assert resolve_type('sqlite', 'INTEGER') == int

    # Test with column name hints
    assert resolve_type('postgresql', 23, 'user_id') == int  # int4 is always int
    assert resolve_type('postgresql', 1082, 'created_date') == datetime.date  # date OID

    # Test name-based resolution when type code is None
    assert resolve_type('postgresql', None, 'user_id') == int
    assert resolve_type('postgresql', None, 'created_at') == datetime.datetime


def test_postgres_types():
    """Test PostgreSQL type resolution"""
    # Test basic PostgreSQL types
    assert resolve_type('postgresql', 23) == int  # int4
    assert resolve_type('postgresql', 25) == str  # text
    assert resolve_type('postgresql', 16) == bool  # bool
    assert resolve_type('postgresql', 1700) == float  # numeric
    assert resolve_type('postgresql', 1082) == datetime.date  # date

    # Test unknown type falls back to string
    assert resolve_type('postgresql', 99999) == str


def test_sqlite_types():
    """Test SQLite type resolution"""
    # Test basic SQLite types
    assert resolve_type('sqlite', 'INTEGER') == int
    assert resolve_type('sqlite', 'TEXT') == str
    assert resolve_type('sqlite', 'REAL') == float
    assert resolve_type('sqlite', 'BLOB') == bytes
    assert resolve_type('sqlite', 'NUMERIC') == float
    assert resolve_type('sqlite', 'BOOLEAN') == bool
    assert resolve_type('sqlite', 'DATE') == datetime.date
    assert resolve_type('sqlite', 'DATETIME') == datetime.datetime

    # Test type with parameters
    assert resolve_type('sqlite', 'NUMERIC(10,2)') == float

    # Test unknown type falls back to string
    assert resolve_type('sqlite', 'CUSTOM_TYPE') == str


def test_resolve_by_column_name():
    """Test type resolution based on column name patterns"""
    # Test ID column patterns
    assert resolve_type('postgresql', None, 'user_id') == int
    assert resolve_type('postgresql', None, 'customer_id') == int
    assert resolve_type('postgresql', None, 'id') == int

    # Test date column patterns
    assert resolve_type('postgresql', None, 'created_date') == datetime.date
    assert resolve_type('postgresql', None, 'start_date') == datetime.date

    # Test datetime column patterns
    assert resolve_type('postgresql', None, 'created_at') == datetime.datetime
    assert resolve_type('postgresql', None, 'updated_at') == datetime.datetime
    assert resolve_type('postgresql', None, 'timestamp') == datetime.datetime
    assert resolve_type('postgresql', None, 'event_datetime') == datetime.datetime

    # Test time column patterns
    assert resolve_type('postgresql', None, 'start_time') == datetime.time
    assert resolve_type('postgresql', None, 'end_time') == datetime.time

    # Test boolean column patterns
    assert resolve_type('postgresql', None, 'is_active') == bool
    assert resolve_type('postgresql', None, 'active_flag') == bool
    assert resolve_type('postgresql', None, 'enabled') == bool

    # Test money column patterns
    assert resolve_type('postgresql', None, 'total_price') == float
    assert resolve_type('postgresql', None, 'amount_paid') == float

    # Test with unknown pattern defaults to str
    assert resolve_type('postgresql', None, 'unknown_column') == str


def test_type_code_is_python_type():
    """Test that passing a Python type returns it unchanged"""
    assert resolve_type('postgresql', int) == int
    assert resolve_type('sqlite', str) == str
    assert resolve_type('postgresql', datetime.datetime) == datetime.datetime
    assert resolve_type('sqlite', bool) == bool


def test_both_type_code_and_column_name():
    """Test when both type code and column name are provided"""
    # Type code takes precedence when it's a valid known type
    assert resolve_type('postgresql', 23, 'some_column') == int  # int4 OID
    assert resolve_type('sqlite', 'INTEGER', 'some_column') == int

    # For unknown type code, column name patterns are used
    assert resolve_type('postgresql', 99999, 'user_id') == int
    assert resolve_type('postgresql', 99999, 'is_active') == bool


if __name__ == '__main__':
    __import__('pytest').main([__file__])
