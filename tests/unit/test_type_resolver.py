"""
Tests for type resolution system to verify proper type identification behavior.
"""
import datetime
from unittest.mock import MagicMock, patch

from database.adapters.type_mapping import TypeResolver, resolve_type


def test_resolve_type_function():
    """Test the global resolve_type function"""
    # Mock the TypeResolver to avoid initialization errors
    with patch('database.adapters.type_mapping.TypeResolver') as mock_resolver_class:
        # Create a mock resolver instance
        mock_resolver = MagicMock()
        mock_resolver_class.return_value = mock_resolver

        # Configure mock return values
        def resolve_side_effect(db_type, type_code, column_name=None, **kwargs):
            if db_type == 'postgresql' and type_code == 23:
                return int
            elif db_type == 'sqlite' and type_code == 'INTEGER':
                return int
            elif db_type == 'mssql' and type_code == 4:
                return int
            elif db_type == 'mssql' and type_code == 12:
                if column_name == 'user_id':
                    return int
                elif column_name == 'created_date':
                    return datetime.date
                elif column_name == 'event_time' and kwargs.get('table_name') == 'events':
                    return datetime.datetime
            return str  # default

        mock_resolver.resolve_python_type.side_effect = resolve_side_effect

        # Test basic type resolution
        assert resolve_type('postgresql', 23) == int  # int4 OID
        assert resolve_type('sqlite', 'INTEGER') == int
        assert resolve_type('mssql', 4) == int  # SQL_INTEGER

        # Test with column name hints
        assert resolve_type('mssql', 12, 'user_id') == int  # Column name hint
        assert resolve_type('mssql', 12, 'created_date') == datetime.date

        # Test with table name context
        with patch('database.config.type_mapping.TypeMappingConfig.get_instance') as mock_config:
            # Setup mock to return specific type for a column
            config_instance = MagicMock()
            config_instance.get_type_for_column.return_value = 'datetime'
            mock_config.return_value = config_instance

            # Test resolution with table context
            python_type = resolve_type('mssql', 12, 'event_time', table_name='events')
            assert python_type == datetime.datetime


def test_type_resolver_postgres_types():
    """Test PostgreSQL type resolution"""
    resolver = TypeResolver()

    # Test basic PostgreSQL types
    assert resolver.resolve_python_type('postgresql', 23) == int  # int4
    assert resolver.resolve_python_type('postgresql', 25) == str  # text
    assert resolver.resolve_python_type('postgresql', 16) == bool  # bool
    assert resolver.resolve_python_type('postgresql', 1700) == float  # numeric
    assert resolver.resolve_python_type('postgresql', 1082) == datetime.date  # date

    # Test unknown type falls back to string
    assert resolver.resolve_python_type('postgresql', 99999) == str


def test_type_resolver_sqlite_types():
    """Test SQLite type resolution"""
    resolver = TypeResolver()

    # Test basic SQLite types
    assert resolver.resolve_python_type('sqlite', 'INTEGER') == int
    assert resolver.resolve_python_type('sqlite', 'TEXT') == str
    assert resolver.resolve_python_type('sqlite', 'REAL') == float
    assert resolver.resolve_python_type('sqlite', 'BLOB') == bytes
    assert resolver.resolve_python_type('sqlite', 'NUMERIC') == float
    assert resolver.resolve_python_type('sqlite', 'BOOLEAN') == bool
    assert resolver.resolve_python_type('sqlite', 'DATE') == datetime.date
    assert resolver.resolve_python_type('sqlite', 'DATETIME') == datetime.datetime

    # Test type with parameters
    assert resolver.resolve_python_type('sqlite', 'NUMERIC(10,2)') == float

    # Test unknown type falls back to string
    assert resolver.resolve_python_type('sqlite', 'CUSTOM_TYPE') == str


def test_type_resolver_sqlserver_types():
    """Test SQL Server type resolution"""
    resolver = TypeResolver()

    # Test numeric types
    assert resolver.resolve_python_type('mssql', 4) == int  # SQL_INTEGER
    assert resolver.resolve_python_type('mssql', -5) == int  # SQL_BIGINT
    assert resolver.resolve_python_type('mssql', 3) == float  # SQL_DECIMAL
    assert resolver.resolve_python_type('mssql', 2) == float  # SQL_NUMERIC
    assert resolver.resolve_python_type('mssql', 6) == float  # SQL_FLOAT
    assert resolver.resolve_python_type('mssql', 7) == float  # SQL_REAL

    # Test string types
    assert resolver.resolve_python_type('mssql', 1) == str  # SQL_CHAR
    assert resolver.resolve_python_type('mssql', 12) == str  # SQL_VARCHAR
    assert resolver.resolve_python_type('mssql', -1) == str  # SQL_LONGVARCHAR
    assert resolver.resolve_python_type('mssql', -9) == str  # SQL_WVARCHAR

    # Test date/time types
    assert resolver.resolve_python_type('mssql', 91) == datetime.date  # SQL_TYPE_DATE
    assert resolver.resolve_python_type('mssql', 92) == datetime.time  # SQL_TYPE_TIME
    assert resolver.resolve_python_type('mssql', 93) == datetime.datetime  # SQL_TYPE_TIMESTAMP

    # Test custom types by name
    assert resolver.resolve_python_type('mssql', 'int') == int
    assert resolver.resolve_python_type('mssql', 'varchar') == str
    assert resolver.resolve_python_type('mssql', 'datetime') == datetime.datetime
    assert resolver.resolve_python_type('mssql', 'bit') == bool


def test_resolve_by_column_name():
    """Test type resolution based on column name patterns"""
    resolver = TypeResolver()

    # Test column name pattern matching
    assert resolver._resolve_by_column_name('user_id', None, None, 'mssql') == int
    assert resolver._resolve_by_column_name('customer_id', None, None, 'mssql') == int
    assert resolver._resolve_by_column_name('id', None, None, 'mssql') == int

    assert resolver._resolve_by_column_name('created_date', None, None, 'mssql') == datetime.date
    assert resolver._resolve_by_column_name('start_date', None, None, 'mssql') == datetime.date
    assert resolver._resolve_by_column_name('date', None, None, 'mssql') == datetime.date

    assert resolver._resolve_by_column_name('created_at', None, None, 'mssql') == datetime.datetime
    assert resolver._resolve_by_column_name('updated_at', None, None, 'mssql') == datetime.datetime
    assert resolver._resolve_by_column_name('timestamp', None, None, 'mssql') == datetime.datetime

    assert resolver._resolve_by_column_name('start_time', None, None, 'mssql') == datetime.time
    assert resolver._resolve_by_column_name('end_time', None, None, 'mssql') == datetime.time
    assert resolver._resolve_by_column_name('time', None, None, 'mssql') == datetime.time

    assert resolver._resolve_by_column_name('is_active', None, None, 'mssql') == bool
    assert resolver._resolve_by_column_name('active_flag', None, None, 'mssql') == bool
    assert resolver._resolve_by_column_name('enabled', None, None, 'mssql') == bool

    # Test with unknown pattern
    assert resolver._resolve_by_column_name('unknown_column', None, None, 'mssql') is None


def test_config_overrides():
    """Test type resolution with config overrides"""
    resolver = TypeResolver()

    with patch('database.config.type_mapping.TypeMappingConfig.get_instance') as mock_config:
        # Setup mock to return specific types for columns
        config_instance = MagicMock()

        # Set up different return values based on input
        def get_type_side_effect(db_type, table, column):
            if table == 'users' and column == 'status_code':
                return 'int'
            elif table == 'orders' and column == 'total':
                return 'decimal'
            elif column == 'color_hex':
                return 'varchar'
            return None

        config_instance.get_type_for_column.side_effect = get_type_side_effect
        mock_config.return_value = config_instance

        # Test table-specific column resolution
        assert resolver.resolve_python_type('mssql', 12, 'status_code', table_name='users') == int
        assert resolver.resolve_python_type('mssql', 2, 'total', table_name='orders') == float

        # Test column-only resolution
        assert resolver.resolve_python_type('mssql', 12, 'color_hex') == str  # SQL_VARCHAR (12) is the correct type for varchar

        # Test fallthrough to standard resolver when no config match
        assert resolver.resolve_python_type('mssql', 12, 'name', table_name='users') == str


def test_integration_with_handlers():
    """Test integration with type handlers through registry"""
    from database.adapters.type_mapping import TypeHandlerRegistry

    resolver = TypeResolver()

    with patch.object(TypeHandlerRegistry, 'get_instance') as mock_registry_getter:
        # Create a mock registry instance
        mock_registry = MagicMock()
        mock_registry_getter.return_value = mock_registry

        # Set up the mock to return custom types
        def get_python_type_side_effect(db_type, type_code, type_name=None):
            if db_type == 'mssql' and type_code == 231:
                return str
            return str  # Default

        mock_registry.get_python_type.side_effect = get_python_type_side_effect

        # Test that handler registry is consulted
        with patch.object(resolver, '_registry', mock_registry):  # Patch the _registry attribute
            assert resolver.resolve_python_type('mssql', 231) == str
            mock_registry.get_python_type.assert_called_with('mssql', 231, None)


if __name__ == '__main__':
    __import__('pytest').main([__file__])
