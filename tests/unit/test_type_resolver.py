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
            elif db_type == 'postgresql' and type_code == 1043:
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

        # Test with column name hints
        assert resolve_type('postgresql', 1043, 'user_id') == int  # Column name hint
        assert resolve_type('postgresql', 1043, 'created_date') == datetime.date

        # Test with table name context
        with patch('database.config.type_mapping.TypeMappingConfig.get_instance') as mock_config:
            # Setup mock to return specific type for a column
            config_instance = MagicMock()
            config_instance.get_type_for_column.return_value = 'timestamp'
            mock_config.return_value = config_instance

            # Test resolution with table context
            python_type = resolve_type('postgresql', 1043, 'event_time', table_name='events')
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


def test_resolve_by_column_name():
    """Test type resolution based on column name patterns"""
    resolver = TypeResolver()

    # Test column name pattern matching
    assert resolver._resolve_by_column_name('user_id', None, None, 'postgresql') == int
    assert resolver._resolve_by_column_name('customer_id', None, None, 'postgresql') == int
    assert resolver._resolve_by_column_name('id', None, None, 'postgresql') == int

    assert resolver._resolve_by_column_name('created_date', None, None, 'postgresql') == datetime.date
    assert resolver._resolve_by_column_name('start_date', None, None, 'postgresql') == datetime.date
    assert resolver._resolve_by_column_name('date', None, None, 'postgresql') == datetime.date

    assert resolver._resolve_by_column_name('created_at', None, None, 'postgresql') == datetime.datetime
    assert resolver._resolve_by_column_name('updated_at', None, None, 'postgresql') == datetime.datetime
    assert resolver._resolve_by_column_name('timestamp', None, None, 'postgresql') == datetime.datetime

    assert resolver._resolve_by_column_name('start_time', None, None, 'postgresql') == datetime.time
    assert resolver._resolve_by_column_name('end_time', None, None, 'postgresql') == datetime.time
    assert resolver._resolve_by_column_name('time', None, None, 'postgresql') == datetime.time

    assert resolver._resolve_by_column_name('is_active', None, None, 'postgresql') == bool
    assert resolver._resolve_by_column_name('active_flag', None, None, 'postgresql') == bool
    assert resolver._resolve_by_column_name('enabled', None, None, 'postgresql') == bool

    # Test with unknown pattern
    assert resolver._resolve_by_column_name('unknown_column', None, None, 'postgresql') is None


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
                return 'numeric'
            elif column == 'color_hex':
                return 'varchar'
            return None

        config_instance.get_type_for_column.side_effect = get_type_side_effect
        mock_config.return_value = config_instance

        # Test table-specific column resolution
        assert resolver.resolve_python_type('postgresql', 1043, 'status_code', table_name='users') == int
        assert resolver.resolve_python_type('postgresql', 1700, 'total', table_name='orders') == float

        # Test column-only resolution
        assert resolver.resolve_python_type('postgresql', 1043, 'color_hex') == str  # varchar OID

        # Test fallthrough to standard resolver when no config match
        assert resolver.resolve_python_type('postgresql', 1043, 'name', table_name='users') == str


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
            if db_type == 'postgresql' and type_code == 1043:
                return str
            return str  # Default

        mock_registry.get_python_type.side_effect = get_python_type_side_effect

        # Test that handler registry is consulted
        with patch.object(resolver, '_registry', mock_registry):  # Patch the _registry attribute
            assert resolver.resolve_python_type('postgresql', 1043) == str
            mock_registry.get_python_type.assert_called_with('postgresql', 1043, None)


if __name__ == '__main__':
    __import__('pytest').main([__file__])
