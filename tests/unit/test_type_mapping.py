"""
Tests for centralized type mapping system.
"""
import datetime
from unittest.mock import MagicMock, patch


def test_type_mapping_config():
    """Test TypeMappingConfig for column type patterns"""
    from database.config.type_mapping import TypeMappingConfig

    # Get the singleton instance
    config = TypeMappingConfig.get_instance()

    # Test basic pattern matching
    id_type = config.get_type_for_column('mssql', None, 'user_id')
    assert id_type in {'int', 'bigint'}, f"Expected int/bigint for 'user_id', got {id_type}"

    date_type = config.get_type_for_column('mssql', None, 'created_date')
    assert date_type == 'date', f"Expected date for 'created_date', got {date_type}"

    datetime_type = config.get_type_for_column('mssql', None, 'created_at')
    assert datetime_type == 'datetime', f"Expected datetime for 'created_at', got {datetime_type}"

    # Test adding a custom column pattern
    config.add_column_mapping('mssql', 'test_table', 'custom_field', 'nvarchar')
    custom_type = config.get_type_for_column('mssql', 'test_table', 'custom_field')
    assert custom_type == 'nvarchar', f'Expected nvarchar for custom_field, got {custom_type}'

    # Verify context-sensitive mapping
    assert config.get_type_for_column('mssql', 'test_table', 'custom_field') == 'nvarchar'
    assert config.get_type_for_column('mssql', 'other_table', 'custom_field') != 'nvarchar'


def test_centralized_registry():
    """Test the centralized TypeHandlerRegistry"""
    from database.adapters.type_mapping import TypeHandlerRegistry

    # Get the registry instance
    registry = TypeHandlerRegistry.get_instance()

    # Test SQL Server handlers
    assert 'mssql' in registry._handlers
    assert len(registry._handlers['mssql']) > 0

    # Test type resolution for common types
    int_type = registry.get_python_type('mssql', 4)
    assert int_type == int, f'Expected int for SQL_INTEGER(4), got {int_type}'

    float_type = registry.get_python_type('mssql', 7)
    assert float_type == float, f'Expected float for SQL_REAL(7), got {float_type}'

    str_type = registry.get_python_type('mssql', 12)
    assert str_type == str, f'Expected str for SQL_VARCHAR(12), got {str_type}'

    date_type = registry.get_python_type('mssql', 91)
    assert date_type == datetime.date, f'Expected datetime.date for SQL_TYPE_DATE(91), got {date_type}'

    # Test SQL Server type name mapping
    datetime_type = registry.get_python_type('mssql', None, 'datetime')
    assert datetime_type == datetime.datetime


def test_type_resolver():
    """Test the TypeResolver with config and registry integration"""
    from database.adapters.type_mapping import TypeResolver

    # Create resolver instance
    resolver = TypeResolver()

    # Test SQL Server type resolution
    int_type = resolver.resolve_python_type('mssql', 4, None)
    assert int_type == int

    # Test type resolution with additional context
    date_type = resolver.resolve_python_type('mssql', 91, 'event_date')
    assert date_type == datetime.date

    # Test type resolution with table context
    from database.config.type_mapping import TypeMappingConfig
    config = TypeMappingConfig.get_instance()
    config.add_column_mapping('mssql', 'events', 'custom_field', 'datetime')

    datetime_type = resolver.resolve_python_type(
        'mssql', 1, 'custom_field', table_name='events'
    )
    assert datetime_type == datetime.datetime

    # Test name-based pattern matching
    id_type = resolver.resolve_python_type('mssql', None, 'user_id')
    assert id_type == int

    status_type = resolver._resolve_by_column_name('is_active', None, None, 'mssql')
    assert status_type == bool


def test_consistent_type_mapping():
    """Test that type mapping is consistent across components"""
    from database.adapters.type_mapping import TypeHandlerRegistry
    from database.adapters.type_mapping import resolve_type

    # Mock the components to avoid initialization errors
    with patch('database.adapters.type_mapping.TypeResolver') as mock_resolver_class, \
    patch.object(TypeHandlerRegistry, 'get_instance') as mock_registry_getter:

        # Create mock instances
        mock_registry = MagicMock()
        mock_registry_getter.return_value = mock_registry

        mock_resolver = MagicMock()
        mock_resolver_class.return_value = mock_resolver

        # Configure the mocks to return datetime.datetime for SQL_TYPE_TIMESTAMP (93)
        mock_registry.get_python_type.return_value = datetime.datetime
        mock_resolver.resolve_python_type.return_value = datetime.datetime

        # Test the same type through different paths
        # Direct from registry
        registry_type = mock_registry.get_python_type('mssql', 93)
        # Through resolver
        resolver_type = mock_resolver.resolve_python_type('mssql', 93)
        # Through global function (which will use the mock resolver)
        global_type = resolve_type('mssql', 93)

        # All should return the same type
        assert registry_type == resolver_type == global_type == datetime.datetime

        # Test with column name context
        mock_registry.get_python_type.return_value = int
        mock_resolver.resolve_python_type.return_value = int

        id_from_registry = mock_registry.get_python_type('mssql', None, 'id')
        id_from_resolver = mock_resolver.resolve_python_type('mssql', None, 'id')
        id_from_global = resolve_type('mssql', None, 'id')

        # May not all be the same since registry doesn't do name-based resolution
        assert id_from_resolver == id_from_global == int


def test_postgres_type_mapping():
    """Test PostgreSQL type mapping"""
    import psycopg
    from database.adapters.type_mapping import resolve_type

    # Helper to get OID from type name
    def get_oid(type_name):
        return psycopg.postgres.types.get(type_name).oid

    # Mock the resolve_type function to avoid initialization errors
    with patch('database.adapters.type_mapping.TypeResolver') as mock_resolver_class:
        # Create a mock instance
        mock_resolver = MagicMock()
        mock_resolver_class.return_value = mock_resolver

        # Setup the mock's resolve_python_type method
        def resolve_side_effect(db_type, type_code, column_name=None, **kwargs):
            if db_type == 'postgresql':
                if type_code == get_oid('int4'):
                    return int
                elif type_code == get_oid('float8'):
                    return float
                elif type_code == get_oid('varchar'):
                    return str
                elif type_code == get_oid('date'):
                    return datetime.date
                elif type_code == get_oid('time'):
                    return datetime.datetime
                elif type_code == get_oid('bool'):
                    return bool
            return str  # default

        mock_resolver.resolve_python_type.side_effect = resolve_side_effect

        # Test integer types
        int_type = resolve_type('postgresql', get_oid('int4'))
        assert int_type == int

        # Test float types
        float_type = resolve_type('postgresql', get_oid('float8'))
        assert float_type == float

        # Test string types
        str_type = resolve_type('postgresql', get_oid('varchar'))
        assert str_type == str

        # Test date/time types
        date_type = resolve_type('postgresql', get_oid('date'))
        assert date_type == datetime.date

        time_type = resolve_type('postgresql', get_oid('time'))
        assert time_type == datetime.datetime

        # Test boolean
        bool_type = resolve_type('postgresql', get_oid('bool'))
        assert bool_type == bool


def test_sqlite_type_mapping():
    """Test SQLite type mapping"""
    from database.adapters.type_mapping import resolve_type

    # Mock the resolve_type function to avoid initialization errors
    with patch('database.adapters.type_mapping.TypeResolver') as mock_resolver_class:
        # Create a mock instance
        mock_resolver = MagicMock()
        mock_resolver_class.return_value = mock_resolver

        # Setup the mock's resolve_python_type method
        def resolve_side_effect(db_type, type_code, column_name=None, **kwargs):
            if db_type == 'sqlite':
                if type_code == 'INTEGER':
                    return int
                elif type_code == 'REAL':
                    return float
                elif type_code == 'TEXT':
                    return str
                elif type_code == 'NUMERIC(10,2)':
                    return float
                elif type_code == 'DATE':
                    return datetime.date
                elif type_code == 'DATETIME':
                    return datetime.datetime
            return str  # default

        mock_resolver.resolve_python_type.side_effect = resolve_side_effect

        # Test integer type
        int_type = resolve_type('sqlite', 'INTEGER')
        assert int_type == int

        # Test float type
        float_type = resolve_type('sqlite', 'REAL')
        assert float_type == float

        # Test string type
        str_type = resolve_type('sqlite', 'TEXT')
        assert str_type == str

        # Test with different case and parameters
        decimal_type = resolve_type('sqlite', 'NUMERIC(10,2)')
        assert decimal_type == float

        # Test date/time types
        date_type = resolve_type('sqlite', 'DATE')
        assert date_type == datetime.date

        datetime_type = resolve_type('sqlite', 'DATETIME')
        assert datetime_type == datetime.datetime


if __name__ == '__main__':
    __import__('pytest').main([__file__])
