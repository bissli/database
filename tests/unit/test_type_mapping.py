"""
Tests for centralized type mapping system.
"""
import datetime


def test_type_mapping_config():
    """Test TypeMappingConfig for column type patterns"""
    from database.config.type_mapping import TypeMappingConfig

    # Get the singleton instance
    config = TypeMappingConfig.get_instance()

    # TypeMappingConfig doesn't have default patterns - they come from config files
    # So querying without adding patterns should return None
    assert config.get_type_for_column('postgresql', None, 'unknown_column') is None

    # Test adding a custom column pattern
    config.add_column_mapping('postgresql', 'test_table', 'custom_field', 'varchar')
    custom_type = config.get_type_for_column('postgresql', 'test_table', 'custom_field')
    assert custom_type == 'varchar', f'Expected varchar for custom_field, got {custom_type}'

    # Verify context-sensitive mapping
    assert config.get_type_for_column('postgresql', 'test_table', 'custom_field') == 'varchar'
    assert config.get_type_for_column('postgresql', 'other_table', 'custom_field') != 'varchar'

    # Test adding more patterns and verify they work
    config.add_column_mapping('postgresql', None, 'global_field', 'int')
    assert config.get_type_for_column('postgresql', None, 'global_field') == 'int'
    assert config.get_type_for_column('postgresql', 'any_table', 'global_field') == 'int'


def test_centralized_registry():
    """Test the centralized TypeHandlerRegistry"""
    from database.adapters.type_mapping import TypeHandlerRegistry
    from database.adapters.type_mapping import TypeResolver

    # Creating a TypeResolver will register handlers in the registry
    resolver = TypeResolver()

    # Get the registry instance
    registry = TypeHandlerRegistry.get_instance()

    # Test PostgreSQL handlers - they're populated by TypeResolver._initialize_type_maps
    assert 'postgresql' in registry._handlers
    assert len(registry._handlers['postgresql']) > 0

    # Test type resolution for common PostgreSQL types (using OIDs)
    int_type = registry.get_python_type('postgresql', 23)  # int4 OID
    assert int_type == int, f'Expected int for int4(23), got {int_type}'

    float_type = registry.get_python_type('postgresql', 701)  # float8 OID
    assert float_type == float, f'Expected float for float8(701), got {float_type}'

    str_type = registry.get_python_type('postgresql', 1043)  # varchar OID
    assert str_type == str, f'Expected str for varchar(1043), got {str_type}'

    date_type = registry.get_python_type('postgresql', 1082)  # date OID
    assert date_type == datetime.date, f'Expected datetime.date for date(1082), got {date_type}'


def test_type_resolver():
    """Test the TypeResolver with config and registry integration"""
    from database.adapters.type_mapping import TypeResolver

    # Create resolver instance
    resolver = TypeResolver()

    # Test PostgreSQL type resolution
    int_type = resolver.resolve_python_type('postgresql', 23, None)  # int4 OID
    assert int_type == int

    # Test type resolution with additional context
    date_type = resolver.resolve_python_type('postgresql', 1082, 'event_date')  # date OID
    assert date_type == datetime.date

    # Test type resolution with table context
    from database.config.type_mapping import TypeMappingConfig
    config = TypeMappingConfig.get_instance()
    config.add_column_mapping('postgresql', 'events', 'custom_field', 'timestamp')

    datetime_type = resolver.resolve_python_type(
        'postgresql', 1, 'custom_field', table_name='events'
    )
    assert datetime_type == datetime.datetime

    # Test name-based pattern matching
    id_type = resolver.resolve_python_type('postgresql', None, 'user_id')
    assert id_type == int

    status_type = resolver._resolve_by_column_name('is_active', None, None, 'postgresql')
    assert status_type == bool


def test_consistent_type_mapping():
    """Test that type mapping is consistent across components"""
    import database.adapters.type_mapping as tm
    from database.adapters.type_mapping import TypeResolver, resolve_type

    # Reset global resolver to ensure fresh state
    tm._global_resolver = None

    # Create a resolver
    resolver = TypeResolver()

    # Test the same type through different paths
    # Through resolver directly
    resolver_type = resolver.resolve_python_type('postgresql', 1114)  # timestamp OID
    # Through global function
    global_type = resolve_type('postgresql', 1114)

    # Both should return datetime.datetime for timestamp
    assert resolver_type == global_type == datetime.datetime

    # Test with integer type
    resolver_int = resolver.resolve_python_type('postgresql', 23)  # int4 OID
    global_int = resolve_type('postgresql', 23)

    assert resolver_int == global_int == int


def test_postgres_type_mapping():
    """Test PostgreSQL type mapping"""
    import database.adapters.type_mapping as tm
    import psycopg
    from database.adapters.type_mapping import resolve_type

    # Reset global resolver to ensure fresh state
    tm._global_resolver = None

    # Helper to get OID from type name
    def get_oid(type_name):
        return psycopg.postgres.types.get(type_name).oid

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
    import database.adapters.type_mapping as tm
    from database.adapters.type_mapping import resolve_type

    # Reset global resolver to ensure fresh state
    tm._global_resolver = None

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
