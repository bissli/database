"""
Unit tests for database strategy caching functionality.
"""

import pytest
from database.strategy import get_db_strategy
from database.utils.cache import CacheManager


@pytest.fixture
def mock_connection(mocker):
    """Create a properly mocked connection for testing"""
    # Create a mock connection that appears to be PostgreSQL
    conn = mocker.Mock()
    conn.driver_connection = None

    # Create a mock cursor with proper description and fetchall
    mock_cursor = mocker.Mock()
    mock_cursor.description = [
        ('col1', 25, None, None, None, None, None),
        ('col2', 25, None, None, None, None, None)
    ]
    mock_cursor.statusmessage = 'SELECT 2'

    # Make fetchall return an iterable list with mock rows
    mock_rows = [
        {'col1': 'value1', 'col2': 'value2'},
        {'col1': 'value3', 'col2': 'value4'}
    ]
    # Create a properly iterable fetched result to avoid 'Mock object is not iterable' errors
    mock_cursor.fetchall.return_value = mock_rows

    # Set up cursor return on connection
    conn.cursor.return_value = mock_cursor

    # Make the cursor execute method just return the cursor for chaining
    mock_cursor.execute.return_value = mock_cursor

    # Set up options for the connection to handle data loading
    conn.options = mocker.Mock()
    conn.options.data_loader = lambda data, columns, **kwargs: data if isinstance(data, list) else []

    # Mock the connection wrapper access from cursor
    mock_cursor.connwrapper = conn

    # Configure connection type checks
    mocker.patch('database.utils.connection_utils.is_sqlite3_connection', return_value=False)
    mocker.patch('database.utils.connection_utils.is_psycopg_connection', return_value=True)

    # Mock the extract_column_info function to return a predetermined list
    mocker.patch('database.operations.query.extract_column_info',
                 return_value=['col1', 'col2'])

    return conn


@pytest.fixture
def strategy(mock_connection):
    """Get the actual strategy instance that will be used in tests"""
    return get_db_strategy(mock_connection)


def test_strategy_caching(mock_connection, strategy, mocker):
    """Test that strategy methods properly cache results"""
    # Setup
    table = 'test_table'

    # First clear any existing caches
    cache_manager = CacheManager.get_instance()
    cache_manager.clear_all()

    # Create a decorated test method that mimics the behavior of get_columns
    # but doesn't actually hit the database
    from database.strategy.decorators import cacheable_strategy

    # Create and patch a test method directly on the strategy that uses our decorator
    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, bypass_cache=False):
        # This is our test implementation that doesn't hit the database
        return ['col1', 'col2']

    # Create a call counter to track when the inner function is called
    call_counter = [0]

    # Modify our test function to track calls
    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, bypass_cache=False):
        # Increment counter when called
        call_counter[0] += 1
        # This is our test implementation that doesn't hit the database
        return ['col1', 'col2']

    # Replace strategy.get_columns with our test method
    strategy.get_columns = test_get_columns.__get__(strategy, type(strategy))

    # First call should execute the method
    result1 = strategy.get_columns(mock_connection, table)
    assert result1 == ['col1', 'col2']
    assert call_counter[0] == 1

    # Second call should use cache
    result2 = strategy.get_columns(mock_connection, table)
    assert result2 == ['col1', 'col2']
    assert call_counter[0] == 1  # Still 1, not incremented

    # Call with bypass_cache should execute again
    result3 = strategy.get_columns(mock_connection, table, bypass_cache=True)
    assert result3 == ['col1', 'col2']
    assert call_counter[0] == 2  # Incremented


def test_cache_isolation(mock_connection, strategy, mocker):
    """Test that different strategy methods use separate caches"""
    # Setup
    table = 'test_table'

    # Clear all caches first
    cache_manager = CacheManager.get_instance()
    cache_manager.clear_all()

    # Create and patch test methods directly on the strategy that use our decorator
    from database.strategy.decorators import cacheable_strategy

    # Create call counters
    columns_counter = [0]
    pk_counter = [0]

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, bypass_cache=False):
        columns_counter[0] += 1
        return ['col1', 'col2']

    @cacheable_strategy('primary_keys', ttl=300, maxsize=50)
    def test_get_primary_keys(self, cn, table, bypass_cache=False):
        pk_counter[0] += 1
        return ['col1']

    # Replace strategy methods with our test methods
    strategy.get_columns = test_get_columns.__get__(strategy, type(strategy))
    strategy.get_primary_keys = test_get_primary_keys.__get__(strategy, type(strategy))

    # First calls should execute the methods
    cols = strategy.get_columns(mock_connection, table)
    pks = strategy.get_primary_keys(mock_connection, table)

    assert cols == ['col1', 'col2']
    assert pks == ['col1']
    assert columns_counter[0] == 1
    assert pk_counter[0] == 1

    # Second calls should use cache
    cols2 = strategy.get_columns(mock_connection, table)
    pks2 = strategy.get_primary_keys(mock_connection, table)

    assert cols2 == ['col1', 'col2']
    assert pks2 == ['col1']
    assert columns_counter[0] == 1  # Still 1
    assert pk_counter[0] == 1  # Still 1


def test_different_tables_different_cache_entries(mock_connection, strategy, mocker):
    """Test that operations on different tables use different cache entries"""
    # Setup
    table1 = 'test_table1'
    table2 = 'test_table2'

    # Clear all caches first
    cache_manager = CacheManager.get_instance()
    cache_manager.clear_all()

    # Create and patch a test method directly on the strategy
    from database.strategy.decorators import cacheable_strategy

    # Create call counter
    columns_counter = [0]

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, *args, **kwargs):
        # Track calls
        columns_counter[0] += 1
        # Return different values based on the table
        if table == table1:
            return ['t1col1', 't1col2']
        return ['t2col1', 't2col2', 't2col3']

    # Replace strategy method with our test method
    strategy.get_columns = test_get_columns.__get__(strategy, type(strategy))

    # Call for first table
    cols1 = strategy.get_columns(mock_connection, table1)
    assert cols1 == ['t1col1', 't1col2']
    assert columns_counter[0] == 1

    # Call for second table should not use cache from first
    cols2 = strategy.get_columns(mock_connection, table2)
    assert cols2 == ['t2col1', 't2col2', 't2col3']
    assert columns_counter[0] == 2

    # Repeat calls should use respective caches
    cols1_again = strategy.get_columns(mock_connection, table1)
    cols2_again = strategy.get_columns(mock_connection, table2)

    assert cols1_again == ['t1col1', 't1col2']
    assert cols2_again == ['t2col1', 't2col2', 't2col3']
    assert columns_counter[0] == 2  # Still 2


def test_bypass_cache_parameter(mock_connection, strategy, mocker):
    """Test that bypass_cache parameter properly bypasses caching"""
    # Setup
    table = 'test_table'

    # Clear all caches first
    cache_manager = CacheManager.get_instance()
    cache_manager.clear_all()

    # Use a counter to provide different values on each call
    call_count = [0]  # Use a list so it can be modified in the closure

    # Create and patch a test method with the counter
    from database.strategy.decorators import cacheable_strategy

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, *args, **kwargs):
        call_count[0] += 1
        return [f'col{call_count[0]}', f'col{call_count[0]+1}']

    # Replace strategy method with our test method
    strategy.get_columns = test_get_columns.__get__(strategy, type(strategy))

    # First call should execute the method
    result1 = strategy.get_columns(mock_connection, table)
    assert result1 == ['col1', 'col2']
    assert call_count[0] == 1

    # Second call with bypass_cache should execute method again
    result2 = strategy.get_columns(mock_connection, table, bypass_cache=True)
    assert result2 == ['col2', 'col3']  # Different result from counter incrementing
    assert call_count[0] == 2

    # Third call without bypass should use cached value from first call
    result3 = strategy.get_columns(mock_connection, table)
    assert result3 == ['col1', 'col2']  # Same as first call
    assert call_count[0] == 2  # Unchanged


def test_cache_integration_with_cache_manager(mock_connection, strategy, mocker):
    """Test integration between strategy caches and CacheManager"""
    # Setup
    table = 'test_table'

    # First clear any existing caches
    cache_manager = CacheManager.get_instance()
    cache_manager.clear_all()

    # Create and patch test method
    from database.strategy.decorators import cacheable_strategy

    # Create call counter
    columns_counter = [0]

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, bypass_cache=False):
        columns_counter[0] += 1
        return ['col1', 'col2']

    # Replace strategy method with our test method
    strategy.get_columns = test_get_columns.__get__(strategy, type(strategy))

    # First call should populate the cache
    result = strategy.get_columns(mock_connection, table)
    assert result == ['col1', 'col2']
    assert columns_counter[0] == 1

    # Get cache manager and check if cache exists
    strategy_caches = cache_manager.get_strategy_caches()

    # There should be at least one strategy cache
    assert len(strategy_caches) > 0

    # Verify we can find a cache with the strategy class name and method name
    strategy_class = strategy.__class__.__name__
    cache_pattern = f'table_columns_{strategy_class}_test_get_columns'
    matching_caches = [name for name in strategy_caches if name == cache_pattern]
    assert len(matching_caches) == 1, f'Should find exactly one cache matching {cache_pattern}'

    # Clear all strategy caches
    cache_manager.clear_strategy_caches()

    # Call the method again - should call the wrapped method again
    result = strategy.get_columns(mock_connection, table)
    assert result == ['col1', 'col2']
    assert columns_counter[0] == 2  # Incremented


def test_cache_clearing_for_specific_table(mock_connection, strategy, mocker):
    """Test clearing cache entries for a specific table"""
    # Setup
    table1 = 'test_table1'
    table2 = 'test_table2'

    # Clear all caches first
    cache_manager = CacheManager.get_instance()
    cache_manager.clear_all()

    # Create and patch test method
    from database.strategy.decorators import cacheable_strategy

    # Create call counter
    columns_counter = [0]

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, bypass_cache=False):
        columns_counter[0] += 1
        return ['t1col1', 't1col2'] if table == table1 else ['t2col1', 't2col2']

    # Replace strategy method with our test method
    strategy.get_columns = test_get_columns.__get__(strategy, type(strategy))

    # Call methods for both tables to populate cache directly
    result1 = strategy.get_columns(mock_connection, table1)
    result2 = strategy.get_columns(mock_connection, table2)
    assert columns_counter[0] == 2

    # Get cache manager and clear cache for table1 only using the new unified method
    cache_manager.clear_caches_for_table(table1)

    # Call method for table1 again - should call wrapped method
    result1_again = strategy.get_columns(mock_connection, table1)
    assert columns_counter[0] == 3

    # Call method for table2 again - should use cache
    result2_again = strategy.get_columns(mock_connection, table2)
    assert columns_counter[0] == 3  # Unchanged


def test_different_methods_separate_caches(mock_connection, strategy, mocker):
    """Test that different methods in the same strategy class use separate caches"""
    # Setup
    table = 'test_table'
    cache_manager = CacheManager.get_instance()

    # Clear existing caches to start fresh
    cache_manager.clear_all()

    # Create and patch test methods
    from database.strategy.decorators import cacheable_strategy

    # Create call counters
    columns_counter = [0]
    pk_counter = [0]

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, bypass_cache=False):
        columns_counter[0] += 1
        return ['col1', 'col2']

    @cacheable_strategy('primary_keys', ttl=300, maxsize=50)
    def test_get_primary_keys(self, cn, table, bypass_cache=False):
        pk_counter[0] += 1
        return ['col1']

    # Replace strategy methods with our test methods
    strategy.get_columns = test_get_columns.__get__(strategy, type(strategy))
    strategy.get_primary_keys = test_get_primary_keys.__get__(strategy, type(strategy))

    # First calls should execute the methods
    columns = strategy.get_columns(mock_connection, table)
    primary_keys = strategy.get_primary_keys(mock_connection, table)

    assert columns == ['col1', 'col2']
    assert primary_keys == ['col1']
    assert columns_counter[0] == 1
    assert pk_counter[0] == 1

    # Second calls should use cache
    cached_columns = strategy.get_columns(mock_connection, table)
    cached_primary_keys = strategy.get_primary_keys(mock_connection, table)

    assert cached_columns == ['col1', 'col2']
    assert cached_primary_keys == ['col1']
    assert columns_counter[0] == 1  # Still 1
    assert pk_counter[0] == 1  # Still 1

    # Now clear only columns cache
    strategy_caches = cache_manager.get_strategy_caches()
    strategy_class = strategy.__class__.__name__
    for cache_name, cache in strategy_caches.items():
        if strategy_class in cache_name and 'table_columns' in cache_name:
            cache.clear()

    # Columns cache should be cleared, pks still cached
    new_columns = strategy.get_columns(mock_connection, table)
    new_primary_keys = strategy.get_primary_keys(mock_connection, table)

    assert columns_counter[0] == 2  # Incremented (cache cleared)
    assert pk_counter[0] == 1  # Still 1 (cache not cleared)


def test_consolidated_cache_clearing(mock_connection, strategy, mocker):
    """Test that the consolidated cache clearing clears both strategy and schema caches"""
    # Setup
    table = 'test_table'
    cache_manager = CacheManager.get_instance()
    schema_cache = mocker.Mock()

    # Patch the SchemaCache.get_instance to return our mock
    mocker.patch('database.utils.schema_cache.SchemaCache.get_instance',
                 return_value=schema_cache)

    # Clear all caches first
    cache_manager.clear_all()

    # Create and patch test method for strategy
    from database.strategy.decorators import cacheable_strategy
    strategy_call_counter = [0]

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, bypass_cache=False):
        strategy_call_counter[0] += 1
        return ['col1', 'col2']

    # Replace strategy method with our test method
    strategy.get_columns = test_get_columns.__get__(strategy, type(strategy))

    # Call once to populate cache
    result = strategy.get_columns(mock_connection, table)
    assert strategy_call_counter[0] == 1

    # Setup schema cache mock
    # Add a schema cache entry
    conn_id = id(mock_connection)
    schema_conn_cache = cache_manager.get_schema_cache(conn_id)
    schema_conn_cache[table.lower()] = {'column1': {'name': 'column1', 'type': 'int'}}

    # Verify both caches have entries
    assert len(cache_manager.get_strategy_caches()) > 0
    assert table.lower() in schema_conn_cache

    # Clear caches for the specific table
    cache_manager.clear_caches_for_table(table)

    # Verify strategy cache was cleared by forcing a call
    result_after_clear = strategy.get_columns(mock_connection, table)
    assert strategy_call_counter[0] == 2  # Should have incremented

    # Verify schema cache was also cleared
    assert table.lower() not in schema_conn_cache


def test_cache_key_generation_with_various_types():
    """Test that cache key generation handles various argument types properly"""
    from database.strategy.decorators import create_cache_key

    # Test basic types
    table_name = 'test_table'
    method_args = [None, 123, 'string', 45.67]
    method_kwargs = {'int_arg': 42, 'str_arg': 'value', 'bool_arg': True}

    key = create_cache_key(table_name, method_args, method_kwargs)

    # Verify key contains the table name and argument values
    assert table_name.lower() in key
    assert '123' in key
    assert 'string' in key  # Will be found with quotes around it
    assert '45.67' in key
    assert 'int_arg=42' in key
    assert 'str_arg=' in key  # Check just for the key part
    assert 'value' in key     # Check value separately
    assert 'bool_arg=' in key
    assert 'true' in key.lower() or 'True' in key  # Test case-insensitive or actual represented value


def test_cache_key_excludes_connection_objects(mocker):
    """Test that connection objects are excluded from cache keys"""
    from database.strategy.decorators import create_cache_key

    # Create mock connection objects
    mock_cn = mocker.Mock()
    mock_cn.cursor = mocker.Mock()
    mock_cn.driver_connection = mocker.Mock()

    # Test with connection in args
    table_name = 'test_table'
    method_args = [None, mock_cn, 'other_arg']
    method_kwargs = {'regular_arg': 42}

    key = create_cache_key(table_name, method_args, method_kwargs)

    # Key should contain table name and other_arg, but not have any reference to the connection
    # The object ID of the mock connection shouldn't be in the key
    assert table_name.lower() in key
    assert 'other_arg' in key
    assert 'regular_arg=42' in key
    assert str(id(mock_cn)) not in key

    # Test with connection in kwargs
    method_args = [None, 'regular_arg']
    method_kwargs = {'conn': mock_cn, 'another_arg': 123}

    key = create_cache_key(table_name, method_args, method_kwargs)

    assert table_name.lower() in key
    assert 'regular_arg' in key
    assert 'another_arg=123' in key
    assert 'conn=' not in key  # The conn kwarg should be excluded


def test_cache_key_handles_none_values():
    """Test that None values are handled properly in cache keys"""
    from database.strategy.decorators import create_cache_key

    table_name = 'test_table'
    method_args = [None, None, 'arg']
    method_kwargs = {'none_arg': None, 'regular_arg': 'value'}

    key = create_cache_key(table_name, method_args, method_kwargs)

    # Key should contain None values properly represented
    assert table_name.lower() in key
    assert 'none' in key.lower()  # Check for None in a case-insensitive way
    assert 'arg' in key
    assert 'none_arg=' in key
    assert 'regular_arg=' in key


def test_cache_key_deterministic_with_kwargs():
    """Test that kwargs order doesn't affect the cache key"""
    from database.strategy.decorators import create_cache_key

    table_name = 'test_table'
    method_args = [None, 'arg']

    # Create two sets of kwargs with different order
    kwargs1 = {'a': 1, 'b': 2, 'c': 3}
    kwargs2 = {'c': 3, 'a': 1, 'b': 2}

    key1 = create_cache_key(table_name, method_args, kwargs1)
    key2 = create_cache_key(table_name, method_args, kwargs2)

    # Keys should be identical despite different kwargs order
    assert key1 == key2


def test_get_detailed_stats(mock_connection, strategy, mocker):
    """Test that detailed cache statistics are properly collected and formatted"""
    # Setup
    cache_manager = CacheManager.get_instance()
    cache_manager.clear_all()

    # Create and populate strategy caches with test data
    from database.strategy.decorators import cacheable_strategy

    # Create test method with our decorator
    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, bypass_cache=False):
        return ['col1', 'col2']

    # Replace strategy method with our test method
    strategy.get_columns = test_get_columns.__get__(strategy, type(strategy))

    # Populate strategy cache
    table1 = 'test_table1'
    table2 = 'test_table2'
    strategy.get_columns(mock_connection, table1)
    strategy.get_columns(mock_connection, table2)

    # Populate schema cache
    conn_id = id(mock_connection)
    schema_cache = cache_manager.get_schema_cache(conn_id)
    schema_cache['test_table1'] = {'col1': {'name': 'col1', 'type': 'int'}}
    schema_cache['test_table2'] = {'col2': {'name': 'col2', 'type': 'varchar'}}

    # Get detailed statistics
    stats = cache_manager.get_detailed_stats()

    # Verify structure of the statistics
    assert 'all_caches' in stats
    assert 'strategy_caches' in stats
    assert 'schema_caches' in stats
    assert 'system_summary' in stats

    # Verify system summary statistics
    summary = stats['system_summary']
    assert summary['total_caches'] > 0
    assert summary['total_strategy_caches'] > 0
    assert summary['total_schema_caches'] > 0
    assert summary['total_entries'] >= 4  # At least our 4 test entries

    # Verify strategy cache statistics
    strategy_stats = stats['strategy_caches']
    assert len(strategy_stats) > 0
    for cache_stats in strategy_stats.values():
        assert 'size' in cache_stats
        assert 'maxsize' in cache_stats
        assert 'ttl' in cache_stats
        assert 'utilization' in cache_stats
        assert 'cache_type' in cache_stats
        assert cache_stats['cache_type'] == 'strategy'

    # Verify schema cache statistics
    schema_stats = stats['schema_caches']
    schema_cache_name = f'schema_{conn_id}'
    assert schema_cache_name in schema_stats
    assert schema_stats[schema_cache_name]['size'] == 2  # Our two test entries
    assert 'tables_cached' in schema_stats[schema_cache_name]
    assert schema_stats[schema_cache_name]['tables_cached'] == 2
    assert 'table_entries' in schema_stats[schema_cache_name]


def test_clearing_nonexistent_table(mock_connection, strategy, mocker):
    """Test that clearing a nonexistent table doesn't cause errors"""
    # Setup
    table = 'test_table'
    nonexistent_table = 'nonexistent_table'
    cache_manager = CacheManager.get_instance()

    # Clear all caches first
    cache_manager.clear_all()

    # Create and patch test method
    from database.strategy.decorators import cacheable_strategy
    call_counter = [0]

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, bypass_cache=False):
        call_counter[0] += 1
        return ['col1', 'col2']

    # Replace strategy method with our test method
    strategy.get_columns = test_get_columns.__get__(strategy, type(strategy))

    # Populate cache for existing table
    result = strategy.get_columns(mock_connection, table)
    assert call_counter[0] == 1

    # Try to clear cache for nonexistent table
    cache_manager.clear_caches_for_table(nonexistent_table)

    # Verify cache for existing table is still intact
    result_after_clear = strategy.get_columns(mock_connection, table)
    assert call_counter[0] == 1  # Should not increment

    # Now clear the real table
    cache_manager.clear_caches_for_table(table)

    # Verify the real table's cache was cleared
    result_after_real_clear = strategy.get_columns(mock_connection, table)
    assert call_counter[0] == 2  # Should increment


def test_table_name_case_sensitivity(mock_connection, strategy, mocker):
    """Test that table name case is handled correctly in caching and clearing"""
    # Setup
    table_lower = 'test_table'
    table_upper = 'TEST_TABLE'
    cache_manager = CacheManager.get_instance()

    # Clear all caches first
    cache_manager.clear_all()

    # Create and patch test method
    from database.strategy.decorators import cacheable_strategy
    call_counter = [0]

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, bypass_cache=False):
        call_counter[0] += 1
        return ['col1', 'col2']

    # Replace strategy method with our test method
    strategy.get_columns = test_get_columns.__get__(strategy, type(strategy))

    # Call with lowercase table name
    result_lower = strategy.get_columns(mock_connection, table_lower)
    assert call_counter[0] == 1

    # Call with uppercase table name - should use the same cache
    result_upper = strategy.get_columns(mock_connection, table_upper)
    assert call_counter[0] == 1  # Should not increment

    # Clear with one case
    cache_manager.clear_caches_for_table(table_upper)

    # Verify both cases are cleared
    result_after_clear_lower = strategy.get_columns(mock_connection, table_lower)
    assert call_counter[0] == 2  # Should increment

    # Cache should be repopulated now
    result_after_clear_upper = strategy.get_columns(mock_connection, table_upper)
    assert call_counter[0] == 2  # Should not increment again


def test_multiple_strategy_classes(mock_connection, mocker):
    """Test that different strategy classes use separate caches"""
    # Setup
    table = 'test_table'
    cache_manager = CacheManager.get_instance()

    # Clear all caches first
    cache_manager.clear_all()

    # Create two different mock strategy classes
    class MockStrategy1:
        pass

    class MockStrategy2:
        pass

    strategy1 = MockStrategy1()
    strategy2 = MockStrategy2()

    # Create and patch test methods
    from database.strategy.decorators import cacheable_strategy

    # Create call counters
    counter1 = [0]
    counter2 = [0]

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns1(self, cn, table, bypass_cache=False):
        counter1[0] += 1
        return ['s1col1', 's1col2']

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns2(self, cn, table, bypass_cache=False):
        counter2[0] += 1
        return ['s2col1', 's2col2']

    # Replace strategy methods with our test methods
    strategy1.get_columns = test_get_columns1.__get__(strategy1, MockStrategy1)
    strategy2.get_columns = test_get_columns2.__get__(strategy2, MockStrategy2)

    # Call first strategy
    result1 = strategy1.get_columns(mock_connection, table)
    assert counter1[0] == 1
    assert result1 == ['s1col1', 's1col2']

    # Call second strategy
    result2 = strategy2.get_columns(mock_connection, table)
    assert counter2[0] == 1  # Should increment for second strategy
    assert result2 == ['s2col1', 's2col2']

    # Call first strategy again - should use cache
    result1_again = strategy1.get_columns(mock_connection, table)
    assert counter1[0] == 1  # Should not increment
    assert result1_again == ['s1col1', 's1col2']

    # Call second strategy again - should use cache
    result2_again = strategy2.get_columns(mock_connection, table)
    assert counter2[0] == 1  # Should not increment
    assert result2_again == ['s2col1', 's2col2']


def test_bypass_cache_performance(mock_connection, strategy, mocker):
    """Test performance characteristics of bypass_cache parameter"""
    # This is a basic test to verify bypass_cache behavior with timing
    import time

    # Setup
    table = 'test_table'
    cache_manager = CacheManager.get_instance()

    # Clear all caches first
    cache_manager.clear_all()

    # Create and patch test method with delay
    from database.strategy.decorators import cacheable_strategy

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def test_get_columns(self, cn, table, bypass_cache=False):
        # Add a small delay to simulate database operation
        time.sleep(0.01)
        return ['col1', 'col2']

    # Replace strategy method with our test method
    strategy.get_columns = test_get_columns.__get__(strategy, type(strategy))

    # Measure time for first call (cache miss)
    start = time.time()
    result = strategy.get_columns(mock_connection, table)
    first_call_time = time.time() - start

    # Measure time for second call (cache hit)
    start = time.time()
    result = strategy.get_columns(mock_connection, table)
    second_call_time = time.time() - start

    # Measure time for call with bypass_cache
    start = time.time()
    result = strategy.get_columns(mock_connection, table, bypass_cache=True)
    bypass_call_time = time.time() - start

    # Cache hit should be faster than cache miss
    assert second_call_time < first_call_time

    # Bypassing cache should be similar to first call
    assert bypass_call_time > second_call_time


if __name__ == '__main__':
    __import__('pytest').main([__file__])
