"""
Unit tests for database strategy caching functionality.
"""

import time

import pytest
from database.cache import Cache, _create_cache_key, cacheable_strategy
from database.strategy.postgres import PostgresStrategy


@pytest.fixture
def mock_connection(mocker):
    """Create a properly mocked connection for testing"""
    conn = mocker.Mock()
    conn.driver_connection = None

    mock_cursor = mocker.Mock()
    mock_cursor.description = [
        ('col1', 25, None, None, None, None, None),
        ('col2', 25, None, None, None, None, None)
    ]
    mock_cursor.statusmessage = 'SELECT 2'

    mock_rows = [
        {'col1': 'value1', 'col2': 'value2'},
        {'col1': 'value3', 'col2': 'value4'}
    ]
    mock_cursor.fetchall.return_value = mock_rows
    conn.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value = mock_cursor

    conn.options = mocker.Mock()
    conn.options.data_loader = lambda data, columns, **kwargs: data if isinstance(data, list) else []
    mock_cursor.connwrapper = conn

    mocker.patch('database.cursor.extract_column_info', return_value=['col1', 'col2'])

    return conn


@pytest.fixture
def strategy():
    """Get the actual strategy instance for tests"""
    return PostgresStrategy()


@pytest.fixture(autouse=True)
def clear_caches():
    """Clear all caches before each test"""
    Cache.get_instance().clear_all()
    yield
    Cache.get_instance().clear_all()


@pytest.fixture
def cache_manager():
    """Provide the cache manager instance"""
    return Cache.get_instance()


class CacheableMethodFactory:
    """Factory for creating cacheable strategy methods with call tracking"""

    def __init__(self, strategy):
        self.strategy = strategy
        self.counters = {}

    def create(self, cache_type, return_value, method_name='get_columns'):
        """Create a cacheable method that tracks calls"""
        counter_key = f'{cache_type}_{method_name}'
        self.counters[counter_key] = 0

        def increment_counter():
            self.counters[counter_key] += 1

        @cacheable_strategy(cache_type, ttl=300, maxsize=50)
        def cached_method(self_inner, cn, table, bypass_cache=False):
            increment_counter()
            if callable(return_value):
                return return_value(table)
            return return_value

        setattr(self.strategy, method_name,
                cached_method.__get__(self.strategy, type(self.strategy)))

        return lambda: self.counters[counter_key]


@pytest.fixture
def method_factory(strategy):
    """Provide a factory for creating cached methods"""
    return CacheableMethodFactory(strategy)


class TestBasicCaching:
    """Tests for fundamental caching behavior"""

    def test_strategy_caching(self, mock_connection, strategy, method_factory):
        """Test that strategy methods properly cache results"""
        get_count = method_factory.create('table_columns', ['col1', 'col2'])

        result1 = strategy.get_columns(mock_connection, 'test_table')
        assert result1 == ['col1', 'col2']
        assert get_count() == 1

        result2 = strategy.get_columns(mock_connection, 'test_table')
        assert result2 == ['col1', 'col2']
        assert get_count() == 1  # Still 1 - cache hit

    def test_bypass_cache_parameter(self, mock_connection, strategy, method_factory):
        """Test that bypass_cache parameter properly bypasses caching"""
        call_values = [0]

        def dynamic_return(table):
            call_values[0] += 1
            return [f'col{call_values[0]}', f'col{call_values[0]+1}']

        get_count = method_factory.create('table_columns', dynamic_return)

        result1 = strategy.get_columns(mock_connection, 'test_table')
        assert result1 == ['col1', 'col2']
        assert get_count() == 1

        result2 = strategy.get_columns(mock_connection, 'test_table', bypass_cache=True)
        assert result2 == ['col2', 'col3']
        assert get_count() == 2

        result3 = strategy.get_columns(mock_connection, 'test_table')
        assert result3 == ['col1', 'col2']  # Cached from first call
        assert get_count() == 2


class TestCacheIsolation:
    """Tests for cache isolation between methods, tables, and strategies"""

    def test_different_tables_different_cache_entries(self, mock_connection, strategy, method_factory):
        """Test that operations on different tables use different cache entries"""
        def table_specific_return(table):
            if table == 'test_table1':
                return ['t1col1', 't1col2']
            return ['t2col1', 't2col2', 't2col3']

        get_count = method_factory.create('table_columns', table_specific_return)

        cols1 = strategy.get_columns(mock_connection, 'test_table1')
        assert cols1 == ['t1col1', 't1col2']
        assert get_count() == 1

        cols2 = strategy.get_columns(mock_connection, 'test_table2')
        assert cols2 == ['t2col1', 't2col2', 't2col3']
        assert get_count() == 2

        # Repeat calls should use caches
        assert strategy.get_columns(mock_connection, 'test_table1') == ['t1col1', 't1col2']
        assert strategy.get_columns(mock_connection, 'test_table2') == ['t2col1', 't2col2', 't2col3']
        assert get_count() == 2

    def test_different_methods_separate_caches(self, mock_connection, strategy, method_factory):
        """Test that different methods use separate caches"""
        get_cols_count = method_factory.create('table_columns', ['col1', 'col2'], 'get_columns')
        get_pks_count = method_factory.create('primary_keys', ['col1'], 'get_primary_keys')

        strategy.get_columns(mock_connection, 'test_table')
        strategy.get_primary_keys(mock_connection, 'test_table')
        assert get_cols_count() == 1
        assert get_pks_count() == 1

        # Second calls use cache
        strategy.get_columns(mock_connection, 'test_table')
        strategy.get_primary_keys(mock_connection, 'test_table')
        assert get_cols_count() == 1
        assert get_pks_count() == 1

    def test_multiple_strategy_classes(self, mock_connection, mocker):
        """Test that different strategy classes use separate caches"""
        class MockStrategy1:
            pass

        class MockStrategy2:
            pass

        strategy1 = MockStrategy1()
        strategy2 = MockStrategy2()
        counters = {'s1': 0, 's2': 0}

        @cacheable_strategy('table_columns', ttl=300, maxsize=50)
        def get_columns1(self, cn, table, bypass_cache=False):
            counters['s1'] += 1
            return ['s1col1', 's1col2']

        @cacheable_strategy('table_columns', ttl=300, maxsize=50)
        def get_columns2(self, cn, table, bypass_cache=False):
            counters['s2'] += 1
            return ['s2col1', 's2col2']

        strategy1.get_columns = get_columns1.__get__(strategy1, MockStrategy1)
        strategy2.get_columns = get_columns2.__get__(strategy2, MockStrategy2)

        assert strategy1.get_columns(mock_connection, 'test_table') == ['s1col1', 's1col2']
        assert counters['s1'] == 1

        assert strategy2.get_columns(mock_connection, 'test_table') == ['s2col1', 's2col2']
        assert counters['s2'] == 1

        # Both should use their respective caches
        strategy1.get_columns(mock_connection, 'test_table')
        strategy2.get_columns(mock_connection, 'test_table')
        assert counters['s1'] == 1
        assert counters['s2'] == 1


class TestCacheClearing:
    """Tests for cache clearing functionality"""

    def test_cache_integration_with_cache_manager(self, mock_connection, strategy,
                                                  method_factory, cache_manager):
        """Test integration between strategy caches and Cache"""
        get_count = method_factory.create('table_columns', ['col1', 'col2'])

        strategy.get_columns(mock_connection, 'test_table')
        assert get_count() == 1

        strategy_caches = cache_manager.get_strategy_caches()
        assert len(strategy_caches) > 0

        # Verify cache was created with table_columns prefix
        strategy_class = strategy.__class__.__name__
        matching_caches = [name for name in strategy_caches
                           if 'table_columns' in name and strategy_class in name]
        assert len(matching_caches) == 1

        cache_manager.clear_strategy_caches()

        strategy.get_columns(mock_connection, 'test_table')
        assert get_count() == 2

    def test_cache_clearing_for_specific_table(self, mock_connection, strategy,
                                               method_factory, cache_manager):
        """Test clearing cache entries for a specific table"""
        def table_specific_return(table):
            return ['t1col1', 't1col2'] if table == 'test_table1' else ['t2col1', 't2col2']

        get_count = method_factory.create('table_columns', table_specific_return)

        strategy.get_columns(mock_connection, 'test_table1')
        strategy.get_columns(mock_connection, 'test_table2')
        assert get_count() == 2

        cache_manager.clear_caches_for_table('test_table1')

        strategy.get_columns(mock_connection, 'test_table1')
        assert get_count() == 3  # Cache miss

        strategy.get_columns(mock_connection, 'test_table2')
        assert get_count() == 3  # Cache hit

    def test_consolidated_cache_clearing(self, mock_connection, strategy,
                                         method_factory, cache_manager):
        """Test that consolidated clearing clears both strategy and schema caches"""
        get_count = method_factory.create('table_columns', ['col1', 'col2'])
        table = 'test_table'

        strategy.get_columns(mock_connection, table)
        assert get_count() == 1

        conn_id = id(mock_connection)
        schema_conn_cache = cache_manager.get_schema_cache(conn_id)
        schema_conn_cache[table.lower()] = {'column1': {'name': 'column1', 'type': 'int'}}

        assert len(cache_manager.get_strategy_caches()) > 0
        assert table.lower() in schema_conn_cache

        cache_manager.clear_caches_for_table(table)

        strategy.get_columns(mock_connection, table)
        assert get_count() == 2
        assert table.lower() not in schema_conn_cache

    def test_clearing_nonexistent_table(self, mock_connection, strategy,
                                        method_factory, cache_manager):
        """Test that clearing a nonexistent table doesn't cause errors"""
        get_count = method_factory.create('table_columns', ['col1', 'col2'])

        strategy.get_columns(mock_connection, 'test_table')
        assert get_count() == 1

        cache_manager.clear_caches_for_table('nonexistent_table')

        strategy.get_columns(mock_connection, 'test_table')
        assert get_count() == 1  # Cache still intact

        cache_manager.clear_caches_for_table('test_table')

        strategy.get_columns(mock_connection, 'test_table')
        assert get_count() == 2

    def test_selective_cache_clearing_by_type(self, mock_connection, strategy,
                                              method_factory, cache_manager):
        """Test that clearing one cache type doesn't affect others"""
        get_cols_count = method_factory.create('table_columns', ['col1', 'col2'], 'get_columns')
        get_pks_count = method_factory.create('primary_keys', ['col1'], 'get_primary_keys')

        strategy.get_columns(mock_connection, 'test_table')
        strategy.get_primary_keys(mock_connection, 'test_table')
        assert get_cols_count() == 1
        assert get_pks_count() == 1

        # Clear only columns cache
        strategy_class = strategy.__class__.__name__
        for cache_name, cache in cache_manager.get_strategy_caches().items():
            if strategy_class in cache_name and 'table_columns' in cache_name:
                cache.clear()

        strategy.get_columns(mock_connection, 'test_table')
        strategy.get_primary_keys(mock_connection, 'test_table')
        assert get_cols_count() == 2  # Cache cleared
        assert get_pks_count() == 1  # Still cached


class TestCacheKeyGeneration:
    """Tests for cache key generation"""

    @pytest.mark.parametrize(('table_name', 'method_args', 'method_kwargs', 'expected_in_key'), [
        # Basic types
        ('test_table', [None, 123, 'string', 45.67], {'int_arg': 42, 'str_arg': 'value'},
         ['test_table', '123', 'string', '45.67', 'int_arg=42', 'str_arg=']),
        # None values
        ('test_table', [None, None, 'arg'], {'none_arg': None, 'regular_arg': 'value'},
         ['test_table', 'arg', 'none_arg=', 'regular_arg=']),
    ], ids=['basic_types', 'none_values'])
    def test_cache_key_contains_expected_values(self, table_name, method_args,
                                                method_kwargs, expected_in_key):
        """Test that cache keys contain expected components"""
        key = _create_cache_key(table_name, method_args, method_kwargs)

        for expected in expected_in_key:
            assert expected.lower() in key.lower(), f"Expected '{expected}' in key: {key}"

    def test_cache_key_excludes_connection_objects(self, mocker):
        """Test that connection objects are excluded from cache keys"""
        mock_cn = mocker.Mock()
        mock_cn.cursor = mocker.Mock()
        mock_cn.driver_connection = mocker.Mock()

        key = _create_cache_key('test_table', [None, mock_cn, 'other_arg'], {'regular_arg': 42})

        assert 'test_table' in key
        assert 'other_arg' in key
        assert 'regular_arg=42' in key
        assert str(id(mock_cn)) not in key

        # Connection in kwargs
        key2 = _create_cache_key('test_table', [None, 'regular_arg'], {'conn': mock_cn, 'another_arg': 123})

        assert 'regular_arg' in key2
        assert 'another_arg=123' in key2
        assert 'conn=' not in key2

    def test_cache_key_deterministic_with_kwargs(self):
        """Test that kwargs order doesn't affect the cache key"""
        args = [None, 'arg']

        key1 = _create_cache_key('test_table', args, {'a': 1, 'b': 2, 'c': 3})
        key2 = _create_cache_key('test_table', args, {'c': 3, 'a': 1, 'b': 2})

        assert key1 == key2


class TestTableNameCaseSensitivity:
    """Tests for table name case handling"""

    def test_table_name_case_sensitivity(self, mock_connection, strategy, method_factory, cache_manager):
        """Test that table name case is handled correctly"""
        get_count = method_factory.create('table_columns', ['col1', 'col2'])

        strategy.get_columns(mock_connection, 'test_table')
        assert get_count() == 1

        # Uppercase should use same cache
        strategy.get_columns(mock_connection, 'TEST_TABLE')
        assert get_count() == 1

        # Clear with one case
        cache_manager.clear_caches_for_table('TEST_TABLE')

        # Both cases should be cleared
        strategy.get_columns(mock_connection, 'test_table')
        assert get_count() == 2

        strategy.get_columns(mock_connection, 'TEST_TABLE')
        assert get_count() == 2  # Cache repopulated


class TestCachePerformance:
    """Tests for cache performance characteristics"""

    def test_bypass_cache_performance(self, mock_connection, strategy):
        """Test performance characteristics of bypass_cache parameter"""
        @cacheable_strategy('table_columns', ttl=300, maxsize=50)
        def slow_get_columns(self, cn, table, bypass_cache=False):
            time.sleep(0.01)
            return ['col1', 'col2']

        strategy.get_columns = slow_get_columns.__get__(strategy, type(strategy))

        # First call (cache miss)
        start = time.time()
        strategy.get_columns(mock_connection, 'test_table')
        first_call_time = time.time() - start

        # Second call (cache hit)
        start = time.time()
        strategy.get_columns(mock_connection, 'test_table')
        second_call_time = time.time() - start

        # Bypass cache
        start = time.time()
        strategy.get_columns(mock_connection, 'test_table', bypass_cache=True)
        bypass_call_time = time.time() - start

        assert second_call_time < first_call_time
        assert bypass_call_time > second_call_time


@pytest.mark.skip(reason='get_detailed_stats removed during cache simplification')
def test_get_detailed_stats():
    """Test that detailed cache statistics are properly collected and formatted"""


if __name__ == '__main__':
    __import__('pytest').main([__file__])
