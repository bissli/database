"""
Unit tests for schema cache utilities.
"""
from database.cache import Cache


def test_cache_singleton():
    """Test that Cache is a singleton"""
    cache1 = Cache.get_instance()
    cache2 = Cache.get_instance()
    assert cache1 is cache2


def test_schema_cache_operations():
    """Test schema cache get/set operations"""
    cache = Cache.get_instance()

    # Get a schema cache for a connection
    connection_cache = cache.get_schema_cache('test_conn')
    connection_cache['test_table'] = {'column1': {'name': 'column1', 'type_name': 'integer'}}

    # Verify the cache has our data
    assert 'test_table' in connection_cache

    # Clear all caches
    cache.clear_all()

    # Get the cache again - should be empty
    connection_cache = cache.get_schema_cache('test_conn')
    assert 'test_table' not in connection_cache


def test_clear_for_table():
    """Test clearing cache entries for a specific table"""
    cache = Cache.get_instance()
    cache.clear_all()

    # Add entries for multiple tables
    schema_cache = cache.get_schema_cache('test_conn')
    schema_cache['table1'] = {'col1': {}}
    schema_cache['table2'] = {'col2': {}}

    # Clear only table1
    cache.clear_for_table('table1')

    # table1 should be cleared, table2 should remain
    assert 'table1' not in schema_cache
    assert 'table2' in schema_cache


if __name__ == '__main__':
    __import__('pytest').main([__file__])
