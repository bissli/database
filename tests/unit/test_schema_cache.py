"""
Unit tests for schema cache utilities.
"""
from unittest.mock import patch

import pytest
from database.utils.schema_cache import SchemaCache


def test_schema_cache_singleton():
    """Test that SchemaCache is a singleton"""
    cache1 = SchemaCache.get_instance()
    cache2 = SchemaCache.get_instance()
    assert cache1 is cache2


@pytest.mark.skip
def test_get_column_metadata(mock_postgres_conn):
    """Test getting column metadata from cache or database"""
    cache = SchemaCache.get_instance()

    # Mock the database query response
    with patch('database.operations.query.select') as mock_select:
        mock_select.return_value = [
            {'column_name': 'id', 'data_type': 'integer', 'character_maximum_length': None,
             'numeric_precision': 32, 'numeric_scale': 0, 'is_nullable': 'NO'},
            {'column_name': 'name', 'data_type': 'character varying', 'character_maximum_length': 100,
             'numeric_precision': None, 'numeric_scale': None, 'is_nullable': 'YES'}
        ]

        # First call should query the database
        metadata = cache.get_column_metadata(mock_postgres_conn, 'users', connection_id='test_conn')

        # Verify metadata was processed correctly
        assert 'id' in metadata
        assert 'name' in metadata
        assert metadata['id']['type_name'] == 'integer'
        assert metadata['name']['max_length'] == 100
        assert not metadata['id']['is_nullable']

        # Query again - should use cached data, not database
        mock_select.reset_mock()
        metadata2 = cache.get_column_metadata(mock_postgres_conn, 'users', connection_id='test_conn')
        mock_select.assert_not_called()

        # Verify same metadata is returned
        assert metadata == metadata2


def test_clear_cache():
    """Test clearing the schema cache"""
    cache = SchemaCache.get_instance()

    # Get or create a connection cache
    connection_cache = cache.get_or_create_connection_cache('test_clear')
    connection_cache['test_table'] = {'column1': {'name': 'column1', 'type_name': 'integer'}}

    # Verify the cache has our data
    assert 'test_table' in connection_cache

    # Clear all caches
    cache.clear()

    # Get the cache again - should be empty
    connection_cache = cache.get_or_create_connection_cache('test_clear')
    assert 'test_table' not in connection_cache


if __name__ == '__main__':
    __import__('pytest').main([__file__])
