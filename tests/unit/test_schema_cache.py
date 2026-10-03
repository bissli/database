"""Unit tests for the schema caches and clearing them across connections.
"""
import logging

import pytest
from database.cache import Cache, _create_cache_key, get_schema_cache


@pytest.fixture
def cache_manager():
    """Singleton cache manager, emptied by the autouse conftest fixture.
    """
    return Cache.get_instance()


def test_get_instance_returns_the_one_shared_manager(cache_manager):
    """Verify Cache.get_instance memoizes a single manager instance.

    Mutation: get_instance returning cls() on every call.
    Oracle: object identity between two handles.
    """
    assert Cache.get_instance() is cache_manager


def test_schema_cache_is_partitioned_by_connection(cache_manager):
    """Verify each connection id gets its own cache, named schema_<id>.

    Mutation: connection_id ignored, or the None branch renamed.
    Oracle: entries absent from the other caches; a named global clear.
    """
    conn_a = cache_manager.get_schema_cache(77103)
    conn_b = cache_manager.get_schema_cache(77104)
    shared = cache_manager.get_schema_cache(None)

    conn_a['orders'] = ['id']
    shared['orders'] = ['id']

    assert 'orders' not in conn_b

    cache_manager.clear_cache('schema_global')

    assert 'orders' not in shared
    assert 'orders' in conn_a


def test_schema_cache_carries_the_schema_limits(cache_manager):
    """Verify a schema cache is built with maxsize 50 and ttl 600.

    Mutation: maxsize/ttl dropped from the get_cache call, or swapped.
    Oracle: hand-written (50, 600), apart from the defaults (100, 300).
    """
    schema_cache = cache_manager.get_schema_cache(77105)
    assert (schema_cache.maxsize, schema_cache.ttl) == (50, 600)

    default_cache = cache_manager.get_cache('schema_limits_probe')
    assert (default_cache.maxsize, default_cache.ttl) == (100, 300)


def test_module_helper_returns_the_method_cache(cache_manager):
    """Verify get_schema_cache() returns the cache the method returns.

    Mutation: the helper naming or sizing its own cache.
    Oracle: hand-written limits; identity for ids 77114, 0, '', None.
    """
    helper_cache = get_schema_cache(77114)
    assert (helper_cache.maxsize, helper_cache.ttl) == (50, 600)

    for connection_id in (77114, 0, '', None):
        assert (get_schema_cache(connection_id)
                is cache_manager.get_schema_cache(connection_id))


def test_clear_all_empties_every_cache_in_place(cache_manager):
    """Verify clear_all empties schema and strategy caches, keeping them.

    Mutation: clear_all discarding the registry, or skipping schema.
    Oracle: held references, empty and still the registered objects.
    """
    conn_cache = cache_manager.get_schema_cache(77107)
    global_cache = cache_manager.get_schema_cache(None)
    strategy_cache = cache_manager.get_cache('primary_keys_ProbeStrategy_get')
    for cache in (conn_cache, global_cache, strategy_cache):
        cache['orders'] = ['id']

    cache_manager.clear_all()

    assert len(conn_cache) == 0
    assert len(global_cache) == 0
    assert len(strategy_cache) == 0
    assert cache_manager.get_schema_cache(77107) is conn_cache


def test_clear_for_table_reaches_every_connection_cache(cache_manager):
    """Verify clear_for_table drops the table from all caches, not one.

    Mutation: clear_for_table walking only the first cache.
    Oracle: 'customers' alone survives in all three caches.
    """
    conn_a = cache_manager.get_schema_cache(77108)
    conn_b = cache_manager.get_schema_cache(77109)
    global_cache = cache_manager.get_schema_cache(None)
    for cache in (conn_a, conn_b, global_cache):
        cache['orders'] = ['id']
        cache['customers'] = ['id']

    cache_manager.clear_for_table('orders')

    assert sorted(conn_a.keys()) == ['customers']
    assert sorted(conn_b.keys()) == ['customers']
    assert sorted(global_cache.keys()) == ['customers']


def test_clear_for_table_folds_case_on_both_sides(cache_manager):
    """Verify the match lowercases the argument and the stored key.

    Mutation: .lower() dropped from table_name or from str(key).
    Oracle: each entry cleared by a name in the opposite case.
    """
    cache = cache_manager.get_schema_cache(77110)
    cache['ORDERS'] = ['id']
    cache['customers'] = ['id']

    cache_manager.clear_for_table('orders')
    assert sorted(cache.keys()) == ['customers']

    cache_manager.clear_for_table('CUSTOMERS')
    assert sorted(cache.keys()) == []


def test_clear_for_table_matches_inside_composite_keys(cache_manager):
    """Verify the table part of a composite or tuple key matches.

    Mutation: the whole str(key) compared, or tuple elements skipped.
    Oracle: a composite key and a tuple key go; 'customers' stays.
    """
    cache = cache_manager.get_schema_cache(77111)
    cache['orders:5:limit=3'] = ['id']
    cache[('columns', 'Orders')] = ['id']
    cache['customers'] = ['id']

    cache_manager.clear_for_table('orders')

    assert list(cache.keys()) == ['customers']


def test_clear_for_table_spares_tables_whose_names_contain_it(cache_manager):
    """Verify clear_for_table matches the table part of a key exactly.

    Mutation: substring containment over str(key), or the schema prefix
    kept in the comparison.
    Oracle: hand-listed survivors among strategy keys for look-alike tables.
    """
    cache = cache_manager.get_cache('table_columns_ExactPin_get_columns')
    for table in ('order', 'public.order', '"Order"', 'orders',
                  'order_items', 'reorder'):
        cache[_create_cache_key(table, (), {})] = table
    cache[_create_cache_key('customers', ('order',), {'sort': 'order'})] = 'customers'

    cache_manager.clear_for_table('order')

    assert sorted(cache.values()) == [
        'customers', 'order_items', 'orders', 'reorder']

    cache[_create_cache_key('order', (), {})] = 'order'
    cache_manager.clear_for_table('main."ORDER"')

    assert 'order' not in cache.values()


def test_falsy_connection_id_keeps_its_own_cache(cache_manager):
    """Verify ids 0 and '' are partitioned, not folded into the global.

    Mutation: `if not connection_id` in place of `is None`.
    Oracle: identity at 0 and ''; clear_cache('schema_0') spares ''.
    """
    zero_cache = cache_manager.get_schema_cache(0)
    empty_cache = cache_manager.get_schema_cache('')
    global_cache = cache_manager.get_schema_cache(None)

    zero_cache['zero_probe'] = ['id']
    empty_cache['empty_probe'] = ['id']

    assert zero_cache is not global_cache
    assert empty_cache is not global_cache
    assert zero_cache is not empty_cache
    assert 'zero_probe' not in global_cache
    assert 'empty_probe' not in global_cache

    cache_manager.clear_cache('schema_0')

    assert 'zero_probe' not in zero_cache
    assert 'empty_probe' in empty_cache


def test_clear_for_table_refuses_an_empty_name(cache_manager, caplog):
    """Verify clear_for_table('') clears nothing and warns instead.

    Mutation: the empty-name guard dropped.
    Oracle: hand-listed survivors, one warning, and a real-name control.
    """
    conn_cache = cache_manager.get_schema_cache(77115)
    conn_cache['invoices'] = ['id']
    conn_cache['shipments'] = ['id']
    global_cache = cache_manager.get_schema_cache(None)
    global_cache['invoices'] = ['id']
    global_keys_before = sorted(str(key) for key in global_cache)

    with caplog.at_level(logging.WARNING, logger='database.cache'):
        cache_manager.clear_for_table('')

    assert sorted(conn_cache.keys()) == ['invoices', 'shipments']
    assert sorted(str(key) for key in global_cache) == global_keys_before

    cache_records = [r for r in caplog.records if r.name == 'database.cache']
    assert [r.levelname for r in cache_records] == ['WARNING']

    cache_manager.clear_for_table('invoices')

    assert sorted(conn_cache.keys()) == ['shipments']
    assert 'invoices' not in global_cache


if __name__ == '__main__':
    __import__('pytest').main([__file__])
