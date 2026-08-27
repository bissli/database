"""
Unit tests for the schema cache surface of database.cache.

Covers the Cache singleton, get_schema_cache (method and module helper),
clear_all, clear_cache, and clear_for_table across several connections.
"""
import logging

import pytest
from database.cache import Cache, get_schema_cache


@pytest.fixture
def cache_manager():
    """Provide the singleton cache manager."""
    return Cache.get_instance()


def test_get_instance_returns_the_one_shared_manager(cache_manager):
    """Verify Cache.get_instance memoizes a single manager instance.

    Mutation: get_instance returning cls() on every call instead of
        storing and reusing cls._instance.
    Oracle: object identity between two independently obtained handles.
    """
    assert Cache.get_instance() is cache_manager


def test_schema_cache_is_reused_not_rebuilt(cache_manager):
    """Verify repeated lookups return the same live schema cache.

    Mutation: get_cache building a fresh TTLCache on every call instead
        of reusing the entry already in self._caches.
    Oracle: object identity plus a value written through the first
        handle and read back through the second.
    """
    first = cache_manager.get_schema_cache(77102)
    first['orders'] = {'id': 'integer'}

    second = cache_manager.get_schema_cache(77102)
    assert second is first
    assert second['orders'] == {'id': 'integer'}


def test_schema_cache_is_partitioned_by_connection(cache_manager):
    """Verify each connection id gets its own cache, named schema_<id>.

    Mutation: get_schema_cache ignoring connection_id and always naming
        the cache 'schema_global', or renaming the None branch.
    Oracle: an entry written for one connection is absent from the
        other, and clear_cache('schema_global') hits only the global one.
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

    Mutation: dropping maxsize/ttl from the get_cache call inside
        get_schema_cache, or swapping the two values.
    Oracle: get_cache's own defaults (100, 300) differ from both schema
        values, so a dropped or swapped argument shows.
    """
    schema_cache = cache_manager.get_schema_cache(77105)
    assert (schema_cache.maxsize, schema_cache.ttl) == (50, 600)

    default_cache = cache_manager.get_cache('schema_limits_probe')
    assert (default_cache.maxsize, default_cache.ttl) == (100, 300)


def test_module_helper_matches_the_method_on_both_branches(cache_manager):
    """Verify the module-level get_schema_cache mirrors the method.

    Mutation: the helper dropping its 'schema_global' branch, spelling
        the per-connection cache name differently from Cache.get_schema_cache,
        or the helper's own maxsize=50/ttl=600 drifting from the method's.
    Oracle: object identity against the method's cache for a connection
        id and for None, which are themselves distinct caches; limits
        asserted against the helper-built cache for a fresh id.
    """
    assert get_schema_cache(77106) is cache_manager.get_schema_cache(77106)
    assert get_schema_cache(None) is cache_manager.get_schema_cache(None)
    assert get_schema_cache(77106) is not get_schema_cache(None)

    helper_cache = get_schema_cache(77114)
    assert (helper_cache.maxsize, helper_cache.ttl) == (50, 600)


def test_clear_all_empties_every_cache_in_place(cache_manager):
    """Verify clear_all empties schema and strategy caches, keeping them.

    Mutation: clear_all discarding the registry (self._caches.clear())
        or clearing only get_strategy_caches().
    Oracle: held references are empty afterwards and are still the
        objects get_schema_cache hands back.
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

    Mutation: clear_for_table walking only the first registered cache
        instead of every value in self._caches.
    Oracle: a hand-listed survivor set per cache - 'orders' gone and
        'customers' left, in all three caches.
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

    Mutation: dropping .lower() from table_name, or from str(key), in
        clear_for_table.
    Oracle: two entries differing only in case, each cleared by the
        opposite case, with the untouched entry hand-listed.
    """
    cache = cache_manager.get_schema_cache(77110)
    cache['ORDERS'] = ['id']
    cache['customers'] = ['id']

    cache_manager.clear_for_table('orders')
    assert sorted(cache.keys()) == ['customers']

    cache_manager.clear_for_table('CUSTOMERS')
    assert sorted(cache.keys()) == []


def test_clear_for_table_matches_inside_composite_keys(cache_manager):
    """Verify the table name matches anywhere in the stringified key.

    Mutation: comparing table_lower == str(key).lower() instead of
        testing containment, or dropping str() around the key.
    Oracle: hand-listed keys - a strategy-style 'orders:5:limit=3' and a
        tuple key ('columns', 'Orders') go, 'customers' stays.
    """
    cache = cache_manager.get_schema_cache(77111)
    cache['orders:5:limit=3'] = ['id']
    cache[('columns', 'Orders')] = ['id']
    cache['customers'] = ['id']

    cache_manager.clear_for_table('orders')

    assert list(cache.keys()) == ['customers']


def test_clear_cache_targets_one_connection_by_name(cache_manager):
    """Verify clear_cache empties only the named cache.

    Mutation: clear_cache dropping its membership guard (KeyError on an
        unknown name) or clearing every cache instead of the named one.
    Oracle: an independently spelled cache name, with the sibling
        connection's entry surviving both calls.
    """
    conn_a = cache_manager.get_schema_cache(77112)
    conn_b = cache_manager.get_schema_cache(77113)
    conn_a['orders'] = ['id']
    conn_b['orders'] = ['id']

    cache_manager.clear_cache('schema_no_such_connection')
    assert 'orders' in conn_a
    assert 'orders' in conn_b

    cache_manager.clear_cache('schema_77112')
    assert 'orders' not in conn_a
    assert 'orders' in conn_b


def test_falsy_connection_id_keeps_its_own_cache(cache_manager):
    """Verify ids 0 and '' are partitioned, not folded into the global.

    Mutation: restoring the truthiness test in Cache.get_schema_cache -
        'schema_global' if not connection_id - which hands ids 0 and ''
        the one cache every other connection shares.
    Oracle: identity at the 0 boundary against the None cache, then
        clear_cache('schema_0') naming the id-0 cache directly while the
        id-'' entry survives.
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


def test_module_helper_keeps_a_falsy_connection_id_separate(cache_manager):
    """Verify the module helper partitions id 0 and id '' from None too.

    Mutation: restoring 'if connection_id else' in the module-level
        get_schema_cache, which returns the global cache for a falsy id
        even while the method keeps its own.
    Oracle: identity of the helper's id-0 cache against the method's
        id-0 cache, and against the None cache, which are distinct.
    """
    assert get_schema_cache(0) is cache_manager.get_schema_cache(0)
    assert get_schema_cache('') is cache_manager.get_schema_cache('')
    assert get_schema_cache(0) is not get_schema_cache(None)
    assert get_schema_cache('') is not get_schema_cache(None)


def test_clear_for_table_refuses_an_empty_name(cache_manager, caplog):
    """Verify clear_for_table('') clears nothing and warns instead.

    Mutation: dropping the empty-name guard, so '' matches as a
        substring of every stringified key and empties every cache.
    Oracle: hand-listed survivors in a fresh cache plus a before/after
        key snapshot of the shared global cache, one warning record from
        database.cache proving the guard branch ran, and a real table
        name clearing the same entry as a positive control.
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
