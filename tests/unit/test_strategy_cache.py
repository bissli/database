"""
Unit tests for strategy result caching.

Covers the TTL cache manager in database/cache.py, the
@cacheable_strategy decorator, and the concrete strategy overrides that
have to re-apply that decorator themselves.
"""
import cachetools
import pytest
from database.cache import Cache, _create_cache_key, cacheable_strategy
from database.cache import get_schema_cache
from database.exceptions import DatabaseError
from database.strategy import get_available_dialects, get_db_strategy
from database.strategy import get_strategy, get_strategy_class
from database.strategy import is_supported_dialect
from database.strategy.postgres import PostgresStrategy
from database.strategy.sqlite import SQLiteStrategy


class FakeConnection:
    """Connection stand-in for cache paths, which never use the cursor.

    Carries the two attributes _create_cache_key uses to recognize a
    connection object and drop it from the key.
    """

    def __init__(self):
        self.cursor = None
        self.driver_connection = None


class SequenceProbeStrategy(SQLiteStrategy):
    """SQLite strategy with counted, scripted metadata lookups.

    Overriding the two lookups the shared finder calls lets a test fix
    the inputs and count how often the finder body actually runs.
    """

    def __init__(self, sequence_cols, primary_keys):
        self.sequence_cols = sequence_cols
        self.primary_keys = primary_keys
        self.lookups = 0

    def get_sequence_columns(self, cn, table, bypass_cache=False):
        self.lookups += 1
        return list(self.sequence_cols)

    def get_primary_keys(self, cn, table, bypass_cache=False):
        self.lookups += 1
        return list(self.primary_keys)


class CachedLookupProbeStrategy(SQLiteStrategy):
    """SQLite strategy whose two metadata lookups carry their own cache.

    SequenceProbeStrategy leaves its overrides undecorated, so it cannot
    show whether a nested lookup answered from cache. These overrides
    apply @cacheable_strategy, so a bypass that never reaches them shows
    up as a lookup that did not happen.
    """

    def __init__(self, sequence_cols, primary_keys):
        self.sequence_cols = sequence_cols
        self.primary_keys = primary_keys
        self.lookups = []

    @cacheable_strategy('sequence_columns', ttl=300, maxsize=50)
    def get_sequence_columns(self, cn, table, bypass_cache=False):
        self.lookups.append(('sequence_columns', bypass_cache))
        return list(self.sequence_cols)

    @cacheable_strategy('primary_keys', ttl=300, maxsize=50)
    def get_primary_keys(self, cn, table, bypass_cache=False):
        self.lookups.append(('primary_keys', bypass_cache))
        return list(self.primary_keys)


class ProbeStrategy:
    """Isolated probe for method-factory tests.

    A distinct class name keeps its cache entries from colliding with the
    real PostgresStrategy caches that TestConcreteStrategyMethodsAreCached
    registers under names like 'primary_keys_PostgresStrategy_*'.
    """


class CacheableMethodFactory:
    """Build decorated strategy methods that count their invocations."""

    def __init__(self, strategy):
        self.strategy = strategy
        self.counters = {}

    def create(self, cache_type, return_value, method_name='get_columns'):
        """Bind a counted, cacheable method and return its counter."""
        counter_key = f'{cache_type}_{method_name}'
        self.counters[counter_key] = 0
        counters = self.counters

        def cached_method(self_inner, cn, table, bypass_cache=False):
            counters[counter_key] += 1
            if callable(return_value):
                return return_value(table)
            return return_value

        # The decorator names the cache after method.__name__, so the
        # stand-in has to answer to the name it is bound under.
        cached_method.__name__ = method_name
        decorated = cacheable_strategy(cache_type, ttl=300, maxsize=50)(
            cached_method)

        setattr(
            self.strategy, method_name,
            decorated.__get__(self.strategy, type(self.strategy)))

        return lambda: counters[counter_key]


@pytest.fixture
def mock_connection():
    """Connection object accepted by every strategy cache path."""
    return FakeConnection()


@pytest.fixture
def strategy():
    """Fresh probe strategy instance, safe to monkey-patch.

    ProbeStrategy is used instead of PostgresStrategy so that the
    method-factory-bound caches never squat on the real strategy cache
    names, which would mask ttl/maxsize mutations in the concrete-strategy
    tests that run in the same session.
    """
    return ProbeStrategy()


@pytest.fixture
def cache_manager():
    """Singleton cache manager, emptied by the autouse conftest fixture."""
    return Cache.get_instance()


@pytest.fixture
def method_factory(strategy):
    """Factory for counted cacheable methods bound to the strategy."""
    return CacheableMethodFactory(strategy)


class TestCacheManager:
    """Tests for the Cache singleton and its named TTL caches."""

    def test_get_cache_honors_requested_limits_and_reuses_the_object(
            self, cache_manager):
        """Verify a named cache is created once with the asked-for limits.

        Mutation: swapping maxsize and ttl in Cache.get_cache's TTLCache
        call, or dropping the `if name not in self._caches` guard so
        every lookup builds a fresh cache.
        Oracle: hand-picked maxsize=3/ttl=99, plus an entry that has to
        survive the second lookup.
        """
        cache = cache_manager.get_cache('limits_probe', maxsize=3, ttl=99)
        cache['key'] = 'value'

        assert isinstance(cache, cachetools.TTLCache)
        assert cache.maxsize == 3
        assert cache.ttl == 99

        again = cache_manager.get_cache('limits_probe', maxsize=8, ttl=1)
        assert again is cache
        assert again['key'] == 'value'
        assert again.maxsize == 3

    def test_clear_all_empties_caches_without_discarding_them(
            self, cache_manager):
        """Verify clear_all() empties every cache and keeps the objects.

        Mutation: replacing the per-cache clear() loop in Cache.clear_all
        with self._caches.clear(), which leaves stale entries visible
        through any reference a caller already holds.
        Oracle: the identity of the held cache object plus its length.
        """
        first = cache_manager.get_cache('clear_all_a')
        second = cache_manager.get_cache('clear_all_b')
        first['x'] = 1
        second['y'] = 2

        cache_manager.clear_all()

        assert len(first) == 0
        assert len(second) == 0
        assert cache_manager.get_cache('clear_all_a') is first

    def test_clear_cache_touches_only_the_named_cache(self, cache_manager):
        """Verify clear_cache() spares its siblings and unknown names.

        Mutation: routing Cache.clear_cache to clear_all, or dropping its
        `if name in self._caches` guard so an unknown name raises.
        Oracle: the sibling entry that must still be readable afterwards.
        """
        target = cache_manager.get_cache('clear_one_target')
        sibling = cache_manager.get_cache('clear_one_sibling')
        target['x'] = 'gone'
        sibling['y'] = 'kept'

        cache_manager.clear_cache('never_created')
        cache_manager.clear_cache('clear_one_target')

        assert len(target) == 0
        assert sibling['y'] == 'kept'

    def test_clear_for_table_matches_case_folded_anywhere_in_the_key(
            self, cache_manager):
        """Verify table clearing folds case and matches inside the key.

        Mutation: dropping .lower() from table_lower in
        Cache.clear_for_table, or narrowing `table_lower in
        str(key).lower()` to a startswith or equality test.
        Oracle: a hand-listed set of surviving keys, one of which holds
        the table name in the middle rather than at the start.
        """
        cache = cache_manager.get_cache('table_clear_probe')
        cache['test_table:a=1:'] = 'leading'
        cache['audit:owner=test_table:'] = 'embedded'
        cache['other_table::'] = 'unrelated'

        cache_manager.clear_for_table('TEST_TABLE')

        assert sorted(cache) == ['other_table::']

    def test_clear_caches_for_table_alias_clears_one_table(
            self, cache_manager):
        """Verify the backwards-compatible alias is the per-table clear.

        Mutation: rebinding clear_caches_for_table to clear_all in
        cache.py.
        Oracle: the untouched second table's entry.
        """
        cache = cache_manager.get_cache('alias_probe')
        cache['alpha::'] = 1
        cache['beta::'] = 2

        cache_manager.clear_caches_for_table('alpha')

        assert sorted(cache) == ['beta::']

    def test_get_strategy_caches_selects_exactly_the_four_prefixes(
            self, cache_manager):
        """Verify only the four strategy cache prefixes are selected.

        Mutation: dropping 'sequence_column_finder_' from
        strategy_prefixes, or relaxing name.startswith(prefix) to
        `prefix in name`, which would also pull in a cache whose name
        merely contains a prefix.
        Oracle: a hand-written expected set over caches tagged with a
        marker suffix, so caches left by other tests cannot mask it.
        """
        tagged = [
            'primary_keys_SelPin',
            'table_columns_SelPin',
            'sequence_columns_SelPin',
            'sequence_column_finder_SelPin',
            'schema_SelPin',
            'custom_primary_keys_SelPin',
            ]
        for name in tagged:
            cache_manager.get_cache(name)

        selected = {
            name for name in cache_manager.get_strategy_caches()
            if name.endswith('_SelPin')
        }

        assert selected == {
            'primary_keys_SelPin',
            'table_columns_SelPin',
            'sequence_columns_SelPin',
            'sequence_column_finder_SelPin',
            }

    def test_clear_strategy_caches_leaves_schema_caches_alone(
            self, cache_manager):
        """Verify strategy clearing does not empty the schema caches.

        Mutation: routing Cache.clear_strategy_caches to clear_all, or
        iterating self._caches instead of get_strategy_caches().
        Oracle: the schema entry, which must still hold its value.
        """
        strategy_cache = cache_manager.get_cache('table_columns_ClearPin_m')
        strategy_cache['users::'] = ['id', 'name']
        schema_cache = cache_manager.get_schema_cache(4242)
        schema_cache['users'] = {'id': 'int'}

        cache_manager.clear_strategy_caches()

        assert len(strategy_cache) == 0
        assert schema_cache['users'] == {'id': 'int'}

    def test_schema_cache_is_per_connection_and_shared_with_the_helper(
            self, cache_manager):
        """Verify schema caches are keyed by connection id, not global.

        Mutation: hardcoding 'schema_global' in the module-level
        get_schema_cache helper, or swapping its maxsize and ttl.
        Oracle: object identity between the helper and the method, and
        the hand-written 50/600 limits.
        """
        from_helper = get_schema_cache(11)
        from_method = cache_manager.get_schema_cache(11)

        assert from_helper is from_method
        assert from_helper.maxsize == 50
        assert from_helper.ttl == 600
        assert cache_manager.get_schema_cache(12) is not from_helper
        assert cache_manager.get_schema_cache(None) is not from_helper


class TestCacheKeyGeneration:
    """Tests for _create_cache_key."""

    @pytest.mark.parametrize(('table_name', 'method_args', 'method_kwargs',
                              'expected_key'), [
        ('test_table', [123, 'string', 45.67],
         {'str_arg': 'value', 'int_arg': 42},
         "test_table:123:'string':45.67:int_arg=42:str_arg='value'"),
        ('Test_Table', [None, 'arg'],
         {'regular_arg': 'value', 'none_arg': None},
         "test_table:none:'arg':none_arg=none:regular_arg='value'"),
    ], ids=['basic_types', 'none_values'])
    def test_cache_key_has_an_exact_form(
        self, table_name, method_args,
        method_kwargs, expected_key):
        """Verify the key layout, ordering, and case folding are fixed.

        Mutation: re-slicing method_args (the [1:] the wrapper's own
        argument stripping already makes wrong), dropping sorted() from
        the kwargs join, dropping the trailing .lower(), or swapping
        repr() for str() so 'arg' and "'arg'" collide.
        Oracle: hand-written key strings; every positional appears, and
        both kwargs dicts are built out of alphabetical order.
        """
        assert _create_cache_key(
            table_name, method_args,
            method_kwargs) == expected_key

    def test_cache_key_drops_connection_like_arguments(self):
        """Verify args and kwargs that look like connections are dropped.

        Mutation: flipping the `not hasattr(arg, 'cursor') and not
        hasattr(arg, 'driver_connection')` guard to `or`, which keeps any
        object carrying only one of the two attributes.
        Oracle: a hand-written key over probes that each carry exactly
        one of the attributes.
        """
        class HasCursor:
            cursor = None

        class HasDriverConnection:
            driver_connection = None

        class Plain:
            def __repr__(self):
                return 'PLAIN'

        key = _create_cache_key(
            'test_table',
            [HasCursor(), HasDriverConnection(), Plain()],
            {'cn': HasCursor(), 'flag': True})

        assert key == 'test_table:plain:flag=true'

    def test_cache_key_ignores_the_bypass_cache_kwarg(self):
        """Verify bypass_cache never widens the key space.

        Mutation: dropping the `if k != 'bypass_cache'` filter from the
        kwargs join in _create_cache_key.
        Oracle: a hand-written key plus the differential against a call
        that omits the kwarg entirely.
        """
        with_flag = _create_cache_key(
            'test_table', ['arg'], {'bypass_cache': True, 'x': 1})
        without_flag = _create_cache_key('test_table', ['arg'], {'x': 1})

        assert with_flag == "test_table:'arg':x=1"
        assert with_flag == without_flag

    def test_cache_key_is_independent_of_kwargs_order(self):
        """Verify two orderings of the same kwargs share one key.

        Mutation: dropping sorted() from the kwargs join in
        _create_cache_key.
        Oracle: a hand-written key both orderings must equal.
        """
        args = ['arg']

        key1 = _create_cache_key('test_table', args, {'a': 1, 'b': 2, 'c': 3})
        key2 = _create_cache_key('test_table', args, {'c': 3, 'a': 1, 'b': 2})

        assert key1 == "test_table:'arg':a=1:b=2:c=3"
        assert key2 == key1


class TestDecoratorPlumbing:
    """Tests for what @cacheable_strategy passes to the cache manager."""

    def test_second_call_returns_the_stored_result(
            self, mock_connection, strategy, method_factory):
        """Verify a repeat call replays the stored value, body unused.

        Mutation: dropping the `if cache_key in cache` early return in
        cacheable_strategy's wrapper, or the `cache[cache_key] = result`
        store that feeds it.
        Oracle: a body whose return value changes on every invocation,
        plus a call counter.
        """
        invocations = []

        def changing_return(table):
            invocations.append(table)
            return [f'col{len(invocations)}']

        get_count = method_factory.create('table_columns', changing_return)

        assert strategy.get_columns(mock_connection, 'test_table') == ['col1']
        assert strategy.get_columns(mock_connection, 'test_table') == ['col1']
        assert get_count() == 1

    def test_bypass_cache_neither_reads_nor_writes_the_cache(
            self, mock_connection, strategy, method_factory, cache_manager):
        """Verify bypass_cache runs the body and leaves the entry alone.

        Mutation: deleting the `if bypass_cache:` early return in
        cacheable_strategy so the flag falls through to the cached path.
        Oracle: a body returning a new value each call, so a bypassed
        call that wrote through would be visible on the next cached read.
        """
        call_values = [0]

        def dynamic_return(table):
            call_values[0] += 1
            return [f'col{call_values[0]}', f'col{call_values[0] + 1}']

        get_count = method_factory.create('table_columns', dynamic_return)

        assert strategy.get_columns(mock_connection, 'test_table') == [
            'col1', 'col2']
        assert get_count() == 1

        bypassed = strategy.get_columns(
            mock_connection, 'test_table', bypass_cache=True)
        assert bypassed == ['col2', 'col3']
        assert get_count() == 2

        assert strategy.get_columns(mock_connection, 'test_table') == [
            'col1', 'col2']
        assert get_count() == 2

        cache = cache_manager.get_cache(
            'table_columns_ProbeStrategy_get_columns')
        assert list(cache.values()) == [['col1', 'col2']]

    def test_cache_name_carries_the_strategy_class_and_method(
            self, mock_connection, strategy, method_factory, cache_manager):
        """Verify each method of each strategy class gets its own cache.

        Mutation: dropping {strategy_class} or {method.__name__} from
        specific_cache_name in cacheable_strategy.
        Oracle: the hand-written name
        'table_columns_ProbeStrategy_get_columns'.
        """
        method_factory.create('table_columns', ['col1', 'col2'])
        strategy.get_columns(mock_connection, 'test_table')

        cache = cache_manager.get_strategy_caches()[
            'table_columns_ProbeStrategy_get_columns']
        assert list(cache.values()) == [['col1', 'col2']]

    def test_zero_ttl_reaches_the_cache_and_disables_caching(
            self, mock_connection, cache_manager):
        """Verify the decorator's ttl argument reaches the TTLCache.

        Mutation: dropping ttl=ttl from the get_cache call in
        cacheable_strategy, or hardcoding the ttl in Cache.get_cache.
        Oracle: ttl=0 expires every entry immediately, so the body has to
        run on all three calls.
        """
        class ZeroTtlStrategy:
            def __init__(self):
                self.calls = 0

            @cacheable_strategy('table_columns', ttl=0, maxsize=50)
            def get_columns(self, cn, table, bypass_cache=False):
                self.calls += 1
                return ['col1']

        probe = ZeroTtlStrategy()
        for _ in range(3):
            probe.get_columns(mock_connection, 'test_table')

        cache = cache_manager.get_cache(
            'table_columns_ZeroTtlStrategy_get_columns')

        assert probe.calls == 3
        assert cache.ttl == 0
        assert len(cache) == 0

    def test_maxsize_reaches_the_cache_and_evicts_the_oldest_table(
            self, mock_connection, cache_manager):
        """Verify the decorator's maxsize bounds the cache it creates.

        Mutation: dropping maxsize=maxsize from the get_cache call in
        cacheable_strategy, or swapping maxsize and ttl in the TTLCache
        that Cache.get_cache builds.
        Oracle: hand-computed LRU eviction - with room for two tables,
        asking for a third drops the first, so re-asking runs the body a
        fourth time while the newest table still answers from cache.
        """
        class BoundedStrategy:
            def __init__(self):
                self.calls = 0

            @cacheable_strategy('table_columns', ttl=77, maxsize=2)
            def get_columns(self, cn, table, bypass_cache=False):
                self.calls += 1
                return [table]

        probe = BoundedStrategy()
        for table in ('t1', 't2', 't3'):
            probe.get_columns(mock_connection, table)
        assert probe.calls == 3

        cache = cache_manager.get_cache(
            'table_columns_BoundedStrategy_get_columns')
        assert cache.maxsize == 2
        assert cache.ttl == 77
        assert sorted(cache) == ['t2::', 't3::']

        probe.get_columns(mock_connection, 't1')
        assert probe.calls == 4

        probe.get_columns(mock_connection, 't3')
        assert probe.calls == 4

    def test_key_failure_falls_back_to_an_uncached_call(
            self, mock_connection, cache_manager):
        """Verify an unbuildable cache key still yields the real result.

        Mutation: narrowing `except (KeyError, TypeError, ValueError)` to
        KeyError alone, or dropping the fallback `return method(...)` in
        that handler.
        Oracle: an argument whose repr() raises ValueError, plus a call
        counter showing every call reached the body and none was stored.
        """
        class Unrepresentable:
            def __repr__(self):
                raise ValueError('no repr')

        class FallbackStrategy:
            def __init__(self):
                self.calls = 0

            @cacheable_strategy('table_columns', ttl=300, maxsize=50)
            def get_columns(self, cn, table, extra=None, bypass_cache=False):
                self.calls += 1
                return ['col1']

        probe = FallbackStrategy()
        first = probe.get_columns(
            mock_connection, 'test_table', extra=Unrepresentable())
        second = probe.get_columns(
            mock_connection, 'test_table', extra=Unrepresentable())

        cache = cache_manager.get_cache(
            'table_columns_FallbackStrategy_get_columns')

        assert first == ['col1']
        assert second == ['col1']
        assert probe.calls == 2
        assert len(cache) == 0

    @pytest.mark.parametrize(
        'error_type', [KeyError, TypeError, ValueError],
        ids=['keyerror', 'typeerror', 'valueerror'])
    def test_a_raising_body_is_invoked_exactly_once(
            self, mock_connection, error_type):
        """Verify a method that raises is not retried as a cache error.

        Mutation: moving `result = method(...)` back inside the try
        whose `except (KeyError, TypeError, ValueError)` handler re-calls
        the method, so a body raising one of those three runs twice.
        Oracle: a call counter reading 1, not 2, for each caught type.
        """
        class ExplodingStrategy:
            def __init__(self):
                self.calls = 0

            @cacheable_strategy('table_columns', ttl=300, maxsize=50)
            def get_columns(self, cn, table, bypass_cache=False):
                self.calls += 1
                raise error_type('boom')

        probe = ExplodingStrategy()
        with pytest.raises(error_type):
            probe.get_columns(mock_connection, 'test_table')

        assert probe.calls == 1

    def test_bypass_cache_is_forwarded_to_the_wrapped_method(
            self, mock_connection):
        """Verify the bypass branch hands bypass_cache=True downward.

        Mutation: restoring `return method(self, cn, table, *args,
        **kwargs)` in cacheable_strategy's bypass branch, which swallows
        the flag and leaves the body reading its default False.
        Oracle: the flag value the body itself records on each call -
        a plain call then a bypassed one give [False, True].
        """
        class BypassRecordingStrategy:
            def __init__(self):
                self.seen = []

            @cacheable_strategy('table_columns', ttl=300, maxsize=50)
            def get_columns(self, cn, table, bypass_cache=False):
                self.seen.append(bypass_cache)
                return ['col1']

        probe = BypassRecordingStrategy()
        probe.get_columns(mock_connection, 'test_table')
        probe.get_columns(mock_connection, 'test_table', bypass_cache=True)

        assert probe.seen == [False, True]

    def test_a_cached_none_counts_as_a_hit(
            self, mock_connection, cache_manager):
        """Verify a stored None is replayed like any other result.

        Mutation: replacing `cache.get(cache_key, _MISS)` and its
        `is not _MISS` guard with a bare `cache.get(cache_key)` tested
        against None, which reads a cached None back as a miss.
        Oracle: a call counter reading 1 across two calls, plus the
        stored entry holding None.
        """
        class NullResultStrategy:
            def __init__(self):
                self.calls = 0

            @cacheable_strategy('table_columns', ttl=300, maxsize=50)
            def get_columns(self, cn, table, bypass_cache=False):
                self.calls += 1

        probe = NullResultStrategy()

        assert probe.get_columns(mock_connection, 'test_table') is None
        assert probe.get_columns(mock_connection, 'test_table') is None
        assert probe.calls == 1

        cache = cache_manager.get_cache(
            'table_columns_NullResultStrategy_get_columns')
        assert list(cache.values()) == [None]


class TestCacheIsolation:
    """Tests for isolation between tables, methods, and strategy classes."""

    def test_different_tables_get_different_entries(
            self, mock_connection, strategy, method_factory):
        """Verify the table name takes part in the cache key.

        Mutation: dropping table_name from the key that
        _create_cache_key returns, so every table shares one entry.
        Oracle: per-table return values, so a shared entry would hand
        table2 the columns of table1.
        """
        def table_specific_return(table):
            if table == 'test_table1':
                return ['t1col1', 't1col2']
            return ['t2col1', 't2col2', 't2col3']

        get_count = method_factory.create(
            'table_columns',
            table_specific_return)

        assert strategy.get_columns(mock_connection, 'test_table1') == [
            't1col1', 't1col2']
        assert get_count() == 1

        assert strategy.get_columns(mock_connection, 'test_table2') == [
            't2col1', 't2col2', 't2col3']
        assert get_count() == 2

        assert strategy.get_columns(mock_connection, 'test_table1') == [
            't1col1', 't1col2']
        assert strategy.get_columns(mock_connection, 'test_table2') == [
            't2col1', 't2col2', 't2col3']
        assert get_count() == 2

    def test_two_methods_sharing_a_cache_type_stay_apart(
            self, mock_connection, strategy, method_factory):
        """Verify the method name, not just the cache type, keys a cache.

        Mutation: dropping {method.__name__} from specific_cache_name in
        cacheable_strategy.
        Oracle: two methods registered under the same 'table_columns'
        cache type but different names, returning different lists - a
        shared cache would answer the second with the first's columns.
        """
        get_cols_count = method_factory.create(
            'table_columns', ['col1', 'col2'], 'get_columns')
        get_ordered_count = method_factory.create(
            'table_columns', ['col2', 'col1'], 'get_ordered_columns')

        assert strategy.get_columns(mock_connection, 'test_table') == [
            'col1', 'col2']
        assert strategy.get_ordered_columns(
            mock_connection, 'test_table') == ['col2', 'col1']
        assert get_cols_count() == 1
        assert get_ordered_count() == 1

        assert strategy.get_columns(mock_connection, 'test_table') == [
            'col1', 'col2']
        assert strategy.get_ordered_columns(
            mock_connection, 'test_table') == ['col2', 'col1']
        assert get_cols_count() == 1
        assert get_ordered_count() == 1

    def test_different_strategy_classes_get_different_caches(
            self, mock_connection):
        """Verify one method name on two classes does not collide.

        Mutation: dropping {strategy_class} from specific_cache_name in
        cacheable_strategy.
        Oracle: per-class return values, so a shared cache would hand the
        second class the first class's columns.
        """
        counters = {'s1': 0, 's2': 0}

        class MockStrategy1:
            @cacheable_strategy('table_columns', ttl=300, maxsize=50)
            def get_columns(self, cn, table, bypass_cache=False):
                counters['s1'] += 1
                return ['s1col1', 's1col2']

        class MockStrategy2:
            @cacheable_strategy('table_columns', ttl=300, maxsize=50)
            def get_columns(self, cn, table, bypass_cache=False):
                counters['s2'] += 1
                return ['s2col1', 's2col2']

        strategy1 = MockStrategy1()
        strategy2 = MockStrategy2()

        assert strategy1.get_columns(mock_connection, 'test_table') == [
            's1col1', 's1col2']
        assert strategy2.get_columns(mock_connection, 'test_table') == [
            's2col1', 's2col2']
        assert counters == {'s1': 1, 's2': 1}

        assert strategy1.get_columns(mock_connection, 'test_table') == [
            's1col1', 's1col2']
        assert strategy2.get_columns(mock_connection, 'test_table') == [
            's2col1', 's2col2']
        assert counters == {'s1': 1, 's2': 1}


class TestCacheClearing:
    """Tests for clearing strategy caches through the cache manager."""

    def test_clear_strategy_caches_reaches_a_decorated_method(
            self, mock_connection, strategy, method_factory, cache_manager):
        """Verify the decorator stores into the manager's own caches.

        Mutation: building the cache with a bare dict inside
        cacheable_strategy instead of Cache.get_instance().get_cache, so
        the manager could no longer reach it.
        Oracle: a call counter that has to rise once the manager clears
        the strategy caches.
        """
        get_count = method_factory.create('table_columns', ['col1', 'col2'])

        strategy.get_columns(mock_connection, 'test_table')
        assert get_count() == 1
        cache = cache_manager.get_strategy_caches()[
            'table_columns_ProbeStrategy_get_columns']
        assert list(cache.values()) == [['col1', 'col2']]

        cache_manager.clear_strategy_caches()

        strategy.get_columns(mock_connection, 'test_table')
        assert get_count() == 2

    def test_clearing_one_table_spares_the_other(
            self, mock_connection, strategy, method_factory, cache_manager):
        """Verify per-table clearing evicts only that table's entry.

        Mutation: routing Cache.clear_for_table to clear_all, or
        widening its key match so any key is dropped.
        Oracle: a body returning a fresh value per invocation, so the
        surviving table must still answer with its original value.
        """
        seen = {'test_table1': 0, 'test_table2': 0}

        def versioned_return(table):
            seen[table] += 1
            return [f'{table}_v{seen[table]}']

        get_count = method_factory.create('table_columns', versioned_return)

        assert strategy.get_columns(mock_connection, 'test_table1') == [
            'test_table1_v1']
        assert strategy.get_columns(mock_connection, 'test_table2') == [
            'test_table2_v1']
        assert get_count() == 2

        cache_manager.clear_caches_for_table('test_table1')

        assert strategy.get_columns(mock_connection, 'test_table1') == [
            'test_table1_v2']
        assert get_count() == 3

        assert strategy.get_columns(mock_connection, 'test_table2') == [
            'test_table2_v1']
        assert get_count() == 3

    def test_clearing_a_table_clears_strategy_and_schema_caches(
            self, mock_connection, strategy, method_factory, cache_manager):
        """Verify one table clear covers strategy and schema caches.

        Mutation: restricting Cache.clear_for_table's loop to
        get_strategy_caches() instead of every managed cache.
        Oracle: the schema entry, keyed by table name, which must be gone
        while a sibling schema entry survives.
        """
        get_count = method_factory.create('table_columns', ['col1', 'col2'])
        table = 'test_table'

        strategy.get_columns(mock_connection, table)
        assert get_count() == 1

        schema_cache = cache_manager.get_schema_cache(id(mock_connection))
        schema_cache[table] = {'column1': {'name': 'column1', 'type': 'int'}}
        schema_cache['unrelated'] = {'column9': {'name': 'column9'}}

        cache_manager.clear_caches_for_table(table)

        strategy.get_columns(mock_connection, table)
        assert get_count() == 2
        assert sorted(schema_cache) == ['unrelated']

    def test_clearing_an_unknown_table_leaves_the_cache_intact(
            self, mock_connection, strategy, method_factory, cache_manager):
        """Verify a clear for an unrelated table is a no-op.

        Mutation: dropping the `if table_lower in str(key).lower()`
        filter in Cache.clear_for_table so it empties everything.
        Oracle: a call counter that must not move for the unrelated
        clear and must move for the matching one.
        """
        get_count = method_factory.create('table_columns', ['col1', 'col2'])

        strategy.get_columns(mock_connection, 'test_table')
        assert get_count() == 1

        cache_manager.clear_caches_for_table('nonexistent_table')

        strategy.get_columns(mock_connection, 'test_table')
        assert get_count() == 1

        cache_manager.clear_caches_for_table('test_table')

        strategy.get_columns(mock_connection, 'test_table')
        assert get_count() == 2

    def test_clearing_one_cache_by_name_spares_the_other_method(
            self, mock_connection, strategy, method_factory, cache_manager):
        """Verify clearing one method's cache leaves the other's filled.

        Mutation: routing Cache.clear_cache to clear_all, which would
        also drop the primary-key entry.
        Oracle: two call counters, only one of which may move.
        """
        get_cols_count = method_factory.create(
            'table_columns', ['col1', 'col2'], 'get_columns')
        get_pks_count = method_factory.create(
            'primary_keys', ['col1'], 'get_primary_keys')

        strategy.get_columns(mock_connection, 'test_table')
        strategy.get_primary_keys(mock_connection, 'test_table')
        assert get_cols_count() == 1
        assert get_pks_count() == 1

        cache_manager.clear_cache('table_columns_ProbeStrategy_get_columns')

        strategy.get_columns(mock_connection, 'test_table')
        strategy.get_primary_keys(mock_connection, 'test_table')
        assert get_cols_count() == 2
        assert get_pks_count() == 1

    def test_table_name_case_shares_one_entry_and_one_clear(
            self, mock_connection, strategy, method_factory, cache_manager):
        """Verify table names are folded for both lookup and clearing.

        Mutation: dropping the trailing .lower() in _create_cache_key, so
        'TEST_TABLE' would take its own entry and survive a clear issued
        in lower case.
        Oracle: a body returning a fresh value per invocation, so the
        upper-case call must replay the lower-case result.
        """
        versions = [0]

        def versioned_return(table):
            versions[0] += 1
            return [f'v{versions[0]}']

        get_count = method_factory.create('table_columns', versioned_return)

        assert strategy.get_columns(mock_connection, 'test_table') == ['v1']
        assert strategy.get_columns(mock_connection, 'TEST_TABLE') == ['v1']
        assert get_count() == 1

        cache_manager.clear_caches_for_table('TEST_TABLE')

        assert strategy.get_columns(mock_connection, 'test_table') == ['v2']
        assert strategy.get_columns(mock_connection, 'TEST_TABLE') == ['v2']
        assert get_count() == 2


class TestConcreteStrategyMethodsAreCached:
    """Tests that the real strategy overrides re-apply the decorator.

    Python drops decorators applied to an abstract method when a
    subclass overrides it, so each concrete strategy has to carry its own
    @cacheable_strategy. Every test here spies on the query helper and
    proves it fires once across two calls.
    """

    def test_postgres_get_primary_keys_caches_and_reads_indisprimary(
            self, mock_connection, mocker):
        """Verify the PostgreSQL primary-key lookup caches its result.

        Mutation: dropping @cacheable_strategy from
        PostgresStrategy.get_primary_keys, or querying i.indisunique in
        place of i.indisprimary.
        Oracle: a spy scripted to return a different list on a second
        call, so an uncached repeat is visible in the value as well as
        the count.
        """
        strategy = PostgresStrategy()
        spy = mocker.patch.object(
            strategy, '_select_column_raw', side_effect=[['id'], ['WRONG']])

        assert strategy.get_primary_keys(mock_connection, 'foo') == ['id']
        assert strategy.get_primary_keys(mock_connection, 'foo') == ['id']

        assert spy.call_count == 1
        sql, params = spy.call_args.args[1], spy.call_args.args[2]
        assert params == ('foo',)
        assert 'i.indisprimary' in sql

    def test_postgres_get_columns_caches_and_quotes_the_table(
            self, mock_connection, mocker):
        """Verify the PostgreSQL column lookup caches and quotes.

        Mutation: dropping @cacheable_strategy from
        PostgresStrategy.get_columns, or interpolating the raw table name
        instead of self.quote_identifier(table).
        Oracle: a spy scripted to change its answer, plus the
        hand-written fragment 'hstore(null::"foo")'.
        """
        strategy = PostgresStrategy()
        spy = mocker.patch.object(
            strategy, '_select_column_raw', side_effect=[['a', 'b'], ['X']])

        assert strategy.get_columns(mock_connection, 'foo') == ['a', 'b']
        assert strategy.get_columns(mock_connection, 'foo') == ['a', 'b']

        assert spy.call_count == 1
        assert 'hstore(null::"foo")' in spy.call_args.args[1]

    def test_postgres_get_sequence_columns_splits_a_qualified_table(
            self, mock_connection, mocker):
        """Verify a schema-qualified table is filtered on both parts and cached.

        Mutation: dropping the `if schema is not None` branch in
        PostgresStrategy.get_sequence_columns, so 'myschema.foo' would be
        matched as a whole table name, or dropping @cacheable_strategy from
        PostgresStrategy.get_sequence_columns so every call hits the database.
        Oracle: hand-written parameter tuples for both branches, the
        table_schema clause that only the qualified branch may carry, and
        spy.call_count == 2 proving the second qualified call was cached.
        """
        strategy = PostgresStrategy()
        spy = mocker.patch.object(
            strategy, '_select_column_raw', return_value=['id'])

        strategy.get_sequence_columns(mock_connection, 'myschema.foo')
        qualified_sql, qualified_params = (spy.call_args.args[1],
                                           spy.call_args.args[2])
        # Second call for 'myschema.foo' must be served from cache.
        strategy.get_sequence_columns(mock_connection, 'myschema.foo')

        strategy.get_sequence_columns(mock_connection, 'foo')
        plain_sql, plain_params = (spy.call_args.args[1],
                                   spy.call_args.args[2])

        assert spy.call_count == 2
        assert qualified_params == ('myschema', 'foo')
        assert 'table_schema' in qualified_sql
        assert plain_params == ('foo',)
        assert 'table_schema' not in plain_sql

    def test_sqlite_get_primary_keys_caches_and_filters_on_pk(
            self, mock_connection, mocker):
        """Verify the SQLite primary-key lookup caches its result.

        Mutation: dropping @cacheable_strategy from
        SQLiteStrategy.get_primary_keys, or relaxing the pragma filter
        `where l.pk <> 0` to `where l.pk = 0`.
        Oracle: a spy scripted to change its answer, plus the
        hand-written filter text.
        """
        strategy = SQLiteStrategy()
        spy = mocker.patch.object(
            strategy, '_select_column_raw', side_effect=[['id'], ['WRONG']])

        assert strategy.get_primary_keys(mock_connection, 'foo') == ['id']
        assert strategy.get_primary_keys(mock_connection, 'foo') == ['id']

        assert spy.call_count == 1
        sql = spy.call_args.args[1]
        assert 'pragma_table_info("foo")' in sql
        assert 'l.pk <> 0' in sql

    def test_sqlite_get_columns_caches_and_lists_every_column(
            self, mock_connection, mocker):
        """Verify the SQLite column lookup caches and skips the pk filter.

        Mutation: dropping @cacheable_strategy from
        SQLiteStrategy.get_columns, or reusing the primary-key query with
        its `l.pk <> 0` filter.
        Oracle: a spy scripted to change its answer, plus the absence of
        the pk filter in the pragma query.
        """
        strategy = SQLiteStrategy()
        spy = mocker.patch.object(
            strategy, '_select_column_raw', side_effect=[['a', 'b'], ['X']])

        assert strategy.get_columns(mock_connection, 'foo') == ['a', 'b']
        assert strategy.get_columns(mock_connection, 'foo') == ['a', 'b']

        assert spy.call_count == 1
        sql = spy.call_args.args[1]
        assert 'pragma_table_info("foo")' in sql
        assert 'pk' not in sql

    def test_sqlite_sequence_columns_reuse_the_primary_key_lookup(
            self, mock_connection, mocker):
        """Verify SQLite reports primary keys as sequence columns and caches them.

        Mutation: giving SQLiteStrategy.get_sequence_columns its own
        query instead of delegating to get_primary_keys (spy.call_count
        would be 2), or dropping @cacheable_strategy from
        SQLiteStrategy.get_sequence_columns (own cache stays empty).
        Oracle: one spy call serving both methods, the primary-key pragma
        filter in the single statement issued, and a direct check that the
        sequence_columns cache holds the result.
        """
        strategy = SQLiteStrategy()
        spy = mocker.patch.object(
            strategy, '_select_column_raw', side_effect=[['id'], ['WRONG']])

        assert strategy.get_sequence_columns(mock_connection, 'foo') == ['id']
        assert strategy.get_primary_keys(mock_connection, 'foo') == ['id']

        assert spy.call_count == 1
        assert 'l.pk <> 0' in spy.call_args.args[1]

        own_cache = Cache.get_instance().get_cache(
            'sequence_columns_SQLiteStrategy_get_sequence_columns')
        assert list(own_cache.values()) == [['id']]


class TestSequenceColumnFinder:
    """Tests for the cached finder shared by both strategies."""

    @pytest.mark.parametrize(('sequence_cols', 'primary_keys', 'expected'), [
        (['aaa', 'row_id'], ['aaa', 'row_id'], 'row_id'),
        (['seq_id', 'other'], ['other'], 'other'),
        (['alpha', 'beta'], ['alpha', 'beta'], 'alpha'),
        (['bbb', 'thing_id'], [], 'thing_id'),
        ([], ['zzz', 'user_id'], 'user_id'),
        ([], [], 'id'),
    ], ids=['pk_sequence_prefers_id', 'pk_sequence_beats_sequence_only',
            'pk_sequence_without_id_takes_first', 'sequence_only_prefers_id',
            'primary_key_only_prefers_id', 'falls_back_to_id'])
    def test_finder_priority_order(
        self, mock_connection, sequence_cols,
        primary_keys, expected):
        """Verify the four-step priority in _find_sequence_column_impl.

        Mutation: emptying pk_sequence_cols so the sequence-only branch
        answers first, dropping the 'id' preference inside a branch, or
        changing the final `return 'id'` fallback.
        Oracle: hand-picked column sets where each branch names a
        different winner - 'seq_id' would win if the sequence-only branch
        ran before the primary-key intersection.
        """
        strategy = SequenceProbeStrategy(sequence_cols, primary_keys)

        assert strategy.find_sequence_column(
            mock_connection, 'probe_table') == expected

    def test_finder_result_is_cached_until_bypassed(self, mock_connection):
        """Verify the finder caches, and that bypass_cache re-runs it.

        Mutation: dropping @cacheable_strategy from
        _find_sequence_column_impl in strategy/base.py, or deleting the
        `if bypass_cache:` early return in the decorator.
        Oracle: a lookup counter - two lookups feed one finder run, so
        the totals must read 2, 2, then 4.
        """
        strategy = SequenceProbeStrategy(['row_id'], ['row_id'])

        assert strategy.find_sequence_column(
            mock_connection, 'probe_table') == 'row_id'
        assert strategy.lookups == 2

        assert strategy.find_sequence_column(
            mock_connection, 'probe_table') == 'row_id'
        assert strategy.lookups == 2

        assert strategy.find_sequence_column(
            mock_connection, 'probe_table', bypass_cache=True) == 'row_id'
        assert strategy.lookups == 4

    def test_finder_caches_per_table(self, mock_connection):
        """Verify the finder's cache key keeps tables apart.

        Mutation: dropping table_name from the key _create_cache_key
        builds, which would give the second table the first one's answer.
        Oracle: two probes with different column sets, so a shared entry
        would be visible in the returned column name.
        """
        first = SequenceProbeStrategy(['row_id'], ['row_id'])
        second = SequenceProbeStrategy(['other_id'], ['other_id'])

        assert first.find_sequence_column(
            mock_connection, 'table_one') == 'row_id'
        assert second.find_sequence_column(
            mock_connection, 'table_two') == 'other_id'
        assert second.lookups == 2

    def test_bypass_reaches_both_nested_finder_lookups(self, mock_connection):
        """Verify bypass_cache travels from the finder into both lookups.

        Mutation: dropping bypass_cache=True from the method call in
        cacheable_strategy's bypass branch, or dropping
        bypass_cache=bypass_cache from either the get_sequence_columns
        or the get_primary_keys call in _find_sequence_column_impl.
        Oracle: the flag each innermost lookup records for itself, plus
        the lookup tally proving a bypassed run re-ran both instead of
        replaying their own cached answers.
        """
        probe = CachedLookupProbeStrategy(['row_id'], ['row_id'])

        assert probe.find_sequence_column(
            mock_connection, 'probe_table') == 'row_id'
        assert probe.lookups == [
            ('sequence_columns', False),
            ('primary_keys', False),
            ]

        assert probe.find_sequence_column(
            mock_connection, 'probe_table') == 'row_id'
        assert len(probe.lookups) == 2

        assert probe.find_sequence_column(
            mock_connection, 'probe_table', bypass_cache=True) == 'row_id'
        assert probe.lookups[2:] == [
            ('sequence_columns', True),
            ('primary_keys', True),
            ]


class TestStrategyRegistry:
    """Tests for the dialect registry in strategy/__init__.py."""

    def test_registry_maps_each_dialect_to_its_class(self):
        """Verify both strategies register under their dialect names.

        Mutation: changing @register_strategy('postgresql') in
        postgres.py to any other spelling, or dropping the registration
        from sqlite.py.
        Oracle: a hand-written dialect-to-class mapping.
        """
        assert sorted(get_available_dialects()) == ['postgresql', 'sqlite']
        assert get_strategy_class('postgresql') is PostgresStrategy
        assert get_strategy_class('sqlite') is SQLiteStrategy
        assert is_supported_dialect('postgresql')
        assert not is_supported_dialect('mysql')

    def test_unknown_dialect_raises_and_names_the_alternatives(self):
        """Verify an unregistered dialect is rejected, not instantiated.

        Mutation: flipping `if dialect not in _STRATEGY_REGISTRY` to
        `if dialect in _STRATEGY_REGISTRY` in _validate_dialect.
        Oracle: the raised message, which must name the rejected dialect
        and list the supported ones.
        """
        with pytest.raises(DatabaseError) as excinfo:
            get_strategy('mysql')

        message = str(excinfo.value)
        assert 'mysql' in message
        assert 'postgresql' in message

    def test_strategy_instances_are_reused_per_dialect(
            self, create_simple_mock_connection):
        """Verify dialect lookup is cached and driven by the connection.

        Mutation: dropping @lru_cache from _get_strategy, or having
        get_db_strategy ignore get_dialect_name and return a fixed
        dialect.
        Oracle: object identity across lookups, and a SQLite connection
        that must not resolve to the PostgreSQL strategy.
        """
        postgres = get_strategy('postgresql')

        assert get_strategy('postgresql') is postgres
        assert get_db_strategy(
            create_simple_mock_connection('postgresql')) is postgres
        assert get_db_strategy(
            create_simple_mock_connection('sqlite')) is get_strategy('sqlite')
        assert get_strategy('sqlite') is not postgres


if __name__ == '__main__':
    __import__('pytest').main([__file__])
