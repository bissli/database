"""Unit tests for the Cache manager, @cacheable_strategy, and the registry.
"""
import cachetools
import pytest
from database.cache import Cache, _create_cache_key, cacheable_strategy
from database.exceptions import DatabaseError
from database.strategy import get_available_dialects, get_db_strategy
from database.strategy import get_strategy, get_strategy_class
from database.strategy import is_supported_dialect
from database.strategy.postgres import PostgresStrategy
from database.strategy.sqlite import SQLiteStrategy


class FakeConnection:
    """Connection stand-in carrying the attributes _create_cache_key drops.
    """

    def __init__(self):
        self.cursor = None
        self.driver_connection = None


class SequenceProbeStrategy(SQLiteStrategy):
    """SQLite strategy with counted, scripted, uncached metadata lookups.
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
    """SQLite strategy whose scripted metadata lookups carry their own cache.
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
    """Strategy stand-in whose class name keeps its caches apart.
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
    """Fresh ProbeStrategy instance, safe to monkey-patch.
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

        Mutation: maxsize and ttl swapped, or the name-exists guard dropped.
        Oracle: hand-picked maxsize=3/ttl=99 and an entry that survives.
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

    def test_clear_cache_touches_only_the_named_cache(self, cache_manager):
        """Verify clear_cache() spares its siblings and unknown names.

        Mutation: clear_cache routed to clear_all, or its name guard dropped.
        Oracle: the sibling entry, still readable afterwards.
        """
        target = cache_manager.get_cache('clear_one_target')
        sibling = cache_manager.get_cache('clear_one_sibling')
        target['x'] = 'gone'
        sibling['y'] = 'kept'

        cache_manager.clear_cache('never_created')
        cache_manager.clear_cache('clear_one_target')

        assert len(target) == 0
        assert sibling['y'] == 'kept'

    def test_clear_caches_for_table_alias_clears_one_table(
            self, cache_manager):
        """Verify the backwards-compatible alias is the per-table clear.

        Mutation: clear_caches_for_table rebound to clear_all.
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

        Mutation: a prefix dropped, or startswith relaxed to `prefix in name`.
        Oracle: a hand-written set over caches tagged with a marker suffix.
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

        Mutation: clear_strategy_caches iterating every cache.
        Oracle: the schema entry, which must still hold its value.
        """
        strategy_cache = cache_manager.get_cache('table_columns_ClearPin_m')
        strategy_cache['users::'] = ['id', 'name']
        schema_cache = cache_manager.get_schema_cache(4242)
        schema_cache['users'] = {'id': 'int'}

        cache_manager.clear_strategy_caches()

        assert len(strategy_cache) == 0
        assert schema_cache['users'] == {'id': 'int'}



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

        Mutation: args sliced [1:], kwargs unsorted, no .lower(), or str().
        Oracle: hand-written keys over kwargs given out of order.
        """
        assert _create_cache_key(
            table_name, method_args,
            method_kwargs) == expected_key

    def test_cache_key_drops_connection_like_arguments(self):
        """Verify args and kwargs that look like connections are dropped.

        Mutation: the two hasattr guards joined with `or` in place of `and`.
        Oracle: a hand-written key over probes carrying one attribute each.
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

        Mutation: the `k != 'bypass_cache'` filter dropped.
        Oracle: a hand-written key, equal to the key without the kwarg.
        """
        with_flag = _create_cache_key(
            'test_table', ['arg'], {'bypass_cache': True, 'x': 1})
        without_flag = _create_cache_key('test_table', ['arg'], {'x': 1})

        assert with_flag == "test_table:'arg':x=1"
        assert with_flag == without_flag



class TestDecoratorPlumbing:
    """Tests for what @cacheable_strategy passes to the cache manager."""

    def test_second_call_returns_the_stored_result(
            self, mock_connection, strategy, method_factory):
        """Verify a repeat call replays the stored value, body unused.

        Mutation: the cache-hit return dropped, or the store after a miss.
        Oracle: a body returning a new value per call, and a call counter.
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

        Mutation: the `if bypass_cache:` early return deleted.
        Oracle: a body returning a new value per call, read back cached.
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

        Mutation: class or method name dropped from specific_cache_name.
        Oracle: the name 'table_columns_ProbeStrategy_get_columns'.
        """
        method_factory.create('table_columns', ['col1', 'col2'])
        strategy.get_columns(mock_connection, 'test_table')

        cache = cache_manager.get_strategy_caches()[
            'table_columns_ProbeStrategy_get_columns']
        assert list(cache.values()) == [['col1', 'col2']]

    def test_zero_ttl_reaches_the_cache_and_disables_caching(
            self, mock_connection, cache_manager):
        """Verify the decorator's ttl argument reaches the TTLCache.

        Mutation: ttl=ttl dropped from the decorator's get_cache call.
        Oracle: ttl=0 expires every entry, so the body runs three times.
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

        Mutation: maxsize=maxsize dropped, or maxsize and ttl swapped.
        Oracle: hand-computed LRU eviction of the first of three tables.
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

        Mutation: the except narrowed to KeyError, or its fallback dropped.
        Oracle: an argument whose repr() raises ValueError; a call counter.
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

        Mutation: the method call moved inside the key-building try.
        Oracle: a call counter reading 1 for each caught type.
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

        Mutation: bypass_cache=True dropped from the bypass branch's call.
        Oracle: the flags the body records: [False, True].
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

        Mutation: the _MISS sentinel replaced by a test against None.
        Oracle: a call counter reading 1 across two calls; the stored None.
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

        Mutation: table_name dropped from the _create_cache_key result.
        Oracle: hand-written per-table return values.
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

        Mutation: {method.__name__} dropped from specific_cache_name.
        Oracle: two same-type methods returning hand-written lists.
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

        Mutation: {strategy_class} dropped from specific_cache_name.
        Oracle: hand-written per-class return values.
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

        Mutation: the decorator caching in a bare dict of its own.
        Oracle: a call counter that rises after the manager clears.
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

        Mutation: clear_for_table routed to clear_all, or matching any key.
        Oracle: a versioned body; the other table keeps its first value.
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

    def test_table_name_case_shares_one_entry_and_one_clear(
            self, mock_connection, strategy, method_factory, cache_manager):
        """Verify table names are folded for both lookup and clearing.

        Mutation: the trailing .lower() in _create_cache_key dropped.
        Oracle: a versioned body; the upper-case call replays 'v1'.
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
    """

    def test_postgres_get_primary_keys_caches_and_reads_indisprimary(
            self, mock_connection, mocker):
        """Verify the PostgreSQL primary-key lookup caches its result.

        Mutation: decorator dropped, or i.indisunique for i.indisprimary.
        Oracle: a spy scripted to give a different list on a second call.
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

        Mutation: decorator dropped, or the table left unquoted.
        Oracle: a scripted spy and the fragment 'hstore(null::"foo")'.
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
        """Verify a qualified table filters on schema and name, and caches.

        Mutation: the `if schema is not None` branch or decorator dropped.
        Oracle: hand-written parameter tuples and a spy call count of 2.
        """
        strategy = PostgresStrategy()
        spy = mocker.patch.object(
            strategy, '_select_column_raw', return_value=['id'])

        strategy.get_sequence_columns(mock_connection, 'myschema.foo')
        qualified_sql, qualified_params = (spy.call_args.args[1],
                                           spy.call_args.args[2])
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

        Mutation: decorator dropped, or `l.pk <> 0` flipped to `l.pk = 0`.
        Oracle: a scripted spy and the hand-written filter text.
        """
        strategy = SQLiteStrategy()
        spy = mocker.patch.object(
            strategy, '_select_column_raw', side_effect=[['id'], ['WRONG']])

        assert strategy.get_primary_keys(mock_connection, 'foo') == ['id']
        assert strategy.get_primary_keys(mock_connection, 'foo') == ['id']

        assert spy.call_count == 1
        sql = spy.call_args.args[1]
        assert 'pragma_table_info(?, ?)' in sql
        assert spy.call_args.args[2] == ('foo', None)
        assert 'l.pk <> 0' in sql

    def test_sqlite_get_columns_caches_and_lists_every_column(
            self, mock_connection, mocker):
        """Verify the SQLite column lookup caches and skips the pk filter.

        Mutation: decorator dropped, or the primary-key query reused.
        Oracle: a scripted spy and no pk filter in the query.
        """
        strategy = SQLiteStrategy()
        spy = mocker.patch.object(
            strategy, '_select_column_raw', side_effect=[['a', 'b'], ['X']])

        assert strategy.get_columns(mock_connection, 'foo') == ['a', 'b']
        assert strategy.get_columns(mock_connection, 'foo') == ['a', 'b']

        assert spy.call_count == 1
        sql = spy.call_args.args[1]
        assert 'pragma_table_info(?, ?)' in sql
        assert spy.call_args.args[2] == ('foo', None)
        assert 'pk' not in sql

    def test_sqlite_sequence_columns_reuse_the_primary_key_lookup(
            self, mock_connection, mocker):
        """Verify SQLite answers sequence columns from its primary keys.

        Mutation: get_sequence_columns given its own query, or undecorated.
        Oracle: one spy call for both methods; its own cache holds ['id'].
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

        Mutation: groups reordered, 'id' preference or fallback dropped.
        Oracle: hand-picked column sets where each group names a winner.
        """
        strategy = SequenceProbeStrategy(sequence_cols, primary_keys)

        assert strategy.find_sequence_column(
            mock_connection, 'probe_table') == expected

    def test_finder_result_is_cached_until_bypassed(self, mock_connection):
        """Verify the finder caches, and that bypass_cache re-runs it.

        Mutation: _find_sequence_column_impl undecorated, or bypass ignored.
        Oracle: a lookup counter reading 2, 2, then 4.
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

    def test_bypass_reaches_both_nested_finder_lookups(self, mock_connection):
        """Verify bypass_cache travels from the finder into both lookups.

        Mutation: bypass_cache not forwarded to either nested lookup.
        Oracle: the flag each innermost lookup records for itself.
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

        Mutation: a misspelled or dropped @register_strategy.
        Oracle: a hand-written dialect-to-class mapping.
        """
        assert sorted(get_available_dialects()) == ['postgresql', 'sqlite']
        assert get_strategy_class('postgresql') is PostgresStrategy
        assert get_strategy_class('sqlite') is SQLiteStrategy
        assert is_supported_dialect('postgresql')
        assert not is_supported_dialect('mysql')

    def test_unknown_dialect_raises_and_names_the_alternatives(self):
        """Verify an unregistered dialect raises DatabaseError.

        Mutation: the membership test in _validate_dialect flipped.
        Oracle: a message naming 'mysql' and listing 'postgresql'.
        """
        with pytest.raises(DatabaseError) as excinfo:
            get_strategy('mysql')

        message = str(excinfo.value)
        assert 'mysql' in message
        assert 'postgresql' in message

    def test_strategy_instances_are_reused_per_dialect(
            self, create_simple_mock_connection):
        """Verify dialect lookup is cached and driven by the connection.

        Mutation: @lru_cache dropped, or get_db_strategy fixed to a dialect.
        Oracle: object identity across lookups and dialects.
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
