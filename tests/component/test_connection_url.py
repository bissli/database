"""Tests for connection URL construction and the engine registry.
"""
import sqlite3

import pytest
import sqlalchemy as sa
from database.connection import _build_engine_registry_key, _engine_registry
from database.connection import connect, create_url_from_options
from database.connection import dispose_all_engines, get_engine_for_options
from database.options import DatabaseOptions
from database.strategy.postgres import PostgresStrategy
from sqlalchemy.pool import NullPool, StaticPool


def _options(**overrides):
    """A minimal valid postgres DatabaseOptions, with overrides applied.
    """
    base = {
        'drivername': 'postgresql',
        'hostname': 'myhost',
        'username': 'myuser',
        'password': 'mypass',
        'database': 'mydb',
        'port': 5432,
        'timeout': 30,
        'appname': 'myapp',
        }
    base.update(overrides)
    return DatabaseOptions(**base)


def _parse(url):
    """The URL string parsed by SQLAlchemy.
    """
    return sa.engine.url.make_url(url)


def _spy_factory():
    """(factory, created): a fake engine_factory and the engines it built.
    """
    created = []

    def factory(url, **kwargs):
        engine = _FakeEngine(url, kwargs)
        created.append(engine)
        return engine

    return factory, created


class _FakeEngine:
    """Stand-in for a SQLAlchemy Engine that records how it was built.
    """

    def __init__(self, url, kwargs):
        self.url = url
        self.kwargs = kwargs
        self.disposed = False

    def dispose(self):
        """Match the one Engine method dispose_all_engines() calls.
        """
        self.disposed = True


@pytest.fixture(autouse=True)
def isolated_engine_registry():
    """Give each test a private engine registry.
    """
    saved = dict(_engine_registry)
    _engine_registry.clear()
    yield
    _engine_registry.clear()
    _engine_registry.update(saved)


class TestPostgresUrlShape:
    """The generated URL is a fixed, parseable contract."""

    def test_url_matches_hand_written_literal(self):
        """Verify the whole postgres URL, keepalive settings included.

        Mutation: a changed keepalive constant, driver token, or part order.
        Oracle: hand-written URL literal.
        """
        url = PostgresStrategy().build_connection_url(_options())
        expected = (
            'postgresql+psycopg://myuser:mypass@myhost:5432/mydb'
            '?keepalives=1&keepalives_idle=30&keepalives_interval=10'
            '&keepalives_count=5&connect_timeout=30'
            '&application_name=myapp'
            )
        assert url == expected

    def test_connect_timeout_carries_the_timeout_option(self):
        """Verify connect_timeout comes from options.timeout.

        Mutation: connect_timeout built from options.pool_wait_timeout.
        Oracle: timeout=45, unlike every pool default.
        """
        url = PostgresStrategy().build_connection_url(_options(timeout=45))
        assert _parse(url).query['connect_timeout'] == '45'

    def test_zero_timeout_omits_connect_timeout(self):
        """Verify a falsy timeout drops connect_timeout entirely.

        Mutation: dropping the `if options.timeout:` guard (0 waits forever).
        Oracle: the full expected query key set.
        """
        # Postgres validation rejects timeout=0 at construction.
        options = _options()
        options.timeout = 0
        url = PostgresStrategy().build_connection_url(options)
        assert 'connect_timeout' not in url
        assert set(_parse(url).query) == {
            'keepalives', 'keepalives_idle', 'keepalives_interval',
            'keepalives_count', 'application_name',
            }

    def test_empty_appname_omits_application_name(self):
        """Verify a blank appname adds no application_name parameter.

        Mutation: dropping the `if options.appname:` guard.
        Oracle: the full expected query key set.
        """
        options = _options()
        options.appname = ''
        url = PostgresStrategy().build_connection_url(options)
        assert 'application_name' not in url
        assert set(_parse(url).query) == {
            'keepalives', 'keepalives_idle', 'keepalives_interval',
            'keepalives_count', 'connect_timeout',
            }


class TestCredentialEncoding:
    """Characters that would corrupt URL parsing must be encoded."""

    @pytest.mark.parametrize(('password', 'encoded'), [
        ('p@ss', 'p%40ss'),
        ('p/ss', 'p%2Fss'),
        ('p?ss', 'p%3Fss'),
        ('p#ss', 'p%23ss'),
        ('pa%ss', 'pa%25ss'),
        ('p&ss', 'p%26ss'),
        ('p=ss', 'p%3Dss'),
        ('pa ss', 'pa%20ss'),
        ('p:ss', 'p%3Ass'),
        ], ids=['at', 'slash', 'question', 'hash', 'percent', 'amp', 'eq',
                'space', 'colon'])
    def test_password_is_percent_encoded(self, password, encoded):
        """Verify each dangerous password character is escaped.

        Mutation: quote() losing safe='', dropped, or swapped for quote_plus.
        Oracle: hand-computed percent-encoding, plus a re-parse.
        """
        url = PostgresStrategy().build_connection_url(_options(password=password))
        assert f'://myuser:{encoded}@myhost:5432/mydb?' in url
        parsed = _parse(url)
        assert parsed.password == password
        assert parsed.host == 'myhost'
        assert parsed.username == 'myuser'

    @pytest.mark.parametrize(('username', 'encoded'), [
        ('user@dom', 'user%40dom'),
        ('user:name', 'user%3Aname'),
        ('user/name', 'user%2Fname'),
        ], ids=['at', 'colon', 'slash'])
    def test_username_is_percent_encoded(self, username, encoded):
        """Verify a username cannot split the authority section.

        Mutation: quote() on the username losing safe='' or dropped.
        Oracle: hand-computed percent-encoding, plus a re-parse.
        """
        url = PostgresStrategy().build_connection_url(_options(username=username))
        assert f'://{encoded}:mypass@myhost:5432/mydb?' in url
        parsed = _parse(url)
        assert parsed.username == username
        assert parsed.password == 'mypass'
        assert parsed.host == 'myhost'

    def test_database_cannot_inject_query_parameters(self):
        """Verify a '?' in the database name stays in the path.

        Mutation: dropping quote() on options.database.
        Oracle: hand-computed 'db%3Fsslmode%3Ddisable' and the query key count.
        """
        url = PostgresStrategy().build_connection_url(
            _options(database='db?sslmode=disable'))
        assert '/db%3Fsslmode%3Ddisable?' in url
        parsed = _parse(url)
        assert 'sslmode' not in parsed.query
        assert len(parsed.query) == 6

    def test_missing_credentials_render_empty_not_none(self):
        """Verify absent credentials become empty strings in the URL.

        Mutation: quote(options.username or '') losing the `or ''` fallback.
        Oracle: hand-written URL prefix for the credential-free case.
        """
        # Postgres validation rejects blank credentials at construction.
        options = _options()
        options.username = None
        options.password = None
        options.database = None
        url = PostgresStrategy().build_connection_url(options)
        assert url.startswith('postgresql+psycopg://:@myhost:5432/?')
        assert 'None' not in url


class TestAppnameInjection:
    """An appname carrying '&' or '=' must not add libpq parameters."""

    def test_appname_with_ampersand_does_not_inject_params(self):
        """Verify an appname cannot smuggle in a second parameter.

        Mutation: dropping quote_plus() around options.appname.
        Oracle: hand-computed encoding, query key count, and a re-parse.
        """
        url = PostgresStrategy().build_connection_url(
            _options(appname='evil&sslmode=disable'))
        assert 'application_name=evil%26sslmode%3Ddisable' in url
        parsed = _parse(url)
        assert 'sslmode' not in parsed.query
        assert len(parsed.query) == 6
        assert parsed.query['application_name'] == 'evil&sslmode=disable'

    @pytest.mark.parametrize(('appname', 'encoded'), [
        ('name=with=equals', 'name%3Dwith%3Dequals'),
        ('name#frag', 'name%23frag'),
        ('my app', 'my+app'),
        ('a/b', 'a%2Fb'),
        ], ids=['equals', 'hash', 'space', 'slash'])
    def test_appname_specials_survive_a_round_trip(self, appname, encoded):
        """Verify the server receives the appname the caller passed.

        Mutation: quote() in place of quote_plus(), or no encoding at all.
        Oracle: hand-computed form encoding per case, plus a re-parse.
        """
        url = PostgresStrategy().build_connection_url(_options(appname=appname))
        assert f'application_name={encoded}' in url
        assert _parse(url).query['application_name'] == appname


class TestCreateUrlFromOptions:
    """connection.create_url_from_options delegates by drivername."""

    def test_postgres_options_return_a_parsed_url_object(self):
        """Verify the helper returns an sa.URL.

        Mutation: returning url_string in place of sa.make_url(url_string).
        Oracle: hand-written drivername, int port, and appname.
        """
        url = create_url_from_options(_options())
        assert isinstance(url, sa.URL)
        assert url.drivername == 'postgresql+psycopg'
        assert url.port == 5432
        assert url.query['application_name'] == 'myapp'

    @pytest.mark.parametrize(('database', 'expected'), [
        ('test.db', 'sqlite:///test.db'),
        ('/tmp/abs.db', 'sqlite:////tmp/abs.db'),
    ], ids=['relative', 'absolute'])
    def test_sqlite_url_is_built_by_the_sqlite_strategy(
        self, database,
        expected):
        """Verify sqlite options use the sqlite URL builder.

        Mutation: get_strategy hardcoded to 'postgresql', or a lost '/'.
        Oracle: hand-written URL literal for each path form.
        """
        url = create_url_from_options(
            DatabaseOptions(drivername='sqlite', database=database))
        assert url.render_as_string() == expected
        assert url.database == database
        assert url.host is None

    def test_url_creator_receives_decoded_credentials_and_query(self):
        """Verify the url_creator hook gets every parsed component.

        Mutation: dropping password, or the query passthrough replaced by {}.
        Oracle: hand-written expected kwargs, password decoded.
        """
        seen = {}

        def creator(**kwargs):
            seen.update(kwargs)
            return sa.URL.create(**kwargs)

        create_url_from_options(_options(password='p@w/d'),
                                url_creator=creator)
        assert seen['drivername'] == 'postgresql+psycopg'
        assert seen['username'] == 'myuser'
        assert seen['password'] == 'p@w/d'
        assert seen['host'] == 'myhost'
        assert seen['port'] == 5432
        assert seen['database'] == 'mydb'
        assert seen['query'] == {
            'keepalives': '1',
            'keepalives_idle': '30',
            'keepalives_interval': '10',
            'keepalives_count': '5',
            'connect_timeout': '30',
            'application_name': 'myapp',
            }

    @pytest.mark.parametrize('database', [
        'my db',
        'a/b',
        'db?sslmode=disable',
        ], ids=['space', 'slash', 'question'])
    def test_special_database_name_survives_the_round_trip(self, database):
        """Verify options.database wins over the percent-encoded path.

        Mutation: dropping the url.set restore (SQLAlchemy 2.0 then opens
            'my%20db'), or quote() on the database.
        Oracle: the caller's database string and the six-key query set.
        """
        url = create_url_from_options(
            _options(database=database, username='u@dom', password='p@w/d'))
        assert url.database == database
        assert set(url.query) == {
            'keepalives', 'keepalives_idle', 'keepalives_interval',
            'keepalives_count', 'connect_timeout', 'application_name',
            }
        assert url.username == 'u@dom'
        assert url.password == 'p@w/d'

    def test_open_mode_leaves_a_postgres_database_name_decoded(self):
        """Verify open_mode set on PostgreSQL keeps the raw database name.

        Mutation: the restore keyed on open_mode without the sqlite check.
        Oracle: the hand-written raw name.
        """
        url = create_url_from_options(_options(database='my db', open_mode='ro'))
        assert url.database == 'my db'

    def test_url_creator_receives_the_raw_database_name(self):
        """Verify the test seam gets the restored database name too.

        Mutation: url_creator fed the parts of a fresh make_url(url_string).
        Oracle: the caller's own name, read off a spy creator.
        """
        seen = {}

        def creator(**kwargs):
            seen.update(kwargs)
            return sa.URL.create(**kwargs)

        url = create_url_from_options(
            _options(database='my db'), url_creator=creator)
        assert seen['database'] == 'my db'
        assert url.database == 'my db'


class TestEngineRegistryKey:
    """The key identifies an engine by non-secret fields only."""

    def test_password_is_absent_from_the_key(self):
        """Verify two configs differing only in password collide.

        Mutation: options.password added to the key tuple.
        Oracle: equality of the two keys, plus a search for the secret.
        """
        first = _build_engine_registry_key(_options(password='secret_abc'),
                                           False, 5, 300, 30)
        second = _build_engine_registry_key(_options(password='secret_xyz'),
                                            False, 5, 300, 30)
        assert first == second
        assert 'secret_abc' not in first
        assert 'secret_xyz' not in first

    def test_a_pipe_inside_a_field_cannot_shift_the_boundary(self):
        """Verify a '|' in one field does not forge another field's value.

        Mutation: repr() back to str() in the key join.
        Oracle: two option sets whose str()-joined keys are identical.
        """
        left = _build_engine_registry_key(
            _options(username='u|x', database='d'), False, 5, 300, 30)
        right = _build_engine_registry_key(
            _options(username='u', database='x|d'), False, 5, 300, 30)

        assert left != right

    def test_timeout_separates_two_otherwise_identical_options(self):
        """Verify timeout is part of the key.

        Mutation: options.timeout dropped from the key tuple.
        Oracle: password, excluded on purpose, still collides.
        """
        fast = _build_engine_registry_key(_options(timeout=1), False, 5, 300, 30)
        slow = _build_engine_registry_key(_options(timeout=99), False, 5, 300, 30)
        assert fast != slow

        one = _build_engine_registry_key(_options(password='a'), False, 5, 300, 30)
        two = _build_engine_registry_key(_options(password='b'), False, 5, 300, 30)
        assert one == two

    def test_open_mode_separates_two_otherwise_identical_options(self):
        """Verify open_mode is part of the key.

        Mutation: options.open_mode left out of the key tuple.
        Oracle: inequality against the same options with another open mode.
        """
        def key(open_mode):
            options = DatabaseOptions(drivername='sqlite', database='x.db',
                                      open_mode=open_mode)
            return _build_engine_registry_key(options, False, 5, 300, 30, True)

        assert key(None) != key('immutable')
        assert key('ro') != key('immutable')

    def test_key_matches_hand_written_literal(self):
        """Verify the key's exact field set, order, and separator.

        Mutation: a dropped or reordered key field, or str() for repr().
        Oracle: hand-written key literal.
        """
        key = _build_engine_registry_key(_options(), False, 5, 300, 30, False)
        assert key == ("'postgresql'|'myhost'|5432|'myuser'|'mydb'|'myapp'"
                       '|30|None|False|5|300|30|False')

    @pytest.mark.parametrize(('field', 'value'), [
        ('drivername', 'sqlite'),
        ('hostname', 'otherhost'),
        ('port', 5433),
        ('username', 'otheruser'),
        ('database', 'otherdb'),
        ('appname', 'otherapp'),
        ])
    def test_identity_field_changes_the_key(self, field, value):
        """Verify each identity field is part of the key.

        Mutation: any one identity field dropped from the key tuple.
        Oracle: inequality against the baseline key, one field at a time.
        """
        baseline = _build_engine_registry_key(_options(), False, 5, 300, 30)
        changed = _build_engine_registry_key(_options(**{field: value}),
                                             False, 5, 300, 30)
        assert changed != baseline

    @pytest.mark.parametrize(('position', 'value'), [
        (0, True),
        (1, 7),
        (2, 111),
        (3, 222),
        ], ids=['use_pool', 'pool_size', 'pool_recycle', 'pool_timeout'])
    def test_pool_setting_changes_the_key(self, position, value):
        """Verify each pool setting is part of the key.

        Mutation: any one pool argument dropped from the key tuple.
        Oracle: inequality against the baseline key, one argument at a time.
        """
        pool_args = [False, 5, 300, 30]
        baseline = _build_engine_registry_key(_options(), *pool_args)
        pool_args[position] = value
        changed = _build_engine_registry_key(_options(), *pool_args)
        assert changed != baseline

    def test_readonly_changes_the_key(self):
        """Verify the reader role gets its own engine.

        Mutation: readonly dropped from the key tuple.
        Oracle: inequality between the two keys, all else equal.
        """
        writer = _build_engine_registry_key(_options(), False, 5, 300, 30, False)
        reader = _build_engine_registry_key(_options(), False, 5, 300, 30, True)
        assert reader != writer


class TestEngineRegistry:
    """get_engine_for_options caches and configures engines."""

    def test_engine_is_reused_across_equal_option_objects(self):
        """Verify a second call with the same identity reuses the engine.

        Mutation: the registry lookup guard flipped to `if is_memory_sqlite`.
        Oracle: a spy factory counting create_engine calls.
        """
        factory, created = _spy_factory()
        first = get_engine_for_options(_options(password='one'),
                                       engine_factory=factory)
        second = get_engine_for_options(_options(password='two'),
                                        engine_factory=factory)
        assert first is second
        assert len(created) == 1

    def test_engines_are_not_shared_across_databases(self):
        """Verify a different database gets its own engine.

        Mutation: options.database dropped from the key tuple.
        Oracle: a spy factory's call count, plus the registry size.
        """
        factory, created = _spy_factory()
        first = get_engine_for_options(_options(database='alpha'),
                                       engine_factory=factory)
        second = get_engine_for_options(_options(database='beta'),
                                        engine_factory=factory)
        assert first is not second
        assert len(created) == 2
        assert len(_engine_registry) == 2

    def test_readonly_engines_are_not_shared_with_writers(self):
        """Verify get_engine_for_options forwards readonly to the key.

        Mutation: readonly dropped from the key call in get_engine_for_options.
        Oracle: a spy factory's call count for the two roles.
        """
        factory, created = _spy_factory()
        writer = get_engine_for_options(_options(), readonly=False,
                                        engine_factory=factory)
        reader = get_engine_for_options(_options(), readonly=True,
                                        engine_factory=factory)
        assert writer is not reader
        assert len(created) == 2

    def test_unpooled_engine_uses_nullpool(self):
        """Verify the default path disables pooling.

        Mutation: `elif not use_pool:` flipped to `elif use_pool:`.
        Oracle: hand-written expected kwargs dict.
        """
        factory, created = _spy_factory()
        get_engine_for_options(_options(), engine_factory=factory)
        assert created[0].kwargs == {'echo': False, 'poolclass': NullPool}

    def test_pooled_engine_gets_pool_and_postgres_guards(self):
        """Verify pooled engines carry the pool sizing and safety kwargs.

        Mutation: a dropped postgres pool guard, or recycle/timeout swapped.
        Oracle: hand-written kwargs dict with three distinct pool numbers.
        """
        factory, created = _spy_factory()
        get_engine_for_options(_options(use_pool=True), use_pool=True,
                               pool_size=7, pool_recycle=111,
                               pool_timeout=222, engine_factory=factory)
        assert created[0].kwargs == {
            'echo': False,
            'max_overflow': 0,
            'pool_pre_ping': True,
            'pool_reset_on_return': 'rollback',
            'pool_size': 7,
            'pool_recycle': 111,
            'pool_timeout': 222,
            }

    def test_caller_kwargs_win_over_defaults(self):
        """Verify explicit create_engine kwargs override the defaults.

        Mutation: engine_kwargs.update(kwargs) moved above the pool block.
        Oracle: echo=True and poolclass=StaticPool read off the spy.
        """
        factory, created = _spy_factory()
        get_engine_for_options(
            _options(),
            engine_factory=factory,
            echo=True,
            poolclass=StaticPool)
        assert created[0].kwargs['echo'] is True
        assert created[0].kwargs['poolclass'] is StaticPool

    def test_dispose_all_engines(self):
        """Verify dispose_all_engines() disposes every registered engine.

        Mutation: the registry cleared without calling engine.dispose().
        Oracle: a per-engine disposed flag plus an emptied registry.
        """
        factory, created = _spy_factory()
        get_engine_for_options(_options(database='alpha'), engine_factory=factory)
        get_engine_for_options(_options(database='beta'), engine_factory=factory)
        dispose_all_engines()
        assert [e.disposed for e in created] == [True, True]
        assert _engine_registry == {}

    def test_memory_sqlite_engine_is_never_cached(self):
        """Verify each ':memory:' call gets a private engine.

        Mutation: is_memory_sqlite forced False in get_engine_for_options.
        Oracle: engine identity, the create count, and an empty registry.
        """
        factory, created = _spy_factory()
        options = DatabaseOptions(drivername='sqlite', database=':memory:')
        first = get_engine_for_options(options, engine_factory=factory)
        second = get_engine_for_options(options, engine_factory=factory)
        assert first is not second
        assert len(created) == 2
        assert _engine_registry == {}
        assert created[0].kwargs['poolclass'] is StaticPool

    def test_memory_sqlite_keeps_strategy_connect_args(self):
        """Verify check_same_thread merges into the strategy connect_args.

        Mutation: setdefault('connect_args', {}) replaced with a fresh dict.
        Oracle: check_same_thread and both detect_types flags in one dict.
        """
        factory, created = _spy_factory()
        options = DatabaseOptions(drivername='sqlite', database=':memory:')
        get_engine_for_options(options, engine_factory=factory)
        connect_args = created[0].kwargs['connect_args']
        assert connect_args['check_same_thread'] is False
        assert connect_args['detect_types'] & sqlite3.PARSE_DECLTYPES
        assert connect_args['detect_types'] & sqlite3.PARSE_COLNAMES

    def test_file_sqlite_engine_is_cached(self):
        """Verify only ':memory:' escapes the registry.

        Mutation: the is_memory_sqlite test widened to any sqlite database.
        Oracle: engine identity across two calls, plus the create count.
        """
        factory, created = _spy_factory()
        options = DatabaseOptions(drivername='sqlite', database='memory.db')
        first = get_engine_for_options(options, engine_factory=factory)
        second = get_engine_for_options(options, engine_factory=factory)
        assert first is second
        assert len(created) == 1


class TestConnectPoolMapping:
    """connect() maps DatabaseOptions pool fields onto the real pool."""

    def test_connect_uses_nullpool_by_default(self, tmp_path):
        """Verify connect() without use_pool gives a NullPool engine.

        Mutation: use_pool hardcoded to True in connect().
        Oracle: pool class read off the live engine.
        """
        options = DatabaseOptions(
            drivername='sqlite',
            database=str(tmp_path / 'plain.db'))
        cn = connect(options)
        try:
            assert isinstance(cn.engine.pool, NullPool)
        finally:
            cn.close()
            cn.engine.dispose()

    def test_connect_maps_pool_options_onto_the_engine_pool(self, tmp_path):
        """Verify each pool option reaches its SQLAlchemy counterpart.

        Mutation: pool_recycle and pool_timeout swapped in connect().
        Oracle: distinct values 7, 111, 222 read off the live QueuePool.
        """
        options = DatabaseOptions(
            drivername='sqlite',
            database=str(tmp_path / 'pool.db'),
            use_pool=True,
            pool_max_connections=7,
            pool_max_idle_time=111,
            pool_wait_timeout=222)
        cn = connect(options)
        try:
            pool = cn.engine.pool
            assert pool.size() == 7
            assert pool._recycle == 111
            assert pool._timeout == 222
        finally:
            cn.close()
            cn.engine.dispose()


if __name__ == '__main__':
    pytest.main([__file__])
