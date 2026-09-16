"""Unit tests for connection URL construction and the engine registry.

Two contracts live here:

- The URL builder must percent-encode username, password, database, and
  appname so that no character can corrupt URL parsing or inject extra
  libpq parameters through the query string.
- The engine registry key must identify an engine by its non-secret
  fields only: two option sets differing solely in password share an
  engine, and a difference in any identity or pool field does not.
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
    """Build a minimal valid postgres DatabaseOptions for URL tests."""
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
    """Parse a connection URL string with SQLAlchemy."""
    return sa.engine.url.make_url(url)


def _spy_factory():
    """Return (factory, created) for get_engine_for_options' engine_factory.

    Every call appends the fake engine it returns to created.
    """
    created = []

    def factory(url, **kwargs):
        engine = _FakeEngine(url, kwargs)
        created.append(engine)
        return engine

    return factory, created


class _FakeEngine:
    """Stand-in for a SQLAlchemy Engine that records how it was built."""

    def __init__(self, url, kwargs):
        self.url = url
        self.kwargs = kwargs
        self.disposed = False

    def dispose(self):
        """Match the one Engine method dispose_all_engines() calls."""
        self.disposed = True


@pytest.fixture(autouse=True)
def isolated_engine_registry():
    """Give each test a private engine registry.

    The registry is a process-wide module global, so a fake engine left
    behind would be handed to an unrelated test or to the atexit
    disposal hook.
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

        Mutation: any change to the keepalive constants in
        build_connection_url ('keepalives_idle=30' -> 300), to the
        'postgresql+psycopg' driver token, or to the host/port/database
        order in the URL f-string.
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

        Mutation: connect_timeout built from options.pool_wait_timeout
        (default 30) instead of options.timeout.
        Oracle: timeout=45, chosen to differ from every pool default.
        """
        url = PostgresStrategy().build_connection_url(_options(timeout=45))
        assert _parse(url).query['connect_timeout'] == '45'

    def test_zero_timeout_omits_connect_timeout(self):
        """Verify a falsy timeout drops connect_timeout entirely.

        Mutation: dropping the `if options.timeout:` guard, which sends
        connect_timeout=0 - 'wait forever' to libpq, the opposite of the
        caller's intent.
        Oracle: the 0/45 boundary; the full expected query key set.
        """
        # Postgres validation rejects timeout=0, so reach the branch by
        # clearing it after construction.
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

        Mutation: dropping the `if options.appname:` guard, which sends
        an empty application_name to the server.
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

        Mutation: quote(options.password or '', safe='') losing safe=''
        (leaves '/' raw) or dropping quote() altogether; quote_plus in
        place of quote turns the space into '+'.
        Oracle: hand-computed percent-encoding per character, plus a
        SQLAlchemy re-parse of the authority section.
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

        Mutation: quote(options.username or '', safe='') losing safe=''
        or dropping quote(); a raw ':' would be read as the start of the
        password and a raw '@' as the start of the host.
        Oracle: hand-computed percent-encoding, plus a re-parse showing
        the password and host survive intact.
        """
        url = PostgresStrategy().build_connection_url(_options(username=username))
        assert f'://{encoded}:mypass@myhost:5432/mydb?' in url
        parsed = _parse(url)
        assert parsed.username == username
        assert parsed.password == 'mypass'
        assert parsed.host == 'myhost'

    def test_database_cannot_inject_query_parameters(self):
        """Verify a '?' in the database name stays in the path.

        Mutation: dropping quote() on options.database, which lets
        'db?sslmode=disable' end the path and add a real libpq
        parameter.
        Oracle: hand-computed 'db%3Fsslmode%3Ddisable', plus the query
        key set proving nothing was injected.
        """
        url = PostgresStrategy().build_connection_url(
            _options(database='db?sslmode=disable'))
        assert '/db%3Fsslmode%3Ddisable?' in url
        parsed = _parse(url)
        assert 'sslmode' not in parsed.query
        assert len(parsed.query) == 6

    def test_missing_credentials_render_empty_not_none(self):
        """Verify absent credentials become empty strings in the URL.

        Mutation: quote(options.username or '') losing the `or ''`
        fallback - quote(None) raises TypeError, and a bare f-string
        would put the literal 'None' in the URL.
        Oracle: hand-written URL prefix for the credential-free case.
        """
        # Postgres validation rejects blank credentials, so clear them
        # after construction to reach the fallbacks.
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

        Mutation: dropping quote_plus() around options.appname, which
        turns 'evil&sslmode=disable' into a real sslmode parameter and
        downgrades the connection to plaintext.
        Oracle: hand-computed 'evil%26sslmode%3Ddisable', the exact
        query key count, and a re-parse of application_name.
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

        Mutation: quote() in place of quote_plus() (encodes the space as
        %20, which parse_qsl still decodes, but drops '+' handling), or
        no encoding at all so '#' truncates the URL at the fragment.
        Oracle: hand-computed form encoding per case, plus a re-parse
        recovering the original text.
        """
        url = PostgresStrategy().build_connection_url(_options(appname=appname))
        assert f'application_name={encoded}' in url
        assert _parse(url).query['application_name'] == appname


class TestCreateUrlFromOptions:
    """connection.create_url_from_options delegates by drivername."""

    def test_postgres_options_return_a_parsed_url_object(self):
        """Verify the helper returns an sa.URL, not the raw string.

        Mutation: returning url_string instead of sa.make_url(
        url_string) from create_url_from_options; the port would then
        never be an int and callers lose .query.
        Oracle: hand-written expected drivername, port int, and appname.
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
        """Verify sqlite options never route through the postgres URL.

        Mutation: get_strategy(options.drivername) hardcoded to
        'postgresql' in create_url_from_options, or the sqlite builder
        losing a slash from 'sqlite:///' so the path becomes a host.
        Oracle: hand-written URL literal for each path form.
        """
        url = create_url_from_options(
            DatabaseOptions(drivername='sqlite', database=database))
        assert url.render_as_string() == expected
        assert url.database == database
        assert url.host is None

    def test_url_creator_receives_decoded_credentials_and_query(self):
        """Verify the url_creator hook gets every parsed component.

        Mutation: dropping password=parsed.password (silent auth
        failure) or replacing the query passthrough with {} (loses the
        keepalive and application_name settings).
        Oracle: hand-written expected kwargs dict, with the password
        decoded back from its percent-encoded form.
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

        Mutation: dropping the url.set(database=options.database)
        restoration in create_url_from_options, which opens 'my db' as
        the literal 'my%20db' - make_url unquotes username and password
        only, never the database. Dropping quote() on the database in
        build_connection_url instead recovers the name but lets
        'db?sslmode=disable' add a real libpq parameter.
        Oracle: the caller's own database string, plus the hand-written
        six-key query set and the decoded credentials.
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

    def test_url_creator_receives_the_raw_database_name(self):
        """Verify the test seam gets the restored database name too.

        Mutation: dropping the parsed.set(database=options.database)
        restoration on the url_creator branch of
        create_url_from_options, so the factory builds an engine for
        'my%20db'.
        Oracle: the caller's own name, read off a spy factory.
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

        Mutation: adding options.password to the key tuple in
        _build_engine_registry_key, which both leaks the secret into a
        process-wide dict and defeats engine reuse after a rotation.
        Oracle: byte equality of the two keys, plus a search for the
        secret text.
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

        Mutation: repr() back to str() in the _build_engine_registry_key
        join, which lets 'u|x' + 'd' and 'u' + 'x|d' render identically.
        Oracle: two option sets differing in where the '|' falls, whose
        str()-joined forms are byte-identical.
        """
        left = _build_engine_registry_key(
            _options(username='u|x', database='d'), False, 5, 300, 30)
        right = _build_engine_registry_key(
            _options(username='u', database='x|d'), False, 5, 300, 30)

        assert left != right

    def test_timeout_separates_two_otherwise_identical_options(self):
        """Verify timeout is in the key, since it is baked into the URL.

        Mutation: dropping options.timeout from the key tuple, which
        hands the second caller the first caller's connect_timeout.
        Oracle: the differential against password, which is deliberately
        excluded and so must still collide.
        """
        fast = _build_engine_registry_key(_options(timeout=1), False, 5, 300, 30)
        slow = _build_engine_registry_key(_options(timeout=99), False, 5, 300, 30)
        assert fast != slow

        one = _build_engine_registry_key(_options(password='a'), False, 5, 300, 30)
        two = _build_engine_registry_key(_options(password='b'), False, 5, 300, 30)
        assert one == two

    def test_key_matches_hand_written_literal(self):
        """Verify the key's exact field set, order, and separator.

        Mutation: dropping options.database, options.appname,
        options.timeout or readonly from the key tuple, reordering
        username and database, or swapping repr() back to str() so a
        '|' inside a field can shift the field boundary.
        Oracle: hand-written key literal.
        """
        key = _build_engine_registry_key(_options(), False, 5, 300, 30, False)
        assert key == ("'postgresql'|'myhost'|5432|'myuser'|'mydb'|'myapp'"
                       '|30|False|5|300|30|False')

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

        Mutation: dropping any one of drivername, hostname, port,
        username, database, or appname from the key tuple, which would
        hand back an engine pointed at a different server or database.
        Oracle: inequality against the baseline key, one field at a
        time.
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

        Mutation: dropping pool_timeout (or any other pool argument)
        from the key tuple, so a caller asking for a different pool
        shape silently gets the first engine built.
        Oracle: inequality against the baseline key, one argument at a
        time.
        """
        pool_args = [False, 5, 300, 30]
        baseline = _build_engine_registry_key(_options(), *pool_args)
        pool_args[position] = value
        changed = _build_engine_registry_key(_options(), *pool_args)
        assert changed != baseline

    def test_readonly_changes_the_key(self):
        """Verify the reader role gets its own engine.

        Mutation: dropping readonly from the key tuple, which lets one
        pooled engine serve both roles - a writer then checks out a
        connection whose session the server holds read only, and every
        write on it fails.
        Oracle: inequality between the two keys, with every other
        field held equal.
        """
        writer = _build_engine_registry_key(_options(), False, 5, 300, 30, False)
        reader = _build_engine_registry_key(_options(), False, 5, 300, 30, True)
        assert reader != writer


class TestEngineRegistry:
    """get_engine_for_options caches and configures engines."""

    def test_engine_is_reused_across_equal_option_objects(self):
        """Verify a second call with the same identity reuses the engine.

        Mutation: flipping the registry lookup guard in
        get_engine_for_options to `if is_memory_sqlite and key in
        _engine_registry`, which rebuilds an engine per call and leaks
        connection pools.
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

        Mutation: dropping options.database from the key tuple in
        _build_engine_registry_key, which would serve queries for one
        database on a connection to another.
        Oracle: a spy factory counting create_engine calls, plus the
        registry size.
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

        Mutation: dropping readonly=readonly from the
        _build_engine_registry_key call inside get_engine_for_options,
        which leaves the key correct in isolation while the two roles
        still collapse onto one engine in practice.
        Oracle: a spy factory counting create_engine calls for the two
        roles over one identical option set.
        """
        factory, created = _spy_factory()
        writer = get_engine_for_options(_options(), readonly=False,
                                        engine_factory=factory)
        reader = get_engine_for_options(_options(), readonly=True,
                                        engine_factory=factory)
        assert writer is not reader
        assert len(created) == 2

    def test_unpooled_engine_uses_nullpool(self):
        """Verify the default path disables pooling outright.

        Mutation: flipping `elif not use_pool:` to `elif use_pool:` in
        get_engine_for_options, which would leave the default engine on
        SQLAlchemy's QueuePool.
        Oracle: hand-written expected kwargs dict.
        """
        factory, created = _spy_factory()
        get_engine_for_options(_options(), engine_factory=factory)
        assert created[0].kwargs == {'echo': False, 'poolclass': NullPool}

    def test_pooled_engine_gets_pool_and_postgres_guards(self):
        """Verify pooled engines carry the pool sizing and safety kwargs.

        Mutation: dropping max_overflow=0, pool_pre_ping, or
        pool_reset_on_return from PostgresStrategy.get_engine_kwargs, or
        swapping pool_recycle and pool_timeout in get_engine_for_options.
        Oracle: hand-written expected kwargs dict with three distinct
        pool numbers.
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

        Mutation: moving engine_kwargs.update(kwargs) above the pool
        block (caller's poolclass=StaticPool then silently overwritten by
        the elif-not-use_pool NullPool branch), or dropping the update so
        a caller's echo=True is ignored.
        Oracle: echo=True confirmed on the spy; poolclass=StaticPool
        survives the NullPool branch.
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

        Mutation: clearing the registry without calling engine.dispose()
        in dispose_all_engines, so pooled connections leak at process exit.
        Oracle: a per-engine disposed flag plus an emptied registry.
        """
        factory, created = _spy_factory()
        get_engine_for_options(_options(database='alpha'), engine_factory=factory)
        get_engine_for_options(_options(database='beta'), engine_factory=factory)
        dispose_all_engines()
        assert [e.disposed for e in created] == [True, True]
        assert _engine_registry == {}

    def test_memory_sqlite_engine_is_never_cached(self):
        """Verify each ':memory:' connect gets a private database.

        Mutation: is_memory_sqlite forced False in
        get_engine_for_options, which caches the StaticPool engine and
        leaks one in-memory database across independent connect()
        calls.
        Oracle: object identity of two engines, the create call count,
        and an empty registry.
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

        Mutation: replacing engine_kwargs.setdefault('connect_args', {})
        with a fresh dict, which drops the sqlite detect_types flags and
        breaks date/datetime conversion on in-memory databases.
        Oracle: both keys present in one dict, with detect_types
        carrying the declared-type and column-name flags.
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

        Mutation: widening the is_memory_sqlite test to any sqlite
        database, which would rebuild a file-backed engine on every
        connect().
        Oracle: object identity across two calls, plus the create call
        count.
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

        Mutation: use_pool hardcoded to True (or options.use_pool dropped)
        in connect(), which would allocate a 5-connection QueuePool per
        database and leak file handles at process exit.
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

        Mutation: swapping pool_recycle=options.pool_max_idle_time and
        pool_timeout=options.pool_wait_timeout in connect(), or wiring
        pool_size to the wrong field.
        Oracle: three distinct values (7, 111, 222) read back off the
        live QueuePool.
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
    __import__('pytest').main([__file__])
