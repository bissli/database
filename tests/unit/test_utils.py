"""Unit tests for database.utils.

Covers get_dialect_name branch precedence, get_raw_connection unwrapping,
and ensure_commit error handling.
"""

import logging
import sqlite3

import psycopg
import pytest
import sqlalchemy as sa
from database.utils import ensure_commit, get_dialect_name, get_raw_connection

logger = logging.getLogger(__name__)


class _Obj:
    """Object exposing exactly the attributes it is built with.

    A MagicMock answers every hasattr() with True, so it cannot tell the
    branches of get_dialect_name apart.
    """

    def __init__(self, **attrs):
        self.__dict__.update(attrs)


class _Dialect:
    """Stand-in for a SQLAlchemy dialect, which exposes only ``name``."""

    def __init__(self, name):
        self.name = name


class _Connection:
    """Connection stub whose commit() counts calls and may raise."""

    def __init__(self, error=None, driver_connection=None):
        self.error = error
        self.commit_count = 0
        if driver_connection is not None:
            self.driver_connection = driver_connection

    def commit(self):
        self.commit_count += 1
        if self.error is not None:
            raise self.error


class TestGetDialectName:
    """Branch-by-branch tests for get_dialect_name."""

    def test_dialect_attribute_beats_engine(self):
        """Verify a direct dialect attribute wins over engine.dialect.

        Mutation: guarding the `hasattr(obj, 'dialect')` branch on
            `not hasattr(obj, 'engine')` so the engine branch runs first.
        Oracle: the two branches carry disagreeing names; only the first
            yields 'sqlite'.
        """
        obj = _Obj(
            dialect=_Dialect('sqlite'),
            engine=_Obj(dialect=_Dialect('postgresql')))
        assert get_dialect_name(obj) == 'sqlite'

    def test_string_dialect_is_lowercased(self):
        """Verify a plain string dialect attribute is returned lowercased.

        Mutation: `return dialect` in place of `return dialect.lower()`,
            or dropping the `isinstance(dialect, str)` branch so a string
            is read through `.name`.
        Oracle: hand-cased 'PostgreSQL' -> 'postgresql'.
        """
        assert get_dialect_name(_Obj(dialect='PostgreSQL')) == 'postgresql'
        assert get_dialect_name(_Obj(dialect='SQLite')) == 'sqlite'

    def test_dialect_object_name_is_lowercased(self):
        """Verify a dialect object resolves through its lowercased name.

        Mutation: `return str(dialect.name)` without `.lower()`.
        Oracle: hand-cased 'PostgreSQL' -> 'postgresql'.
        """
        assert get_dialect_name(_Obj(dialect=_Dialect('PostgreSQL'))) == 'postgresql'

    def test_engine_branch_beats_sa_connection(self):
        """Verify engine.dialect is read before sa_connection.engine.

        Mutation: guarding the engine branch on `not hasattr(obj,
            'sa_connection')` so the sa_connection branch runs first.
        Oracle: the two branches carry disagreeing names; only the engine
            branch yields 'postgresql'.
        """
        obj = _Obj(
            engine=_Obj(dialect=_Dialect('postgresql')),
            sa_connection=_Obj(engine=_Obj(dialect=_Dialect('sqlite'))))
        assert get_dialect_name(obj) == 'postgresql'
        engine_only = _Obj(engine=_Obj(dialect=_Dialect('SQLite')))
        assert get_dialect_name(engine_only) == 'sqlite'

    def test_sa_connection_branch_beats_dbapi_recursion(
            self, create_simple_mock_connection):
        """Verify sa_connection.engine.dialect is read before dbapi_connection.

        Mutation: guarding the sa_connection branch on `not hasattr(obj,
            'dbapi_connection')` so the recursive branch runs first.
        Oracle: the wrapped DBAPI connection is a psycopg type, so only
            the sa_connection branch yields 'sqlite'.
        """
        obj = _Obj(
            sa_connection=_Obj(engine=_Obj(dialect=_Dialect('SQLite'))),
            dbapi_connection=create_simple_mock_connection('postgresql'))
        assert get_dialect_name(obj) == 'sqlite'

    def test_dbapi_connection_resolves_recursively(self, create_simple_mock_connection):
        """Verify dbapi_connection is re-resolved through the full ladder.

        Mutation: returning a fixed 'postgresql' instead of recursing on
            `obj.dbapi_connection`.
        Oracle: the inner object answers only through a later branch than
            the outer one - a dialect string, then a nested psycopg type.
        """
        wrapped = _Obj(dbapi_connection=_Obj(dialect='SQLite'))
        assert get_dialect_name(wrapped) == 'sqlite'
        nested = _Obj(
            dbapi_connection=_Obj(
                dbapi_connection=create_simple_mock_connection('postgresql')))
        assert get_dialect_name(nested) == 'postgresql'

    @pytest.mark.parametrize(
        ('conn_type', 'expected'),
        [
            ('postgresql', 'postgresql'),
            ('sqlite', 'sqlite'),
            ])
    def test_driver_module_fallback(
            self, create_simple_mock_connection, conn_type, expected):
        """Verify a bare DBAPI connection is typed by its driver module.

        Mutation: dropping `type(obj).__module__` from `type_name`, or
            swapping the 'postgresql' and 'sqlite' returns.
        Oracle: both drivers name their class 'Connection', so only the
            module half separates them.
        """
        conn = create_simple_mock_connection(conn_type)
        assert get_dialect_name(conn) == expected

    def test_sqlite3_real_connection_resolves(self):
        """Verify a real sqlite3 connection resolves through its driver module.

        Mutation: dropping 'sqlite3' from the module check in
            get_dialect_name, or returning 'postgresql' for that branch.
        Oracle: sqlite3.connect(':memory:') has type_name
            'sqlite3.Connection'; the module check is the only path that
            returns 'sqlite'.
        """
        conn = sqlite3.connect(':memory:')
        try:
            assert get_dialect_name(conn) == 'sqlite'
        finally:
            conn.close()

    def test_engine_without_a_dialect_falls_through(
            self, create_simple_mock_connection):
        """Verify a None engine and sa_connection do not short-circuit.

        Mutation: dropping `hasattr(obj.engine, 'dialect')` (or
            `hasattr(obj.sa_connection, 'engine')`) from its guard, which
            raises on a ConnectionWrapper whose engine is None.
        Oracle: the sqlite-typed dbapi_connection, reachable only by
            falling through both None guards.
        """
        obj = _Obj(
            engine=None,
            sa_connection=None,
            dbapi_connection=create_simple_mock_connection('sqlite'))
        assert get_dialect_name(obj) == 'sqlite'

    def test_unknown_object_raises(self, create_simple_mock_connection):
        """Verify an unrecognized object raises instead of guessing.

        Mutation: replacing the final `raise AttributeError` with a
            default return of 'postgresql'.
        Oracle: an object from an unknown driver module and a bare
            object(); neither can be typed.
        """
        with pytest.raises(AttributeError, match='Cannot determine dialect'):
            get_dialect_name(create_simple_mock_connection('unknown'))
        with pytest.raises(AttributeError, match='Cannot determine dialect'):
            get_dialect_name(object())


class TestGetRawConnection:
    """Tests for get_raw_connection unwrapping."""

    def test_unwraps_driver_connection(self):
        """Verify the driver_connection is returned, not the wrapper.

        Mutation: `raw_conn = connection` inside the hasattr branch.
        Oracle: identity of the sentinel the wrapper holds.
        """
        raw = object()
        assert get_raw_connection(_Obj(driver_connection=raw)) is raw

    def test_passes_through_plain_connection(self):
        """Verify a connection with no driver_connection is returned as is.

        Mutation: `raw_conn = None` as the default, or accessing
            `connection.driver_connection` unconditionally.
        Oracle: identity of the object passed in.
        """
        conn = _Obj()
        assert get_raw_connection(conn) is conn

    def test_unwraps_a_real_sqlalchemy_proxy(self):
        """Verify a live SQLAlchemy pool proxy unwraps to the DBAPI object.

        Mutation: `raw_conn = connection` inside the hasattr branch, which
            hands back the pool proxy instead of the driver connection.
        Oracle: isinstance against sqlite3.Connection - the proxy is not
            one, the object behind driver_connection is.
        """
        engine = sa.create_engine('sqlite://')
        try:
            with engine.connect() as sa_conn:
                proxy = sa_conn.connection
                assert not isinstance(proxy, sqlite3.Connection)
                assert isinstance(get_raw_connection(proxy), sqlite3.Connection)
        finally:
            engine.dispose()

    def test_unwraps_exactly_one_level(self):
        """Verify unwrapping stops after one hop.

        Mutation: turning the `if` into a `while` loop that unwraps until
            no driver_connection remains.
        Oracle: a two-deep chain whose middle and inner links differ by
            identity.
        """
        inner = object()
        middle = _Obj(driver_connection=inner)
        assert get_raw_connection(_Obj(driver_connection=middle)) is middle


class TestEnsureCommit:
    """Tests for ensure_commit fallback and error handling."""

    def test_commits_once_and_skips_fallback(self):
        """Verify a successful commit stops before the driver fallback.

        Mutation: dropping the `return` after `connection.commit()`, which
            commits the driver connection a second time.
        Oracle: spy counts - one commit on the wrapper, none on the driver.
        """
        conn = _Connection(driver_connection=_Connection())
        ensure_commit(conn)
        assert conn.commit_count == 1
        assert conn.driver_connection.commit_count == 0

    def test_commit_reaches_the_database(self, tmp_path):
        """Verify ensure_commit flushes a pending write, not just logs.

        Mutation: dropping the `connection.commit()` call from the first
            branch so the pending transaction is never flushed.
        Oracle: a second sqlite3 connection to the same file, which sees
            the row only after a real commit.
        """
        db_path = tmp_path / 'commit.db'
        writer = sqlite3.connect(db_path)
        reader = sqlite3.connect(db_path)
        try:
            writer.execute('create table t (id integer)')
            writer.commit()
            writer.execute('insert into t values (1)')
            ensure_commit(writer)
            assert reader.execute('select count(*) from t').fetchone()[0] == 1
        finally:
            reader.close()
            writer.close()

    @pytest.mark.parametrize(
        'error',
        [
            psycopg.ProgrammingError,
            psycopg.InterfaceError,
            psycopg.OperationalError,
            sqlite3.ProgrammingError,
            sqlite3.InterfaceError,
            sqlite3.OperationalError,
            ])
    def test_falls_back_to_driver_after_expected_error(self, error):
        """Verify each _CommitError member routes the commit to the driver.

        Mutation: dropping an entry from the `_CommitError` tuple, which
            lets that error escape instead of reaching the fallback.
        Oracle: spy counts - one commit attempt on each connection, and no
            exception out of ensure_commit.
        """
        conn = _Connection(error=error('boom'), driver_connection=_Connection())
        ensure_commit(conn)
        assert conn.commit_count == 1
        assert conn.driver_connection.commit_count == 1

    def test_unexpected_error_propagates(self):
        """Verify an error outside _CommitError is not swallowed.

        Mutation: widening `except _CommitError` to `except Exception`.
        Oracle: RuntimeError sits outside the tuple, so it must surface
            and leave the driver connection untouched.
        """
        conn = _Connection(error=RuntimeError('boom'), driver_connection=_Connection())
        with pytest.raises(RuntimeError, match='boom'):
            ensure_commit(conn)
        assert conn.driver_connection.commit_count == 0

    def test_driver_commit_error_is_swallowed(self):
        """Verify a failing driver commit is logged, not raised.

        Mutation: narrowing the fallback `except _CommitError` to a single
            driver exception, so an InterfaceError escapes.
        Oracle: spy count proves the fallback ran; the absence of an
            exception proves it was caught.
        """
        driver = _Connection(error=psycopg.InterfaceError('closed'))
        assert ensure_commit(_Obj(driver_connection=driver)) is None
        assert driver.commit_count == 1

    def test_no_commit_anywhere_is_a_noop(self):
        """Verify an object graph with no commit() is left alone.

        Mutation: dropping `hasattr(connection.driver_connection,
            'commit')` from the fallback guard, which raises AttributeError.
        Oracle: neither object exposes commit, so returning None is the
            only correct outcome.
        """
        assert ensure_commit(_Obj()) is None
        assert ensure_commit(_Obj(driver_connection=_Obj())) is None


if __name__ == '__main__':
    __import__('pytest').main([__file__])
