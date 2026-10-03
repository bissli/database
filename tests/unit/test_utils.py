import sqlite3

import psycopg
import pytest
import sqlalchemy as sa
from database.utils import ensure_commit, get_dialect_name, get_raw_connection


class _Obj:
    """Exact-attribute stub; a MagicMock would pass every hasattr() check.
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

        Mutation: the engine branch checked before the dialect attribute.
        Oracle: disagreeing names; only the dialect branch gives 'sqlite'.
        """
        obj = _Obj(
            dialect=_Dialect('sqlite'),
            engine=_Obj(dialect=_Dialect('postgresql')))
        assert get_dialect_name(obj) == 'sqlite'

    def test_string_dialect_is_lowercased(self):
        """Verify a plain string dialect attribute is returned lowercased.

        Mutation: `.lower()` dropped, or a string read through `.name`.
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

        Mutation: the sa_connection branch checked before the engine branch.
        Oracle: disagreeing names; only the engine branch gives 'postgresql'.
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

        Mutation: the dbapi_connection branch checked before sa_connection.
        Oracle: psycopg-typed dbapi_connection; sa_connection gives 'sqlite'.
        """
        obj = _Obj(
            sa_connection=_Obj(engine=_Obj(dialect=_Dialect('SQLite'))),
            dbapi_connection=create_simple_mock_connection('postgresql'))
        assert get_dialect_name(obj) == 'sqlite'

    def test_dbapi_connection_resolves_recursively(self, create_simple_mock_connection):
        """Verify dbapi_connection is re-resolved through the full ladder.

        Mutation: a fixed 'postgresql' returned in place of the recursion.
        Oracle: inner objects that resolve only through later branches.
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

        Mutation: __module__ dropped from type_name, or the returns swapped.
        Oracle: both classes are named 'Connection'; only the module differs.
        """
        conn = create_simple_mock_connection(conn_type)
        assert get_dialect_name(conn) == expected

    def test_sqlite3_real_connection_resolves(self):
        """Verify a real sqlite3 connection resolves through its driver module.

        Mutation: 'sqlite3' dropped from the module check.
        Oracle: a real sqlite3.connect(':memory:') connection.
        """
        conn = sqlite3.connect(':memory:')
        try:
            assert get_dialect_name(conn) == 'sqlite'
        finally:
            conn.close()

    def test_engine_without_a_dialect_falls_through(
            self, create_simple_mock_connection):
        """Verify a None engine and sa_connection do not short-circuit.

        Mutation: a hasattr guard on obj.engine or obj.sa_connection dropped.
        Oracle: a sqlite-typed dbapi_connection behind both None attributes.
        """
        obj = _Obj(
            engine=None,
            sa_connection=None,
            dbapi_connection=create_simple_mock_connection('sqlite'))
        assert get_dialect_name(obj) == 'sqlite'

    def test_unknown_object_raises(self, create_simple_mock_connection):
        """Verify an unrecognized object raises instead of guessing.

        Mutation: the final raise replaced by a default 'postgresql'.
        Oracle: an unknown driver module and a bare object().
        """
        with pytest.raises(AttributeError, match='Cannot determine dialect'):
            get_dialect_name(create_simple_mock_connection('unknown'))
        with pytest.raises(AttributeError, match='Cannot determine dialect'):
            get_dialect_name(object())


class TestGetRawConnection:
    """Tests for get_raw_connection unwrapping."""

    def test_passes_through_plain_connection(self):
        """Verify a connection with no driver_connection is returned as is.

        Mutation: None as the getattr default.
        Oracle: identity of the object passed in.
        """
        conn = _Obj()
        assert get_raw_connection(conn) is conn

    def test_unwraps_a_real_sqlalchemy_proxy(self):
        """Verify a live SQLAlchemy pool proxy unwraps to the DBAPI object.

        Mutation: connection returned as is, so the pool proxy comes back.
        Oracle: isinstance against sqlite3.Connection.
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

        Mutation: unwrapping in a loop until no driver_connection is left.
        Oracle: a two-deep chain; the middle link comes back.
        """
        inner = object()
        middle = _Obj(driver_connection=inner)
        assert get_raw_connection(_Obj(driver_connection=middle)) is middle


class TestEnsureCommit:
    """Tests for ensure_commit fallback and error handling."""

    def test_commits_once_and_skips_fallback(self):
        """Verify a successful commit stops before the driver fallback.

        Mutation: the `return` after `connection.commit()` dropped.
        Oracle: spy counts, one wrapper commit and no driver commit.
        """
        conn = _Connection(driver_connection=_Connection())
        ensure_commit(conn)
        assert conn.commit_count == 1
        assert conn.driver_connection.commit_count == 0

    def test_commit_reaches_the_database(self, tmp_path):
        """Verify ensure_commit makes a pending write visible to a reader.

        Mutation: the `connection.commit()` call dropped.
        Oracle: a second sqlite3 connection to the file sees the row.
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
        """Verify each _COMMIT_ERRORS member routes the commit to the driver.

        Mutation: an entry dropped from _COMMIT_ERRORS.
        Oracle: spy counts, one commit on each connection and no raise.
        """
        conn = _Connection(error=error('boom'), driver_connection=_Connection())
        ensure_commit(conn)
        assert conn.commit_count == 1
        assert conn.driver_connection.commit_count == 1

    def test_unexpected_error_propagates(self):
        """Verify an error outside _COMMIT_ERRORS propagates.

        Mutation: `except _COMMIT_ERRORS` widened to `except Exception`.
        Oracle: RuntimeError escapes and the driver is untouched.
        """
        conn = _Connection(error=RuntimeError('boom'), driver_connection=_Connection())
        with pytest.raises(RuntimeError, match='boom'):
            ensure_commit(conn)
        assert conn.driver_connection.commit_count == 0

    def test_driver_commit_error_is_swallowed(self):
        """Verify a failing driver commit is swallowed.

        Mutation: the fallback except narrowed so InterfaceError escapes.
        Oracle: spy count 1 and no exception.
        """
        driver = _Connection(error=psycopg.InterfaceError('closed'))
        assert ensure_commit(_Obj(driver_connection=driver)) is None
        assert driver.commit_count == 1

    def test_no_commit_anywhere_is_a_noop(self):
        """Verify an object graph with no commit() is left alone.

        Mutation: the hasattr commit guard dropped from the fallback.
        Oracle: neither object has commit; None is the only outcome.
        """
        assert ensure_commit(_Obj()) is None
        assert ensure_commit(_Obj(driver_connection=_Obj())) is None


if __name__ == '__main__':
    __import__('pytest').main([__file__])
