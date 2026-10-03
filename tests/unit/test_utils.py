import sqlite3

import pytest
import sqlalchemy as sa
from database.utils import get_dialect_name, get_raw_connection


class _Obj:
    """Exact-attribute stub; a MagicMock would pass every hasattr() check.
    """

    def __init__(self, **attrs):
        self.__dict__.update(attrs)


class _Dialect:
    """Stand-in for a SQLAlchemy dialect, which exposes only ``name``."""

    def __init__(self, name):
        self.name = name


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


if __name__ == '__main__':
    __import__('pytest').main([__file__])
