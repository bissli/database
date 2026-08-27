import sys

import pytest
from database.exceptions import ValidationError
from database.options import DatabaseOptions, iterdict_data_loader
from database.options import pandas_numpy_data_loader
from database.options import pandas_pyarrow_data_loader
from database.options import use_iterdict_data_loader
from database.types import Column

REQUIRED_POSTGRES_FIELDS = [
    'hostname',
    'username',
    'password',
    'database',
    'port',
    'timeout',
    ]

SUPPORTED_DIALECTS = ['postgresql', 'sqlite']


def _make_options(**overrides):
    """Build a valid PostgreSQL DatabaseOptions, overriding named fields."""
    base = {
        'hostname': 'testhost',
        'username': 'testuser',
        'password': 'secret_value_xyz',
        'database': 'testdb',
        'port': 1234,
        'timeout': 30,
        'appname': 'fixture_app',
        }
    base.update(overrides)
    return DatabaseOptions(**base)


class _FalsyLoader:
    """Callable data loader that evaluates false in a boolean test."""

    def __bool__(self):
        return False

    def __call__(self, data, columns, **kwargs):
        return list(data)


class _FakeConnection:
    """Stand-in for ConnectionWrapper: carries options, no `connection`."""

    def __init__(self, options):
        self.options = options


class _FakeTransaction:
    """Stand-in for Transaction: carries `connection`, no options."""

    def __init__(self, connection):
        self.connection = connection


def test_defaults_match_declared_field_defaults():
    """Verify unset fields take the documented defaults.

    Mutation: changing a declared default in DatabaseOptions, e.g.
        `use_pool: bool = False` -> True or `pool_max_idle_time: int = 300`.
    Oracle: hand-written table of defaults from the class docstring.
    """
    options = _make_options()

    defaults = {
        'drivername': options.drivername,
        'use_pool': options.use_pool,
        'pool_max_connections': options.pool_max_connections,
        'pool_max_idle_time': options.pool_max_idle_time,
        'pool_wait_timeout': options.pool_wait_timeout,
        }
    assert defaults == {
        'drivername': 'postgresql',
        'use_pool': False,
        'pool_max_connections': 5,
        'pool_max_idle_time': 300,
        'pool_wait_timeout': 30,
        }
    assert options.data_loader is pandas_numpy_data_loader


def test_pooling_fields_are_kept_and_never_required():
    """Verify falsy pool values construct without raising.

    Mutation: 'pool_max_connections' appended to PostgresStrategy.get_required_options,
        so pool_max_connections=0 triggers a ValidationError.
    Oracle: all four pool fields set to falsy values construct and read back
        as (False, 0, 0, 0).
    """
    options = _make_options(
        use_pool=False,
        pool_max_connections=0,
        pool_max_idle_time=0,
        pool_wait_timeout=0)

    assert (
        options.use_pool,
        options.pool_max_connections,
        options.pool_max_idle_time,
        options.pool_wait_timeout,
        ) == (False, 0, 0, 0)


def test_unknown_drivername_rejected_and_supported_ones_accepted():
    """Verify the drivername guard rejects only unregistered dialects.

    Mutation: `if not is_supported_dialect(self.drivername)` losing its `not`.
    Oracle: 'sybase' raises while both registered dialect names construct.
    """
    with pytest.raises(ValidationError) as excinfo:
        _make_options(drivername='sybase')
    message = str(excinfo.value)
    for dialect in SUPPORTED_DIALECTS:
        assert dialect in message

    assert _make_options(drivername='postgresql').drivername == 'postgresql'
    assert DatabaseOptions(drivername='sqlite', database='x.db').drivername == 'sqlite'


@pytest.mark.parametrize('field', REQUIRED_POSTGRES_FIELDS)
def test_missing_required_postgres_field_rejected(field):
    """Verify every field PostgreSQL requires is enforced by name.

    Mutation: dropping an entry from `return ['hostname', 'username',
        'password', 'database', 'port', 'timeout']` in PostgresStrategy.
    Oracle: hand-written list of the six required fields; the error names
        the field it rejected.
    """
    unset = 0 if field in {'port', 'timeout'} else None
    with pytest.raises(ValidationError) as excinfo:
        _make_options(**{field: unset})
    assert field in str(excinfo.value)


def test_zero_is_rejected_for_port_and_timeout():
    """Verify validation rejects 0, not merely None, for numeric fields.

    Mutation: `if not getattr(options, field)` in validate_options weakened to
        `if getattr(options, field) is None`.
    Oracle: boundary pair 0 -> raises, 1 -> constructs, for both fields.
    """
    for field in ('port', 'timeout'):
        with pytest.raises(ValidationError):
            _make_options(**{field: 0})
        assert getattr(_make_options(**{field: 1}), field) == 1


def test_sqlite_requires_only_database():
    """Verify SQLite validates against its own required list, not PostgreSQL's.

    Mutation: SQLiteStrategy.get_required_options returning the PostgreSQL
        list instead of `return ['database']`.
    Oracle: options with database alone construct while every credential
        stays unset; database=None raises and names the field.
    """
    options = DatabaseOptions(drivername='sqlite', database='test.db')

    assert options.database == 'test.db'
    assert (options.hostname, options.username, options.password) == (None, None, None)
    assert (options.port, options.timeout) == (0, 0)

    with pytest.raises(ValidationError) as excinfo:
        DatabaseOptions(drivername='sqlite')
    assert 'database' in str(excinfo.value)


def test_supplied_appname_wins_over_script_name(monkeypatch):
    """Verify a supplied appname is never overwritten by the script name.

    Mutation: reordering `self.appname or scriptname() or 'python_console'`
        to put scriptname() first.
    Oracle: sys.argv[0] set to a distinct script; appname stays as supplied.
    """
    monkeypatch.setattr(sys, 'argv', ['/opt/bin/report_runner.py'])

    assert _make_options(appname='my_app').appname == 'my_app'
    assert _make_options(appname=None).appname == 'report_runner'


def test_appname_falls_back_to_python_console(monkeypatch):
    """Verify a blank script name still yields a usable appname.

    Mutation: dropping the `or 'python_console'` tail of the appname
        assignment in __post_init__.
    Oracle: sys.argv[0] = '' makes scriptname() return '', so the literal
        fallback is the only source left.
    """
    monkeypatch.setattr(sys, 'argv', [''])

    assert _make_options(appname=None).appname == 'python_console'


def test_default_data_loader_only_fills_an_unset_loader():
    """Verify a caller-supplied data loader survives __post_init__.

    Mutation: `if self.data_loader is None:` dropped, so the pandas loader is
        assigned unconditionally.
    Oracle: identity of the loader handed in.
    """
    assert _make_options(data_loader=iterdict_data_loader).data_loader \
        is iterdict_data_loader
    assert _make_options().data_loader is pandas_numpy_data_loader


def test_falsy_custom_data_loader_is_kept():
    """Verify the unset test is identity against None, not truthiness.

    Mutation: `if self.data_loader is None:` relaxed to
        `if not self.data_loader:`.
    Oracle: a callable whose __bool__ is False still comes back unreplaced.
    """
    loader = _FalsyLoader()

    assert _make_options(data_loader=loader).data_loader is loader


def test_iterdict_data_loader_returns_rows_unchanged_in_a_new_list():
    """Verify the minimal loader copies rows into a list and edits nothing.

    Mutation: `return list(data)` reduced to `return data`, or the rows
        projected onto Column.get_names(columns).
    Oracle: hand-written expected list; the input is a tuple whose rows carry
        a key absent from the column metadata.
    """
    rows = (
        {'id': 1, 'name': 'Alice', 'extra': 'kept'},
        {'id': 2, 'name': 'Bob', 'extra': 'kept'},
        )
    columns = [Column(name='id', type_code=None), Column(name='name', type_code=None)]

    result = iterdict_data_loader(rows, columns, table_name='people')

    assert result == [
        {'id': 1, 'name': 'Alice', 'extra': 'kept'},
        {'id': 2, 'name': 'Bob', 'extra': 'kept'},
        ]
    assert type(result) is list
    assert result is not rows


def test_iterdict_data_loader_returns_empty_list_for_no_rows():
    """Verify the empty branch yields a sized empty list, never None.

    Mutation: `return []` in the `if not data` branch changed to
        `return None`, which breaks the len() checks in select_row.
    Oracle: [] for both an empty sequence and None, with len() defined.
    """
    columns = [Column(name='id', type_code=None)]

    assert iterdict_data_loader([], columns) == []
    assert iterdict_data_loader(None, columns) == []
    assert len(iterdict_data_loader([], columns)) == 0


class TestPasswordRedaction:
    """The password must never reach repr, str, or a format string, while the
    remaining fields stay readable for diagnosis.
    """

    def test_repr_masks_password_exactly(self):
        """Verify repr renders every field in the documented order, masked.

        Mutation: deleting the hand-written __repr__, which lets the dataclass
            generate one that prints the password.
        Oracle: independently written expected string.
        """
        options = DatabaseOptions(
            drivername='postgresql',
            hostname='db.example.internal',
            username='reporting',
            password='hunter2-plaintext',
            database='analytics',
            port=5432,
            timeout=30,
            appname='fixture_app')

        assert repr(options) == (
            "DatabaseOptions(drivername='postgresql', "
            "hostname='db.example.internal', username='reporting', "
            "password='***', database='analytics', port=5432, "
            "appname='fixture_app')"
            )

    def test_password_absent_from_every_string_form(self):
        """Verify no string rendering of the options leaks the password.

        Mutation: `masked = '***' if self.password else None` replaced by
            `masked = self.password`.
        Oracle: a password sharing no character with the '***' mask, checked
            against repr, str, f-string, format(), and %-interpolation.
        """
        password = 'zq7-plaintext-secret'
        options = _make_options(password=password)

        # Notes:
        # - Each entry must reach __repr__ by a DIFFERENT route, so the
        #   noqa markers are load-bearing: ruff's UP032/UP031 rewrite the
        #   last two into f-strings, collapsing five paths into three
        #   copies of one and silently gutting the test.
        forms = [
            repr(options),
            str(options),
            f'{options}',
            f'{options!r}',
            '{}'.format(options),  # noqa: UP032
            '%s' % options,  # noqa: UP031
            ]
        for form in forms:
            assert password not in form
            assert "password='***'" in form

    def test_mask_does_not_reveal_password_length(self):
        """Verify a password shorter than '***' still renders as exactly '***'.

        Mutation: `'***'` replaced by `'*' * len(self.password)` in __repr__,
            which yields '**' for a 2-char password - a case no sibling
            exercises because all siblings use passwords of 9-plus characters.
        Oracle: "password='***'," in repr for password='ab'; the mutation
            yields "password='**'," which the assertion rejects.
        """
        options = _make_options(password='ab')

        assert "password='***'," in repr(options)

    def test_repr_keeps_username_that_equals_the_password(self):
        """Verify masking targets the password field, not the password text.

        Mutation: masking by scrubbing the rendered string, e.g.
            `text.replace(self.password, '***')`, which also blanks any other
            field holding the same value.
        Oracle: username and password set to the same string; the username
            must still read in full.
        """
        shared = 'reporting'
        options = _make_options(username=shared, password=shared)

        rendered = repr(options)
        assert "username='reporting'" in rendered
        assert "password='***'" in rendered

    def test_repr_shows_none_when_no_password(self):
        """Verify an absent password renders as None, not as a mask.

        Mutation: `masked = '***' if self.password else None` reduced to
            `masked = '***'`, which claims a secret that does not exist.
        Oracle: independently written expected string for SQLite options.
        """
        options = DatabaseOptions(
            drivername='sqlite',
            database='local.db',
            appname='sqlite_app')

        assert repr(options) == (
            "DatabaseOptions(drivername='sqlite', hostname=None, "
            "username=None, password=None, database='local.db', port=0, "
            "appname='sqlite_app')"
            )

    def test_empty_password_is_masked_and_only_none_reads_as_absent(self):
        """Verify '' renders as '***' while None alone renders as None.

        Mutation: `masked = None if self.password is None else '***'`
            reverted to `masked = '***' if self.password else None`, which
            renders an empty password as no password at all.
        Oracle: independently written whole-repr strings for the pair
            straddling the threshold - password='' and password=None - which
            differ only in the password field. SQLite is the dialect here
            because PostgreSQL validation rejects a falsy password.
        """
        empty = DatabaseOptions(
            drivername='sqlite',
            database='local.db',
            password='',
            appname='sqlite_app')
        absent = DatabaseOptions(
            drivername='sqlite',
            database='local.db',
            password=None,
            appname='sqlite_app')

        assert repr(empty) == (
            "DatabaseOptions(drivername='sqlite', hostname=None, "
            "username=None, password='***', database='local.db', port=0, "
            "appname='sqlite_app')"
            )
        assert repr(absent) == (
            "DatabaseOptions(drivername='sqlite', hostname=None, "
            "username=None, password=None, database='local.db', port=0, "
            "appname='sqlite_app')"
            )


class TestUseIterdictDataLoader:
    """The decorator swaps in the minimal loader for one call, then restores
    whatever the connection had.
    """

    def test_swaps_loader_for_the_call_only(self):
        """Verify the minimal loader applies during the call and not after.

        Mutation: dropping `cn.options.data_loader = iterdict_data_loader`, or
            restoring the original before `func` runs rather than after.
        Oracle: a spy recording the loader seen inside the call.
        """
        cn = _FakeConnection(_make_options(data_loader=pandas_numpy_data_loader))
        seen = []

        @use_iterdict_data_loader
        def probe(conn, value):
            seen.append(conn.options.data_loader)
            return value * 2

        assert probe(cn, 21) == 42
        assert seen == [iterdict_data_loader]
        assert cn.options.data_loader is pandas_numpy_data_loader

    def test_restores_loader_when_the_call_raises(self):
        """Verify the original loader is restored on the exception path.

        Mutation: replacing the `try/finally` with a plain call followed by
            the restore, so a raising query leaves the loader swapped.
        Oracle: loader identity after a deliberate ValueError.
        """
        cn = _FakeConnection(_make_options(data_loader=pandas_pyarrow_data_loader))

        @use_iterdict_data_loader
        def probe(conn):
            raise ValueError('query failed')

        with pytest.raises(ValueError):
            probe(cn)
        assert cn.options.data_loader is pandas_pyarrow_data_loader

    def test_unwraps_a_transaction_like_first_argument(self):
        """Verify an argument without options is unwrapped to its connection.

        Mutation: dropping `cn = cn.connection`, which raises AttributeError on
            a Transaction, or inverting `not hasattr(cn, 'options')`.
        Oracle: a spy on the inner connection, which is the only object
            carrying options.
        """
        inner = _FakeConnection(_make_options(data_loader=pandas_numpy_data_loader))
        tx = _FakeTransaction(inner)
        seen = []

        @use_iterdict_data_loader
        def probe(conn):
            seen.append(inner.options.data_loader)
            return 'done'

        assert probe(tx) == 'done'
        assert seen == [iterdict_data_loader]
        assert inner.options.data_loader is pandas_numpy_data_loader

    def test_prefers_the_first_argument_when_it_carries_options(self):
        """Verify an argument that carries options is never unwrapped.

        Mutation: dropping `and not hasattr(cn, 'options')`, which swaps the
            loader on the inner connection and leaves the caller's own loader
            in force.
        Oracle: two distinct real loaders; the inner one must never change.
        """
        inner = _FakeConnection(_make_options(data_loader=pandas_pyarrow_data_loader))
        outer = _FakeConnection(_make_options(data_loader=pandas_numpy_data_loader))
        outer.connection = inner
        seen = []

        @use_iterdict_data_loader
        def probe(conn):
            seen.append(outer.options.data_loader)
            seen.append(inner.options.data_loader)

        probe(outer)
        assert seen == [iterdict_data_loader, pandas_pyarrow_data_loader]
        assert outer.options.data_loader is pandas_numpy_data_loader
        assert inner.options.data_loader is pandas_pyarrow_data_loader

    def test_preserves_wrapped_function_identity(self):
        """Verify the decorated connection methods keep their own name and doc.

        Mutation: dropping `@wraps(func)` from the inner function, which
            renames every decorated ConnectionWrapper method to 'inner'.
        Oracle: name and docstring of the undecorated function.
        """
        @use_iterdict_data_loader
        def select_row(conn):
            """Execute a query and return a single row."""

        assert select_row.__name__ == 'select_row'
        assert select_row.__doc__ == 'Execute a query and return a single row.'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
