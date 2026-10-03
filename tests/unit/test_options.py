import subprocess
import sys

import pytest
from database.exceptions import ValidationError
from database.options import DatabaseOptions, iterdict_data_loader
from database.options import pandas_numpy_data_loader

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


def test_defaults_match_declared_field_defaults():
    """Verify unset fields take the documented defaults.

    Mutation: a changed default, e.g. use_pool True or journal_mode 'delete'.
    Oracle: hand-written table of the documented defaults.
    """
    options = _make_options()

    defaults = {
        'drivername': options.drivername,
        'reader_hostname': options.reader_hostname,
        'reader_port': options.reader_port,
        'use_pool': options.use_pool,
        'pool_max_connections': options.pool_max_connections,
        'pool_max_idle_time': options.pool_max_idle_time,
        'pool_wait_timeout': options.pool_wait_timeout,
        'journal_mode': options.journal_mode,
        'open_mode': options.open_mode,
        }
    assert defaults == {
        'drivername': 'postgresql',
        'reader_hostname': None,
        'reader_port': 0,
        'use_pool': False,
        'pool_max_connections': 5,
        'pool_max_idle_time': 300,
        'pool_wait_timeout': 30,
        'journal_mode': 'wal',
        'open_mode': None,
        }


def test_pooling_fields_are_kept_and_never_required():
    """Verify falsy pool values construct without raising.

    Mutation: 'pool_max_connections' added to PostgreSQL's required options.
    Oracle: falsy pool fields read back as (False, 0, 0, 0).
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

    Mutation: the `not` dropped from the is_supported_dialect guard.
    Oracle: 'sybase' raises; both registered dialects construct.
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
    """Verify each field PostgreSQL requires is enforced by name.

    Mutation: an entry dropped from PostgresStrategy.get_required_options.
    Oracle: hand-written list of six fields; the error names the field.
    """
    unset = 0 if field in {'port', 'timeout'} else None
    with pytest.raises(ValidationError) as excinfo:
        _make_options(**{field: unset})
    assert field in str(excinfo.value)


def test_zero_is_rejected_for_port_and_timeout():
    """Verify validation rejects 0 for port and timeout.

    Mutation: the falsy check in validate_options narrowed to `is None`.
    Oracle: boundary pair 0 raises, 1 constructs, for both fields.
    """
    for field in ('port', 'timeout'):
        with pytest.raises(ValidationError):
            _make_options(**{field: 0})
        assert getattr(_make_options(**{field: 1}), field) == 1


def test_sqlite_requires_only_database():
    """Verify SQLite requires database alone.

    Mutation: SQLiteStrategy.get_required_options returning PostgreSQL's list.
    Oracle: database alone constructs; database=None raises naming the field.
    """
    options = DatabaseOptions(drivername='sqlite', database='test.db')

    assert options.database == 'test.db'
    assert (options.hostname, options.username, options.password) == (None, None, None)
    assert (options.port, options.timeout) == (0, 0)

    with pytest.raises(ValidationError) as excinfo:
        DatabaseOptions(drivername='sqlite')
    assert 'database' in str(excinfo.value)


def test_sqlite_journal_mode_accepts_only_durable_modes():
    """Verify journal_mode accepts the four durable modes and nothing else.

    Mutation: journal_mode left unchecked, or JOURNAL_MODES gaining 'off'.
    Oracle: SQLite's documented durable modes, plus upper-case 'WAL'.
    """
    for mode in ('wal', 'delete', 'truncate', 'persist'):
        options = DatabaseOptions(drivername='sqlite', database='x.db',
                                  journal_mode=mode)
        assert options.journal_mode == mode

    for mode in ('off', 'memory', 'WAL'):
        with pytest.raises(ValidationError) as excinfo:
            DatabaseOptions(drivername='sqlite', database='x.db',
                            journal_mode=mode)
        assert repr(mode) in str(excinfo.value)


def test_sqlite_open_mode_accepts_only_the_read_only_modes():
    """Verify open_mode accepts None, 'ro' and 'immutable' and nothing else.

    Mutation: open_mode left unchecked, or OPEN_MODES gaining 'rw' or 'rwc'.
    Oracle: SQLite's read-only mode values, plus upper-case 'RO'.
    """
    for mode in (None, 'ro', 'immutable'):
        options = DatabaseOptions(drivername='sqlite', database='x.db',
                                  open_mode=mode)
        assert options.open_mode == mode

    for mode in ('rw', 'rwc', 'memory', 'RO'):
        with pytest.raises(ValidationError) as excinfo:
            DatabaseOptions(drivername='sqlite', database='x.db',
                            open_mode=mode)
        assert repr(mode) in str(excinfo.value)


def test_supplied_appname_wins_over_script_name(monkeypatch):
    """Verify a supplied appname is never overwritten by the script name.

    Mutation: scriptname() ordered before self.appname in __post_init__.
    Oracle: a distinct sys.argv[0]; the supplied appname stays.
    """
    monkeypatch.setattr(sys, 'argv', ['/opt/bin/report_runner.py'])

    assert _make_options(appname='my_app').appname == 'my_app'
    assert _make_options(appname=None).appname == 'report_runner'


def test_appname_falls_back_to_python_console(monkeypatch):
    """Verify a blank script name still yields a usable appname.

    Mutation: the `or 'python_console'` tail dropped from __post_init__.
    Oracle: sys.argv[0] = '' leaves the literal as the only source.
    """
    monkeypatch.setattr(sys, 'argv', [''])

    assert _make_options(appname=None).appname == 'python_console'


def test_default_data_loader_only_fills_an_unset_loader():
    """Verify a caller-supplied data loader survives __post_init__.

    Mutation: the `is None` guard dropped, so the default always wins.
    Oracle: identity of the loader handed in.
    """
    assert _make_options(data_loader=iterdict_data_loader).data_loader \
        is iterdict_data_loader
    assert _make_options().data_loader is pandas_numpy_data_loader


def test_falsy_custom_data_loader_is_kept():
    """Verify a falsy custom loader survives __post_init__.

    Mutation: `if self.data_loader is None:` relaxed to `if not ...`.
    Oracle: a callable whose __bool__ is False comes back unreplaced.
    """
    loader = _FalsyLoader()

    assert _make_options(data_loader=loader).data_loader is loader


class TestPasswordRedaction:
    """repr, str and format strings never show the password.
    """

    def test_repr_masks_password_exactly(self):
        """Verify repr renders every field in the documented order, masked.

        Mutation: __repr__ deleted, so the dataclass repr prints the password.
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

        Mutation: `masked = self.password` in __repr__.
        Oracle: a password sharing no character with '***', in six forms.
        """
        password = 'zq7-plaintext-secret'
        options = _make_options(password=password)

        # Without the noqa markers ruff rewrites the last two as
        # f-strings, so they stop testing format() and %.
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

        Mutation: `'*' * len(self.password)` in place of `'***'`.
        Oracle: password='ab' renders as "password='***',".
        """
        options = _make_options(password='ab')

        assert "password='***'," in repr(options)

    def test_repr_keeps_username_that_equals_the_password(self):
        """Verify a username equal to the password still renders in full.

        Mutation: scrubbing the password text from the rendered string.
        Oracle: username and password set to one string.
        """
        shared = 'reporting'
        options = _make_options(username=shared, password=shared)

        rendered = repr(options)
        assert "username='reporting'" in rendered
        assert "password='***'" in rendered

    def test_repr_shows_none_when_no_password(self):
        """Verify an absent password renders as None.

        Mutation: `masked = '***'` unconditionally in __repr__.
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

        Mutation: `masked = '***' if self.password else None` in __repr__.
        Oracle: whole-repr strings for password='' and password=None.
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


def test_import_without_pyarrow():
    """Verify database imports and selects when pyarrow is absent.

    Mutation: a module-level `import pyarrow` in options.py or types.py.
    Oracle: a child interpreter with pyarrow blocked in sys.modules.
    """
    code = (
        "import sys; sys.modules['pyarrow'] = None; import database; "
        "cn = database.connect({'drivername': 'sqlite', 'database': ':memory:', "
        "'data_loader': database.options.iterdict_data_loader}); "
        "print(cn.select('select ? as a', 2))")
    result = subprocess.run(
        [sys.executable, '-c', code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "[{'a': 2}]" in result.stdout


if __name__ == '__main__':
    __import__('pytest').main([__file__])
