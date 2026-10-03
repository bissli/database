"""Component tests for connect()'s role argument.
"""
import config
import database as db
import database.connection
import pytest
from database.connection import _CONNECTION_ROLES
from database.options import DatabaseOptions

from libb import Setting


class _EngineRequested(Exception):
    """Stops connect() at the point it asks for an engine.
    """


def _sqlite_options(tmp_path):
    """A minimal SQLite option dict over a fresh file.
    """
    return {'drivername': 'sqlite', 'database': str(tmp_path / 'role.db')}


@pytest.fixture
def sqlite_config(tmp_path):
    """tests/config.py with its sqlite database pointed at tmp_path.
    """
    original = config.sqlite.database
    Setting.unlock()
    config.sqlite.database = str(tmp_path / 'config.db')
    Setting.lock()
    try:
        yield config
    finally:
        Setting.unlock()
        config.sqlite.database = original
        Setting.lock()


def test_role_survives_every_options_shape(tmp_path, sqlite_config):
    """Verify role reaches connect() however the options arrive.

    Mutation: connect() decorated with libb's load_options.
    Oracle: cn.readonly on a connection built each of the four ways.
    """
    options = _sqlite_options(tmp_path)
    connections = [
        db.connect(dict(options), role='reader'),
        db.connect(DatabaseOptions(**options), role='reader'),
        db.connect(role='reader', **options),
        db.connect('sqlite', config=sqlite_config, role='reader'),
        ]
    try:
        assert [cn.readonly for cn in connections] == [True] * 4
    finally:
        for cn in connections:
            cn.close()


def test_writer_is_the_default_for_every_options_shape(tmp_path, sqlite_config):
    """Verify omitting role opens a writer.

    Mutation: role defaulting to 'reader', or the readonly test inverted.
    Oracle: cn.readonly on the four shapes with role omitted.
    """
    options = _sqlite_options(tmp_path)
    connections = [
        db.connect(dict(options)),
        db.connect(DatabaseOptions(**options)),
        db.connect(**options),
        db.connect('sqlite', config=sqlite_config),
        ]
    try:
        assert [cn.readonly for cn in connections] == [False] * 4
    finally:
        for cn in connections:
            cn.close()


def test_role_passed_positionally_is_rejected(tmp_path):
    """Verify connect(options, 'reader') raises.

    Mutation: dropping the isinstance(config, str) test, or the '*' on role.
    Oracle: ValidationError naming the string that landed in config.
    """
    with pytest.raises(db.ValidationError, match='reader'):
        db.connect(_sqlite_options(tmp_path), 'reader')


@pytest.mark.parametrize('role', ['read', 'replica', 'READER', '', None])
def test_unknown_role_is_rejected(tmp_path, role):
    """Verify anything but the two accepted values raises.

    Mutation: the membership test replaced by truthiness, or input lowercased.
    Oracle: ValidationError, plus the accepted set pinned to two roles.
    """
    assert _CONNECTION_ROLES == {'writer', 'reader'}

    with pytest.raises(db.ValidationError, match='role must be one of'):
        db.connect(_sqlite_options(tmp_path), role=role)


def test_reader_options_reopen_as_the_same_role(tmp_path):
    """Verify cn.options reopens a reader.

    Mutation: engine_options handed to ConnectionWrapper in place of options.
    Oracle: readonly on a connection reopened from the first one's options.
    """
    reader = db.connect(_sqlite_options(tmp_path), role='reader')
    try:
        reopened = db.connect(reader.options, role='reader')
        try:
            assert (reader.readonly, reopened.readonly) == (True, True)
        finally:
            reopened.close()
    finally:
        reader.close()


@pytest.mark.parametrize(
    ('role', 'reader_hostname', 'reader_port', 'expected'), [
        ('reader', 'replica.example', 6543, ('replica.example', 6543)),
        ('reader', 'replica.example', 0, ('replica.example', 5432)),
        ('reader', None, 6543, ('writer.example', 6543)),
        ('writer', 'replica.example', 6543, ('writer.example', 5432)),
        ], ids=['reader', 'reader-host-only', 'reader-port-only', 'writer'])
def test_role_picks_the_endpoint_the_engine_opens(
        monkeypatch, role, reader_hostname, reader_port, expected):
    """Verify the engine gets the role's host and port, each falling back.

    Mutation: the replace() call dropped, or host and port resolved together.
    Oracle: hand-written (host, port) per case, read off a recording stub.
    """
    requested = []

    def record_request(engine_options, **kwargs):
        requested.append((engine_options, kwargs))
        raise _EngineRequested

    monkeypatch.setattr(
        database.connection, 'get_engine_for_options', record_request)
    options = DatabaseOptions(
        drivername='postgresql', hostname='writer.example', port=5432,
        reader_hostname=reader_hostname, reader_port=reader_port,
        username='u', password='p', database='d', timeout=30)

    with pytest.raises(_EngineRequested):
        db.connect(options, role=role)

    [(engine_options, kwargs)] = requested
    assert (engine_options.hostname, engine_options.port) == expected
    assert kwargs['readonly'] is (role == 'reader')
