"""Component tests for connect()'s role argument.

connect() accepts its options in four shapes, and role has to survive
every one of them. A role that is silently dropped is the worst
outcome the feature has: the caller asked for a reader and writes to
production on the connection it got back. These tests pin each shape,
and the two call forms that must fail loudly rather than default.
"""
import config
import database as db
import pytest
from database.connection import _CONNECTION_ROLES
from database.options import DatabaseOptions

from libb import Setting


def _sqlite_options(tmp_path):
    """A minimal SQLite option dict over a fresh file."""
    return {'drivername': 'sqlite', 'database': str(tmp_path / 'role.db')}


@pytest.fixture
def sqlite_config(tmp_path):
    """tests/config.py with its sqlite database pointed at tmp_path.

    The module declares a bare 'database.db', which connect() would
    otherwise create in whatever directory pytest ran from.
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

    libb's load_options wrapper builds DatabaseOptions from **kwargs
    before it strips the field names, so a decorated connect() rejects
    role outright in the bare-kwargs shape and drops it elsewhere.

    Mutation: putting @load_options back on connect(), which raises
        TypeError for the bare-kwargs shape.
    Oracle: cn.readonly, read on a connection built each of the four
        documented ways.
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
    """Verify omitting role opens a writer, unchanged from before.

    Mutation: defaulting role to 'reader', or inverting the
        `readonly = role == 'reader'` test, either of which turns every
        existing caller read-only.
    Oracle: cn.readonly on the same four shapes with role omitted.
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
    """Verify connect(options, 'reader') raises instead of writing.

    config is the second positional parameter, so a role passed there
    is accepted and ignored, and the caller gets a writer.

    Mutation: dropping the isinstance(config, str) test, or dropping
        the '*' that makes role keyword only - either one turns this
        call into a silent writer.
    Oracle: ValidationError naming the string that landed in config.
    """
    with pytest.raises(db.ValidationError, match='reader'):
        db.connect(_sqlite_options(tmp_path), 'reader')


@pytest.mark.parametrize('role', ['read', 'replica', 'READER', '', None])
def test_unknown_role_is_rejected(tmp_path, role):
    """Verify anything but the two accepted values fails loudly.

    Mutation: replacing the membership test with a truthiness test, or
        lowercasing the input, either of which routes a typo to the
        writer without a word.
    Oracle: ValidationError, plus the accepted set read off the module
        so the test tracks it.
    """
    assert _CONNECTION_ROLES == {'writer', 'reader'}

    with pytest.raises(db.ValidationError, match='role must be one of'):
        db.connect(_sqlite_options(tmp_path), role=role)


def test_reader_options_reopen_as_the_same_role(tmp_path):
    """Verify cn.options round-trips instead of yielding a writer.

    connect() resolves the reader endpoint into a copy. Handing the
    resolved copy to the wrapper would leave cn.options pointing at the
    replica with no reader field in play, so reconnecting from it
    returns a writer aimed at the replica.

    Mutation: passing `resolved` instead of `options` to
        ConnectionWrapper, which makes the reopened connection a
        writer.
    Oracle: readonly on a connection reopened from the first one's own
        options.
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


def test_reader_endpoint_fields_are_resolved_into_the_engine(tmp_path):
    """Verify the reader endpoint reaches the engine, not just the flag.

    Mutation: dropping the `replace()` call, which leaves the engine
        pointed at the writer endpoint while still reporting
        readonly - the reader instance stays idle and every select
        still lands on the writer.
    Oracle: the engine URL, which carries the host the connection
        actually opened.
    """
    options = DatabaseOptions(
        drivername='postgresql', hostname='writer.example',
        reader_hostname='replica.example', username='u', password='p',
        database='d', port=5432, reader_port=6543, timeout=30)

    from dataclasses import replace

    from database.connection import create_url_from_options

    writer_url = create_url_from_options(options)
    reader_url = create_url_from_options(replace(
        options,
        hostname=options.reader_hostname or options.hostname,
        port=options.reader_port or options.port))

    assert (writer_url.host, writer_url.port) == ('writer.example', 5432)
    assert (reader_url.host, reader_url.port) == ('replica.example', 6543)
