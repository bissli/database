"""Integration tests for the SQLite open_mode option on a reader.
"""
import sqlite3

import database as db
import pytest
import sqlalchemy as sa
from database.exceptions import ValidationError

pytestmark = [pytest.mark.sqlite, pytest.mark.integration]


def _delete_mode_file(directory):
    directory.mkdir(parents=True, exist_ok=True)
    db_file = directory / 'store.db'
    setup = sqlite3.connect(db_file)
    setup.execute('pragma journal_mode = delete')
    setup.execute('create table t (a int)')
    setup.execute('insert into t values (1)')
    setup.commit()
    setup.close()
    return db_file


def _options(db_file, open_mode):
    return {'drivername': 'sqlite', 'database': str(db_file),
            'open_mode': open_mode}


@pytest.mark.parametrize('open_mode', ['ro', 'immutable'])
def test_reader_refuses_a_missing_file(tmp_path, open_mode):
    """Verify each open mode raises on a missing path and creates nothing.

    Mutation: create_url_from_options writing options.database back over
        the file: URI, which opens a new read-write file named
        'missing.db?mode=ro', or mode=ro left out of either mode's URL,
        since immutable=1 alone creates an empty file.
    Oracle: the directory listing, empty before and after the connect.
    """
    with pytest.raises(sa.exc.OperationalError):
        db.connect(_options(tmp_path / 'missing.db', open_mode), role='reader')

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('open_mode', ['ro', 'immutable'])
def test_reader_opens_a_path_holding_uri_delimiters(tmp_path, open_mode):
    """Verify each open mode reads a file whose directory holds '#' and '?'.

    Mutation: the path put into the URI unencoded in place of
        Path.as_uri(), so '#' or '?' ends the path early, or a second
        'file:' prefix in front of the URI.
    Oracle: the one row written by plain sqlite3 before the connect.
    """
    db_file = _delete_mode_file(tmp_path / 'a b#c?d')

    cn = db.connect(_options(db_file, open_mode), role='reader')
    try:
        assert db.select_scalar(cn, 'select count(*) from t') == 1
    finally:
        cn.close()


def test_immutable_reader_reads_through_an_exclusive_lock(tmp_path):
    """Verify open_mode='immutable' reads while another connection locks.

    Mutation: immutable=1 left out of the URL, or 'immutable' built as
        mode=ro, either of which waits on the lock and raises 'database
        is locked'.
    Oracle: the one row committed before a second sqlite3 connection
        takes BEGIN EXCLUSIVE, which blocks every locking reader.
    """
    db_file = _delete_mode_file(tmp_path)
    holder = sqlite3.connect(db_file, isolation_level=None)
    holder.execute('begin exclusive')
    try:
        cn = db.connect(_options(db_file, 'immutable'), role='reader')
        try:
            assert db.select_scalar(cn, 'select count(*) from t') == 1
        finally:
            cn.close()
    finally:
        holder.close()


@pytest.mark.parametrize('open_mode', ['ro', 'immutable'])
def test_open_mode_on_a_writer_raises(tmp_path, open_mode):
    """Verify a writer asking for a read-only open raises ValidationError.

    Mutation: dropping the role check in connect, so the writer hook
        runs on a read-only file and the write guard stays off.
    Oracle: the connect inputs at the rule, open_mode set with the
        default role='writer'.
    """
    db_file = _delete_mode_file(tmp_path)

    with pytest.raises(ValidationError, match='reader'):
        db.connect(_options(db_file, open_mode))
