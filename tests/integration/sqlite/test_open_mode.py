"""Integration tests for the SQLite open_mode option on a reader.
"""
import sqlite3

import database as db
import pytest
import sqlalchemy as sa
from database.exceptions import ValidationError

pytestmark = [pytest.mark.sqlite, pytest.mark.integration]


def _delete_mode_file(directory):
    """Path of a new delete-mode store.db in directory, table t, one row.
    """
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
    """Connect options for db_file opened in open_mode.
    """
    return {'drivername': 'sqlite', 'database': str(db_file),
            'open_mode': open_mode}


@pytest.mark.parametrize('open_mode', ['ro', 'immutable'])
def test_reader_refuses_a_missing_file(tmp_path, open_mode):
    """Verify each open mode raises on a missing path and creates nothing.

    Mutation: options.database restored over the URI, or mode=ro left out.
    Oracle: the directory listing, empty before and after the connect.
    """
    with pytest.raises(sa.exc.OperationalError):
        db.connect(_options(tmp_path / 'missing.db', open_mode), role='reader')

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('open_mode', ['ro', 'immutable'])
def test_reader_opens_a_path_holding_uri_delimiters(tmp_path, open_mode):
    """Verify each open mode reads a file whose directory holds '#' and '?'.

    Mutation: the path unencoded in place of Path.as_uri(), or 'file:' twice.
    Oracle: the one row written by plain sqlite3 before the connect.
    """
    db_file = _delete_mode_file(tmp_path / 'a b#c?d')

    cn = db.connect(_options(db_file, open_mode), role='reader')
    try:
        assert db.select_scalar(cn, 'select count(*) from t') == 1
    finally:
        cn.close()


@pytest.mark.parametrize('open_mode', ['ro', 'immutable'])
def test_reader_opens_a_path_holding_a_percent_escape(tmp_path, open_mode):
    """Verify each open mode reads a file whose directory name holds '%20'.

    Mutation: create_url_from_options keeping the URI make_url unquoted.
    Oracle: the one row written by plain sqlite3 before the connect.
    """
    db_file = _delete_mode_file(tmp_path / 'p%20q')

    cn = db.connect(_options(db_file, open_mode), role='reader')
    try:
        assert db.select_scalar(cn, 'select count(*) from t') == 1
    finally:
        cn.close()


def test_immutable_reader_reads_through_an_exclusive_lock(tmp_path):
    """Verify open_mode='immutable' reads while another connection locks.

    Mutation: immutable=1 left out of the URL.
    Oracle: the committed row, read under another connection's lock.
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

    Mutation: dropping the role check in connect.
    Oracle: open_mode set with the default role='writer'.
    """
    db_file = _delete_mode_file(tmp_path)

    with pytest.raises(ValidationError, match='reader'):
        db.connect(_options(db_file, open_mode))
