"""Integration tests for the SQLite journal_mode option on a writer.
"""
import sqlite3

import database as db
import pytest
from database.options import DatabaseOptions
from database.strategy.sqlite import SQLiteStrategy

pytestmark = [pytest.mark.sqlite, pytest.mark.integration]


def _delete_mode_file(tmp_path):
    """Path of a new delete-mode database holding table t with one row.
    """
    db_file = tmp_path / 'journal.db'
    setup = sqlite3.connect(db_file)
    setup.execute('create table t (a int)')
    setup.execute('insert into t values (1)')
    setup.commit()
    setup.close()
    return db_file


def _journal_mode_on_disk(db_file):
    """Journal mode a fresh sqlite3 connection reports for db_file.
    """
    check = sqlite3.connect(db_file)
    mode = check.execute('pragma journal_mode').fetchone()[0]
    check.close()
    return mode


def test_delete_mode_writer_connects_while_another_connection_reads(tmp_path):
    """Verify journal_mode='delete' keeps a delete-mode file in delete mode.

    Mutation: configure_writer_connection ignoring journal_mode, setting WAL.
    Oracle: journal_mode on a separate connection, with a read held open.
    """
    db_file = _delete_mode_file(tmp_path)
    holder = sqlite3.connect(db_file, isolation_level=None)
    holder.execute('begin')
    holder.execute('select * from t').fetchall()
    try:
        cn = db.connect({'drivername': 'sqlite', 'database': str(db_file),
                         'journal_mode': 'delete'})
        assert db.select_scalar(cn, 'select count(*) from t') == 1
        cn.close()
    finally:
        holder.close()

    assert _journal_mode_on_disk(db_file) == 'delete'


def test_synchronous_follows_the_journal_mode(tmp_path):
    """Verify each mode sets its own synchronous level over a stale one.

    Mutation: the non-WAL branch keeping a stale level, or levels swapped.
    Oracle: SQLite's documented levels, normal = 1 and full = 2.
    """
    db_file = _delete_mode_file(tmp_path)
    strategy = SQLiteStrategy()

    cases = (
        ('delete', 'normal', 2),
        ('truncate', 'normal', 2),
        ('persist', 'normal', 2),
        ('wal', 'full', 1),
        )
    for mode, stale, expected in cases:
        raw = sqlite3.connect(db_file, isolation_level=None)
        raw.execute(f'pragma synchronous = {stale}')
        options = DatabaseOptions(drivername='sqlite', database=str(db_file),
                                  journal_mode=mode)
        strategy.configure_writer_connection(raw, options)
        assert raw.execute('pragma journal_mode').fetchone()[0] == mode
        assert raw.execute('pragma synchronous').fetchone()[0] == expected
        raw.close()


def test_writer_keeps_its_journal_mode_across_a_rebuild(tmp_path):
    """Verify a reconnect applies the caller's journal_mode again.

    Mutation: _ensure_connection not passing options to configure_connection.
    Oracle: journal_mode on a separate connection, and synchronous full = 2.
    """
    db_file = _delete_mode_file(tmp_path)
    cn = db.connect({'drivername': 'sqlite', 'database': str(db_file),
                     'journal_mode': 'delete'})
    cn._invalidate()
    assert db.select_scalar(cn, 'select count(*) from t') == 1
    raw = cn.dbapi_connection.driver_connection
    assert raw.execute('pragma synchronous').fetchone()[0] == 2
    cn.close()

    assert _journal_mode_on_disk(db_file) == 'delete'


def _switch_to_wal_and_hold_open(db_file):
    """Open sqlite3 connection that switched db_file to WAL and read it.
    """
    other = sqlite3.connect(db_file, isolation_level=None)
    other.execute('pragma journal_mode = wal')
    other.execute('select * from t').fetchall()
    return other


def test_failed_rebuild_is_retried_rather_than_kept(tmp_path):
    """Verify a reconnect whose setup raises leaves no connection behind.

    Mutation: _ensure_connection keeping the rebuilt connection on failure.
    Oracle: SQLite refusing to leave WAL while another connection reads.
    """
    db_file = _delete_mode_file(tmp_path)
    cn = db.connect({'drivername': 'sqlite', 'database': str(db_file),
                     'journal_mode': 'delete'})
    cn._invalidate()
    other = _switch_to_wal_and_hold_open(db_file)
    try:
        for _ in range(2):
            with pytest.raises(sqlite3.OperationalError, match='locked'):
                db.execute(cn, 'insert into t values (2)')
    finally:
        other.close()

    db.execute(cn, 'insert into t values (3)')
    assert db.select_column(cn, 'select a from t order by a') == [1, 3]
    cn.close()
    assert _journal_mode_on_disk(db_file) == 'delete'


def test_failed_connect_releases_its_connection(tmp_path):
    """Verify a connect whose setup raises does not hold the file open.

    Mutation: connect() leaving the half-configured connection open.
    Oracle: SQLite refusing to leave WAL while another connection reads.
    """
    db_file = _delete_mode_file(tmp_path)
    options = {'drivername': 'sqlite', 'database': str(db_file),
               'journal_mode': 'delete'}
    other = _switch_to_wal_and_hold_open(db_file)
    with pytest.raises(sqlite3.OperationalError, match='locked') as excinfo:
        db.connect(dict(options))
    other.close()

    # excinfo keeps any leaked connection alive until the retry runs.
    cn = db.connect(dict(options))
    cn.close()
    del excinfo
    assert _journal_mode_on_disk(db_file) == 'delete'
