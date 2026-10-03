"""Session pragmas a default SQLite connection carries.
"""
import database as db
import pytest

pytestmark = [pytest.mark.sqlite, pytest.mark.integration]


def test_file_db_uses_wal_journal_mode(tmp_path):
    """Verify a file database defaults to WAL.

    Mutation: a default journal_mode other than 'wal', or no writer hook.
    Oracle: SQLite's own report of journal_mode.
    """
    conn = db.connect({
        'drivername': 'sqlite',
        'database': str(tmp_path / 'pragma_test.db'),
        })
    try:
        assert db.select_scalar(conn, 'pragma journal_mode') == 'wal'
    finally:
        conn.close()


def test_memory_db_skips_the_writer_pragmas():
    """Verify ':memory:' keeps SQLite's default synchronous level.

    Mutation: dropping the _is_memory_db check in configure_writer_connection.
    Oracle: SQLite's default level, full = 2.
    """
    conn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    try:
        assert db.select_scalar(conn, 'pragma synchronous') == 2
    finally:
        conn.close()


@pytest.mark.parametrize('database', ['pragma_test.db', ':memory:'])
def test_busy_timeout_is_set(tmp_path, database):
    """Verify every SQLite connection waits 5000 ms on a lock.

    Mutation: busy_timeout moved into the writer hook, or a different value.
    Oracle: SQLite's own report, against the 5000 ms the strategy sets.
    """
    if database != ':memory:':
        database = str(tmp_path / database)
    conn = db.connect({'drivername': 'sqlite', 'database': database})
    try:
        assert db.select_scalar(conn, 'pragma busy_timeout') == 5000
    finally:
        conn.close()
