"""
SQLite connection pragmas: WAL journal mode + explicit busy_timeout.
File-based connections enable WAL; in-memory connections do not.
"""
import database as db
import pytest


@pytest.mark.sqlite
@pytest.mark.integration
def test_file_db_uses_wal_journal_mode(tmp_path):
    """File-based SQLite connections must enable WAL journal mode."""
    db_file = tmp_path / 'pragma_test.db'
    conn = db.connect({'drivername': 'sqlite', 'database': str(db_file)})
    try:
        mode = db.select_scalar(conn, 'PRAGMA journal_mode')
        assert str(mode).lower() == 'wal'
    finally:
        conn.close()


@pytest.mark.sqlite
@pytest.mark.integration
def test_file_db_sets_busy_timeout(tmp_path):
    """File-based SQLite connections must set busy_timeout explicitly to 5000ms."""
    db_file = tmp_path / 'pragma_test.db'
    conn = db.connect({'drivername': 'sqlite', 'database': str(db_file)})
    try:
        timeout = db.select_scalar(conn, 'PRAGMA busy_timeout')
        assert int(timeout) == 5000
    finally:
        conn.close()


@pytest.mark.sqlite
@pytest.mark.integration
def test_memory_db_does_not_use_wal():
    """In-memory SQLite has no on-disk log, so WAL is pointless and must be skipped."""
    conn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    try:
        mode = db.select_scalar(conn, 'PRAGMA journal_mode')
        assert str(mode).lower() != 'wal'
    finally:
        conn.close()


@pytest.mark.sqlite
@pytest.mark.integration
def test_memory_db_still_sets_busy_timeout():
    """busy_timeout should be set on every SQLite connection regardless of backing store."""
    conn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    try:
        timeout = db.select_scalar(conn, 'PRAGMA busy_timeout')
        assert int(timeout) == 5000
    finally:
        conn.close()
