"""
Integration tests for auto-commit functionality with SQLite.

Note: Common auto-commit tests are in tests/integration/common/test_auto_commit.py
This file contains only SQLite-specific tests (e.g., connection reopening with file-based DB).
"""
import logging

import database as db
from database.utils.auto_commit import diagnose_connection

logger = logging.getLogger(__name__)


class TestSQLiteAutoCommit:
    """Test auto-commit functionality with SQLite connections.

    Uses sqlite_file_conn (file-based SQLite) for tests that need to close and reopen connections.
    """

    def test_changes_persist_with_auto_commit(self, sqlite_file_conn, test_table_prefix):
        """Test that changes persist without explicit commit when auto-commit is enabled."""
        test_table_name = f'{test_table_prefix}_persist'
        db.execute(sqlite_file_conn, f"""
        CREATE TABLE {test_table_name} (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
        """)

        db.execute(sqlite_file_conn, f"INSERT INTO {test_table_name} (data) VALUES ('test1')")

        db_path = sqlite_file_conn.options.database
        sqlite_file_conn.close()

        new_conn = db.connect({
            'drivername': 'sqlite',
            'database': db_path
        })

        try:
            count = db.select_scalar(new_conn, f'SELECT COUNT(*) FROM {test_table_name}')
            assert count == 1, 'Data should be committed automatically'

            data = db.select_scalar(new_conn,
                                    f'SELECT data FROM {test_table_name} WHERE rowid = 1')
            assert data == 'test1', 'Correct data should be committed'
        finally:
            new_conn.close()

    def test_transaction_commits_on_exit(self, sqlite_file_conn, test_table_prefix):
        """Test that Transaction commits changes when exiting normally."""
        test_table_name = f'{test_table_prefix}_commit'
        db.execute(sqlite_file_conn, f"""
        CREATE TABLE {test_table_name} (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
        """)

        with db.transaction(sqlite_file_conn) as tx:
            tx.execute(f"INSERT INTO {test_table_name} (data) VALUES ('test_tx')")

            assert sqlite_file_conn.in_transaction is True

            info = diagnose_connection(sqlite_file_conn)
            assert info['auto_commit'] is False, 'Auto-commit should be disabled in transaction'

        assert sqlite_file_conn.in_transaction is False

        info = diagnose_connection(sqlite_file_conn)
        assert info['auto_commit'] is True, 'Auto-commit should be re-enabled after transaction'

        db_path = sqlite_file_conn.options.database
        sqlite_file_conn.close()

        new_conn = db.connect({
            'drivername': 'sqlite',
            'database': db_path
        })

        try:
            count = db.select_scalar(new_conn, f'SELECT COUNT(*) FROM {test_table_name}')
            assert count == 1, 'Transaction should commit data on exit'

            data = db.select_scalar(new_conn,
                                    f'SELECT data FROM {test_table_name} WHERE rowid = 1')
            assert data == 'test_tx', 'Correct data should be committed'
        finally:
            new_conn.close()

    def test_transaction_rollback_on_exception(self, sl_conn, test_table_prefix):
        """Test that Transaction rolls back changes on exception."""
        test_table_name = f'{test_table_prefix}_rollback'
        db.execute(sl_conn, f"""
        CREATE TABLE {test_table_name} (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
        """)

        try:
            with db.transaction(sl_conn) as tx:
                tx.execute(f"INSERT INTO {test_table_name} (data) VALUES ('should_rollback')")
                raise ValueError('Test exception to trigger rollback')
        except ValueError:
            pass  # Expected exception

        assert sl_conn.in_transaction is False

        info = diagnose_connection(sl_conn)
        assert info['auto_commit'] is True, 'Auto-commit should be re-enabled after exception'

        count = db.select_scalar(sl_conn, f'SELECT COUNT(*) FROM {test_table_name}')
        assert count == 0, 'Transaction should rollback data on exception'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
