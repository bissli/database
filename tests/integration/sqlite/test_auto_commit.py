"""
Integration tests for auto-commit functionality with SQLite.
"""
import logging
import os
import time

import database as db
import pytest
from database.utils.auto_commit import diagnose_connection

logger = logging.getLogger(__name__)


@pytest.fixture
def test_table_prefix():
    """Generate a unique test table prefix for isolation"""
    return f'test_autocommit_{int(time.time())}'


@pytest.fixture
def sqlite_conn():
    """Create a file-based SQLite connection for testing auto-commit"""
    # Create a temporary file database
    db_file = f'./test_autocommit_{int(time.time())}.db'

    # Create connection
    conn = db.connect({
        'drivername': 'sqlite',
        'database': db_file
    })

    yield conn

    # Clean up
    conn.close()
    if os.path.exists(db_file):
        os.unlink(db_file)


class TestSQLiteAutoCommit:
    """Test auto-commit functionality with SQLite connections"""

    def test_auto_commit_enabled_by_default(self, sqlite_conn):
        """Test that auto-commit is enabled by default for new connections"""
        # Get connection diagnostics
        info = diagnose_connection(sqlite_conn)

        # Verify auto-commit is enabled
        assert info['auto_commit'] is True, 'Auto-commit should be enabled by default'
        assert info['in_transaction'] is False, 'Connection should not be in a transaction'

    def test_changes_persist_with_auto_commit(self, sqlite_conn, test_table_prefix):
        # Create unique table name for this test
        test_table_name = f"{test_table_prefix}_persist"
        """Test that changes persist without explicit commit when auto-commit is enabled"""
        # Create a test table
        db.execute(sqlite_conn, f"""
        CREATE TABLE {test_table_name} (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
        """)

        # Insert data without explicit commit
        db.execute(sqlite_conn, f"INSERT INTO {test_table_name} (data) VALUES ('test1')")

        # Get the database file path
        db_path = sqlite_conn.options.database

        # Close the connection
        sqlite_conn.close()

        # Create a new connection to verify the data was committed
        new_conn = db.connect({
            'drivername': 'sqlite',
            'database': db_path
        })

        try:
            # Verify data exists in the new connection
            count = db.select_scalar(new_conn, f'SELECT COUNT(*) FROM {test_table_name}')
            assert count == 1, 'Data should be committed automatically'

            # Verify the content
            data = db.select_scalar(new_conn,
                                    f'SELECT data FROM {test_table_name} WHERE rowid = 1')
            assert data == 'test1', 'Correct data should be committed'
        finally:
            new_conn.close()

    def test_transaction_commits_on_exit(self, sqlite_conn, test_table_prefix):
        # Create unique table name for this test
        test_table_name = f"{test_table_prefix}_commit"
        """Test that Transaction commits changes when exiting normally"""
        # Create a test table
        db.execute(sqlite_conn, f"""
        CREATE TABLE {test_table_name} (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
        """)

        # Use a transaction to insert data
        with db.transaction(sqlite_conn) as tx:
            tx.execute(f"INSERT INTO {test_table_name} (data) VALUES ('test_tx')")

            # Verify connection is marked as in a transaction
            assert sqlite_conn.in_transaction is True

            # Verify auto-commit is disabled during transaction
            info = diagnose_connection(sqlite_conn)
            assert info['auto_commit'] is False, 'Auto-commit should be disabled in transaction'

        # Verify connection is no longer in a transaction
        assert sqlite_conn.in_transaction is False

        # Verify auto-commit is re-enabled after transaction
        info = diagnose_connection(sqlite_conn)
        assert info['auto_commit'] is True, 'Auto-commit should be re-enabled after transaction'

        # Get the database file path
        db_path = sqlite_conn.options.database

        # Close the connection
        sqlite_conn.close()

        # Create a new connection to verify the data was committed
        new_conn = db.connect({
            'drivername': 'sqlite',
            'database': db_path
        })

        try:
            # Verify data exists in the new connection
            count = db.select_scalar(new_conn, f'SELECT COUNT(*) FROM {test_table_name}')
            assert count == 1, 'Transaction should commit data on exit'

            # Verify the content
            data = db.select_scalar(new_conn,
                                    f'SELECT data FROM {test_table_name} WHERE rowid = 1')
            assert data == 'test_tx', 'Correct data should be committed'
        finally:
            new_conn.close()

    def test_transaction_rollback_on_exception(self, sqlite_conn, test_table_prefix):
        # Create unique table name for this test
        test_table_name = f"{test_table_prefix}_rollback"
        """Test that Transaction rolls back changes on exception"""
        # Create a test table
        db.execute(sqlite_conn, f"""
        CREATE TABLE {test_table_name} (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
        """)

        # Use a transaction that raises an exception
        try:
            with db.transaction(sqlite_conn) as tx:
                tx.execute(f"INSERT INTO {test_table_name} (data) VALUES ('should_rollback')")
                # Force an error
                raise ValueError('Test exception to trigger rollback')
        except ValueError:
            pass  # Expected exception

        # Verify connection is no longer in a transaction
        assert sqlite_conn.in_transaction is False

        # Verify auto-commit is re-enabled after transaction
        info = diagnose_connection(sqlite_conn)
        assert info['auto_commit'] is True, 'Auto-commit should be re-enabled after exception'

        # Verify no data exists
        count = db.select_scalar(sqlite_conn, f'SELECT COUNT(*) FROM {test_table_name}')
        assert count == 0, 'Transaction should rollback data on exception'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
