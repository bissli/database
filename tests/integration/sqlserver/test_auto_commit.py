"""
Integration tests for auto-commit functionality with SQL Server.
"""
import logging
import time

import database as db
import pytest
from database.utils.auto_commit import diagnose_connection

logger = logging.getLogger(__name__)


@pytest.fixture
def test_table_prefix():
    """Generate a unique test table prefix for isolation"""
    return f'#test_autocommit_{int(time.time())}'  # Using temp table for SQL Server


class TestSQLServerAutoCommit:
    """Test auto-commit functionality with SQL Server connections"""

    def test_auto_commit_enabled_by_default(self, sconn):
        """Test that auto-commit is enabled by default for new connections"""
        # Get connection diagnostics
        info = diagnose_connection(sconn)

        # Verify auto-commit is enabled
        assert info['auto_commit'] is True, 'Auto-commit should be enabled by default'
        assert info['in_transaction'] is False, 'Connection should not be in a transaction'

    def test_changes_persist_with_auto_commit(self, sconn, test_table_prefix):
        # Create unique table name for this test
        test_table_name = f"{test_table_prefix}_persist"
        """Test that changes persist without explicit commit when auto-commit is enabled"""
        # Create a test table (using temporary table since we can't test with multiple connections)
        db.execute(sconn, f"""
        CREATE TABLE {test_table_name} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            data NVARCHAR(100)
        )
        """)

        try:
            # Insert data without explicit commit
            db.execute(sconn, f"INSERT INTO {test_table_name} (data) VALUES ('test1')")

            # Verify data exists
            count = db.select_scalar(sconn, f'SELECT COUNT(*) FROM {test_table_name}')
            assert count == 1, 'Data should be committed automatically'

            # Verify the content
            data = db.select_scalar(sconn, f'SELECT data FROM {test_table_name} WHERE id = 1')
            assert data == 'test1', 'Correct data should be committed'
        finally:
            # Clean up the test table (automatically dropped at end of connection for temp tables)
            pass

    def test_transaction_commits_on_exit(self, sconn, test_table_prefix):
        # Create unique table name for this test
        test_table_name = f"{test_table_prefix}_commit"
        """Test that Transaction commits changes when exiting normally"""
        # Create a test table
        db.execute(sconn, f"""
        CREATE TABLE {test_table_name} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            data NVARCHAR(100)
        )
        """)

        try:
            # Use a transaction to insert data
            with db.transaction(sconn) as tx:
                tx.execute(f"INSERT INTO {test_table_name} (data) VALUES ('test_tx')")

                # Verify connection is marked as in a transaction
                assert sconn.in_transaction is True

                # Verify auto-commit is disabled during transaction
                info = diagnose_connection(sconn)
                assert info['auto_commit'] is False, 'Auto-commit should be disabled in transaction'

            # Verify connection is no longer in a transaction
            assert sconn.in_transaction is False

            # Verify auto-commit is re-enabled after transaction
            info = diagnose_connection(sconn)
            assert info['auto_commit'] is True, 'Auto-commit should be re-enabled after transaction'

            # Verify data exists
            count = db.select_scalar(sconn, f'SELECT COUNT(*) FROM {test_table_name}')
            assert count == 1, 'Transaction should commit data on exit'

            # Verify the content
            data = db.select_scalar(sconn, f'SELECT data FROM {test_table_name} WHERE id = 1')
            assert data == 'test_tx', 'Correct data should be committed'
        finally:
            # Clean up the test table
            db.execute(sconn, f"IF OBJECT_ID('{test_table_name}', 'U') IS NOT NULL DROP TABLE {test_table_name}")

    def test_transaction_rollback_on_exception(self, sconn, test_table_prefix):
        # Create unique table name for this test
        test_table_name = f"{test_table_prefix}_rollback"
        """Test that Transaction rolls back changes on exception"""
        # Create a test table
        db.execute(sconn, f"""
        CREATE TABLE {test_table_name} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            data NVARCHAR(100)
        )
        """)

        try:
            # Use a transaction that raises an exception
            try:
                with db.transaction(sconn) as tx:
                    tx.execute(f"INSERT INTO {test_table_name} (data) VALUES ('should_rollback')")
                    # Force an error
                    raise ValueError('Test exception to trigger rollback')
            except ValueError:
                pass  # Expected exception

            # Verify connection is no longer in a transaction
            assert sconn.in_transaction is False

            # Verify auto-commit is re-enabled after transaction
            info = diagnose_connection(sconn)
            assert info['auto_commit'] is True, 'Auto-commit should be re-enabled after exception'

            # Verify no data exists
            count = db.select_scalar(sconn, f'SELECT COUNT(*) FROM {test_table_name}')
            assert count == 0, 'Transaction should rollback data on exception'
        finally:
            # Clean up the test table
            db.execute(sconn, f"IF OBJECT_ID('{test_table_name}', 'U') IS NOT NULL DROP TABLE {test_table_name}")


if __name__ == '__main__':
    __import__('pytest').main([__file__])
