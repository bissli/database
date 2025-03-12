"""
Integration tests for auto-commit functionality with PostgreSQL.
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
    return f'test_autocommit_{int(time.time())}'


@pytest.mark.usefixtures('psql_docker', 'conn')
class TestPostgresAutoCommit:
    """Test auto-commit functionality with real PostgreSQL connections"""

    def test_auto_commit_enabled_by_default(self, conn):
        """Test that auto-commit is enabled by default for new connections"""
        # Get connection diagnostics
        info = diagnose_connection(conn)

        # Verify auto-commit is enabled
        assert info['auto_commit'] is True, 'Auto-commit should be enabled by default'
        assert info['in_transaction'] is False, 'Connection should not be in a transaction'

    def test_changes_persist_with_auto_commit(self, conn, test_table_prefix):
        # Create unique table name for this test
        test_table_name = f"{test_table_prefix}_persist"
        """Test that changes persist without explicit commit when auto-commit is enabled"""
        # Create a test table
        db.execute(conn, f"""
        CREATE TABLE {test_table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        )
        """)

        try:
            # Insert data without explicit commit
            db.execute(conn, f"INSERT INTO {test_table_name} (data) VALUES ('test1')")

            # Create a new connection to verify the data was committed
            new_conn = db.connect(**conn.options.__dict__)
            try:
                # Verify data exists in the new connection
                count = db.select_scalar(new_conn, f'SELECT COUNT(*) FROM {test_table_name}')
                assert count == 1, 'Data should be committed automatically'

                # Verify the content
                data = db.select_scalar(new_conn,
                                        f'SELECT data FROM {test_table_name} WHERE id = 1')
                assert data == 'test1', 'Correct data should be committed'
            finally:
                new_conn.close()
        finally:
            # Clean up the test table
            db.execute(conn, f'DROP TABLE IF EXISTS {test_table_name}')

    def test_transaction_commits_on_exit(self, conn, test_table_prefix):
        # Create unique table name for this test
        test_table_name = f"{test_table_prefix}_commit"
        """Test that Transaction commits changes when exiting normally"""
        # Create a test table
        db.execute(conn, f"""
        CREATE TABLE {test_table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        )
        """)

        try:
            # Use a transaction to insert data
            with db.transaction(conn) as tx:
                tx.execute(f"INSERT INTO {test_table_name} (data) VALUES ('test_tx')")

                # Verify connection is marked as in a transaction
                assert conn.in_transaction is True

                # Verify auto-commit is disabled during transaction
                info = diagnose_connection(conn)
                assert info['auto_commit'] is False, 'Auto-commit should be disabled in transaction'

            # Verify connection is no longer in a transaction
            assert conn.in_transaction is False

            # Verify auto-commit is re-enabled after transaction
            info = diagnose_connection(conn)
            assert info['auto_commit'] is True, 'Auto-commit should be re-enabled after transaction'

            # Create a new connection to verify the data was committed
            new_conn = db.connect(**conn.options.__dict__)
            try:
                # Verify data exists in the new connection
                count = db.select_scalar(new_conn, f'SELECT COUNT(*) FROM {test_table_name}')
                assert count == 1, 'Transaction should commit data on exit'

                # Verify the content
                data = db.select_scalar(new_conn,
                                        f'SELECT data FROM {test_table_name} WHERE id = 1')
                assert data == 'test_tx', 'Correct data should be committed'
            finally:
                new_conn.close()
        finally:
            # Clean up the test table
            db.execute(conn, f'DROP TABLE IF EXISTS {test_table_name}')

    def test_transaction_rollback_on_exception(self, conn, test_table_prefix):
        # Create unique table name for this test
        test_table_name = f"{test_table_prefix}_rollback"
        """Test that Transaction rolls back changes on exception"""
        # Create a test table
        db.execute(conn, f"""
        CREATE TABLE {test_table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        )
        """)

        try:
            # Use a transaction that raises an exception
            try:
                with db.transaction(conn) as tx:
                    tx.execute(f"INSERT INTO {test_table_name} (data) VALUES ('should_rollback')")
                    # Force an error
                    raise ValueError('Test exception to trigger rollback')
            except ValueError:
                pass  # Expected exception

            # Verify connection is no longer in a transaction
            assert conn.in_transaction is False

            # Verify auto-commit is re-enabled after transaction
            info = diagnose_connection(conn)
            assert info['auto_commit'] is True, 'Auto-commit should be re-enabled after exception'

            # Create a new connection to verify the data was NOT committed
            new_conn = db.connect(**conn.options.__dict__)
            try:
                # Verify no data exists
                count = db.select_scalar(new_conn, f'SELECT COUNT(*) FROM {test_table_name}')
                assert count == 0, 'Transaction should rollback data on exception'
            finally:
                new_conn.close()
        finally:
            # Clean up the test table
            db.execute(conn, f'DROP TABLE IF EXISTS {test_table_name}')

    def test_connection_closes_properly(self, conn, test_table_prefix):
        # Create unique table name for this test
        test_table_name = f"{test_table_prefix}_close"
        """Test that connection close properly commits any pending changes"""
        # Create a test table
        db.execute(conn, f"""
        CREATE TABLE {test_table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        )
        """)

        # Create a new connection for this specific test
        test_conn = db.connect(**conn.options.__dict__)

        try:
            # Insert data
            db.execute(test_conn, f"INSERT INTO {test_table_name} (data) VALUES ('close_test')")

            # Close the connection explicitly
            test_conn.close()

            # Create another new connection to verify the data was committed
            verify_conn = db.connect(**conn.options.__dict__)
            try:
                # Verify data exists in the new connection
                count = db.select_scalar(verify_conn, f'SELECT COUNT(*) FROM {test_table_name}')
                assert count == 1, 'Data should be committed when connection is closed'

                # Verify the content
                data = db.select_scalar(verify_conn,
                                        f'SELECT data FROM {test_table_name} WHERE id = 1')
                assert data == 'close_test', 'Correct data should be committed'
            finally:
                verify_conn.close()
        finally:
            # Clean up the test table
            db.execute(conn, f'DROP TABLE IF EXISTS {test_table_name}')


if __name__ == '__main__':
    __import__('pytest').main([__file__])
