"""
Database-agnostic tests for transaction operations.

These tests run against both PostgreSQL and SQLite to verify
consistent transaction behavior across database backends.
"""
import database as db
import pytest
from tests.integration.common.conftest import row


class TestTransactionBasics:
    """Tests for basic transaction functionality."""

    def test_transaction_commits_on_normal_exit(self, db_conn, dialect):
        """Test that transaction commits when exiting normally."""
        # Create a test table for this test
        table_name = 'tx_test_commit'
        if dialect == 'postgresql':
            db.execute(db_conn, f'DROP TABLE IF EXISTS {table_name}')
            db.execute(db_conn, f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    data TEXT
                )
            """)
        else:
            db.execute(db_conn, f'DROP TABLE IF EXISTS {table_name}')
            db.execute(db_conn, f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    data TEXT
                )
            """)

        try:
            # Use transaction to insert data
            with db.transaction(db_conn) as tx:
                tx.execute(f"INSERT INTO {table_name} (data) VALUES ('committed')")
                assert db_conn.in_transaction is True

            # Verify connection is no longer in transaction
            assert db_conn.in_transaction is False

            # Verify data was committed
            count = db.select_scalar(db_conn, f'SELECT COUNT(*) FROM {table_name}')
            assert count == 1
        finally:
            db.execute(db_conn, f'DROP TABLE IF EXISTS {table_name}')

    def test_transaction_rollback_on_exception(self, db_conn, dialect):
        """Test that transaction rolls back on exception."""
        table_name = 'tx_test_rollback'
        if dialect == 'postgresql':
            db.execute(db_conn, f'DROP TABLE IF EXISTS {table_name}')
            db.execute(db_conn, f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    data TEXT
                )
            """)
        else:
            db.execute(db_conn, f'DROP TABLE IF EXISTS {table_name}')
            db.execute(db_conn, f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    data TEXT
                )
            """)

        try:
            # Transaction that raises an exception
            with pytest.raises(ValueError):
                with db.transaction(db_conn) as tx:
                    tx.execute(f"INSERT INTO {table_name} (data) VALUES ('should_rollback')")
                    raise ValueError('Test rollback')

            # Verify no data was committed
            count = db.select_scalar(db_conn, f'SELECT COUNT(*) FROM {table_name}')
            assert count == 0
        finally:
            db.execute(db_conn, f'DROP TABLE IF EXISTS {table_name}')


class TestTransactionSelect:
    """Tests for SELECT operations within transactions."""

    def test_transaction_select(self, db_conn):
        """Test SELECT within a transaction."""
        with db.transaction(db_conn) as tx:
            result = tx.select('SELECT * FROM test_table ORDER BY id')
            assert len(result) == 3

    def test_transaction_select_with_params(self, db_conn):
        """Test SELECT with parameters within a transaction."""
        with db.transaction(db_conn) as tx:
            result = tx.select('SELECT * FROM test_table WHERE name = %s', 'Alice')
            assert len(result) == 1
            assert row(result, 0)['name'] == 'Alice'

    def test_transaction_select_row(self, db_conn):
        """Test select_row within a transaction."""
        with db.transaction(db_conn) as tx:
            row = tx.select_row("SELECT * FROM test_table WHERE name = 'Bob'")
            assert row.name == 'Bob'
            assert row.value == 20

    def test_transaction_select_scalar(self, db_conn):
        """Test select_scalar within a transaction."""
        with db.transaction(db_conn) as tx:
            count = tx.select_scalar('SELECT COUNT(*) FROM test_table')
            assert count == 3


class TestTransactionModifications:
    """Tests for data modification within transactions."""

    def test_transaction_insert(self, db_conn):
        """Test INSERT within a transaction."""
        initial_count = db.select_scalar(db_conn, 'SELECT COUNT(*) FROM test_table')

        with db.transaction(db_conn) as tx:
            tx.execute("INSERT INTO test_table (name, value) VALUES ('TxTest', 100)")

        final_count = db.select_scalar(db_conn, 'SELECT COUNT(*) FROM test_table')
        assert final_count == initial_count + 1

        # Verify the inserted row
        row = db.select_row(db_conn, "SELECT * FROM test_table WHERE name = 'TxTest'")
        assert row.value == 100

    def test_transaction_update(self, db_conn):
        """Test UPDATE within a transaction."""
        with db.transaction(db_conn) as tx:
            tx.execute("UPDATE test_table SET value = 999 WHERE name = 'Alice'")

        # Verify update
        value = db.select_scalar(db_conn, "SELECT value FROM test_table WHERE name = 'Alice'")
        assert value == 999

    def test_transaction_delete(self, db_conn):
        """Test DELETE within a transaction."""
        initial_count = db.select_scalar(db_conn, 'SELECT COUNT(*) FROM test_table')

        with db.transaction(db_conn) as tx:
            tx.execute("DELETE FROM test_table WHERE name = 'Charlie'")

        final_count = db.select_scalar(db_conn, 'SELECT COUNT(*) FROM test_table')
        assert final_count == initial_count - 1

    def test_transaction_multiple_operations(self, db_conn, dialect):
        """Test multiple operations in a single transaction."""
        table_name = 'tx_multi_ops'
        if dialect == 'postgresql':
            db.execute(db_conn, f'DROP TABLE IF EXISTS {table_name}')
            db.execute(db_conn, f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    data TEXT
                )
            """)
        else:
            db.execute(db_conn, f'DROP TABLE IF EXISTS {table_name}')
            db.execute(db_conn, f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    data TEXT
                )
            """)

        try:
            with db.transaction(db_conn) as tx:
                tx.execute(f"INSERT INTO {table_name} (data) VALUES ('first')")
                tx.execute(f"INSERT INTO {table_name} (data) VALUES ('second')")
                tx.execute(f"UPDATE {table_name} SET data = 'updated' WHERE data = 'first'")

            # Verify all operations committed
            count = db.select_scalar(db_conn, f'SELECT COUNT(*) FROM {table_name}')
            assert count == 2

            updated = db.select_scalar(db_conn, f"SELECT COUNT(*) FROM {table_name} WHERE data = 'updated'")
            assert updated == 1
        finally:
            db.execute(db_conn, f'DROP TABLE IF EXISTS {table_name}')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
