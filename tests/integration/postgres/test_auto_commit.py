"""
Integration tests for auto-commit functionality with PostgreSQL.

Note: Common auto-commit tests are in tests/integration/common/test_auto_commit.py
This file contains only PostgreSQL-specific tests (e.g., connection reopening).
"""
import logging

import database as db
import pytest
from database.transaction import diagnose_connection

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures('psql_docker', 'pg_conn')
class TestPostgresAutoCommit:
    """Test auto-commit functionality with real PostgreSQL connections."""

    def test_changes_persist_with_auto_commit(self, pg_conn, test_table_prefix):
        """Test that changes persist without explicit commit when auto-commit is enabled."""
        test_table_name = f'{test_table_prefix}_persist'
        db.execute(pg_conn, f"""
        CREATE TABLE {test_table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        )
        """)

        try:
            # Insert data without explicit commit
            db.execute(pg_conn, f"INSERT INTO {test_table_name} (data) VALUES ('test1')")

            # Create a new connection to verify the data was committed
            new_conn = db.connect(**pg_conn.options.__dict__)
            try:
                count = db.select_scalar(new_conn, f'SELECT COUNT(*) FROM {test_table_name}')
                assert count == 1, 'Data should be committed automatically'

                data = db.select_scalar(new_conn,
                                        f'SELECT data FROM {test_table_name} WHERE id = 1')
                assert data == 'test1', 'Correct data should be committed'
            finally:
                new_conn.close()
        finally:
            db.execute(pg_conn, f'DROP TABLE IF EXISTS {test_table_name}')

    def test_transaction_commits_on_exit(self, pg_conn, test_table_prefix):
        """Test that Transaction commits changes when exiting normally."""
        test_table_name = f'{test_table_prefix}_commit'
        db.execute(pg_conn, f"""
        CREATE TABLE {test_table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        )
        """)

        try:
            with db.transaction(pg_conn) as tx:
                tx.execute(f"INSERT INTO {test_table_name} (data) VALUES ('test_tx')")

                assert pg_conn.in_transaction is True

                info = diagnose_connection(pg_conn)
                assert info['auto_commit'] is False, 'Auto-commit should be disabled in transaction'

            assert pg_conn.in_transaction is False

            info = diagnose_connection(pg_conn)
            assert info['auto_commit'] is True, 'Auto-commit should be re-enabled after transaction'

            new_conn = db.connect(**pg_conn.options.__dict__)
            try:
                count = db.select_scalar(new_conn, f'SELECT COUNT(*) FROM {test_table_name}')
                assert count == 1, 'Transaction should commit data on exit'

                data = db.select_scalar(new_conn,
                                        f'SELECT data FROM {test_table_name} WHERE id = 1')
                assert data == 'test_tx', 'Correct data should be committed'
            finally:
                new_conn.close()
        finally:
            db.execute(pg_conn, f'DROP TABLE IF EXISTS {test_table_name}')

    def test_transaction_rollback_on_exception(self, pg_conn, test_table_prefix):
        """Test that Transaction rolls back changes on exception."""
        test_table_name = f'{test_table_prefix}_rollback'
        db.execute(pg_conn, f"""
        CREATE TABLE {test_table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        )
        """)

        try:
            try:
                with db.transaction(pg_conn) as tx:
                    tx.execute(f"INSERT INTO {test_table_name} (data) VALUES ('should_rollback')")
                    raise ValueError('Test exception to trigger rollback')
            except ValueError:
                pass  # Expected exception

            assert pg_conn.in_transaction is False

            info = diagnose_connection(pg_conn)
            assert info['auto_commit'] is True, 'Auto-commit should be re-enabled after exception'

            new_conn = db.connect(**pg_conn.options.__dict__)
            try:
                count = db.select_scalar(new_conn, f'SELECT COUNT(*) FROM {test_table_name}')
                assert count == 0, 'Transaction should rollback data on exception'
            finally:
                new_conn.close()
        finally:
            db.execute(pg_conn, f'DROP TABLE IF EXISTS {test_table_name}')

    def test_connection_closes_properly(self, pg_conn, test_table_prefix):
        """Test that connection close properly commits any pending changes."""
        test_table_name = f'{test_table_prefix}_close'
        db.execute(pg_conn, f"""
        CREATE TABLE {test_table_name} (
            id SERIAL PRIMARY KEY,
            data TEXT
        )
        """)

        test_conn = db.connect(**pg_conn.options.__dict__)

        try:
            db.execute(test_conn, f"INSERT INTO {test_table_name} (data) VALUES ('close_test')")
            test_conn.close()

            verify_conn = db.connect(**pg_conn.options.__dict__)
            try:
                count = db.select_scalar(verify_conn, f'SELECT COUNT(*) FROM {test_table_name}')
                assert count == 1, 'Data should be committed when connection is closed'

                data = db.select_scalar(verify_conn,
                                        f'SELECT data FROM {test_table_name} WHERE id = 1')
                assert data == 'close_test', 'Correct data should be committed'
            finally:
                verify_conn.close()
        finally:
            db.execute(pg_conn, f'DROP TABLE IF EXISTS {test_table_name}')


if __name__ == '__main__':
    __import__('pytest').main([__file__])
