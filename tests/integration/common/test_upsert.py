"""
Database-agnostic tests for UPSERT operations.

These tests run against both PostgreSQL and SQLite to verify
consistent upsert behavior across database backends.
"""
import database as db
import pytest
from tests.integration.common.conftest import row


class TestUpsertBasic:
    """Basic upsert functionality tests."""

    def test_upsert_insert_new_rows(self, db_conn):
        """Test upsert inserts new rows correctly."""
        rows = [{'name': 'Barry', 'value': 50}, {'name': 'Wallace', 'value': 92}]
        row_count = db.upsert_rows(db_conn, 'test_table', rows, update_cols_always=['value'])
        assert row_count == 2, 'upsert should return 2 for new rows'

        result = db.select(db_conn, 'SELECT name, value FROM test_table WHERE name IN (%s, %s) ORDER BY name',
                           'Barry', 'Wallace')
        assert len(result) == 2
        assert row(result, 0)['name'] == 'Barry'
        assert row(result, 0)['value'] == 50
        assert row(result, 1)['name'] == 'Wallace'
        assert row(result, 1)['value'] == 92

    def test_upsert_update_existing_rows(self, db_conn):
        """Test upsert updates existing rows."""
        # First insert
        rows = [{'name': 'UpsertUpdate', 'value': 50}]
        db.upsert_rows(db_conn, 'test_table', rows, update_cols_always=['value'])

        # Then update
        rows = [{'name': 'UpsertUpdate', 'value': 150}]
        db.upsert_rows(db_conn, 'test_table', rows, update_cols_always=['value'])

        result = db.select_scalar(db_conn, 'SELECT value FROM test_table WHERE name = %s', 'UpsertUpdate')
        assert result == 150

    def test_upsert_empty_rows(self, db_conn):
        """Test upsert with empty rows list returns 0."""
        result = db.upsert_rows(db_conn, 'test_table', [])
        assert result == 0


class TestUpsertIfNull:
    """Tests for update_cols_ifnull option."""

    def test_upsert_ifnull_does_not_overwrite(self, db_conn):
        """Test that update_cols_ifnull doesn't overwrite existing values."""
        db.insert(db_conn, 'INSERT INTO test_table (name, value) VALUES (%s, %s)', 'UpsertNull', 100)

        rows = [{'name': 'UpsertNull', 'value': 200}]
        db.upsert_rows(db_conn, 'test_table', rows, update_cols_ifnull=['value'])

        result = db.select_scalar(db_conn, 'SELECT value FROM test_table WHERE name = %s', 'UpsertNull')
        assert result == 100, 'Value should not be updated when using update_cols_ifnull'

    def test_upsert_ifnull_updates_null_value(self, db_conn, dialect):
        """Test that update_cols_ifnull updates NULL values."""
        # Create nullable table
        if dialect == 'postgresql':
            db.execute(db_conn, 'DROP TABLE IF EXISTS test_nullable')
            db.execute(db_conn, """
                CREATE TABLE test_nullable (
                    name VARCHAR(50) PRIMARY KEY,
                    value INTEGER NULL
                )
            """)
        else:
            db.execute(db_conn, 'DROP TABLE IF EXISTS test_nullable')
            db.execute(db_conn, """
                CREATE TABLE test_nullable (
                    name TEXT PRIMARY KEY,
                    value INTEGER NULL
                )
            """)

        try:
            db.insert(db_conn, 'INSERT INTO test_nullable (name, value) VALUES (%s, %s)', 'UpsertNull', 100)
            db.execute(db_conn, 'UPDATE test_nullable SET value = NULL WHERE name = %s', 'UpsertNull')

            rows = [{'name': 'UpsertNull', 'value': 200}]
            db.upsert_rows(db_conn, 'test_nullable', rows, update_cols_ifnull=['value'])

            result = db.select_scalar(db_conn, 'SELECT value FROM test_nullable WHERE name = %s', 'UpsertNull')
            assert result == 200, 'Value should be updated when target is NULL'
        finally:
            db.execute(db_conn, 'DROP TABLE IF EXISTS test_nullable')


class TestUpsertMixedOperations:
    """Tests for mixed insert/update operations."""

    def test_upsert_mixed_inserts_and_updates(self, db_conn):
        """Test upsert with mix of inserts and updates."""
        rows = [
            {'name': 'Alice', 'value': 1000},  # Existing - update
            {'name': 'NewPerson1', 'value': 500},  # New - insert
            {'name': 'NewPerson2', 'value': 600}   # New - insert
        ]

        row_count = db.upsert_rows(db_conn, 'test_table', rows, update_cols_always=['value'])
        assert row_count == 3

        result = db.select(db_conn, """
            SELECT name, value FROM test_table
            WHERE name IN (%s, %s, %s)
            ORDER BY name
        """, 'Alice', 'NewPerson1', 'NewPerson2')

        assert len(result) == 3
        assert row(result, 0)['name'] == 'Alice'
        assert row(result, 0)['value'] == 1000
        assert row(result, 1)['name'] == 'NewPerson1'
        assert row(result, 1)['value'] == 500
        assert row(result, 2)['name'] == 'NewPerson2'
        assert row(result, 2)['value'] == 600


class TestUpsertInvalidColumns:
    """Tests for handling invalid columns."""

    def test_upsert_filters_invalid_columns(self, db_conn):
        """Test that invalid columns are filtered out."""
        rows = [{'name': 'InvalidTest', 'value': 100, 'nonexistent': 'should be filtered'}]
        db.upsert_rows(db_conn, 'test_table', rows)

        result = db.select_row(db_conn, 'SELECT name, value FROM test_table WHERE name = %s', 'InvalidTest')
        assert result.name == 'InvalidTest'
        assert result.value == 100


class TestUpsertNoPrimaryKey:
    """Tests for tables without primary keys."""

    def test_upsert_no_primary_keys_inserts_all(self, db_conn, dialect):
        """Test upsert on table without primary keys inserts all rows."""
        # Create table without primary key
        if dialect == 'postgresql':
            db.execute(db_conn, 'DROP TABLE IF EXISTS test_no_pk')
            db.execute(db_conn, """
                CREATE TABLE test_no_pk (
                    name VARCHAR(50) NOT NULL,
                    value INTEGER NOT NULL
                )
            """)
        else:
            db.execute(db_conn, 'DROP TABLE IF EXISTS test_no_pk')
            db.execute(db_conn, """
                CREATE TABLE test_no_pk (
                    name TEXT NOT NULL,
                    value INTEGER NOT NULL
                )
            """)

        try:
            rows = [
                {'name': 'NoPK1', 'value': 100},
                {'name': 'NoPK2', 'value': 200}
            ]

            row_count = db.upsert_rows(db_conn, 'test_no_pk', rows)
            assert row_count == 2

            # Without primary keys, upsert again should insert new rows
            rows = [
                {'name': 'NoPK1', 'value': 101},
                {'name': 'NoPK2', 'value': 201}
            ]

            row_count = db.upsert_rows(db_conn, 'test_no_pk', rows, update_cols_always=['value'])
            assert row_count == 2

            # Should have 4 rows total
            result = db.select(db_conn, 'SELECT name, value FROM test_no_pk ORDER BY name, value')
            assert len(result) == 4
        finally:
            db.execute(db_conn, 'DROP TABLE IF EXISTS test_no_pk')


class TestUpsertLargeBatch:
    """Tests for large batch upsert operations."""

    def test_upsert_large_batch(self, db_conn, dialect):
        """Test upsert with a large number of rows."""
        import time

        # Create test table
        if dialect == 'postgresql':
            db.execute(db_conn, 'DROP TABLE IF EXISTS test_large_batch')
            db.execute(db_conn, """
                CREATE TABLE test_large_batch (
                    id INTEGER PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
        else:
            db.execute(db_conn, 'DROP TABLE IF EXISTS test_large_batch')
            db.execute(db_conn, """
                CREATE TABLE test_large_batch (
                    id INTEGER PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)

        try:
            # Use same batch size for both to ensure consistency
            batch_size = 500
            rows = [{'id': i, 'value': f'value-{i}'} for i in range(1, batch_size + 1)]

            start = time.time()
            row_count = db.upsert_rows(db_conn, 'test_large_batch', rows)
            insert_time = time.time() - start

            assert row_count == batch_size

            # Verify sample values
            for id_val in [1, batch_size // 2, batch_size]:
                result = db.select_scalar(db_conn, 'SELECT value FROM test_large_batch WHERE id = %s', id_val)
                assert result == f'value-{id_val}'

            # Test update
            update_rows = [{'id': i, 'value': f'updated-{i}'} for i in range(1, batch_size + 1)]

            start = time.time()
            update_count = db.upsert_rows(db_conn, 'test_large_batch', update_rows, update_cols_always=['value'])
            update_time = time.time() - start

            assert update_count == batch_size

            # Verify updates
            for id_val in [1, batch_size // 2, batch_size]:
                result = db.select_scalar(db_conn, 'SELECT value FROM test_large_batch WHERE id = %s', id_val)
                assert result == f'updated-{id_val}'

            # Reasonable performance thresholds
            assert insert_time < 30, f'Insert too slow: {insert_time:.2f}s'
            assert update_time < 30, f'Update too slow: {update_time:.2f}s'
        finally:
            db.execute(db_conn, 'DROP TABLE IF EXISTS test_large_batch')


class TestUpsertColumnOrder:
    """Tests for column order handling."""

    def test_upsert_column_order_independence(self, db_conn):
        """Test that upsert works regardless of column order in dictionaries."""
        # Different column orders in rows
        rows = [
            {'name': 'OrderTest1', 'value': 300},  # name first
            {'value': 400, 'name': 'OrderTest2'}   # value first
        ]

        row_count = db.upsert_rows(db_conn, 'test_table', rows)
        assert row_count == 2

        result = db.select(db_conn, 'SELECT name, value FROM test_table WHERE name IN (%s, %s) ORDER BY name',
                           'OrderTest1', 'OrderTest2')

        assert len(result) == 2
        assert row(result, 0)['name'] == 'OrderTest1'
        assert row(result, 0)['value'] == 300
        assert row(result, 1)['name'] == 'OrderTest2'
        assert row(result, 1)['value'] == 400


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
