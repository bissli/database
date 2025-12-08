"""
Database-agnostic tests for SELECT operations.

These tests run against both PostgreSQL and SQLite to verify
consistent behavior across database backends.
"""
import database as db
import pytest
from tests.integration.common.conftest import col, row


class TestSelectOperations:
    """Tests for basic SELECT operations across databases."""

    def test_select_all(self, db_conn):
        """Test selecting all rows from test table."""
        result = db.select(db_conn, 'SELECT * FROM test_table ORDER BY id')
        assert len(result) == 3

    def test_select_with_where(self, db_conn):
        """Test SELECT with WHERE clause."""
        result = db.select(db_conn, "SELECT * FROM test_table WHERE name = 'Alice'")
        assert len(result) == 1
        assert row(result, 0)['name'] == 'Alice'
        assert row(result, 0)['value'] == 10

    def test_select_with_param(self, db_conn, dialect):
        """Test SELECT with parameter binding."""
        result = db.select(db_conn, 'SELECT * FROM test_table WHERE name = %s', 'Bob')
        assert len(result) == 1
        assert row(result, 0)['name'] == 'Bob'

    def test_select_column(self, db_conn):
        """Test select_column returns single column as list."""
        names = db.select_column(db_conn, 'SELECT name FROM test_table ORDER BY id')
        assert names == ['Alice', 'Bob', 'Charlie']

    def test_select_row(self, db_conn):
        """Test select_row returns single row."""
        row = db.select_row(db_conn, "SELECT * FROM test_table WHERE name = 'Alice'")
        assert row.name == 'Alice'
        assert row.value == 10

    def test_select_row_or_none_with_result(self, db_conn):
        """Test select_row_or_none with matching row."""
        row = db.select_row_or_none(db_conn, "SELECT * FROM test_table WHERE name = 'Alice'")
        assert row is not None
        assert row.name == 'Alice'

    def test_select_row_or_none_without_result(self, db_conn):
        """Test select_row_or_none with no matching row."""
        row = db.select_row_or_none(db_conn, "SELECT * FROM test_table WHERE name = 'Nonexistent'")
        assert row is None

    def test_select_scalar(self, db_conn):
        """Test select_scalar returns single value."""
        count = db.select_scalar(db_conn, 'SELECT COUNT(*) FROM test_table')
        assert count == 3

    def test_select_scalar_or_none_with_result(self, db_conn):
        """Test select_scalar_or_none with result."""
        value = db.select_scalar_or_none(db_conn, "SELECT value FROM test_table WHERE name = 'Alice'")
        assert value == 10

    def test_select_scalar_or_none_without_result(self, db_conn):
        """Test select_scalar_or_none without result."""
        value = db.select_scalar_or_none(db_conn, "SELECT value FROM test_table WHERE name = 'Nonexistent'")
        assert value is None


class TestSelectWithMultipleParams:
    """Tests for SELECT with multiple parameters."""

    def test_select_with_multiple_params(self, db_conn):
        """Test SELECT with multiple positional parameters."""
        result = db.select(
            db_conn,
            'SELECT * FROM test_table WHERE name = %s OR name = %s ORDER BY id',
            'Alice', 'Bob'
        )
        assert len(result) == 2
        assert col(result, 'name') == ['Alice', 'Bob']

    def test_select_with_in_clause(self, db_conn):
        """Test SELECT with IN clause parameter expansion."""
        names = ('Alice', 'Charlie')
        result = db.select(
            db_conn,
            'SELECT * FROM test_table WHERE name IN %s ORDER BY id',
            names
        )
        assert len(result) == 2
        assert col(result, 'name') == ['Alice', 'Charlie']


class TestEmptyResults:
    """Tests for handling empty result sets."""

    def test_select_empty_result(self, db_conn):
        """Test SELECT that returns no rows."""
        result = db.select(db_conn, "SELECT * FROM test_table WHERE name = 'Nonexistent'")
        assert len(result) == 0

    def test_select_column_empty(self, db_conn):
        """Test select_column with no rows returns empty list."""
        result = db.select_column(db_conn, "SELECT name FROM test_table WHERE name = 'Nonexistent'")
        assert result == []


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
