"""
Database-agnostic tests for auto-commit functionality.

These tests run against both PostgreSQL and SQLite to verify
consistent auto-commit behavior across database backends.
"""
import pytest
from database.utils.auto_commit import diagnose_connection


class TestAutoCommitBasics:
    """Tests for basic auto-commit functionality."""

    def test_auto_commit_enabled_by_default(self, db_conn):
        """Test that auto-commit is enabled by default for new connections."""
        info = diagnose_connection(db_conn)

        assert info['auto_commit'] is True, 'Auto-commit should be enabled by default'
        assert info['in_transaction'] is False, 'Connection should not be in a transaction'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
