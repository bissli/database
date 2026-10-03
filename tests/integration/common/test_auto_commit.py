"""Auto-commit state of a new connection, on PostgreSQL and SQLite.
"""
import pytest
from database.transaction import diagnose_connection


def test_new_connection_auto_commits_outside_a_transaction(db_conn):
    """Verify a new connection is in auto-commit mode and in no transaction.

    Mutation: auto-commit left off at connect, or in_transaction starting True.
    Oracle: the driver's own flag, read through diagnose_connection.
    """
    info = diagnose_connection(db_conn)

    assert info['auto_commit'] is True
    assert info['in_transaction'] is False


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
