"""Auto-commit and Transaction commits as a reopened SQLite file sees them.
"""
import database as db
import pytest
from database.transaction import diagnose_connection


def rows_after_reopen(conn, table):
    """Close conn, reopen its file, and read table's data column by rowid.
    """
    path = conn.options.database
    conn.close()
    reopened = db.connect({'drivername': 'sqlite', 'database': path})
    try:
        return db.select_column(reopened, f'select data from {table} order by rowid')
    finally:
        reopened.close()


def create_scratch_table(conn, name):
    """Create an empty table with an integer id and a data column.
    """
    db.execute(conn, f'create table {name} (id integer primary key, data text)')


class TestSQLiteAutoCommit:
    """Commit visibility across reopened connections on a SQLite file."""

    def test_statement_outside_a_transaction_commits(
            self, sqlite_file_conn, test_table_prefix):
        """Verify a bare execute commits before returning.

        Mutation: the strategy leaving the driver's auto-commit off.
        Oracle: a fresh connection to the file, which sees only committed rows.
        """
        table = f'{test_table_prefix}_persist'
        create_scratch_table(sqlite_file_conn, table)
        db.execute(sqlite_file_conn, f"insert into {table} (data) values ('test1')")

        assert rows_after_reopen(sqlite_file_conn, table) == ['test1']

    def test_transaction_turns_auto_commit_off_then_back_on(
            self, sqlite_file_conn, test_table_prefix):
        """Verify auto-commit is off inside a block and on after it commits.

        Mutation: auto-commit left on in the block, or left off after it.
        Oracle: sqlite3's isolation_level, and a fresh connection's rows.
        """
        table = f'{test_table_prefix}_commit'
        create_scratch_table(sqlite_file_conn, table)

        with db.transaction(sqlite_file_conn) as tx:
            tx.execute(f"insert into {table} (data) values ('test_tx')")
            assert sqlite_file_conn.in_transaction is True
            assert diagnose_connection(sqlite_file_conn)['auto_commit'] is False

        assert sqlite_file_conn.in_transaction is False
        assert diagnose_connection(sqlite_file_conn)['auto_commit'] is True
        assert rows_after_reopen(sqlite_file_conn, table) == ['test_tx']

    def test_exception_rolls_back_and_restores_auto_commit(
            self, sl_conn, test_table_prefix):
        """Verify a failed block leaves no row and auto-commit on.

        Mutation: __exit__ committing on an exception, or skipping the restore.
        Oracle: an empty table, and sqlite3's isolation_level after the block.
        """
        table = f'{test_table_prefix}_rollback'
        create_scratch_table(sl_conn, table)

        with pytest.raises(ValueError, match='rollback'):
            with db.transaction(sl_conn) as tx:
                tx.execute(f"insert into {table} (data) values ('should_rollback')")
                raise ValueError('rollback')

        assert sl_conn.in_transaction is False
        assert diagnose_connection(sl_conn)['auto_commit'] is True
        assert db.select_scalar(sl_conn, f'select count(*) from {table}') == 0


if __name__ == '__main__':
    __import__('pytest').main([__file__])
