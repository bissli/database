"""Auto-commit and Transaction commits, seen from a second PostgreSQL session.
"""
import database as db
import pytest
from database.transaction import diagnose_connection, disable_auto_commit


@pytest.fixture
def scratch_table(pg_conn, test_table_prefix):
    """Name of an empty table with a serial id and a data column.
    """
    name = f'{test_table_prefix}_scratch'
    db.execute(pg_conn, f'create table {name} (id serial primary key, data text)')
    yield name
    db.execute(pg_conn, f'drop table if exists {name}')


def rows_seen_by_new_session(pg_conn, table):
    """Values of table's data column, read over a fresh connection.
    """
    other = db.connect(**pg_conn.options.__dict__)
    try:
        return db.select_column(other, f'select data from {table} order by id')
    finally:
        other.close()


@pytest.mark.usefixtures('psql_docker')
class TestPostgresAutoCommit:
    """Commit visibility across sessions on a real PostgreSQL server."""

    def test_statement_outside_a_transaction_commits(self, pg_conn, scratch_table):
        """Verify a bare execute commits before returning.

        Mutation: the strategy leaving the driver's auto-commit off.
        Oracle: a second session, which sees only committed rows.
        """
        db.execute(pg_conn, f"insert into {scratch_table} (data) values ('test1')")

        assert rows_seen_by_new_session(pg_conn, scratch_table) == ['test1']

    def test_statement_commits_when_driver_auto_commit_is_off(
            self, pg_conn, scratch_table):
        """Verify execute and executemany commit with driver auto-commit off.

        Mutation: committing through sa_connection, which tracks no
            transaction for the raw cursor's statements, or no commit.
        Oracle: a second session, which sees only committed rows.
        """
        pg_conn.dbapi_connection.driver_connection.autocommit = False

        db.execute(pg_conn, f"insert into {scratch_table} (data) values ('one')")
        assert rows_seen_by_new_session(pg_conn, scratch_table) == ['one']

        pg_conn.cursor().executemany(
            f'insert into {scratch_table} (data) values (%s)', [('two',)])
        assert rows_seen_by_new_session(pg_conn, scratch_table) == ['one', 'two']

    def test_commit_publishes_uncommitted_cursor_work(self, pg_conn, scratch_table):
        """Verify cn.commit() commits a raw-cursor insert left uncommitted.

        Mutation: commit() calling only sa_connection.commit(), which
            tracks no transaction for the raw cursor's statements.
        Oracle: a second session, which sees only committed rows.
        """
        disable_auto_commit(pg_conn)
        pg_conn.cursor().execute(
            f"insert into {scratch_table} (data) values ('kept')", auto_commit=False)
        assert rows_seen_by_new_session(pg_conn, scratch_table) == []

        pg_conn.commit()

        assert rows_seen_by_new_session(pg_conn, scratch_table) == ['kept']

    def test_rollback_discards_uncommitted_cursor_work(self, pg_conn, scratch_table):
        """Verify cn.rollback() discards a raw-cursor insert left uncommitted.

        Mutation: rollback() reaching only sa_connection.rollback(),
            which tracks no transaction for the raw cursor's statements.
        Oracle: the inserting session itself, which sees its own
            uncommitted row until a rollback discards it.
        """
        disable_auto_commit(pg_conn)
        pg_conn.cursor().execute(
            f"insert into {scratch_table} (data) values ('dropped')", auto_commit=False)

        pg_conn.rollback()

        assert db.select_column(pg_conn, f'select data from {scratch_table}') == []

    def test_close_commits_uncommitted_cursor_work(self, pg_conn, scratch_table):
        """Verify close() commits a raw-cursor insert outside a transaction.

        Mutation: close() committing only through sa_connection, which
            tracks no transaction for the raw cursor's statements.
        Oracle: a second session, which sees only committed rows.
        """
        other = db.connect(**pg_conn.options.__dict__)
        disable_auto_commit(other)
        other.cursor().execute(
            f"insert into {scratch_table} (data) values ('closed')", auto_commit=False)

        other.close()

        assert rows_seen_by_new_session(pg_conn, scratch_table) == ['closed']

    def test_transaction_turns_auto_commit_off_then_back_on(
            self, pg_conn, scratch_table):
        """Verify auto-commit is off inside a block and on after it commits.

        Mutation: auto-commit left on in the block, or left off after it.
        Oracle: psycopg's autocommit flag, and a second session's rows.
        """
        with db.transaction(pg_conn) as tx:
            tx.execute(f"insert into {scratch_table} (data) values ('test_tx')")
            assert pg_conn.in_transaction is True
            assert diagnose_connection(pg_conn)['auto_commit'] is False

        assert pg_conn.in_transaction is False
        assert diagnose_connection(pg_conn)['auto_commit'] is True
        assert rows_seen_by_new_session(pg_conn, scratch_table) == ['test_tx']

    def test_exception_rolls_back_and_restores_auto_commit(
            self, pg_conn, scratch_table):
        """Verify a failed block leaves nothing committed and auto-commit on.

        Mutation: __exit__ committing on an exception, or skipping the restore.
        Oracle: no rows in a second session, and psycopg's autocommit flag.
        """
        with pytest.raises(ValueError, match='rollback'):
            with db.transaction(pg_conn) as tx:
                tx.execute(
                    f"insert into {scratch_table} (data) values ('should_rollback')")
                raise ValueError('rollback')

        assert pg_conn.in_transaction is False
        assert diagnose_connection(pg_conn)['auto_commit'] is True
        assert rows_seen_by_new_session(pg_conn, scratch_table) == []


if __name__ == '__main__':
    __import__('pytest').main([__file__])
