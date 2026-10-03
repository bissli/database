"""Transactions on both backends; db_conn stages Alice 10, Bob 20, Charlie 30.
"""
import database as db
import pytest
from tests.integration.common.conftest import row

INSERT_SQL = 'insert into test_table (name, value) values (%s, %s)'


def test_clean_exit_commits_every_statement(db_conn):
    """Verify a clean block commits its writes and flags only its inside.

    Mutation: __exit__ skipping the commit, or leaving in_transaction set.
    Oracle: a raw rollback after the block; hand-computed final rows.
    """
    with db.transaction(db_conn) as tx:
        tx.execute(INSERT_SQL, 'Dana', 40)
        tx.execute('update test_table set value = %s where name = %s', 99, 'Alice')
        tx.execute('delete from test_table where name = %s', 'Charlie')
        assert db_conn.in_transaction is True

    assert db_conn.in_transaction is False
    db_conn.dbapi_connection.rollback()

    names = db.select_column(db_conn, 'select name from test_table order by name')
    values = db.select_column(db_conn, 'select value from test_table order by name')
    assert names == ['Alice', 'Bob', 'Dana']
    assert values == [99, 20, 40]


def test_exception_rolls_back_and_propagates(db_conn):
    """Verify an exception in the block discards its writes and escapes.

    Mutation: __exit__ committing on an exception, or auto-commit left on.
    Oracle: the three staged rows, unchanged after the failed block.
    """
    with pytest.raises(ValueError, match='boom'):
        with db.transaction(db_conn) as tx:
            tx.execute(INSERT_SQL, 'Dana', 40)
            raise ValueError('boom')

    assert db_conn.in_transaction is False
    assert db.select_scalar(db_conn, 'select count(*) from test_table') == 3


def test_select_helpers_see_the_blocks_own_writes(db_conn):
    """Verify each select helper reads the block's uncommitted writes.

    Mutation: a select helper running outside the block's transaction.
    Oracle: a row inserted earlier in the same block.
    """
    with db.transaction(db_conn) as tx:
        tx.execute(INSERT_SQL, 'Dana', 40)

        result = tx.select('select name, value from test_table where name = %s', 'Dana')
        assert len(result) == 1
        assert row(result, 0)['value'] == 40
        assert tx.select_column(
            'select name from test_table where value > %s', 30) == ['Dana']
        assert tx.select_row("select * from test_table where name = 'Dana'").value == 40
        assert tx.select_row_or_none(
            'select * from test_table where name = %s', 'Dana').value == 40
        assert tx.select_scalar('select count(*) from test_table') == 4


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
