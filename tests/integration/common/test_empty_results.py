"""Empty-result and column-order tests run against PostgreSQL and SQLite.
"""
import database as db
import pandas as pd


def _columns(result: list[dict] | pd.DataFrame) -> list[str]:
    """Column names of a non-empty select result, in select-list order.
    """
    if hasattr(result, 'columns'):
        return list(result.columns)
    return list(result[0])


def test_empty_results_handling(db_conn):
    """Verify each select variant returns its empty form when no row matches.

    Mutation: select_column returning None, select_row_or_none raising,
        or select_scalar_or_none returning None for count(*).
    Oracle: 'where 1=0' matches nothing; count(*) over nothing is 0.
    """
    assert len(db.select(db_conn, 'select * from test_table where 1=0')) == 0
    assert db.select_column(db_conn, 'select name from test_table where 1=0') == []
    assert db.select_row_or_none(db_conn, 'select * from test_table where 1=0') is None
    count = db.select_scalar_or_none(
        db_conn, 'select count(*) from test_table where 1=0')
    assert count == 0

    with db.transaction(db_conn) as tx:
        assert len(tx.select('select * from test_table where 1=0')) == 0
        assert tx.select_column('select name from test_table where 1=0') == []


def test_select_keeps_select_list_column_order(db_conn):
    """Verify a result's columns follow the order of the select list.

    Mutation: a loader ordering columns by table definition or by name.
    Oracle: one select list in table order, one reordered.
    """
    with db.transaction(db_conn) as tx:
        tx.execute('drop table if exists empty_test')
        tx.execute("""
create table empty_test (
    id integer primary key,
    name text,
    value double precision,
    created_at text
)
""")
        assert len(tx.select('select id, name, value, created_at from empty_test')) == 0

        tx.execute("insert into empty_test values (1, 'test', 1.0, 'stamp')")
        populated = tx.select('select id, name, value, created_at from empty_test')
        assert _columns(populated) == ['id', 'name', 'value', 'created_at']

        reordered = tx.select('select created_at, name, id from empty_test')
        assert _columns(reordered) == ['created_at', 'name', 'id']

        tx.execute('drop table empty_test')


if __name__ == '__main__':
    __import__('pytest').main([__file__])
