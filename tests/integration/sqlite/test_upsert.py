"""SQLite-only upsert behavior: rowid preservation.
"""
import database as db
import pytest


@pytest.mark.sqlite
def test_upsert_rowid(sl_conn):
    """Verify an upsert update keeps its rowid and an insert gets a new one.

    Mutation: build_upsert_sql writing 'insert or replace'.
    Oracle: the rowids read before the upsert, on a table with no primary key.
    """
    db.execute(sl_conn, """
create table test_rowid_table (
    name text unique not null,
    value integer not null
)
""")
    insert_sql = 'insert into test_rowid_table (name, value) values (%s, %s)'
    rowid_sql = 'select rowid from test_rowid_table where name = %s'
    db.insert(sl_conn, insert_sql, 'First', 100)
    db.insert(sl_conn, insert_sql, 'Second', 200)
    first_rowid = db.select_scalar(sl_conn, rowid_sql, 'First')
    second_rowid = db.select_scalar(sl_conn, rowid_sql, 'Second')

    db.upsert_rows(sl_conn, 'test_rowid_table', [{'name': 'Third', 'value': 300}])
    db.upsert_rows(
        sl_conn,
        'test_rowid_table',
        [{'name': 'First', 'value': 150}],
        update_cols_always=['value'])

    assert db.select_scalar(sl_conn, rowid_sql, 'First') == first_rowid
    first_value = db.select_scalar(
        sl_conn, 'select value from test_rowid_table where name = %s', 'First')
    assert first_value == 150
    assert db.select_scalar(sl_conn, rowid_sql, 'Third') > second_rowid


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
