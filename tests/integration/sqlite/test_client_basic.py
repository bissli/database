"""Basic select, insert, update and delete through the SQLite client.
"""
import datetime

import database as db
import pandas as pd


def test_sqlite_select(sl_conn):
    """Verify select returns a DataFrame of every row in query order.

    Mutation: the default loader returning dicts, or dropping a column.
    Oracle: the three rows sl_conn stages, by hand.
    """
    result = db.select(sl_conn, 'select name, value from test_table order by value')

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ['name', 'value']
    assert result.to_dict('list') == {
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [10, 20, 30],
        }


def test_sqlite_insert(sl_conn):
    """Verify insert binds '?' parameters and returns the rowcount.

    Mutation: insert returning the lastrowid, or binding out of order.
    Oracle: one inserted row read back by name.
    """
    row_count = db.insert(
        sl_conn, 'insert into test_table (name, value) values (?, ?)', 'Diana', 40)

    assert row_count == 1
    result = db.select(
        sl_conn, "select name, value from test_table where name = 'Diana'")
    assert result.to_dict('list') == {'name': ['Diana'], 'value': [40]}


def test_sqlite_update(sl_conn):
    """Verify update binds '?' parameters and returns the rowcount.

    Mutation: update returning None, or binding parameters out of order.
    Oracle: the hand-chosen value read back for the named row.
    """
    row_count = db.update(
        sl_conn, 'update test_table set value = ? where name = ?', 25, 'Bob')

    assert row_count == 1
    value = db.select_scalar(sl_conn, "select value from test_table where name = 'Bob'")
    assert value == 25


def test_sqlite_delete(sl_conn):
    """Verify delete binds '?' parameters and returns the rowcount.

    Mutation: delete returning None, or ignoring the bound parameter.
    Oracle: the named row gone, counted by hand.
    """
    row_count = db.delete(sl_conn, 'delete from test_table where name = ?', 'Bob')

    assert row_count == 1
    remaining = db.select_scalar(
        sl_conn, "select count(*) from test_table where name = 'Bob'")
    assert remaining == 0


def test_sqlite_placeholder_conversion(sl_conn):
    """Verify a '%s' placeholder binds on SQLite.

    Mutation: standardize_sql leaving '%s' in place.
    Oracle: the rows above the threshold 15, by hand.
    """
    result = db.select(sl_conn, 'select name from test_table where value > %s', 15)

    assert list(result['name']) == ['Bob', 'Charlie']


def test_sqlite_file_database(sqlite_file_conn):
    """Verify a file database reads and writes through the client.

    Mutation: build_connection_url dropping the path.
    Oracle: the three staged rows, plus one inserted.
    """
    query = 'select count(*) from test_table'
    assert db.select_scalar(sqlite_file_conn, query) == 3

    db.insert(
        sqlite_file_conn,
        'insert into test_table (name, value) values (?, ?)',
        'FileBased',
        100)

    assert db.select_scalar(sqlite_file_conn, query) == 4


def test_sqlite_adapters(sl_conn):
    """Verify date and datetime columns round-trip as date and datetime.

    Mutation: dropping or swapping a converter, or losing microseconds.
    Oracle: the date and datetime values bound in.
    """
    db.execute(sl_conn, """
create table adapter_test (
    id integer primary key,
    date_val date,
    datetime_val datetime
)
""")
    today = datetime.date(2024, 2, 29)
    now = datetime.datetime(2024, 2, 29, 13, 45, 7, 123456)
    db.insert(sl_conn, 'insert into adapter_test (date_val) values (?)', today)
    db.insert(sl_conn, 'insert into adapter_test (datetime_val) values (?)', now)

    retrieved_date = db.select_scalar(
        sl_conn, 'select date_val from adapter_test where date_val is not null')
    retrieved_datetime = db.select_scalar(
        sl_conn, 'select datetime_val from adapter_test where datetime_val is not null')

    assert type(retrieved_date) is datetime.date
    assert retrieved_date == today
    assert isinstance(retrieved_datetime, datetime.datetime)
    assert retrieved_datetime == now


def test_commit_and_rollback_after_close_do_nothing():
    """Verify commit() and rollback() on a closed connection return quietly.

    Mutation: the closed guard dropped, so the DBAPI call reaches the
              released pool proxy and raises AttributeError.
    Oracle: both calls returning None after close().
    """
    cn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    cn.close()

    assert cn.commit() is None
    assert cn.rollback() is None


if __name__ == '__main__':
    __import__('pytest').main([__file__])
