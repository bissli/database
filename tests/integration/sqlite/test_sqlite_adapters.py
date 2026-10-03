"""RowAdapter and the select helpers over sqlite3.Row rows.
"""
import database as db
import pytest
from database.exceptions import ValidationError
from database.types import RowAdapter


def test_sqlite_row_adapter(sl_conn):
    """Verify RowAdapter reads a sqlite3.Row by key, by position and as attrs.

    Mutation: get_value() returning the last column, or to_dict losing names.
    Oracle: the staged row for Alice, id 1 in the first column.
    """
    cursor = sl_conn.cursor()
    cursor.execute("select * from test_table where name = 'Alice'")
    adapter = RowAdapter.create(sl_conn, cursor.fetchone())

    row_dict = adapter.to_dict()
    assert row_dict['name'] == 'Alice'
    assert row_dict['value'] == 10
    assert adapter.get_value('name') == 'Alice'
    assert adapter.get_value() == 1
    attr_dict = adapter.to_attrdict()
    assert attr_dict.name == 'Alice'
    assert attr_dict.value == 10


def test_sqlite_adapter_in_select_column(sl_conn):
    """Verify select_column returns the first column of every row.

    Mutation: select_column returning whole rows, or only the first row.
    Oracle: the staged names and values, in id order.
    """
    names = db.select_column(sl_conn, 'select name from test_table order by id')
    assert names == ['Alice', 'Bob', 'Charlie']

    name = db.select_column(sl_conn, 'select name from test_table where id = 1')
    assert name == ['Alice']

    values = db.select_column(sl_conn, 'select value from test_table order by id')
    assert values == [10, 20, 30]


def test_sqlite_adapter_in_select_scalar(sl_conn):
    """Verify select_scalar returns one value and raises on no row.

    Mutation: select_scalar returning None on no row in place of raising.
    Oracle: the staged row with id 1, and an id no row carries.
    """
    name = db.select_scalar(sl_conn, 'select name from test_table where id = 1')
    assert name == 'Alice'

    value = db.select_scalar(sl_conn, 'select value from test_table where id = 1')
    assert value == 10

    with pytest.raises(ValidationError):
        db.select_scalar(sl_conn, 'select name from test_table where id = 999')


def test_sqlite_adapter_in_select_row(sl_conn):
    """Verify select_row returns one row by attribute and raises on no row.

    Mutation: select_row returning None on no row in place of raising.
    Oracle: the staged row with id 1, and an id no row carries.
    """
    row = db.select_row(sl_conn, 'select * from test_table where id = 1')
    assert row.name == 'Alice'
    assert row.value == 10

    with pytest.raises(ValidationError):
        db.select_row(sl_conn, 'select * from test_table where id = 999')


if __name__ == '__main__':
    __import__('pytest').main([__file__])
