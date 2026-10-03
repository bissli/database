"""Python types SQLite columns read back as, through the iterdict loader.
"""
import datetime

import database as db
import pytest
from database.options import iterdict_data_loader


@pytest.mark.sqlite
def test_sqlite_type_consistency(sl_conn, value_dict, monkeypatch):
    """Verify each declared column type reads back as its Python type.

    Mutation: dropping a date converter, parsing JSON text, or 0/1 as bool.
    Oracle: value_dict and SQLite's affinity rules for numeric and time.
    """
    monkeypatch.setattr(sl_conn.options, 'data_loader', iterdict_data_loader)

    with db.transaction(sl_conn) as tx:
        tx.execute("""
create table type_test (
    int_col integer,
    bigint_col integer,
    smallint_col integer,
    bool_true_col integer,
    bool_false_col integer,
    float_col real,
    decimal_col numeric,
    money_col numeric,
    char_col text,
    varchar_col text,
    text_col text,
    date_col date,
    time_col time,
    datetime_col datetime,
    blob_col blob,
    null_col text,
    json_col text
)
""")
        tx.execute(
            'insert into type_test values '
            '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            value_dict['int_value'],
            value_dict['big_int'],
            value_dict['small_int'],
            1,
            0,
            value_dict['float_value'],
            str(value_dict['decimal_value']),
            str(value_dict['money_value']),
            value_dict['char_value'],
            value_dict['varchar_value'],
            value_dict['text_value'],
            value_dict['date_value'].isoformat(),
            value_dict['time_value'].isoformat(),
            value_dict['datetime_value'].isoformat(),
            value_dict['binary_value'],
            value_dict['null_value'],
            value_dict['json_value'])

        row = tx.select('select * from type_test')[0]

        for column, value in (
                ('int_col', value_dict['int_value']),
                ('bigint_col', value_dict['big_int']),
                ('smallint_col', value_dict['small_int']),
                ('bool_true_col', 1),
                ('bool_false_col', 0),
                ):
            assert type(row[column]) is int
            assert row[column] == value

        assert type(row['float_col']) is float
        assert abs(row['float_col'] - value_dict['float_value']) < 0.00001

        assert type(row['decimal_col']) is float
        assert abs(row['decimal_col'] - float(value_dict['decimal_value'])) < 0.000001
        assert row['money_col'] == float(value_dict['money_value'])

        assert row['char_col'] == value_dict['char_value']
        assert row['varchar_col'] == value_dict['varchar_value']
        assert row['text_col'] == value_dict['text_value']

        assert type(row['date_col']) is datetime.date
        assert row['date_col'] == value_dict['date_value']
        assert row['time_col'] == value_dict['time_value'].isoformat()
        assert type(row['datetime_col']) is datetime.datetime
        assert row['datetime_col'] == value_dict['datetime_value']

        assert row['blob_col'] == value_dict['binary_value']
        assert row['json_col'] == value_dict['json_value']
        assert row['null_col'] is None

        scalar = tx.select_scalar('select int_col from type_test')
        assert scalar == value_dict['int_value']
