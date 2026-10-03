"""Type round trips and null binding against a live PostgreSQL server.
"""
import datetime

import database as db
import numpy as np
import pandas as pd
from database.cursor import get_dict_cursor

NULL_BINDING_VALUES = [
    ('Python float NaN', float('nan')),
    ('NumPy float32 NaN', np.float32('nan')),
    ('NumPy float64 NaN', np.float64('nan')),
    ('NumPy datetime64 NaT', np.datetime64('NaT')),
    ('Pandas NaT', pd.NaT),
    ('Python None', None),
    ('Pandas NA', pd.NA),
    ('Empty string', ''),
    ('Regular integer', 42),
    ]
EXPECTED_INT_COL = {
    label: 42 if label == 'Regular integer' else None
    for label, _ in NULL_BINDING_VALUES
    }


def test_postgres_type_consistency(psql_docker, pg_conn, value_dict):
    """Verify each column type comes back as its Python type and value.

    Mutation: numeric read as Decimal, date as datetime, or jsonb as text.
    Oracle: value_dict, the hand-written values that were inserted.
    """
    with db.transaction(pg_conn) as tx:
        tx.execute('drop table if exists type_test')
        tx.execute("""
create table type_test (
    int_col integer,
    bigint_col bigint,
    smallint_col smallint,
    bool_true_col boolean,
    bool_false_col boolean,
    float_col float,
    decimal_col decimal(18,6),
    money_col money,
    char_col char(1),
    varchar_col varchar(100),
    text_col text,
    date_col date,
    time_col time,
    datetime_col timestamp,
    bytea_col bytea,
    null_col varchar(100),
    json_col jsonb
)
""")
        tx.execute(
            f"insert into type_test values ({', '.join(['%s'] * 17)})",
            value_dict['int_value'],
            value_dict['big_int'],
            value_dict['small_int'],
            value_dict['bool_true'],
            value_dict['bool_false'],
            value_dict['float_value'],
            value_dict['decimal_value'],
            value_dict['money_value'],
            value_dict['char_value'],
            value_dict['varchar_value'],
            value_dict['text_value'],
            value_dict['date_value'],
            value_dict['time_value'],
            value_dict['datetime_value'],
            value_dict['binary_value'],
            value_dict['null_value'],
            value_dict['json_value'])

        rows = tx.select('select * from type_test')
        assert len(rows) == 1
        row = rows[0]

        assert type(row['int_col']) is int
        assert row['int_col'] == value_dict['int_value']
        assert type(row['bigint_col']) is int
        assert row['bigint_col'] == value_dict['big_int']
        assert type(row['smallint_col']) is int
        assert row['smallint_col'] == value_dict['small_int']

        assert row['bool_true_col'] is True
        assert row['bool_false_col'] is False

        assert type(row['float_col']) is float
        assert abs(row['float_col'] - value_dict['float_value']) < 0.00001
        assert type(row['decimal_col']) is float
        assert abs(row['decimal_col'] - float(value_dict['decimal_value'])) < 0.000001

        assert row['char_col'] == value_dict['char_value']
        assert row['varchar_col'] == value_dict['varchar_value']
        assert row['text_col'] == value_dict['text_value']

        assert type(row['date_col']) is datetime.date
        assert row['date_col'] == value_dict['date_value']
        assert type(row['time_col']) is datetime.time
        assert row['time_col'] == value_dict['time_value']
        assert type(row['datetime_col']) is datetime.datetime
        assert row['datetime_col'] == value_dict['datetime_value']

        assert bytes(row['bytea_col']) == value_dict['binary_value']

        assert row['json_col'] == {'key': 'value', 'numbers': [1, 2, 3]}

        assert row['null_col'] is None

        int_scalar = tx.select_scalar('select int_col from type_test')
        assert int_scalar == value_dict['int_value']
        date_scalar = tx.select_scalar('select date_col from type_test')
        assert type(date_scalar) is datetime.date
        assert date_scalar == value_dict['date_value']


def test_postgres_nan_nat_handling(psql_docker, pg_conn):
    """Verify NaN, NaT, NA, None and '' bind as null through execute.

    Mutation: dropping the float NaN guard or the str '' arm in
        TypeConverter.convert_value.
    Oracle: hand-labeled values an integer column rejects unless null.
    """
    with db.transaction(pg_conn) as tx:
        tx.execute('drop table if exists nan_test')
        tx.execute('create table nan_test (id serial primary key, int_col integer)')

        for _, value in NULL_BINDING_VALUES:
            tx.execute('insert into nan_test (int_col) values (%s)', value)

        rows = tx.select('select * from nan_test order by id')
        assert len(rows) == len(NULL_BINDING_VALUES)
        stored = {label: row['int_col']
                  for (label, _), row in zip(NULL_BINDING_VALUES, rows)}
        assert stored == EXPECTED_INT_COL

        tx.execute('insert into nan_test (int_col) values (%s::integer)', float('nan'))
        result = tx.select_scalar('select int_col from nan_test where id = %s',
                                  len(NULL_BINDING_VALUES) + 1)
        assert result is None, 'Explicitly cast NaN should be NULL'


def test_postgres_cursor_executemany(psql_docker, pg_conn):
    """Verify executemany on a dict cursor binds the same values as null.

    Mutation: Cursor.executemany skipping TypeConverter.
    Oracle: hand-labeled values, all null but the integer 42.
    """
    db.execute(pg_conn, 'drop table if exists executemany_test')
    db.execute(pg_conn, """
create table executemany_test (
    id serial primary key,
    int_col integer,
    label varchar(100)
)
""")

    cursor = get_dict_cursor(pg_conn)
    try:
        cursor.executemany(
            'insert into executemany_test (int_col, label) values (%s, %s)',
            [(value, label) for label, value in NULL_BINDING_VALUES])
        pg_conn.commit()
    finally:
        cursor.close()

    rows = db.select(pg_conn, 'select label, int_col from executemany_test')
    assert {row['label']: row['int_col'] for row in rows} == EXPECTED_INT_COL


if __name__ == '__main__':
    __import__('pytest').main([__file__])
