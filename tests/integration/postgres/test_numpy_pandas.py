"""NumPy and pandas parameter values bound against PostgreSQL.
"""
import datetime
import math

import database as db
import numpy as np
import pandas as pd
from database.options import iterdict_data_loader, pandas_numpy_data_loader


def _insert_and_select_numpy_row(cn, table_name):
    """Store one row of NumPy and pandas values in a temp table; read it back.
    """
    insert_params = [
        np.int64(42),
        np.float64(math.pi),
        list(np.array([1, 2, 3, 4, 5])),
        pd.Series([1, 2, 3, None], dtype='Int64')[0],
        pd.NA,
        ]
    with db.transaction(cn) as tx:
        tx.execute(f'drop table if exists {table_name}')
        tx.execute(f"""
create temporary table {table_name} (
    int_col integer,
    float_col float,
    array_col integer[],
    nullable_col integer,
    null_col integer
)
""")
        tx.execute(
            f'insert into {table_name} values (%s, %s, %s, %s, %s)', *insert_params)
        return tx.select(f'select * from {table_name}')


def test_numpy_pandas_types_pandas_loader(psql_docker, pg_conn):
    """Verify NumPy and pandas values bind and read back through a DataFrame.

    Mutation: TypeConverter passing pd.NA to the driver unconverted.
    Oracle: the hand-chosen inputs 42, pi, 1, and NA.
    """
    pg_conn.options.data_loader = pandas_numpy_data_loader

    result = _insert_and_select_numpy_row(pg_conn, 'np_pd_test')

    assert len(result) == 1
    assert isinstance(result['int_col'].iloc[0], int | np.int64)
    assert result['int_col'].iloc[0] == 42
    assert isinstance(result['float_col'].iloc[0], float | np.float64)
    assert abs(result['float_col'].iloc[0] - math.pi) < 0.00001
    assert result['nullable_col'].iloc[0] == 1
    assert pd.isna(result['null_col'].iloc[0])


def test_numpy_pandas_types_iterdict_loader(psql_docker, pg_conn):
    """Verify NumPy and pandas values bind and read back as builtin types.

    Mutation: TypeConverter passing pd.NA to the driver unconverted.
    Oracle: the hand-chosen inputs 42, pi, [1..5], 1, and NA as None.
    """
    pg_conn.options.data_loader = iterdict_data_loader

    result = _insert_and_select_numpy_row(pg_conn, 'np_pd_test_iterdict')

    assert len(result) == 1
    row = result[0]
    assert type(row['int_col']) is int
    assert row['int_col'] == 42
    assert type(row['float_col']) is float
    assert abs(row['float_col'] - math.pi) < 0.00001
    assert row['array_col'] == [1, 2, 3, 4, 5]
    assert row['nullable_col'] == 1
    assert row['null_col'] is None


def test_datetime_values_keep_microseconds(psql_docker, pg_conn):
    """Verify pd.Timestamp and datetime64 values store with microseconds.

    Mutation: a datetime64[s] rescale in TypeConverter.
    Oracle: the hand-chosen 03:04:05.123456 read back field for field.
    """
    stamp = datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)
    db.execute(pg_conn, 'drop table if exists stamps')
    db.execute(pg_conn, 'create table stamps (id integer, ts timestamp)')
    rows = [{'id': 1, 'ts': np.datetime64(stamp)}, {'id': 2, 'ts': pd.Timestamp(stamp)}]

    db.insert_rows(pg_conn, 'stamps', rows)
    db.execute(pg_conn, 'insert into stamps values (%s, %s)', 3, np.datetime64(stamp))

    assert db.select_column(pg_conn, 'select ts from stamps order by id') == [stamp] * 3


if __name__ == '__main__':
    __import__('pytest').main([__file__])
