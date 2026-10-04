"""NumPy and pandas parameter values bound on SQLite.
"""
import datetime
import json
import math

import database as db
import numpy as np
import pandas as pd
import pytest
from database.options import iterdict_data_loader

pytestmark = pytest.mark.sqlite


def _insert_and_read_numpy_pandas_row(conn, table):
    """Bind one row of NumPy and pandas values into a temp table, read it back.
    """
    with db.transaction(conn) as tx:
        tx.execute(f"""
create temporary table {table} (
    int_col integer,
    float_col real,
    array_col text,
    nullable_col integer,
    null_col integer
)
""")
        tx.execute(
            f'insert into {table} values (?, ?, ?, ?, ?)',
            np.int64(42),
            np.float64(math.pi),
            json.dumps(np.array([1, 2, 3, 4, 5]).tolist()),
            pd.Series([1, 2, 3, None], dtype='Int64')[0],
            pd.NA)
        return tx.select(f'select * from {table}')


def test_numpy_pandas_types_pandas_loader(sl_conn):
    """Verify NumPy scalars and pd.NA bind and read back through a DataFrame.

    Mutation: TypeConverter dropping its NumPy integer or pd.NA conversion.
    Oracle: the hand-chosen values 42, pi, 1 and null.
    """
    result = _insert_and_read_numpy_pandas_row(sl_conn, 'np_pd_test')

    assert len(result) == 1
    assert isinstance(result['int_col'].iloc[0], int | np.int64)
    assert result['int_col'].iloc[0] == 42
    assert isinstance(result['float_col'].iloc[0], float | np.float64)
    assert abs(result['float_col'].iloc[0] - math.pi) < 0.00001
    assert result['nullable_col'].iloc[0] == 1
    assert pd.isna(result['null_col'].iloc[0])


def test_numpy_pandas_types_iterdict_loader(sl_conn, monkeypatch):
    """Verify NumPy scalars and pd.NA read back as Python types and None.

    Mutation: TypeConverter dropping a conversion, or binding pd.NA as text.
    Oracle: the hand-chosen values 42, pi, 1 and None.
    """
    monkeypatch.setattr(sl_conn.options, 'data_loader', iterdict_data_loader)

    result = _insert_and_read_numpy_pandas_row(sl_conn, 'np_pd_test_iterdict')

    assert len(result) == 1
    row = result[0]
    assert isinstance(row['int_col'], int)
    assert row['int_col'] == 42
    assert isinstance(row['float_col'], float)
    assert abs(row['float_col'] - math.pi) < 0.00001
    assert row['nullable_col'] == 1
    assert row['null_col'] is None


def test_datetime_values_from_a_dataframe_round_trip(sl_conn):
    """Verify pd.Timestamp and datetime64 values store with microseconds.

    Mutation: dropping the pd.Timestamp branch of TypeConverter (sqlite3
        raises ProgrammingError), or a datetime64[s] rescale.
    Oracle: the hand-chosen 03:04:05.123456 read back field for field.
    """
    stamp = datetime.datetime(2026, 1, 2, 3, 4, 5, 123456)
    db.execute(sl_conn, 'create table stamps (id integer, ts timestamp)')
    rows = pd.DataFrame({'id': [1], 'ts': [pd.Timestamp(stamp)]}).to_dict('records')

    db.insert_rows(sl_conn, 'stamps', rows)
    db.execute(sl_conn, 'insert into stamps values (?, ?)', 2, pd.Timestamp(stamp))
    db.execute(sl_conn, 'insert into stamps values (?, ?)', 3, np.datetime64(stamp))

    assert db.select_column(sl_conn, 'select ts from stamps order by id') == [stamp] * 3
