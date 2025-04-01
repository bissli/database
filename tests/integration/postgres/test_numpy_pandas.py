"""
Integration tests for NumPy and Pandas type handling with PostgreSQL.
"""
import math

import database as db
import numpy as np
import pandas as pd
from database.options import iterdict_data_loader, pandas_numpy_data_loader


def test_numpy_pandas_types_pandas_loader(psql_docker, conn):
    """Test numpy and pandas type handling with PostgreSQL using pandas data loader"""

    conn.options.data_loader = pandas_numpy_data_loader

    np_int = np.int64(42)
    np_float = np.float64(math.pi)
    np_array = np.array([1, 2, 3, 4, 5])
    pd_series = pd.Series([1, 2, 3, None], dtype='Int64')
    pd_na = pd.NA

    table_name = 'np_pd_test'

    create_sql = f"""
    CREATE TEMPORARY TABLE {table_name} (
        int_col INTEGER,
        float_col FLOAT,
        array_col INTEGER[],
        nullable_col INTEGER,
        null_col INTEGER
    )
    """
    insert_sql = f"""
    INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s)
    """
    insert_params = [np_int, np_float, list(np_array), pd_series[0], pd_na]

    with db.transaction(conn) as tx:
        tx.execute(f'DROP TABLE IF EXISTS {table_name}')

        tx.execute(create_sql)
        tx.execute(insert_sql, *insert_params)

        result = tx.select(f'SELECT * FROM {table_name}')

        assert len(result) == 1

        # pandas DataFrame assertions
        assert isinstance(result['int_col'].iloc[0], int | np.int64)
        assert result['int_col'].iloc[0] == 42
        assert isinstance(result['float_col'].iloc[0], float | np.float64)
        assert abs(result['float_col'].iloc[0] - math.pi) < 0.00001
        assert result['nullable_col'].iloc[0] == 1
        assert pd.isna(result['null_col'].iloc[0])


def test_numpy_pandas_types_iterdict_loader(psql_docker, conn):
    """Test numpy and pandas type handling with PostgreSQL using iterdict data loader"""

    conn.options.data_loader = iterdict_data_loader

    np_int = np.int64(42)
    np_float = np.float64(math.pi)
    np_array = np.array([1, 2, 3, 4, 5])
    pd_series = pd.Series([1, 2, 3, None], dtype='Int64')
    pd_na = pd.NA

    table_name = 'np_pd_test_iterdict'

    create_sql = f"""
    CREATE TEMPORARY TABLE {table_name} (
        int_col INTEGER,
        float_col FLOAT,
        array_col INTEGER[],
        nullable_col INTEGER,
        null_col INTEGER
    )
    """
    insert_sql = f"""
    INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s)
    """
    insert_params = [np_int, np_float, list(np_array), pd_series[0], pd_na]

    with db.transaction(conn) as tx:
        tx.execute(f'DROP TABLE IF EXISTS {table_name}')

        tx.execute(create_sql)
        tx.execute(insert_sql, *insert_params)

        result = tx.select(f'SELECT * FROM {table_name}')

        assert len(result) == 1

        # iterdict_data_loader assertions
        row = result[0]
        assert isinstance(row['int_col'], int)
        assert row['int_col'] == 42
        assert isinstance(row['float_col'], float)
        assert abs(row['float_col'] - math.pi) < 0.00001
        assert row['nullable_col'] == 1
        assert row['null_col'] is None


if __name__ == '__main__':
    __import__('pytest').main([__file__])
