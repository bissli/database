"""
Integration tests for NumPy and Pandas type handling with SQL Server.
"""
import json
import math

import numpy as np
import pandas as pd
import pytest
import database as db
from database.options import iterdict_data_loader


@pytest.mark.sqlserver
def test_numpy_pandas_types_pandas_loader(sconn):
    """Test numpy and pandas type handling with SQL Server using pandas data loader"""
    # Skip if not connected to SQL Server
    if not db.is_pyodbc_connection(sconn):
        pytest.skip('Not connected to SQL Server')

    # Create numpy and pandas test values
    np_int = np.int64(42)
    np_float = np.float64(math.pi)
    np_array = np.array([1, 2, 3, 4, 5])
    pd_series = pd.Series([1, 2, 3, None], dtype='Int64')
    pd_na = pd.NA

    # Create a temporary table
    table_name = '#np_pd_test'  # SQL Server temp table

    # SQL Server test
    create_sql = f"""
    CREATE TABLE {table_name} (
        int_col INT,
        float_col FLOAT,
        array_col NVARCHAR(MAX),  -- Store as JSON string
        nullable_col INT,
        null_col INT
    )
    """
    insert_sql = f"""
    INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)
    """
    insert_params = [np_int, np_float, json.dumps(np_array.tolist()), pd_series[0], pd_na]

    # Execute the test
    with db.transaction(sconn) as tx:
        tx.execute(create_sql)
        tx.execute(insert_sql, *insert_params)

        # Query the data
        result = tx.select(f'SELECT * FROM {table_name}')

        # Should be a single row
        assert len(result) == 1

        # pandas DataFrame assertions
        assert isinstance(result['int_col'].iloc[0], int | np.int64)
        assert result['int_col'].iloc[0] == 42
        assert isinstance(result['float_col'].iloc[0], float | np.float64)
        assert abs(result['float_col'].iloc[0] - math.pi) < 0.00001
        assert result['nullable_col'].iloc[0] == 1
        assert pd.isna(result['null_col'].iloc[0])


@pytest.mark.sqlserver
def test_numpy_pandas_types_iterdict_loader(sconn):
    """Test numpy and pandas type handling with SQL Server using iterdict data loader"""
    # Skip if not connected to SQL Server
    if not db.is_pyodbc_connection(sconn):
        pytest.skip('Not connected to SQL Server')

    # Save original data loader
    original_loader = sconn.options.data_loader
    # Use iterdict_data_loader for this test
    sconn.options.data_loader = iterdict_data_loader

    try:
        # Create numpy and pandas test values
        np_int = np.int64(42)
        np_float = np.float64(math.pi)
        np_array = np.array([1, 2, 3, 4, 5])
        pd_series = pd.Series([1, 2, 3, None], dtype='Int64')
        pd_na = pd.NA

        # Create a temporary table
        table_name = '#np_pd_test_iterdict'  # SQL Server temp table

        # SQL Server test
        create_sql = f"""
        CREATE TABLE {table_name} (
            int_col INT,
            float_col FLOAT,
            array_col NVARCHAR(MAX),  -- Store as JSON string
            nullable_col INT,
            null_col INT
        )
        """
        insert_sql = f"""
        INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)
        """
        insert_params = [np_int, np_float, json.dumps(np_array.tolist()), pd_series[0], pd_na]

        # Execute the test
        with db.transaction(sconn) as tx:
            tx.execute(create_sql)
            tx.execute(insert_sql, *insert_params)

            # Query the data
            result = tx.select(f'SELECT * FROM {table_name}')

            # Should be a single row
            assert len(result) == 1

            # iterdict_data_loader assertions
            row = result[0]
            assert isinstance(row['int_col'], int)
            assert row['int_col'] == 42
            assert isinstance(row['float_col'], float)
            assert abs(row['float_col'] - math.pi) < 0.00001
            assert row['nullable_col'] == 1
            assert row['null_col'] is None
    finally:
        # Restore original data loader
        sconn.options.data_loader = original_loader
