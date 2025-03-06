from unittest.mock import patch, MagicMock

import database as db
import pytest
from database.strategy import SQLServerStrategy

# Use fixture from fixtures directory


def test_sqlserver_upsert_sql_generation(mock_sqlserver_conn):
    """Test SQL Server MERGE statement generation"""
    from database.operations.upsert import _build_upsert_sql

    sql = _build_upsert_sql(
        cn=mock_sqlserver_conn,
        table='test_table',
        columns=('id', 'name', 'value'),
        key_columns=['id'],
        update_always=['name', 'value'],
        driver='sqlserver'
    )

    # Verify SQL contains MERGE INTO and expected clauses
    assert 'MERGE INTO [test_table]' in sql
    assert 'WHEN MATCHED THEN UPDATE SET' in sql
    assert 'WHEN NOT MATCHED THEN INSERT' in sql
    assert '[name]' in sql
    assert '[value]' in sql


def test_sqlserver_upsert_with_null_preservation(mock_sqlserver_conn):
    """Test SQL Server upsert with NULL preservation"""
    # We need to patch both _get_driver_type and _prepare_rows_for_upsert to ensure
    # our mock is called in the right path
    with patch('database.operations.upsert._get_driver_type', return_value='sqlserver'), \
         patch('database.operations.upsert._prepare_rows_for_upsert', return_value=[{'id': 1, 'name': 'Test', 'nullable_field': 'new_value'}]), \
         patch('database.operations.upsert._upsert_sqlserver_with_nulls', return_value=1) as mock_upsert:

        # Set up test data
        rows = [{'id': 1, 'name': 'Test', 'nullable_field': 'new_value'}]

        # Call upsert_rows with update_cols_ifnull
        result = db.upsert_rows(
            mock_sqlserver_conn,
            'test_table',
            rows,
            update_cols_key=['id'],
            update_cols_ifnull=['nullable_field']
        )

        # Verify the special SQL Server NULL-preserving function was called
        mock_upsert.assert_called_once()
        
        # Verify the parameters we expected were passed
        mock_upsert.assert_called_with(
            mock_sqlserver_conn, 
            'test_table', 
            [{'id': 1, 'name': 'Test', 'nullable_field': 'new_value'}], 
            ('id', 'name', 'nullable_field'), 
            ['id'], 
            [], 
            ['nullable_field']
        )


@patch('database.operations.upsert._fetch_existing_rows')
def test_sqlserver_null_preserving_logic(mock_fetch_rows, mock_sqlserver_conn):
    """Test SQL Server-specific NULL preservation logic"""
    from database.operations.upsert import _upsert_sqlserver_with_nulls

    # Mock existing rows with a non-null value
    mock_fetch_rows.return_value = {
        (1,): {'id': 1, 'name': 'Existing', 'nullable_field': 'existing_value'}
    }

    # Set up test data with a new value
    rows = [{'id': 1, 'name': 'Updated', 'nullable_field': 'new_value'}]

    # Call the function
    with patch('database.operations.upsert._build_upsert_sql') as mock_build_sql, \
    patch.object(mock_sqlserver_conn, 'execute'):

        mock_build_sql.return_value = 'MERGE SQL'
        _upsert_sqlserver_with_nulls(
            mock_sqlserver_conn,
            'test_table',
            rows,
            ('id', 'name', 'nullable_field'),
            ['id'],
            ['name'],
            ['nullable_field']
        )

        # The rows should have been modified to preserve the existing value
        assert rows[0]['nullable_field'] == 'existing_value'


def test_sqlserver_strategy():
    """Test SQL Server strategy implementation"""
    strategy = SQLServerStrategy()

    # Create a mock connection
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    # Test quote_identifier
    assert strategy.quote_identifier('table_name') == '[table_name]'
    assert strategy.quote_identifier('column]with]brackets') == '[column]]with]]brackets]'

    # Test reset_sequence - should execute DBCC CHECKIDENT
    strategy.reset_sequence(conn, 'test_table', 'id')

    # Get cursor.execute args
    args, kwargs = cursor.execute.call_args
    sql = args[0]

    # Verify DBCC CHECKIDENT was called
    assert 'DBCC CHECKIDENT' in sql


if __name__ == '__main__':
    __import__('pytest').main([__file__])
