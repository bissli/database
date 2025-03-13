"""
Unit tests for insert row chunking functionality.
"""
from unittest.mock import MagicMock, patch

from database.operations.data import _insert_rows_chunked


class TestInsertChunking:
    """Test the chunking mechanism for large batch inserts"""

    def test_insert_below_limit(self):
        """Test that rows below the parameter limit are inserted in one batch"""
        mock_cn = MagicMock()

        # Create test data with small number of rows
        rows = [
            {'col1': 'val1', 'col2': 123},
            {'col1': 'val2', 'col2': 456}
        ]

        # Mock the insert function
        with patch('database.operations.data.insert') as mock_insert:
            mock_insert.return_value = 2  # Rows affected

            # Call the function
            result = _insert_rows_chunked(mock_cn, 'test_table', rows, 'postgresql')

            # Verify insert was called exactly once
            assert mock_insert.call_count == 1
            assert result == 2

    def test_insert_above_limit(self):
        """Test that rows above the parameter limit are chunked properly"""
        mock_cn = MagicMock()

        # Create a large number of rows that would exceed parameter limits
        # With 10 columns and param_limit of 50, we can only do 5 rows per batch
        rows = []
        for i in range(12):  # 12 rows with 10 columns = 120 parameters
            row = {f'col{j}': f'val{i}_{j}' for j in range(10)}
            rows.append(row)

        # Mock the get_param_limit_for_db function to return a small limit
        with patch('database.utils.sql.get_param_limit_for_db') as mock_get_limit:
            # Set a small parameter limit to force chunking
            # With 10 columns and param_limit of 50, max_rows_per_batch will be 5
            mock_get_limit.return_value = 50
            
            # Patch the insert function to track calls and return values
            with patch('database.operations.data.insert') as mock_insert:
                # With 12 rows and max 5 rows per batch (50 params / 10 columns),
                # we'll have 3 chunks of 5, 5, and 2 rows
                # Return different values for each chunk
                mock_insert.side_effect = [5, 5, 2]

                # Call the function (no need to pass param_limit anymore)
                result = _insert_rows_chunked(mock_cn, 'test_table', rows, 'postgresql')

                # The function should have called insert 3 times with our mock
                assert mock_insert.call_count == 3
                assert result == 12  # Total rows affected (5+5+2)
                
                # Verify the get_param_limit_for_db was called with the correct DB type
                mock_get_limit.assert_called_once_with('postgresql')

    def test_different_db_limits(self):
        """Test that different database types use appropriate parameter limits"""
        mock_cn = MagicMock()

        # Create a row with 10 columns
        row = {'col1': 'val1', 'col2': 'val2', 'col3': 'val3', 'col4': 'val4', 'col5': 'val5',
               'col6': 'val6', 'col7': 'val7', 'col8': 'val8', 'col9': 'val9', 'col10': 'val10'}

        # Test with different database types
        db_types = ['postgresql', 'sqlite', 'mssql', 'unknown']

        for db_type in db_types:
            # Call the function with a single row (well below any limit)
            with patch('database.operations.data.insert') as mock_insert:
                mock_insert.return_value = 1

                result = _insert_rows_chunked(mock_cn, 'test_table', [row], db_type)

                # Verify insert was called exactly once
                assert mock_insert.call_count == 1
                assert result == 1

                # Check that appropriate quoting was used based on db_type
                args, _ = mock_insert.call_args
                sql = args[1]

                # Verify db_type was passed correctly to quote_identifier
                if db_type == 'postgresql':
                    assert 'insert into ' in sql.lower()
                elif db_type == 'sqlite':
                    assert 'insert into ' in sql.lower()
                elif db_type == 'mssql':
                    assert 'insert into ' in sql.lower()
                else:
                    assert 'insert into ' in sql.lower()


if __name__ == '__main__':
    __import__('pytest').main([__file__])
