"""Unit tests for parameter handling in database queries."""

import datetime
from unittest.mock import patch

import pytest
from database.utils.sql import handle_in_clause_params
from database.utils.sql import handle_null_is_operators
from database.utils.sql import process_query_parameters


@pytest.fixture
def mock_postgres_connection(mocker):
    """Create a mock PostgreSQL connection for parameter testing."""
    mock_conn = mocker.Mock()

    def mock_get_dialect_name(conn):
        return 'postgresql' if conn is mock_conn else None

    with patch('database.utils.connection_utils.get_dialect_name',
               side_effect=mock_get_dialect_name):
        yield mock_conn


class TestDatabaseParameterHandling:
    """Test class for database parameter handling functionality."""

    def test_basic_postgres_parameters(self, mock_postgres_connection):
        """Test basic parameter handling with PostgreSQL-style placeholders."""
        sql = """
        select
            foo
        from table
        where
            date between %s and %s
            and value > %s
        group by
            date,
            strategy
        """

        # Test with date objects and a numeric value
        test_date1 = datetime.date(2025, 1, 1)
        test_date2 = datetime.date(2025, 3, 11)
        test_value = 100
        args = (test_date1, test_date2, test_value)

        # Process parameters
        processed_sql, processed_args = process_query_parameters(mock_postgres_connection, sql, args)

        # Check SQL is unchanged (PostgreSQL uses %s placeholders)
        assert 'between %s and %s' in processed_sql
        assert 'value > %s' in processed_sql

        # Check parameters are passed through correctly
        assert processed_args == args
        assert processed_args[0] == test_date1
        assert processed_args[1] == test_date2
        assert processed_args[2] == test_value

    def test_nested_parameters(self, mock_postgres_connection):
        """Test handling of nested parameter tuples."""
        sql = """
        select
            foo
        from table
        where
            date between %s and %s
            and value is not %s
        group by
            date,
            strategy
        """

        # Test with nested tuple format
        test_date1 = datetime.date(2025, 1, 1)
        test_date2 = datetime.date(2025, 3, 11)
        args = [(test_date1, test_date2, None)]

        # Process parameters
        processed_sql, processed_args = process_query_parameters(mock_postgres_connection, sql, args)

        # Check SQL is unchanged
        assert 'between %s and %s' in processed_sql
        assert 'is not %s' in processed_sql

        # Check parameters are flattened correctly from nested tuple
        assert processed_args == (test_date1, test_date2, None)
        assert processed_args[0] == test_date1
        assert processed_args[1] == test_date2
        assert processed_args[2] is None

    def test_sqlite_placeholder_conversion(self, mocker):
        """Test conversion of placeholders for SQLite connections."""
        sql = """
        select
            foo
        from table
        where
            date between %s and %s
            and value is not %s
        group by
            date,
            strategy
        """

        # Create a mock SQLite connection
        mock_conn = mocker.Mock()

        # Configure the mock to be identified as a SQLite connection
        def mock_get_dialect_name(conn):
            return 'sqlite' if conn is mock_conn else None

        with patch('database.utils.connection_utils.get_dialect_name',
                   side_effect=mock_get_dialect_name):

            # Process parameters
            test_date1 = datetime.date(2025, 1, 1)
            test_date2 = datetime.date(2025, 3, 11)
            args = (test_date1, test_date2, None)

            processed_sql, processed_args = process_query_parameters(mock_conn, sql, args)

            # Check SQL is converted to SQLite format (? instead of %s)
            assert 'between ? and ?' in processed_sql
            assert 'is not ?' in processed_sql

            # Check parameters remain unchanged
            assert processed_args == args

    def test_in_clause_expansion(self):
        """Test expansion of IN clause parameters."""
        sql = 'select * from table where id in %s and type = %s'
        args = [(1, 2, 3), 'active']

        # Process IN clause parameters
        processed_sql, processed_args = handle_in_clause_params(sql, args)

        # Check SQL has expanded placeholders
        assert 'in (%s, %s, %s)' in processed_sql.lower() or 'in (?, ?, ?)' in processed_sql.lower()

        # Check parameters are correctly expanded
        assert processed_args == (1, 2, 3, 'active')

    def test_named_params_with_dates(self, mock_postgres_connection):
        """Test named parameters with date objects."""
        sql = """
        select
            foo
        from table
        where
            date between %(start_date)s and %(end_date)s
            and value is not %(null_value)s
        """

        # Test with named parameters
        args = {
            'start_date': datetime.date(2025, 1, 1),
            'end_date': datetime.date(2025, 3, 11),
            'null_value': None
        }

        # Process parameters
        processed_sql, processed_args = process_query_parameters(mock_postgres_connection, sql, args)

        # Check SQL is unchanged for named parameters
        assert 'between %(start_date)s and %(end_date)s' in processed_sql
        assert 'is not %(null_value)s' in processed_sql

        # Check parameter dictionary is unchanged
        assert processed_args == args
        assert processed_args['start_date'] == datetime.date(2025, 1, 1)
        assert processed_args['end_date'] == datetime.date(2025, 3, 11)
        assert processed_args['null_value'] is None

    def test_combined_in_clause_and_dates(self, mock_postgres_connection):
        """Test combination of IN clause and date parameters."""
        sql = """
        select
            foo
        from table
        where
            date between %s and %s
            and category in %s
        """

        # Test with date objects and an IN clause parameter
        test_date1 = datetime.date(2025, 1, 1)
        test_date2 = datetime.date(2025, 3, 11)
        categories = ('cat1', 'cat2', 'cat3')
        args = (test_date1, test_date2, categories)

        # Process parameters
        processed_sql, processed_args = process_query_parameters(mock_postgres_connection, sql, args)

        # Check SQL has appropriate placeholders
        assert 'between %s and %s' in processed_sql
        assert 'in (%s, %s, %s)' in processed_sql.lower()

        # Check parameters are correctly expanded
        assert processed_args == (test_date1, test_date2, 'cat1', 'cat2', 'cat3')

    def test_is_null_handling(self):
        """Test handling of NULL values with IS and IS NOT operators."""
        # Test 1: Basic IS NULL replacement
        sql = 'SELECT * FROM table WHERE value IS %s'
        args = [None]

        processed_sql, processed_args = handle_null_is_operators(sql, args)

        assert processed_sql == 'SELECT * FROM table WHERE value IS NULL'
        assert processed_args == []

        # Test 2: Basic IS NOT NULL replacement
        sql = 'SELECT * FROM table WHERE value IS NOT %s'
        args = [None]

        processed_sql, processed_args = handle_null_is_operators(sql, args)

        assert processed_sql == 'SELECT * FROM table WHERE value IS NOT NULL'
        assert processed_args == []

        # Test 3: Multiple NULL values in one query
        sql = 'SELECT * FROM table WHERE value1 IS %s AND value2 IS NOT %s'
        args = [None, None]

        processed_sql, processed_args = handle_null_is_operators(sql, args)

        assert processed_sql == 'SELECT * FROM table WHERE value1 IS NULL AND value2 IS NOT NULL'
        assert processed_args == []

        # Test 4: Mix of NULL and non-NULL values
        sql = 'SELECT * FROM table WHERE value1 IS %s AND value2 = %s AND value3 IS NOT %s'
        args = [None, 'test', None]

        processed_sql, processed_args = handle_null_is_operators(sql, args)

        assert processed_sql == 'SELECT * FROM table WHERE value1 IS NULL AND value2 = %s AND value3 IS NOT NULL'
        assert processed_args == ['test']

        # Test 5: Named parameters with NULL
        sql = 'SELECT * FROM table WHERE value IS %(param1)s AND name = %(param2)s'
        args = {'param1': None, 'param2': 'test'}

        processed_sql, processed_args = handle_null_is_operators(sql, args)

        assert processed_sql == 'SELECT * FROM table WHERE value IS NULL AND name = %(param2)s'
        assert 'param1' not in processed_args
        assert processed_args['param2'] == 'test'

        # Test 6: Case insensitivity
        sql = 'SELECT * FROM table WHERE value is %s AND other_value IS not %s'
        args = [None, None]

        processed_sql, processed_args = handle_null_is_operators(sql, args)

        assert 'IS NULL' in processed_sql.upper()
        assert 'IS NOT NULL' in processed_sql.upper()
        assert processed_args == []

        # Test 7: No parameters to replace
        sql = 'SELECT * FROM table WHERE value IS %s'
        args = ['not_null']

        processed_sql, processed_args = handle_null_is_operators(sql, args)

        # SQL should be unchanged since parameter is not None
        assert processed_sql == 'SELECT * FROM table WHERE value IS %s'
        assert processed_args == ['not_null']

        # Test 8: SQL with no IS operators
        sql = 'SELECT * FROM table WHERE value = %s'
        args = [None]

        processed_sql, processed_args = handle_null_is_operators(sql, args)

        # SQL should be unchanged since there's no IS operator
        assert processed_sql == 'SELECT * FROM table WHERE value = %s'
        assert processed_args == [None]

    def test_null_handling_with_full_parameter_processing(self, mock_postgres_connection):
        """Test handling of NULL values with full parameter processing pipeline."""

        # Test 1: IS NOT NULL with positional parameters
        sql = """
        select
            foo
        from table
        where
            date between %s and %s
            and value is not %s
        group by
            date,
            strategy
        """

        test_date1 = datetime.date(2025, 1, 1)
        test_date2 = datetime.date(2025, 3, 11)
        args = (test_date1, test_date2, None)

        processed_sql, processed_args = process_query_parameters(mock_postgres_connection, sql, args)

        # Check SQL has been correctly modified to use IS NOT NULL
        assert 'between %s and %s' in processed_sql
        assert 'IS NOT NULL' in processed_sql.upper()
        assert 'is not %s' not in processed_sql.lower()

        # Check parameters - None should be removed
        assert processed_args == (test_date1, test_date2)
        assert len(processed_args) == 2

        # Test 2: IS NULL with positional parameters
        sql_is_null = """
        select
            foo
        from table
        where
            date between %s and %s
            and value is %s
        """

        args_is_null = (test_date1, test_date2, None)

        processed_sql_is_null, processed_args_is_null = process_query_parameters(
            mock_postgres_connection, sql_is_null, args_is_null
        )

        # Check SQL has been correctly modified to use IS NULL
        assert 'between %s and %s' in processed_sql_is_null
        assert 'value IS NULL' in processed_sql_is_null.upper()
        assert 'value is %s' not in processed_sql_is_null.lower()

        # Check parameters - None should be removed
        assert processed_args_is_null == (test_date1, test_date2)
        assert len(processed_args_is_null) == 2

        # Test 3: IS NOT NULL with named parameters
        sql_named = """
        select
            foo
        from table
        where
            date between %(start_date)s and %(end_date)s
            and value is not %(null_value)s
        group by
            date,
            strategy
        """

        args_named = {
            'start_date': test_date1,
            'end_date': test_date2,
            'null_value': None
        }

        processed_sql_named, processed_args_named = process_query_parameters(
            mock_postgres_connection, sql_named, args_named
        )

        # Check SQL has been correctly modified for named parameters
        assert 'between %(start_date)s and %(end_date)s' in processed_sql_named
        assert 'IS NOT NULL' in processed_sql_named.upper()
        assert 'is not %(null_value)s' not in processed_sql_named.lower()

        # Check parameters - null_value should be removed
        assert 'start_date' in processed_args_named
        assert 'end_date' in processed_args_named
        assert 'null_value' not in processed_args_named
        assert len(processed_args_named) == 2

        # Test 4: IS NULL with named parameters
        sql_named_null = """
        select
            foo
        from table
        where
            date between %(start_date)s and %(end_date)s
            and value is %(null_value)s
        """

        args_named_null = {
            'start_date': test_date1,
            'end_date': test_date2,
            'null_value': None
        }

        processed_sql_named_null, processed_args_named_null = process_query_parameters(
            mock_postgres_connection, sql_named_null, args_named_null
        )

        # Check SQL has been correctly modified for named parameters
        assert 'between %(start_date)s and %(end_date)s' in processed_sql_named_null
        assert 'IS NULL' in processed_sql_named_null.upper()
        assert 'is %(null_value)s' not in processed_sql_named_null.lower()

        # Check parameters - null_value should be removed
        assert 'start_date' in processed_args_named_null
        assert 'end_date' in processed_args_named_null
        assert 'null_value' not in processed_args_named_null
        assert len(processed_args_named_null) == 2

    def test_error_case_is_not_null(self, mock_postgres_connection):
        """Test the specific error case with IS NOT and NULL value."""
        # This replicates the exact error case from the logs
        sql = """
        select
            foo
        from table
        where
            date between %s and %s
            and value is not %s
        """

        # Use the exact parameters from the error case
        test_date1 = datetime.date(2025, 1, 1)
        test_date2 = datetime.date(2025, 3, 11)
        args = (test_date1, test_date2, None)

        # Process parameters
        processed_sql, processed_args = process_query_parameters(mock_postgres_connection, sql, args)

        # Check SQL has been properly modified to use IS NOT NULL
        assert 'IS NOT NULL' in processed_sql.upper()
        assert 'is not %s' not in processed_sql.lower()

        # Ensure parameters are correctly processed with None removed
        assert processed_args == (test_date1, test_date2)
        assert len(processed_args) == 2


if __name__ == '__main__':
    __import__('pytest').main([__file__])
