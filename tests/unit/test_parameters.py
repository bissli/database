"""Unit tests for parameter handling in database queries."""

import datetime

import pytest
from database.utils.sql import _generate_named_placeholders
from database.utils.sql import _unwrap_nested_parameters
from database.utils.sql import escape_percent_signs_in_literals
from database.utils.sql import handle_in_clause_params
from database.utils.sql import handle_null_is_operators, has_placeholders
from database.utils.sql import process_query_parameters, quote_identifier


class TestDatabaseParameterHandling:
    """Test class for database parameter handling functionality."""

    def test_basic_postgres_parameters(self, create_simple_mock_connection):
        """Test basic parameter handling with PostgreSQL-style placeholders."""
        mock_postgres_connection = create_simple_mock_connection('postgresql')
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

    def test_nested_parameters(self, create_simple_mock_connection):
        """Test handling of nested parameter tuples."""
        mock_postgres_connection = create_simple_mock_connection('postgresql')
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

        # Check SQL is unchanged for date parameters but NULL is handled
        assert 'between %s and %s' in processed_sql
        assert 'IS NOT NULL' in processed_sql.upper()
        assert 'is not %s' not in processed_sql.lower()

        # Check parameters are flattened correctly from nested tuple with None removed
        assert processed_args == (test_date1, test_date2)
        assert processed_args[0] == test_date1
        assert processed_args[1] == test_date2
        assert len(processed_args) == 2

    def test_sqlite_placeholder_conversion(self, create_simple_mock_connection):
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

        mock_conn = create_simple_mock_connection('sqlite')

        # Process parameters
        test_date1 = datetime.date(2025, 1, 1)
        test_date2 = datetime.date(2025, 3, 11)
        args = (test_date1, test_date2, None)

        processed_sql, processed_args = process_query_parameters(mock_conn, sql, args)

        # Check SQL is converted to SQLite format (? instead of %s) and NULL is handled
        assert 'between ? and ?' in processed_sql
        assert 'IS NOT NULL' in processed_sql.upper()
        assert 'is not ?' not in processed_sql.lower()

        # Check parameters with None removed
        assert processed_args == (test_date1, test_date2)
        assert len(processed_args) == 2

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

    def test_named_params_with_dates(self, create_simple_mock_connection):
        """Test named parameters with date objects."""
        mock_postgres_connection = create_simple_mock_connection('postgresql')
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

        # Check SQL is unchanged for date parameters but NULL is handled
        assert 'between %(start_date)s and %(end_date)s' in processed_sql
        assert 'IS NOT NULL' in processed_sql.upper()
        assert 'is not %(null_value)s' not in processed_sql.lower()

        # Check parameter dictionary with null_value removed
        assert 'start_date' in processed_args
        assert 'end_date' in processed_args
        assert 'null_value' not in processed_args
        assert processed_args['start_date'] == datetime.date(2025, 1, 1)
        assert processed_args['end_date'] == datetime.date(2025, 3, 11)
        assert len(processed_args) == 2

    def test_combined_in_clause_and_dates(self, create_simple_mock_connection):
        """Test combination of IN clause and date parameters."""
        mock_postgres_connection = create_simple_mock_connection('postgresql')
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

    def test_null_handling_with_full_parameter_processing(self, create_simple_mock_connection):
        """Test handling of NULL values with full parameter processing pipeline."""
        mock_postgres_connection = create_simple_mock_connection('postgresql')
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
        assert 'IS NULL' in processed_sql_is_null.upper()
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

    def test_error_case_is_not_null(self, create_simple_mock_connection):
        """Test the specific error case with IS NOT and NULL value.
        """
        mock_conn = create_simple_mock_connection('postgresql')
        sql = """
        select
            foo
        from table
        where
            date between %s and %s
            and value is not %s
        """

        test_date1 = datetime.date(2025, 1, 1)
        test_date2 = datetime.date(2025, 3, 11)
        args = (test_date1, test_date2, None)

        processed_sql, processed_args = process_query_parameters(mock_conn, sql, args)

        assert 'IS NOT NULL' in processed_sql.upper()
        assert 'is not %s' not in processed_sql.lower()

        assert processed_args == (test_date1, test_date2)
        assert len(processed_args) == 2


def test_quote_identifier():
    """Test database identifier quoting for different dialects."""

    # PostgreSQL (default) quoting
    assert quote_identifier('my_table') == '"my_table"'
    assert quote_identifier('user') == '"user"'  # Reserved word safe
    assert quote_identifier('table_with"quotes') == '"table_with""quotes"'  # Escapes quotes by doubling

    # SQLite uses the same quoting style as PostgreSQL
    assert quote_identifier('my_table', dialect='sqlite') == '"my_table"'
    assert quote_identifier('column"with"quotes', dialect='sqlite') == '"column""with""quotes"'

    # Unknown dialects raise an error
    with pytest.raises(ValueError, match='Unknown dialect: unknown'):
        quote_identifier('my_table', dialect='unknown')


def test_escape_percent_signs_in_literals():
    """Test escaping percent signs in string literals."""

    # Empty SQL or SQL without percent signs returns unchanged
    assert escape_percent_signs_in_literals('') == ''
    assert escape_percent_signs_in_literals('SELECT * FROM users') == 'SELECT * FROM users'

    # SELECT queries with percent signs in literals but no placeholders
    assert escape_percent_signs_in_literals("SELECT * FROM table WHERE col = '%value'") == "SELECT * FROM table WHERE col = '%value'"

    # Single-quoted strings with percent signs get escaped
    assert escape_percent_signs_in_literals("UPDATE users SET status = 'progress: 50%'") == "UPDATE users SET status = 'progress: 50%%'"

    # Double-quoted strings with percent signs get escaped
    assert escape_percent_signs_in_literals('UPDATE config SET message = "Complete: 75%"') == 'UPDATE config SET message = "Complete: 75%%"'

    # SQL with percent signs in literals and placeholders
    assert escape_percent_signs_in_literals("UPDATE users SET name = %s, status = 'progress: 25%'") == "UPDATE users SET name = %s, status = 'progress: 25%%'"

    # Already escaped percent signs remain unchanged
    assert escape_percent_signs_in_literals("SELECT * FROM table WHERE col LIKE 'pre%%post'") == "SELECT * FROM table WHERE col LIKE 'pre%%post'"

    # Complex query with multiple escapes
    assert escape_percent_signs_in_literals("INSERT INTO logs VALUES ('%data%', 'progress: %%30%')") == "INSERT INTO logs VALUES ('%%data%%', 'progress: %%30%%')"

    # SQL with escaped quotes within string literals
    assert escape_percent_signs_in_literals("SELECT * FROM users WHERE text = 'It''s 100% done'") == "SELECT * FROM users WHERE text = 'It''s 100%% done'"
    assert escape_percent_signs_in_literals('SELECT * FROM config WHERE message = "Say ""hello"" at 50%"') == 'SELECT * FROM config WHERE message = "Say ""hello"" at 50%%"'


def test_has_placeholders():
    """Test detection of SQL parameter placeholders."""

    # Empty or None SQL handling
    assert has_placeholders('') is False
    assert has_placeholders(None) is False

    # SQL without any placeholders
    assert has_placeholders('SELECT * FROM users') is False
    assert has_placeholders('SELECT * FROM stats WHERE growth > 10%') is False

    # SQL with positional placeholders
    assert has_placeholders('SELECT * FROM users WHERE id = %s') is True
    assert has_placeholders('SELECT * FROM users WHERE id = ?') is True
    assert has_placeholders('INSERT INTO users VALUES (%s, %s, ?)') is True

    # SQL with named placeholders
    assert has_placeholders('SELECT * FROM users WHERE id = %(user_id)s') is True
    assert has_placeholders('INSERT INTO users VALUES (%(id)s, %(name)s)') is True

    # SQL with mixed placeholder types
    assert has_placeholders('SELECT * FROM users WHERE id = %s AND name = %(name)s') is True
    assert has_placeholders('SELECT * FROM users WHERE id IN (?, ?, ?)') is True

    # SQL with placeholders inside literals (still detected as placeholders)
    assert has_placeholders("SELECT * FROM users WHERE format LIKE '%s'") is True
    assert has_placeholders("SELECT * FROM users WHERE name = '?'") is True


def test_unwrap_nested_parameters():
    """Test unwrapping nested parameters for SQL queries."""

    # Non-sequence args remain unchanged
    assert _unwrap_nested_parameters('SELECT * FROM users; INSERT INTO logs VALUES (?)', 'not_a_sequence') == 'not_a_sequence'
    assert _unwrap_nested_parameters('SELECT * FROM users; INSERT INTO logs VALUES (?)', 42) == 42
    assert _unwrap_nested_parameters('SELECT * FROM users; INSERT INTO logs VALUES (?)', None) is None

    # Empty sequences remain unchanged
    assert _unwrap_nested_parameters('SELECT * FROM users; INSERT INTO logs VALUES (?)', []) == []
    assert _unwrap_nested_parameters('SELECT * FROM users; INSERT INTO logs VALUES (?)', ()) == ()

    # Single-statement queries get unwrapped when placeholder count matches
    assert _unwrap_nested_parameters('SELECT * FROM users WHERE id = ?', [(1,)]) == (1,)

    # Queries with multiple args remain unchanged
    assert _unwrap_nested_parameters('SELECT ?; INSERT ?', [1, 2]) == [1, 2]

    # Queries with one arg that's a string remain unchanged
    assert _unwrap_nested_parameters('SELECT ?; INSERT ?', ['not_a_sequence']) == ['not_a_sequence']

    # Queries with mismatched placeholder and parameter counts remain unchanged
    assert _unwrap_nested_parameters('SELECT ?; INSERT ?', [(1, 2, 3)]) == [(1, 2, 3)]
    assert _unwrap_nested_parameters('SELECT * FROM users WHERE id = ?', [(1, 2, 3)]) == [(1, 2, 3)]

    # Unwraps when placeholder count matches inner sequence length
    assert _unwrap_nested_parameters('SELECT ?; INSERT ?', [(1, 2)]) == (1, 2)
    assert _unwrap_nested_parameters('INSERT INTO users VALUES (?, ?); SELECT ?', [('John', 25, 42)]) == ('John', 25, 42)


def test_generate_named_placeholders():
    """Test generation of named placeholders for IN clauses."""

    # Basic usage with a list of integers
    args = {}
    placeholders = _generate_named_placeholders('ids', [1, 2, 3], args)
    assert placeholders == ['%(ids_0)s', '%(ids_1)s', '%(ids_2)s']
    assert sorted(args.items()) == [('ids_0', 1), ('ids_1', 2), ('ids_2', 3)]

    # Works with tuples and preserves value types
    args = {}
    placeholders = _generate_named_placeholders('vals', (10.5, 'text', None), args)
    assert placeholders == ['%(vals_0)s', '%(vals_1)s', '%(vals_2)s']
    assert sorted(args.items()) == [('vals_0', 10.5), ('vals_1', 'text'), ('vals_2', None)]

    # Empty collection case
    args = {}
    placeholders = _generate_named_placeholders('empty', [], args)
    assert placeholders == []
    assert args == {}

    # Multiple parameters with the same base name
    args = {'status_0': 'active'}
    placeholders = _generate_named_placeholders('status', ['pending', 'deleted'], args)
    assert placeholders == ['%(status_1)s', '%(status_2)s']
    assert sorted(args.items()) == [('status_0', 'active'), ('status_1', 'pending'), ('status_2', 'deleted')]

    # Works with complex data types
    args = {}
    dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
    placeholders = _generate_named_placeholders('dates', [dt], args)
    assert placeholders == ['%(dates_0)s']
    assert list(args.values())[0] == dt


if __name__ == '__main__':
    __import__('pytest').main([__file__])
