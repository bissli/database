"""Unit tests for parameter handling in database queries."""

import datetime

import pytest
from database.sql import _generate_named_placeholders
from database.sql import _unwrap_nested_parameters
from database.sql import escape_percent_signs_in_literals
from database.sql import handle_in_clause_params, handle_null_is_operators
from database.sql import has_placeholders, process_query_parameters
from database.sql import quote_identifier

# =============================================================================
# Database parameter handling tests
# =============================================================================


class TestDatabaseParameterHandling:
    """Test class for database parameter handling functionality."""

    def test_basic_postgres_parameters(self, create_simple_mock_connection):
        """Test basic parameter handling with PostgreSQL-style placeholders."""
        mock_conn = create_simple_mock_connection('postgresql')
        sql = """
        select foo from table
        where date between %s and %s and value > %s
        group by date, strategy
        """
        test_date1 = datetime.date(2025, 1, 1)
        test_date2 = datetime.date(2025, 3, 11)
        args = (test_date1, test_date2, 100)

        processed_sql, processed_args = process_query_parameters(mock_conn, sql, args)

        assert 'between %s and %s' in processed_sql
        assert 'value > %s' in processed_sql
        assert processed_args == args

    def test_nested_parameters(self, create_simple_mock_connection):
        """Test handling of nested parameter tuples."""
        mock_conn = create_simple_mock_connection('postgresql')
        sql = """
        select foo from table
        where date between %s and %s and value is not %s
        """
        test_date1 = datetime.date(2025, 1, 1)
        test_date2 = datetime.date(2025, 3, 11)
        args = [(test_date1, test_date2, None)]

        processed_sql, processed_args = process_query_parameters(mock_conn, sql, args)

        assert 'between %s and %s' in processed_sql
        assert 'IS NOT NULL' in processed_sql.upper()
        assert 'is not %s' not in processed_sql.lower()
        assert processed_args == (test_date1, test_date2)

    def test_sqlite_placeholder_conversion(self, create_simple_mock_connection):
        """Test conversion of placeholders for SQLite connections."""
        mock_conn = create_simple_mock_connection('sqlite')
        sql = """
        select foo from table
        where date between %s and %s and value is not %s
        """
        test_date1 = datetime.date(2025, 1, 1)
        test_date2 = datetime.date(2025, 3, 11)
        args = (test_date1, test_date2, None)

        processed_sql, processed_args = process_query_parameters(mock_conn, sql, args)

        assert 'between ? and ?' in processed_sql
        assert 'IS NOT NULL' in processed_sql.upper()
        assert processed_args == (test_date1, test_date2)

    def test_in_clause_expansion(self):
        """Test expansion of IN clause parameters."""
        sql = 'select * from table where id in %s and type = %s'
        args = [(1, 2, 3), 'active']

        processed_sql, processed_args = handle_in_clause_params(sql, args)

        assert 'in (%s, %s, %s)' in processed_sql.lower() or 'in (?, ?, ?)' in processed_sql.lower()
        assert processed_args == (1, 2, 3, 'active')

    def test_named_params_with_dates(self, create_simple_mock_connection):
        """Test named parameters with date objects."""
        mock_conn = create_simple_mock_connection('postgresql')
        sql = """
        select foo from table
        where date between %(start_date)s and %(end_date)s
        and value is not %(null_value)s
        """
        args = {
            'start_date': datetime.date(2025, 1, 1),
            'end_date': datetime.date(2025, 3, 11),
            'null_value': None
        }

        processed_sql, processed_args = process_query_parameters(mock_conn, sql, args)

        assert 'between %(start_date)s and %(end_date)s' in processed_sql
        assert 'IS NOT NULL' in processed_sql.upper()
        assert 'null_value' not in processed_args
        assert len(processed_args) == 2

    def test_combined_in_clause_and_dates(self, create_simple_mock_connection):
        """Test combination of IN clause and date parameters."""
        mock_conn = create_simple_mock_connection('postgresql')
        sql = """
        select foo from table
        where date between %s and %s and category in %s
        """
        test_date1 = datetime.date(2025, 1, 1)
        test_date2 = datetime.date(2025, 3, 11)
        categories = ('cat1', 'cat2', 'cat3')
        args = (test_date1, test_date2, categories)

        processed_sql, processed_args = process_query_parameters(mock_conn, sql, args)

        assert 'between %s and %s' in processed_sql
        assert 'in (%s, %s, %s)' in processed_sql.lower()
        assert processed_args == (test_date1, test_date2, 'cat1', 'cat2', 'cat3')


# =============================================================================
# IS NULL handling tests - parametrized
# =============================================================================

class TestIsNullHandling:
    """Test handling of NULL values with IS and IS NOT operators."""

    @pytest.mark.parametrize(('sql', 'args', 'expected_sql', 'expected_args'), [
        # Basic IS NULL
        ('SELECT * FROM table WHERE value IS %s', [None],
         'SELECT * FROM table WHERE value IS NULL', []),
        # Basic IS NOT NULL
        ('SELECT * FROM table WHERE value IS NOT %s', [None],
         'SELECT * FROM table WHERE value IS NOT NULL', []),
        # Multiple NULL values
        ('SELECT * FROM table WHERE v1 IS %s AND v2 IS NOT %s', [None, None],
         'SELECT * FROM table WHERE v1 IS NULL AND v2 IS NOT NULL', []),
        # Mix of NULL and non-NULL
        ('SELECT * FROM table WHERE v1 IS %s AND v2 = %s AND v3 IS NOT %s', [None, 'test', None],
         'SELECT * FROM table WHERE v1 IS NULL AND v2 = %s AND v3 IS NOT NULL', ['test']),
        # Case insensitivity
        ('SELECT * FROM table WHERE value is %s AND other IS not %s', [None, None],
         None, []),  # Special handling below
        # Non-NULL value unchanged
        ('SELECT * FROM table WHERE value IS %s', ['not_null'],
         'SELECT * FROM table WHERE value IS %s', ['not_null']),
        # No IS operator unchanged
        ('SELECT * FROM table WHERE value = %s', [None],
         'SELECT * FROM table WHERE value = %s', [None]),
    ], ids=[
        'is_null', 'is_not_null', 'multiple_nulls', 'mixed',
        'case_insensitive', 'non_null', 'no_is_operator'
    ])
    def test_null_handling_positional(self, sql, args, expected_sql, expected_args):
        """Test NULL handling with positional parameters."""
        processed_sql, processed_args = handle_null_is_operators(sql, args)

        if expected_sql is None:
            # Case insensitive test
            assert 'IS NULL' in processed_sql.upper()
            assert 'IS NOT NULL' in processed_sql.upper()
        else:
            assert processed_sql == expected_sql
        assert processed_args == expected_args

    def test_named_params_null(self):
        """Test named parameters with NULL."""
        sql = 'SELECT * FROM table WHERE value IS %(param1)s AND name = %(param2)s'
        args = {'param1': None, 'param2': 'test'}

        processed_sql, processed_args = handle_null_is_operators(sql, args)

        assert processed_sql == 'SELECT * FROM table WHERE value IS NULL AND name = %(param2)s'
        assert 'param1' not in processed_args
        assert processed_args['param2'] == 'test'


class TestFullPipelineNullHandling:
    """Test NULL handling through the full parameter processing pipeline."""

    @pytest.mark.parametrize(('sql_template', 'is_not'), [
        ('select foo from table where date between %s and %s and value is not %s', True),
        ('select foo from table where date between %s and %s and value is %s', False),
    ], ids=['is_not_null', 'is_null'])
    def test_positional_null_handling(self, create_simple_mock_connection, sql_template, is_not):
        """Test IS NULL/IS NOT NULL with positional parameters."""
        mock_conn = create_simple_mock_connection('postgresql')
        test_date1 = datetime.date(2025, 1, 1)
        test_date2 = datetime.date(2025, 3, 11)
        args = (test_date1, test_date2, None)

        processed_sql, processed_args = process_query_parameters(mock_conn, sql_template, args)

        expected_pattern = 'IS NOT NULL' if is_not else 'IS NULL'
        assert expected_pattern in processed_sql.upper()
        assert processed_args == (test_date1, test_date2)

    @pytest.mark.parametrize(('sql_template', 'is_not'), [
        ('select foo from table where date between %(start_date)s and %(end_date)s and value is not %(null_value)s', True),
        ('select foo from table where date between %(start_date)s and %(end_date)s and value is %(null_value)s', False),
    ], ids=['is_not_null_named', 'is_null_named'])
    def test_named_null_handling(self, create_simple_mock_connection, sql_template, is_not):
        """Test IS NULL/IS NOT NULL with named parameters."""
        mock_conn = create_simple_mock_connection('postgresql')
        args = {
            'start_date': datetime.date(2025, 1, 1),
            'end_date': datetime.date(2025, 3, 11),
            'null_value': None
        }

        processed_sql, processed_args = process_query_parameters(mock_conn, sql_template, args)

        expected_pattern = 'IS NOT NULL' if is_not else 'IS NULL'
        assert expected_pattern in processed_sql.upper()
        assert 'null_value' not in processed_args
        assert len(processed_args) == 2


# =============================================================================
# Quote identifier tests - parametrized
# =============================================================================

class TestQuoteIdentifier:
    """Test database identifier quoting for different dialects."""

    @pytest.mark.parametrize(('identifier', 'dialect', 'expected'), [
        # PostgreSQL (default)
        ('my_table', None, '"my_table"'),
        ('user', None, '"user"'),
        ('table_with"quotes', None, '"table_with""quotes"'),
        # SQLite
        ('my_table', 'sqlite', '"my_table"'),
        ('column"with"quotes', 'sqlite', '"column""with""quotes"'),
    ], ids=[
        'pg_basic', 'pg_reserved', 'pg_quotes', 'sqlite_basic', 'sqlite_quotes'
    ])
    def test_quote_identifier(self, identifier, dialect, expected):
        """Test identifier quoting for various dialects."""
        if dialect:
            assert quote_identifier(identifier, dialect=dialect) == expected
        else:
            assert quote_identifier(identifier) == expected

    def test_unknown_dialect_error(self):
        """Test that unknown dialects raise an error."""
        with pytest.raises(ValueError, match='Unknown dialect: unknown'):
            quote_identifier('my_table', dialect='unknown')


# =============================================================================
# Escape percent signs tests - parametrized
# =============================================================================

class TestEscapePercentSigns:
    """Test escaping percent signs in string literals."""

    @pytest.mark.parametrize(('sql', 'expected'), [
        # Empty or no percent signs
        ('', ''),
        ('SELECT * FROM users', 'SELECT * FROM users'),
        # SELECT with percent in literal but no placeholders
        ("SELECT * FROM table WHERE col = '%value'", "SELECT * FROM table WHERE col = '%value'"),
        # Single-quoted with percent
        ("UPDATE users SET status = 'progress: 50%'", "UPDATE users SET status = 'progress: 50%%'"),
        # Double-quoted with percent
        ('UPDATE config SET message = "Complete: 75%"', 'UPDATE config SET message = "Complete: 75%%"'),
        # With placeholders
        ("UPDATE users SET name = %s, status = 'progress: 25%'", "UPDATE users SET name = %s, status = 'progress: 25%%'"),
        # Already escaped
        ("SELECT * FROM table WHERE col LIKE 'pre%%post'", "SELECT * FROM table WHERE col LIKE 'pre%%post'"),
        # Complex multiple escapes
        ("INSERT INTO logs VALUES ('%data%', 'progress: %%30%')", "INSERT INTO logs VALUES ('%%data%%', 'progress: %%30%%')"),
        # Escaped quotes within literals
        ("SELECT * FROM users WHERE text = 'It''s 100% done'", "SELECT * FROM users WHERE text = 'It''s 100%% done'"),
        ('SELECT * FROM config WHERE message = "Say ""hello"" at 50%"', 'SELECT * FROM config WHERE message = "Say ""hello"" at 50%%"'),
    ], ids=[
        'empty', 'no_percent', 'select_percent_literal', 'single_quote',
        'double_quote', 'with_placeholder', 'already_escaped',
        'complex', 'escaped_quotes_single', 'escaped_quotes_double'
    ])
    def test_escape_percent_signs(self, sql, expected):
        """Test percent sign escaping in various SQL patterns."""
        assert escape_percent_signs_in_literals(sql) == expected


# =============================================================================
# Has placeholders tests - parametrized
# =============================================================================

class TestHasPlaceholders:
    """Test detection of SQL parameter placeholders."""

    @pytest.mark.parametrize(('sql', 'expected'), [
        # No placeholders
        ('', False),
        (None, False),
        ('SELECT * FROM users', False),
        ('SELECT * FROM stats WHERE growth > 10%', False),
        # Positional placeholders
        ('SELECT * FROM users WHERE id = %s', True),
        ('SELECT * FROM users WHERE id = ?', True),
        ('INSERT INTO users VALUES (%s, %s, ?)', True),
        # Named placeholders
        ('SELECT * FROM users WHERE id = %(user_id)s', True),
        ('INSERT INTO users VALUES (%(id)s, %(name)s)', True),
        # Mixed
        ('SELECT * FROM users WHERE id = %s AND name = %(name)s', True),
        ('SELECT * FROM users WHERE id IN (?, ?, ?)', True),
        # Inside literals (still detected)
        ("SELECT * FROM users WHERE format LIKE '%s'", True),
        ("SELECT * FROM users WHERE name = '?'", True),
    ], ids=[
        'empty', 'none', 'no_placeholders', 'percent_not_placeholder',
        'percent_s', 'qmark', 'mixed_positional',
        'named_single', 'named_multiple', 'mixed_named', 'qmark_multiple',
        'in_single_literal', 'qmark_in_literal'
    ])
    def test_has_placeholders(self, sql, expected):
        """Test placeholder detection for various SQL patterns."""
        assert has_placeholders(sql) is expected


# =============================================================================
# Unwrap nested parameters tests - parametrized
# =============================================================================

class TestUnwrapNestedParameters:
    """Test unwrapping nested parameters for SQL queries."""

    @pytest.mark.parametrize(('sql', 'args', 'expected'), [
        # Non-sequence unchanged
        ('SELECT ?; INSERT ?', 'not_a_sequence', 'not_a_sequence'),
        ('SELECT ?; INSERT ?', 42, 42),
        ('SELECT ?; INSERT ?', None, None),
        # Empty sequences unchanged
        ('SELECT ?; INSERT ?', [], []),
        ('SELECT ?; INSERT ?', (), ()),
        # Single-statement unwrapped
        ('SELECT * FROM users WHERE id = ?', [(1,)], (1,)),
        # Multiple args unchanged
        ('SELECT ?; INSERT ?', [1, 2], [1, 2]),
        # String arg unchanged
        ('SELECT ?; INSERT ?', ['not_a_sequence'], ['not_a_sequence']),
        # Mismatched counts unchanged
        ('SELECT ?; INSERT ?', [(1, 2, 3)], [(1, 2, 3)]),
        ('SELECT * FROM users WHERE id = ?', [(1, 2, 3)], [(1, 2, 3)]),
        # Matching counts unwrapped
        ('SELECT ?; INSERT ?', [(1, 2)], (1, 2)),
        ('INSERT INTO users VALUES (?, ?); SELECT ?', [('John', 25, 42)], ('John', 25, 42)),
    ], ids=[
        'string', 'int', 'none', 'empty_list', 'empty_tuple',
        'single_unwrap', 'multiple_unchanged', 'string_list',
        'mismatched_1', 'mismatched_2', 'matched_2', 'matched_3'
    ])
    def test_unwrap_nested_parameters(self, sql, args, expected):
        """Test parameter unwrapping for various scenarios."""
        result = _unwrap_nested_parameters(sql, args)
        if expected is None:
            assert result is None
        else:
            assert result == expected


# =============================================================================
# Generate named placeholders tests - parametrized
# =============================================================================

class TestGenerateNamedPlaceholders:
    """Test generation of named placeholders for IN clauses."""

    def test_basic_integers(self):
        """Test with a list of integers."""
        args = {}
        placeholders = _generate_named_placeholders('ids', [1, 2, 3], args)

        assert placeholders == ['%(ids_0)s', '%(ids_1)s', '%(ids_2)s']
        assert sorted(args.items()) == [('ids_0', 1), ('ids_1', 2), ('ids_2', 3)]

    def test_mixed_types(self):
        """Test with tuples and mixed value types."""
        args = {}
        placeholders = _generate_named_placeholders('vals', (10.5, 'text', None), args)

        assert placeholders == ['%(vals_0)s', '%(vals_1)s', '%(vals_2)s']
        assert sorted(args.items()) == [('vals_0', 10.5), ('vals_1', 'text'), ('vals_2', None)]

    def test_empty_collection(self):
        """Test with empty collection."""
        args = {}
        placeholders = _generate_named_placeholders('empty', [], args)

        assert placeholders == []
        assert args == {}

    def test_existing_params(self):
        """Test with existing parameters in args dict."""
        args = {'status_0': 'active'}
        placeholders = _generate_named_placeholders('status', ['pending', 'deleted'], args)

        assert placeholders == ['%(status_1)s', '%(status_2)s']
        assert sorted(args.items()) == [('status_0', 'active'), ('status_1', 'pending'), ('status_2', 'deleted')]

    def test_datetime_values(self):
        """Test with datetime values."""
        args = {}
        dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        placeholders = _generate_named_placeholders('dates', [dt], args)

        assert placeholders == ['%(dates_0)s']
        assert list(args.values())[0] == dt


if __name__ == '__main__':
    __import__('pytest').main([__file__])
