"""Tests for SQL IN clause parameter handling.

These tests verify the behavior of the handle_in_clause_params function
which is responsible for expanding parameters in SQL IN clauses.
"""

import datetime

import pytest
from database.sql import handle_in_clause_params
from database.utils.connection_utils import get_dialect_name

# Helper functions


def assert_lower(string1, string2, message=None):
    """Assert that two strings are equal when converted to lowercase."""
    assert string1.lower() == string2.lower(), message or f"'{string1}' != '{string2}'"


def assert_contains_lower(string_to_search, substring, message=None):
    """Assert that a string contains a substring, case-insensitive."""
    assert substring.lower() in string_to_search.lower(), message or f"'{substring}' not in '{string_to_search}'"


def matches_placeholder_pattern(sql, pattern_count):
    """Check if SQL contains the expected number of placeholders."""
    qmark = f"in ({', '.join(['?'] * pattern_count)})"
    percent = f"in ({', '.join(['%s'] * pattern_count)})"
    return qmark in sql.lower() or percent in sql.lower()


# =============================================================================
# Standard (positional) parameter tests - parametrized
# =============================================================================

class TestStandardPositionalParams:
    """Tests for standard %s positional parameter expansion."""

    @pytest.mark.parametrize(('sql', 'args', 'expected_count', 'expected_args'), [
        # Basic sequence expansion
        ('SELECT * FROM users WHERE id IN %s', [(1, 2, 3)], 3, (1, 2, 3)),
        # Single item
        ('SELECT * FROM users WHERE id IN %s', [(42,)], 1, (42,)),
        # Direct tuple
        ('SELECT * FROM users WHERE id IN %s', (1, 2, 3), 3, (1, 2, 3)),
        # Nested list
        ('SELECT * FROM users WHERE id IN %s', [[1, 2, 3]], 3, (1, 2, 3)),
        # Double-nested tuple
        ('SELECT * FROM users WHERE id IN %s', ((1, 2, 3),), 3, (1, 2, 3)),
        # Direct list
        ('SELECT * FROM users WHERE id IN %s', [1, 2, 3], 3, (1, 2, 3)),
        # Single-item list
        ('SELECT * FROM users WHERE id IN %s', [101], 1, (101,)),
        # Nested single-item list
        ('SELECT * FROM users WHERE id IN %s', [[101]], 1, (101,)),
    ], ids=[
        'basic_tuple', 'single_item', 'direct_tuple', 'nested_list',
        'double_nested', 'direct_list', 'single_item_list', 'nested_single'
    ])
    def test_in_clause_expansion(self, sql, args, expected_count, expected_args):
        """Test various formats of IN clause parameter expansion."""
        result_sql, result_args = handle_in_clause_params(sql, args)

        assert matches_placeholder_pattern(result_sql, expected_count)
        assert result_args == expected_args

    def test_empty_sequence(self):
        """Test empty sequence handling in standard IN clause."""
        sql = 'SELECT * FROM users WHERE id IN %s'
        args = [()]

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert_contains_lower(result_sql, 'IN (null)')
        assert len(result_args) == 0

    def test_multiple_in_clauses(self):
        """Test multiple IN clauses with standard parameters."""
        sql = 'SELECT * FROM users WHERE id IN %s AND status IN %s'
        args = [(1, 2, 3), ('active', 'pending')]

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert matches_placeholder_pattern(result_sql.split('AND')[0], 3)
        assert 'status' in result_sql.lower()
        assert result_args == (1, 2, 3, 'active', 'pending')


# =============================================================================
# Named parameter tests - parametrized
# =============================================================================

class TestNamedParams:
    """Tests for named %(param)s parameter expansion."""

    @pytest.mark.parametrize(('sql', 'args', 'expected_sql_contains', 'expected_keys'), [
        # Basic named param
        ('SELECT * FROM users WHERE id IN %(user_ids)s',
         {'user_ids': (1, 2, 3)},
         'IN (%(user_ids_0)s, %(user_ids_1)s, %(user_ids_2)s)',
         {'user_ids_0': 1, 'user_ids_1': 2, 'user_ids_2': 3}),
        # Single item
        ('SELECT * FROM users WHERE id IN %(user_ids)s',
         {'user_ids': (42,)},
         'IN (%(user_ids_0)s)',
         {'user_ids_0': 42}),
        # List instead of tuple
        ('SELECT * FROM users WHERE id IN %(ids)s',
         {'ids': [1, 2, 3]},
         'IN (%(ids_0)s, %(ids_1)s, %(ids_2)s)',
         {'ids_0': 1, 'ids_1': 2, 'ids_2': 3}),
    ], ids=['basic', 'single_item', 'list_value'])
    def test_named_param_expansion(self, sql, args, expected_sql_contains, expected_keys):
        """Test named parameter expansion variants."""
        result_sql, result_args = handle_in_clause_params(sql, args)

        assert_contains_lower(result_sql, expected_sql_contains)
        for key, value in expected_keys.items():
            assert result_args[key] == value

    def test_empty_sequence(self):
        """Test empty sequence handling with named parameters."""
        sql = 'SELECT * FROM users WHERE id IN %(user_ids)s'
        args = {'user_ids': ()}

        result_sql, result_args = handle_in_clause_params(sql, args)
        assert_contains_lower(result_sql, 'IN (null)')

    def test_multiple_in_clauses(self):
        """Test multiple IN clauses with named parameters."""
        sql = 'SELECT * FROM users WHERE id IN %(user_ids)s AND status IN %(statuses)s'
        args = {'user_ids': (1, 2, 3), 'statuses': ('active', 'pending')}

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert_contains_lower(result_sql, 'IN (%(user_ids_0)s, %(user_ids_1)s, %(user_ids_2)s)')
        assert_contains_lower(result_sql, 'IN (%(statuses_0)s, %(statuses_1)s)')

        for i, val in enumerate((1, 2, 3)):
            assert result_args[f'user_ids_{i}'] == val
        for i, val in enumerate(('active', 'pending')):
            assert result_args[f'statuses_{i}'] == val

    def test_with_regular_params(self):
        """Test IN clause with named parameters alongside regular parameters."""
        sql = 'SELECT * FROM users WHERE id IN %(item_ids)s AND name = %(user_name)s'
        args = {'item_ids': ('item1',), 'user_name': 'username'}

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert_contains_lower(result_sql, 'IN (%(item_ids_0)s)')
        assert 'name = %(user_name)s' in result_sql
        assert result_args['item_ids_0'] == 'item1'
        assert result_args['user_name'] == 'username'

    def test_single_item_tuple_expansion(self):
        """Test handling of single-item tuples in IN clause with named parameters."""
        sql = """
select col1, col2
from table
where id in %(ids)s
"""
        args = {'ids': ('single_value',)}

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert 'in (%(ids_0)s)' in result_sql
        assert 'ids' not in result_args
        assert 'ids_0' in result_args
        assert result_args['ids_0'] == 'single_value'


# =============================================================================
# Edge cases - parametrized
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    @pytest.mark.parametrize(('sql', 'args', 'expected_sql', 'expected_args'), [
        # No IN clause - unchanged
        ('SELECT * FROM users WHERE id = %s', (1,),
         'SELECT * FROM users WHERE id = %s', (1,)),
        # None args
        ('SELECT * FROM users WHERE id IN %s', None,
         'SELECT * FROM users WHERE id IN %s', None),
        # Empty args tuple
        ('SELECT * FROM users WHERE id IN %s', (),
         'SELECT * FROM users WHERE id IN %s', ()),
    ], ids=['no_in_clause', 'none_args', 'empty_args'])
    def test_passthrough_cases(self, sql, args, expected_sql, expected_args):
        """Test cases where SQL/args should pass through unchanged."""
        result_sql, result_args = handle_in_clause_params(sql, args)
        assert result_sql == expected_sql
        assert result_args == expected_args

    def test_non_sequence_param(self):
        """Test IN clause with non-sequence parameter."""
        sql = 'SELECT * FROM users WHERE id IN %s'
        args = ['not-a-sequence']

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert 'IN (%s)' in result_sql
        assert result_args == ('not-a-sequence',)

    def test_mixed_standard_and_named_params(self):
        """Test mix of standard and named parameters in list context."""
        sql = 'SELECT * FROM users WHERE id IN %s AND status IN %(statuses)s'
        args = [(1, 2, 3), {'statuses': ('active', 'pending')}]

        result_sql, result_args = handle_in_clause_params(sql, args)

        # Standard parameter should be expanded
        assert matches_placeholder_pattern(result_sql.split('AND')[0], 3)
        # Named parameter should be unchanged since args is not a dict
        assert '%(statuses)s' in result_sql

    @pytest.mark.parametrize(('args', 'expected_first_arg_type'), [
        (['(101)'], str),   # String that looks like tuple
        ([None], type(None)),  # None value
    ], ids=['string_tuple_lookalike', 'none_value'])
    def test_special_values(self, args, expected_first_arg_type):
        """Test handling of special values in lists."""
        sql = 'SELECT * FROM table WHERE id IN %s'
        result_sql, result_args = handle_in_clause_params(sql, args)

        assert matches_placeholder_pattern(result_sql, 1)
        assert isinstance(result_args[0], expected_first_arg_type)

    def test_empty_list_parameter(self):
        """Test empty list parameter."""
        sql = 'SELECT * FROM table WHERE id IN %s'
        args = [[]]

        result_sql, result_args = handle_in_clause_params(sql, args)
        assert_contains_lower(result_sql, 'IN (NULL)')
        assert len(result_args) == 0


# =============================================================================
# Multiple IN clauses with various positions
# =============================================================================

class TestMultiParameterInClause:
    """Test SQL queries with multiple parameters including IN clause variations."""

    @pytest.mark.parametrize(('sql', 'params', 'item_count', 'expected_args'), [
        # IN clause as last condition
        ("""select * from user_items
            where created_date = %s and user_id = %s and item_id in %s""",
         ('2025-08-08', 'USER_A', ['ITM001', 'ITM002', 'ITM003', 'ITM004']),
         4,
         ('2025-08-08', 'USER_A', 'ITM001', 'ITM002', 'ITM003', 'ITM004')),
        # IN clause as first condition
        ("""select * from user_items
            where item_id in %s and created_date = %s and user_id = %s""",
         (['ITM001', 'ITM002', 'ITM003'], '2025-08-08', 'USER_A'),
         3,
         ('ITM001', 'ITM002', 'ITM003', '2025-08-08', 'USER_A')),
    ], ids=['in_clause_last', 'in_clause_first'])
    def test_in_clause_position(self, sql, params, item_count, expected_args):
        """Test IN clause at various positions in WHERE clause."""
        result_sql, result_args = handle_in_clause_params(sql, params)

        assert matches_placeholder_pattern(result_sql, item_count)
        assert result_args == expected_args

    def test_parenthesized_in_clause(self):
        """Test SQL query with parenthesized 'item_id in (%s)' format."""
        sql = """select * from user_items
            where created_date = %s and user_id = %s and item_id in (%s)"""
        item_ids = ('ITM001', 'ITM002', 'ITM003', 'ITM004')
        params = (datetime.date(2025, 8, 8), 'USER_A', item_ids)

        result_sql, result_args = handle_in_clause_params(sql, params)

        assert matches_placeholder_pattern(result_sql, 4)
        assert result_args == (datetime.date(2025, 8, 8), 'USER_A') + item_ids

    def test_parenthesized_in_clause_no_double_parens(self):
        """Ensure IN clause already containing parentheses doesn't get double parens."""
        sql = 'SELECT * FROM items WHERE item_id IN (%s)'
        args = (('ITM001', 'ITM002', 'ITM003', 'ITM004'),)

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert '))' not in result_sql
        assert matches_placeholder_pattern(result_sql, 4)
        assert result_args == ('ITM001', 'ITM002', 'ITM003', 'ITM004')

    def test_explicit_placeholders_unchanged(self):
        """Test SQL query with explicit placeholders in IN clause are unchanged."""
        sql = """select * from user_items
            where (created_date) = %s and (user_id = %s) and (item_id in (%s, %s, %s))"""
        params = ('2025-08-08', 'USER_A', 'ITM001', 'ITM002', 'ITM003')

        result_sql, result_args = handle_in_clause_params(sql, params)

        assert 'item_id in (%s, %s, %s)' in result_sql
        assert result_args == params

    def test_in_clause_middle_position(self):
        """Test SQL query where IN clause is in the middle of WHERE conditions."""
        sql = """select * from user_items
            where created_date = %s and item_id in (%s) and user_id = %s and status = %s"""
        item_ids = ['ITM001', 'ITM002']
        params = ('2025-08-08', item_ids, 'USER_A', 'active')

        result_sql, result_args = handle_in_clause_params(sql, params)

        assert matches_placeholder_pattern(result_sql, 2)
        assert result_args == ('2025-08-08', 'ITM001', 'ITM002', 'USER_A', 'active')

    def test_multiple_in_clauses_various_positions(self):
        """Test SQL query with multiple IN clauses in different positions."""
        sql = """select * from user_items
            where item_id in (%s) and created_date in (%s)
            and status in (%s) and user_id in (%s)"""
        item_ids = ('ITM001', 'ITM002')
        statuses = ('active', 'pending', 'archived')
        params = (item_ids, '2025-08-08', statuses, 'USER_A')

        result_sql, result_args = handle_in_clause_params(sql, params)

        expected_args = ('ITM001', 'ITM002', '2025-08-08', 'active', 'pending', 'archived', 'USER_A')
        assert result_args == expected_args

    def test_named_params_first_position(self):
        """Test SQL query with IN clause first using named parameters."""
        sql = """select * from user_items
            where item_id in %(item_ids)s
            and created_date = %(created_date)s
            and user_id = %(user_id)s"""
        params = {
            'item_ids': ['ITM001', 'ITM002', 'ITM003'],
            'created_date': '2025-08-08',
            'user_id': 'USER_A'
        }

        result_sql, result_args = handle_in_clause_params(sql, params)

        assert 'item_id in (%(item_ids_0)s, %(item_ids_1)s, %(item_ids_2)s)' in result_sql.lower()
        assert result_args['item_ids_0'] == 'ITM001'
        assert result_args['item_ids_1'] == 'ITM002'
        assert result_args['item_ids_2'] == 'ITM003'
        assert result_args['created_date'] == '2025-08-08'
        assert result_args['user_id'] == 'USER_A'
        assert 'item_ids' not in result_args


# =============================================================================
# Direct list parameter tests
# =============================================================================

class TestDirectListParams:
    """Tests for direct list parameters (not wrapped in tuple)."""

    def test_multiple_direct_lists(self):
        """Test multiple direct list parameters for IN clauses."""
        sql = 'SELECT * FROM users WHERE id IN %s AND status IN %s'
        args = [[1, 2, 3], ['active', 'pending']]

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert result_args[:3] == (1, 2, 3)
        assert result_args[3:5] == ('active', 'pending')
        assert len(result_args) == 5

    def test_mixed_list_sizes(self):
        """Test with one multi-item list and one single-item list."""
        sql = 'SELECT * FROM users WHERE id IN %s AND status IN %s'
        args = [[1, 2, 3], ['active']]

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert result_args[:3] == (1, 2, 3)
        assert result_args[3] == 'active'
        assert len(result_args) == 4


# =============================================================================
# Connection type detection tests
# =============================================================================

class TestConnectionTypeDetection:
    """Tests for connection type detection with various connection objects."""

    def test_dialect_detection(self, simple_mock_postgresql_connection, simple_mock_sqlite_connection):
        """Test that connection type detection works correctly."""
        assert get_dialect_name(simple_mock_postgresql_connection) == 'postgresql'
        assert get_dialect_name(simple_mock_sqlite_connection) == 'sqlite'

    def test_connection_wrapper_detection(self, simple_mock_postgresql_connection, simple_mock_transaction_factory):
        """Test dialect detection with connection wrappers."""
        pg_wrapper = simple_mock_transaction_factory(simple_mock_postgresql_connection)
        assert get_dialect_name(pg_wrapper) == 'postgresql'

    def test_transaction_compatible(self, simple_mock_postgresql_connection, simple_mock_transaction_factory):
        """Test that IN clause parameter processing works with transaction-like objects."""
        mock_tx = simple_mock_transaction_factory(simple_mock_postgresql_connection)

        sql = 'DELETE FROM users WHERE id IN %s'
        args = [[1, 2, 3]]

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert '(%s, %s, %s)' in result_sql or '(?, ?, ?)' in result_sql
        assert result_args == (1, 2, 3)


# =============================================================================
# Regression tests for specific issues
# =============================================================================

class TestRegressions:
    """Regression tests for specific bugs that were fixed."""

    @pytest.mark.parametrize('args_format', [
        ([101],),      # Single item list in tuple
        [101],         # Direct single item list
    ], ids=['in_tuple', 'direct'])
    def test_single_item_list_not_converted_to_string(self, args_format):
        """Test that [101] is not incorrectly converted to '(101)' string.

        Regression test for issue where single-item lists were being
        converted to string representations.
        """
        sql = 'select name, value from test_table where value IN %s'
        args = args_format

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert 'in (%s)' in result_sql.lower() or 'in (?)' in result_sql.lower()
        assert result_args == (101,)
        assert isinstance(result_args[0], int)
        assert not isinstance(result_args[0], str)


# =============================================================================
# Syntax variant tests
# =============================================================================

class TestSyntaxVariants:
    """Tests for different IN clause syntax formats."""

    def test_named_parameter_format(self):
        """Test standard named parameter format: IN %(foo)s"""
        test_values = (1, 2, 3)
        sql = 'SELECT * FROM users WHERE id IN %(ids)s'
        args = {'ids': test_values}

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert 'ids_0' in result_args
        assert 'ids_1' in result_args
        assert 'ids_2' in result_args
        for i, val in enumerate(test_values):
            assert result_args[f'ids_{i}'] == val
        assert 'in (%(ids_0)s, %(ids_1)s, %(ids_2)s)' in result_sql.lower()

    def test_positional_parameter_format(self):
        """Test standard positional parameter format: IN %s"""
        test_values = (1, 2, 3)
        sql = 'SELECT * FROM users WHERE id IN %s'
        args = [test_values]

        result_sql, result_args = handle_in_clause_params(sql, args)

        assert result_args == test_values
        assert matches_placeholder_pattern(result_sql, 3)


if __name__ == '__main__':
    __import__('pytest').main([__file__])
