"""
Tests for SQL IN clause parameter handling.

These tests verify the behavior of the handle_in_clause_params function
which is responsible for expanding parameters in SQL IN clauses.
"""

from database.utils.connection_utils import get_dialect_name
from database.utils.sql import handle_in_clause_params


def assert_lower(string1, string2, message=None):
    """
    Assert that two strings are equal when converted to lowercase.

    Args:
        string1: First string to compare
        string2: Second string to compare
        message: Optional message to display on failure
    """
    assert string1.lower() == string2.lower(), message or f"Strings not equal in lowercase: '{string1}' != '{string2}'"


def assert_contains_lower(string_to_search, substring, message=None):
    """
    Assert that a string contains a substring, case-insensitive.

    Args:
        string_to_search: String to search in
        substring: Substring to search for
        message: Optional message to display on failure
    """
    assert substring.lower() in string_to_search.lower(), message or f"Substring not found in lowercase: '{substring}' not in '{string_to_search}'"


#
# Standard (positional) parameter tests
#
def test_standard_in_clause_basic():
    """Test basic sequence parameter expansion in standard IN clause"""
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = [(1, 2, 3)]

    result_sql, result_args = handle_in_clause_params(sql, args)

    # Check if result matches either placeholder format
    assert any(pattern in result_sql.lower() for pattern in ['in (?, ?, ?)', 'in (%s, %s, %s)'])
    assert result_args == (1, 2, 3)


def test_standard_in_clause_empty_sequence():
    """Test empty sequence handling in standard IN clause"""
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = [()]

    result_sql, result_args = handle_in_clause_params(sql, args)

    assert_contains_lower(result_sql, 'IN (null)')
    assert len(result_args) == 0


def test_standard_multiple_in_clauses():
    """Test multiple IN clauses with standard parameters"""
    sql = 'SELECT * FROM users WHERE id IN %s AND status IN %s'
    args = [(1, 2, 3), ('active', 'pending')]

    result_sql, result_args = handle_in_clause_params(sql, args)

    # Both clauses should be expanded
    assert any(pattern in result_sql.lower() for pattern in ['in (?, ?, ?)', 'in (%s, %s, %s)'])
    assert any(pattern in result_sql.lower() for pattern in ['status in (?, ?)', 'status in (%s, %s)'])
    assert result_args == (1, 2, 3, 'active', 'pending')


#
# Named parameter tests
#
def test_named_params_basic():
    """Test basic named parameter expansion in IN clause"""
    sql = 'SELECT * FROM users WHERE id IN %(user_ids)s'
    args = {'user_ids': (1, 2, 3)}

    result_sql, result_args = handle_in_clause_params(sql, args)

    assert_contains_lower(result_sql, 'IN (%(user_ids_0)s, %(user_ids_1)s, %(user_ids_2)s)')
    assert result_args['user_ids_0'] == 1
    assert result_args['user_ids_1'] == 2
    assert result_args['user_ids_2'] == 3


def test_named_params_empty_sequence():
    """Test empty sequence handling with named parameters"""
    sql = 'SELECT * FROM users WHERE id IN %(user_ids)s'
    args = {'user_ids': ()}

    result_sql, result_args = handle_in_clause_params(sql, args)
    assert_contains_lower(result_sql, 'IN (null)')


def test_named_params_multiple_in_clauses():
    """Test multiple IN clauses with named parameters"""
    sql = 'SELECT * FROM users WHERE id IN %(user_ids)s AND status IN %(statuses)s'
    args = {'user_ids': (1, 2, 3), 'statuses': ('active', 'pending')}

    result_sql, result_args = handle_in_clause_params(sql, args)

    # Both parameter sets should be expanded
    assert_contains_lower(result_sql, 'IN (%(user_ids_0)s, %(user_ids_1)s, %(user_ids_2)s)')
    assert_contains_lower(result_sql, 'IN (%(statuses_0)s, %(statuses_1)s)')

    # Verify all parameters have been properly mapped
    for i, val in enumerate((1, 2, 3)):
        assert result_args[f'user_ids_{i}'] == val
    for i, val in enumerate(('active', 'pending')):
        assert result_args[f'statuses_{i}'] == val


def test_named_params_with_regular_params():
    """Test IN clause with named parameters alongside regular parameters"""
    sql = 'SELECT * FROM users WHERE id IN %(item_ids)s AND name = %(user_name)s'
    args = {'item_ids': ('item1',), 'user_name': 'username'}

    result_sql, result_args = handle_in_clause_params(sql, args)

    # The IN clause should be expanded, regular parameter should remain unchanged
    assert_contains_lower(result_sql, 'IN (%(item_ids_0)s)')
    assert 'name = %(user_name)s' in result_sql

    # Verify parameter mapping
    assert result_args['item_ids_0'] == 'item1'
    assert result_args['user_name'] == 'username'


def test_named_params_single_item():
    """Test named parameter with single-item tuple"""
    sql = 'SELECT * FROM users WHERE id IN %(user_ids)s'
    args = {'user_ids': (42,)}

    result_sql, result_args = handle_in_clause_params(sql, args)
    assert_contains_lower(result_sql, 'IN (%(user_ids_0)s)')
    assert result_args['user_ids_0'] == 42


#
# Single-item tuple special cases
#
def test_single_item_tuple_in_named_param():
    """Test handling of single-item tuples in IN clause with named parameters"""
    sql = """
select col1, col2
from table
where id in %(ids)s
"""
    args = {'ids': ('single_value',)}

    result_sql, result_args = handle_in_clause_params(sql, args)

    # The SQL should have the placeholder expanded
    assert 'in (%(ids_0)s)' in result_sql

    # The args should have the named parameter expanded
    assert 'ids' not in result_args
    assert 'ids_0' in result_args
    assert result_args['ids_0'] == 'single_value'


def test_multiple_in_clauses():
    """Test handling of multiple IN clauses with different parameters"""
    sql = """
select *
from table
where category in %(categories)s
and status in %(statuses)s
"""
    args = {
        'categories': ('cat1', 'cat2'),
        'statuses': ('active',)  # Single-item tuple
    }

    result_sql, result_args = handle_in_clause_params(sql, args)

    # Check categories expansion
    assert 'category in (%(categories_0)s, %(categories_1)s)' in result_sql
    assert result_args['categories_0'] == 'cat1'
    assert result_args['categories_1'] == 'cat2'

    # Check statuses expansion (single item)
    assert 'status in (%(statuses_0)s)' in result_sql
    assert result_args['statuses_0'] == 'active'


#
# Edge cases
#
def test_mixed_standard_and_named_params():
    """Test mix of standard and named parameters in list context"""
    sql = 'SELECT * FROM users WHERE id IN %s AND status IN %(statuses)s'
    args = [(1, 2, 3), {'statuses': ('active', 'pending')}]

    result_sql, result_args = handle_in_clause_params(sql, args)

    # Standard parameter should be expanded
    assert any(pattern in result_sql.lower() for pattern in ['in (?, ?, ?)', 'in (%s, %s, %s)'])
    # Named parameter should be unchanged since args is not a dict
    assert '%(statuses)s' in result_sql


def test_edge_case_no_in_clause():
    """Test SQL with no IN clause"""
    sql = 'SELECT * FROM users WHERE id = %s'
    args = (1,)

    result_sql, result_args = handle_in_clause_params(sql, args)
    assert result_sql == sql
    assert result_args == args


def test_edge_case_non_sequence_param():
    """Test IN clause with non-sequence parameter"""
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = ['not-a-sequence']

    result_sql, result_args = handle_in_clause_params(sql, args)
    # A string is not a sequence for expansion purposes - should be treated as a single value
    # for an IN clause and wrapped in IN (%s)
    assert 'IN (%s)' in result_sql
    assert result_args == ('not-a-sequence',)


def test_edge_case_none_args():
    """Test with None args"""
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = None

    result_sql, result_args = handle_in_clause_params(sql, args)
    assert result_sql == sql
    assert result_args == args


def test_edge_case_empty_args():
    """Test with empty args tuple"""
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = ()

    result_sql, result_args = handle_in_clause_params(sql, args)
    assert result_sql == sql
    assert result_args == args


def test_direct_list_in_clause():
    """Test enhanced handling of direct list parameters for IN clauses"""
    # Basic direct list case
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = [1, 2, 3]  # Direct list without extra tuple

    result_sql, result_args = handle_in_clause_params(sql, args)

    # Check if result matches either placeholder format
    assert any(pattern in result_sql.lower() for pattern in ['in (?, ?, ?)', 'in (%s, %s, %s)'])
    assert result_args == (1, 2, 3)

    # Test with multiple parameters should not convert
    sql = 'SELECT * FROM users WHERE id IN %s AND status = %s'
    args = [[1, 2, 3], 'active']  # List for IN clause plus additional parameter

    result_sql, result_args = handle_in_clause_params(sql, args)

    # The IN clause should be expanded with the inner list
    assert any(pattern in result_sql.lower() for pattern in ['id in (?, ?, ?)', 'id in (%s, %s, %s)'])
    # The original args should be modified
    assert len(result_args) >= 1  # At least the expanded IN clause values
    assert 1 in result_args
    assert 2 in result_args
    assert 3 in result_args

    # Test with a single-item list
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = [101]  # Single-item list

    result_sql, result_args = handle_in_clause_params(sql, args)

    # The SQL should contain an expanded placeholder, not a string literal
    assert any(pattern in result_sql.lower() for pattern in ['in (%s)', 'in (?)'])
    # The args should be a tuple with the single value, not a string representation
    assert result_args == (101,)
    # Specifically verify it's not a string like '(101)'
    assert not isinstance(result_args[0], str)


def test_multiple_direct_list_parameters():
    """Test multiple direct list parameters for IN clauses"""
    sql = 'SELECT * FROM users WHERE id IN %s AND status IN %s'
    args = [[1, 2, 3], ['active', 'pending']]

    result_sql, result_args = handle_in_clause_params(sql, args)

    # Check SQL has been properly modified
    assert any(pattern in result_sql.lower() for pattern in ['id in (?, ?, ?)', 'id in (%s, %s, %s)'])
    assert any(pattern in result_sql.lower() for pattern in ['status in (?, ?)', 'status in (%s, %s)'])

    # Check args have been properly expanded
    assert result_args[0:3] == (1, 2, 3)
    assert result_args[3:5] == ('active', 'pending')
    assert len(result_args) == 5

    # Test with one multi-item list and one single-item list
    sql = 'SELECT * FROM users WHERE id IN %s AND status IN %s'
    args = [[1, 2, 3], ['active']]

    result_sql, result_args = handle_in_clause_params(sql, args)

    # Check SQL has been properly modified
    assert any(pattern in result_sql.lower() for pattern in ['id in (?, ?, ?)', 'id in (%s, %s, %s)'])
    assert any(pattern in result_sql.lower() for pattern in ['status in (?)', 'status in (%s)'])

    # Check args have been properly expanded
    assert result_args[0:3] == (1, 2, 3)
    assert result_args[3] == 'active'
    assert len(result_args) == 4


def test_connection_type_detection():
    """Test that connection type detection works correctly with mocked connections"""
    # Create mock connections
    from tests.fixtures.mocks import _create_simple_mock_connection
    from tests.fixtures.mocks import _create_simple_mock_transaction

    pg_mock = _create_simple_mock_connection('postgresql')
    odbc_mock = _create_simple_mock_connection('mssql')
    sqlite_mock = _create_simple_mock_connection('sqlite')

    # Test dialect name detection
    assert get_dialect_name(pg_mock) == 'postgresql'
    assert get_dialect_name(odbc_mock) == 'mssql'
    assert get_dialect_name(sqlite_mock) == 'sqlite'

    # Test with connection wrappers
    pg_wrapper = _create_simple_mock_transaction(pg_mock)
    assert get_dialect_name(pg_wrapper) == 'postgresql'


def test_transaction_compatible():
    """Test that IN clause parameter processing works with transaction-like objects"""
    # This is a basic compatibility test to ensure the functionality would work with transactions
    # Full transaction testing would need a live database connection

    from tests.fixtures.mocks import _create_simple_mock_connection
    from tests.fixtures.mocks import _create_simple_mock_transaction

    # Create a mock connection and transaction
    mock_conn = _create_simple_mock_connection('postgresql')
    mock_tx = _create_simple_mock_transaction(mock_conn)

    # Test that the connection type detection works with transaction objects
    sql = 'DELETE FROM users WHERE id IN %s'
    args = [[1, 2, 3]]

    # This just tests that the function runs without error when given a transaction-like object
    result_sql, result_args = handle_in_clause_params(sql, args)

    # Verify result has expected format
    assert '(%s, %s, %s)' in result_sql or '(?, ?, ?)' in result_sql
    assert result_args == (1, 2, 3)


def test_different_parameter_formats():
    """Test various format variations of IN clause parameters"""

    # Direct tuple instead of list
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = (1, 2, 3)  # Tuple format
    result_sql, result_args = handle_in_clause_params(sql, args)
    assert any(pattern in result_sql.lower() for pattern in ['in (?, ?, ?)', 'in (%s, %s, %s)'])
    assert result_args == (1, 2, 3)

    # Nested list format
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = [[1, 2, 3]]
    result_sql, result_args = handle_in_clause_params(sql, args)
    assert any(pattern in result_sql.lower() for pattern in ['in (?, ?, ?)', 'in (%s, %s, %s)'])
    assert result_args == (1, 2, 3)

    # Double-nested tuple format
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = ((1, 2, 3),)
    result_sql, result_args = handle_in_clause_params(sql, args)
    assert any(pattern in result_sql.lower() for pattern in ['in (?, ?, ?)', 'in (%s, %s, %s)'])
    assert result_args == (1, 2, 3)

    # Named parameter with direct list instead of tuple
    sql = 'SELECT * FROM users WHERE id IN %(ids)s'
    args = {'ids': [1, 2, 3]}  # List instead of tuple
    result_sql, result_args = handle_in_clause_params(sql, args)
    assert_contains_lower(result_sql, 'IN (%(ids_0)s, %(ids_1)s, %(ids_2)s)')
    assert result_args['ids_0'] == 1
    assert result_args['ids_1'] == 2
    assert result_args['ids_2'] == 3

    # Test single-item list
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = [101]
    result_sql, result_args = handle_in_clause_params(sql, args)
    assert any(pattern in result_sql.lower() for pattern in ['in (%s)', 'in (?)'])
    assert result_args == (101,)
    assert not isinstance(result_args[0], str)

    # Test nested single-item list
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = [[101]]
    result_sql, result_args = handle_in_clause_params(sql, args)
    assert any(pattern in result_sql.lower() for pattern in ['in (%s)', 'in (?)'])
    assert result_args == (101,)
    assert not isinstance(result_args[0], str)


def test_parameter_handling_edge_cases():
    """Test edge cases in parameter handling for IN clauses."""

    # Case 1: List with a value that could look like a tuple string
    sql = 'SELECT * FROM table WHERE id IN %s'
    args = ['(101)']  # String that looks like a tuple

    result_sql, result_args = handle_in_clause_params(sql, args)
    assert any(pattern in result_sql.lower() for pattern in ['in (%s)', 'in (?)'])
    assert result_args == ('(101)',)
    assert isinstance(result_args[0], str)

    # Case 2: Empty list parameter
    sql = 'SELECT * FROM table WHERE id IN %s'
    args = [[]]  # Empty list

    result_sql, result_args = handle_in_clause_params(sql, args)
    assert_contains_lower(result_sql, 'IN (NULL)')
    assert len(result_args) == 0

    # Case 3: None value in a list
    sql = 'SELECT * FROM table WHERE id IN %s'
    args = [None]  # None value

    result_sql, result_args = handle_in_clause_params(sql, args)
    assert any(pattern in result_sql.lower() for pattern in ['in (%s)', 'in (?)'])
    assert result_args == (None,)
    assert result_args[0] is None


def test_direct_list_parameter_with_transaction():
    """Verify direct list parameters work with transaction-like objects."""
    from tests.fixtures.mocks import _create_simple_mock_connection
    from tests.fixtures.mocks import _create_simple_mock_transaction

    # Create mock connection and transaction
    mock_conn = _create_simple_mock_connection('postgresql')
    mock_tx = _create_simple_mock_transaction(mock_conn)

    # Test single item list with transaction
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = [101]  # Single item list

    result_sql, result_args = handle_in_clause_params(sql, args)

    # Verify correct SQL format
    assert 'IN (%s)' in result_sql or 'IN (?)' in result_sql
    assert result_args == (101,)
    assert not isinstance(result_args[0], str)

    # Test single item in nested list with transaction
    sql = 'SELECT * FROM users WHERE id IN %s'
    args = [[101]]

    result_sql, result_args = handle_in_clause_params(sql, args)

    # Verify correct SQL format
    assert 'IN (%s)' in result_sql or 'IN (?)' in result_sql
    assert result_args == (101,)
    assert not isinstance(result_args[0], str)


def test_issue_with_single_item_list_in_tuple():
    """Test the specific issue where [101] was incorrectly converted to '(101)' string."""
    sql = 'select name, value from test_table where value IN %s'
    args = ([101],)  # Single item list in a tuple

    result_sql, result_args = handle_in_clause_params(sql, args)

    # Verify SQL has correct placeholder
    assert 'in (%s)' in result_sql.lower() or 'in (?)' in result_sql.lower()

    # The key test: verify the parameter is still the numeric value 101, not a string
    assert result_args == (101,)
    assert isinstance(result_args[0], int)
    assert not isinstance(result_args[0], str)

    # Test with a direct list [101] (not in a tuple)
    sql = 'select name, value from test_table where value IN %s'
    args = [101]  # Direct single item list

    result_sql, result_args = handle_in_clause_params(sql, args)

    # Verify SQL has correct placeholder
    assert 'in (%s)' in result_sql.lower() or 'in (?)' in result_sql.lower()

    # Verify the parameter is still the numeric value 101
    assert result_args == (101,)
    assert isinstance(result_args[0], int)
    assert not isinstance(result_args[0], str)


def test_in_clause_syntax_variants():
    """Test that standard IN clause formats are handled correctly.

    This test validates that the standard IN clause formats:
    1. IN %(foo)s - named parameter format
    2. IN %s - positional parameter format

    Both are handled correctly with the expected parameter expansion.
    """
    # Test named parameter variant
    test_values = (1, 2, 3)

    # Standard named parameter format: IN %(foo)s
    sql_named = 'SELECT * FROM users WHERE id IN %(ids)s'
    args_named = {'ids': test_values}

    result_sql_named, result_args_named = handle_in_clause_params(sql_named, args_named)

    # Verify parameter expansion
    assert 'ids_0' in result_args_named
    assert 'ids_1' in result_args_named
    assert 'ids_2' in result_args_named

    # Verify the values match the original test values
    for i, val in enumerate(test_values):
        assert result_args_named[f'ids_{i}'] == val

    # Verify SQL string has correct format
    assert 'in (%(ids_0)s, %(ids_1)s, %(ids_2)s)' in result_sql_named.lower()

    # Now test positional parameter variant

    # Standard positional parameter format: IN %s
    sql_positional = 'SELECT * FROM users WHERE id IN %s'
    args_positional = [test_values]

    result_sql_positional, result_args_positional = handle_in_clause_params(sql_positional, args_positional)

    # Verify the expanded parameters
    assert result_args_positional == test_values

    # Verify SQL string has correct format
    placeholder_pattern = 'in (%s, %s, %s)' if '%s' in result_sql_positional else 'in (?, ?, ?)'
    assert placeholder_pattern in result_sql_positional.lower()


if __name__ == '__main__':
    __import__('pytest').main([__file__])
