"""
Tests for SQL IN clause parameter handling.

These tests verify the behavior of the handle_in_clause_params function
which is responsible for expanding parameters in SQL IN clauses.
"""
import pytest
from database.utils.sql import handle_in_clause_params
from tests.utils import assert_contains_lower


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
    assert result_sql == sql
    assert list(result_args) == args  # Convert tuple to list for comparison


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


if __name__ == '__main__':
    pytest.main([__file__])
