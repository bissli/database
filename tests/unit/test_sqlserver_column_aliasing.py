"""
Unit tests for SQL Server column aliasing functionality.

With ODBC Driver 18 for SQL Server, column names are preserved correctly without truncation,
so aliasing is only needed for expressions without names (like COUNT(*) or calculations).
"""
import re

from database.utils.sqlserver_utils import add_explicit_column_aliases
from database.utils.sqlserver_utils import generate_expression_alias
from database.utils.sqlserver_utils import parse_sql_columns


def test_simple_column_aliasing():
    """Test that simple columns don't need aliases with Driver 18+"""
    # Simple query with column references - should be unchanged
    sql = 'SELECT id, name, value FROM test_table'
    result = add_explicit_column_aliases(sql)
    # With Driver 18+, we don't need to alias simple column references
    assert result == sql


def test_qualified_column_aliasing():
    """Test that table-qualified columns don't need aliases with Driver 18+"""
    # Table-qualified column references - should be unchanged
    sql = 'SELECT t.id, t.name, t.value FROM test_table t'
    result = add_explicit_column_aliases(sql)
    # With Driver 18+, we don't need to alias qualified column references
    assert result == sql


def test_expression_aliasing():
    """Test aliasing for expressions and function calls"""
    # Complex expressions - only these need aliases with Driver 18+
    sql = "SELECT id, COUNT(*), SUM(value), name + ' ' + value FROM test_table"
    result = add_explicit_column_aliases(sql)

    # Regular columns don't need aliases
    assert 'id,' in result  # No alias for regular column
    # But expressions still need aliases
    assert 'COUNT(*) AS [count_result]' in result
    assert 'SUM(value) AS [sum_result]' in result
    assert re.search("name \\+ ' ' \\+ value AS \\[calc_result\\]", result)


def test_existing_aliases_preserved():
    """Test that existing aliases are preserved"""
    # Query with explicit aliases
    sql = 'SELECT id AS user_id, name AS user_name FROM users'
    result = add_explicit_column_aliases(sql)
    assert result == 'SELECT id AS user_id, name AS user_name FROM users'


def test_case_statement_with_alias():
    """Test that CASE statements with aliases are preserved"""
    # CASE statement with an alias
    sql = "SELECT id, CASE WHEN value > 10 THEN 'High' ELSE 'Low' END AS level FROM test_table"
    result = add_explicit_column_aliases(sql)
    assert result == sql  # Should be unchanged with Driver 18+


def test_expression_with_alias():
    """Test that expressions with aliases are preserved"""
    # Various expressions with aliases
    sql = "SELECT SUM(value) AS total, price * quantity AS subtotal, name + ' ' + description AS full_desc FROM products"
    result = add_explicit_column_aliases(sql)
    assert result == sql  # Should be unchanged with Driver 18+


def test_generate_expression_alias():
    """Test alias generation for complex expressions"""

    # Function call - these need aliases with Driver 18+
    assert generate_expression_alias('COUNT(*)') == 'count_result'
    assert generate_expression_alias('SUM(value)') == 'sum_result'

    # Mathematical expressions - need aliases
    assert generate_expression_alias('price * quantity') == 'calc_result'
    assert generate_expression_alias('price + tax') == 'calc_result'

    # CASE expressions - need aliases
    assert generate_expression_alias("CASE WHEN value > 10 THEN 'High' ELSE 'Low' END") == 'case_result'

    # Subquery - needs alias
    assert generate_expression_alias('(SELECT MAX(value) FROM details)') == 'subquery_result'

    # Other expressions should get hash-based names
    expr_alias = generate_expression_alias('some_complex_expression()')
    assert expr_alias.startswith('expr_')
    assert len(expr_alias) == 13  # "expr_" + 8 char hash


def test_union_query():
    """Test handling of UNION queries"""
    # Simple UNION query
    sql = """
    SELECT id, name FROM table1
    UNION
    SELECT id, description FROM table2
    """
    result = add_explicit_column_aliases(sql)

    # With Driver 18+, no need to alias simple columns, even in first part of UNION
    # Both parts should remain unchanged
    assert 'SELECT id, name FROM table1' in result
    assert 'UNION' in result
    assert 'SELECT id, description FROM table2' in result


def test_complex_union():
    """Test handling of complex UNION ALL queries"""
    # More complex UNION ALL with expressions
    sql = """
    SELECT id, COUNT(*) FROM table1 GROUP BY id
    UNION ALL
    SELECT id, SUM(value) FROM table2 GROUP BY id
    """
    result = add_explicit_column_aliases(sql)

    # First SELECT should have expression alias but not simple column alias
    assert 'id,' in result  # Simple column doesn't need an alias with Driver 18+
    assert 'COUNT(*) AS [count_result]' in result

    # UNION ALL should be preserved
    assert 'UNION ALL' in result

    # Second SELECT should be unchanged
    assert 'SELECT id, SUM(value) FROM table2 GROUP BY id' in result


def test_cte_query():
    """Test handling of queries with CTEs (WITH clause)"""
    # Query with CTE
    sql = """
    WITH cte_data AS (
        SELECT id, SUM(value) AS total FROM source_table GROUP BY id
    )
    SELECT c.id, c.total, t.name
    FROM cte_data c
    JOIN lookup_table t ON c.id = t.id
    """
    result = add_explicit_column_aliases(sql)

    # CTE part should be unchanged
    assert 'WITH cte_data AS (' in result
    assert 'SELECT id, SUM(value) AS total FROM source_table GROUP BY id' in result

    # Main query should be unchanged with Driver 18+ (simple columns don't need aliases)
    assert 'SELECT c.id, c.total, t.name' in result


def test_subquery_in_from():
    """Test handling of subqueries in the FROM clause"""
    # Query with subquery in FROM
    sql = """
    SELECT a.col1, b.col2
    FROM table1 a
    JOIN (SELECT id, MAX(value) as max_val FROM table2 GROUP BY id) b
    ON a.id = b.id
    """
    result = add_explicit_column_aliases(sql)

    # Main query should be unchanged with Driver 18+ (simple col refs)
    assert 'SELECT a.col1, b.col2' in result

    # Subquery should be unchanged
    assert 'SELECT id, MAX(value) as max_val FROM table2 GROUP BY id' in result


def test_complex_nested_query():
    """Test handling of complex nested queries"""
    # Complex nested query
    sql = """
    WITH summary AS (
        SELECT
            department_id,
            AVG(salary) AS avg_salary
        FROM employees
        GROUP BY department_id
    )
    SELECT
        d.name,
        s.avg_salary,
        (SELECT COUNT(*) FROM employees e WHERE e.department_id = d.id) AS employee_count
    FROM
        departments d
    JOIN
        summary s ON d.id = s.department_id
    WHERE
        s.avg_salary > (SELECT AVG(salary) FROM employees)
    """
    result = add_explicit_column_aliases(sql)

    # CTE part should be unchanged
    assert 'WITH summary AS (' in result
    assert 'AVG(salary) AS avg_salary' in result

    # Main query should be unchanged for simple columns with Driver 18+
    assert 'd.name,' in result
    assert 's.avg_salary,' in result

    # Existing aliases should be preserved
    assert 'AS employee_count' in result


def test_multi_line_statement_aliasing():
    """Test aliasing multi-line column expressions"""
    # SQL with multi-line expressions
    sql = """
    SELECT
        id,
        CASE
            WHEN value > 100 THEN 'High'
            WHEN value > 50 THEN 'Medium'
            ELSE 'Low'
        END,
        (
            SELECT MAX(price)
            FROM products
            WHERE category_id = c.id
        )
    FROM
        categories c
    """
    result = add_explicit_column_aliases(sql)

    # Check for correct aliasing - id is a simple column and shouldn't need alias with Driver 18+
    assert 'id,' in result
    assert re.search(r'CASE\s+WHEN value > 100 THEN .+?END AS \[case_result\]',
                     result, re.DOTALL)

    # We have two valid patterns to check:
    # 1. The entire subquery is aliased (preferred)
    # 2. The MAX(price) inside the subquery is aliased
    subquery_pattern1 = r'\(\s*SELECT\s+MAX\(price\).+?\)\s+AS\s+\[subquery_result\]'
    subquery_pattern2 = r'\(\s*SELECT\s+MAX\(price\)\s+AS\s+\[subquery_result\].+?\)'

    assert (re.search(subquery_pattern1, result, re.DOTALL) or
            re.search(subquery_pattern2, result, re.DOTALL)), f'Failed to find subquery aliasing pattern.\nActual result:\n{result}'


def test_multiple_subquery_patterns():
    """Test handling of subqueries in different formats"""
    # Test inline subquery
    inline_sql = 'SELECT id, (SELECT MAX(value) FROM values WHERE id=t.id) FROM table t'
    inline_result = add_explicit_column_aliases(inline_sql)
    # Should be aliased as a whole expression
    assert re.search(r'\(SELECT MAX\(value\).+?\)\s+AS\s+\[subquery_result\]', inline_result) or \
    re.search(r'\(SELECT MAX\(value\)\s+AS\s+\[.+?\].+?\)', inline_result)

    # Test multi-line subquery with indentation
    multiline_sql = """
    SELECT
        id,
        (
            SELECT COUNT(*)
            FROM related
            WHERE parent_id = main.id
        )
    FROM main
    """
    multiline_result = add_explicit_column_aliases(multiline_sql)
    # Should be aliased appropriately
    assert re.search(r'\(\s*SELECT\s+COUNT\(\*\).+?\)\s+AS\s+\[subquery_result\]', multiline_result, re.DOTALL) or \
    re.search(r'\(\s*SELECT\s+COUNT\(\*\)\s+AS\s+\[.+?\].+?\)', multiline_result, re.DOTALL)


def test_parse_sql_columns():
    """Test parsing SQL column expressions"""
    # Simple columns
    cols = parse_sql_columns('id, name, value')
    assert cols == ['id', 'name', 'value']

    # Function calls
    cols = parse_sql_columns('id, COUNT(*), SUM(value)')
    assert cols == ['id', 'COUNT(*)', 'SUM(value)']

    # Nested functions
    cols = parse_sql_columns('id, CONVERT(VARCHAR, GETDATE())')
    assert cols == ['id', 'CONVERT(VARCHAR, GETDATE())']

    # Quoted strings
    cols = parse_sql_columns("id, 'literal, with comma', name")
    assert cols == ['id', "'literal, with comma'", 'name']

    # Complex expression with quotes and nested parentheses
    cols = parse_sql_columns("CASE WHEN id > 10 THEN 'High, Value' ELSE 'Low, Value' END, name")
    assert cols == ["CASE WHEN id > 10 THEN 'High, Value' ELSE 'Low, Value' END", 'name']


if __name__ == '__main__':
    __import__('pytest').main([__file__])
