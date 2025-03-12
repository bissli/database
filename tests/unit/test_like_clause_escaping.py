from database.utils.sql import escape_like_clause_placeholders


def test_escape_like_clause_placeholders_basic():
    """Test basic LIKE clause escaping"""
    # Simple LIKE pattern with single quotes
    sql = "SELECT * FROM users WHERE name LIKE 'test%'"
    result = escape_like_clause_placeholders(sql)
    assert result == "SELECT * FROM users WHERE name LIKE 'test%%'"

    # Simple LIKE pattern with double quotes
    sql = 'SELECT * FROM users WHERE name LIKE "test%"'
    result = escape_like_clause_placeholders(sql)
    assert result == 'SELECT * FROM users WHERE name LIKE "test%%"'


def test_escape_like_clause_placeholders_multiple():
    """Test LIKE clause escaping with multiple patterns"""
    # Multiple LIKE patterns
    sql = "SELECT * FROM users WHERE name LIKE 'test%' OR email LIKE 'example%'"
    result = escape_like_clause_placeholders(sql)
    assert result == "SELECT * FROM users WHERE name LIKE 'test%%' OR email LIKE 'example%%'"


def test_escape_like_clause_placeholders_case_insensitive():
    """Test LIKE clause escaping with case insensitivity"""
    # Mixed case LIKE
    sql = "SELECT * FROM users WHERE name like 'test%'"
    result = escape_like_clause_placeholders(sql)
    assert result == "SELECT * FROM users WHERE name like 'test%%'"


def test_escape_like_clause_placeholders_with_params():
    """Test LIKE clause escaping doesn't affect parameter placeholders"""
    # With parameter placeholders
    sql = "SELECT * FROM users WHERE name LIKE 'prefix%' AND age > %s"
    result = escape_like_clause_placeholders(sql)
    assert result == "SELECT * FROM users WHERE name LIKE 'prefix%%' AND age > %s"

    # With named parameters
    sql = "SELECT * FROM users WHERE name LIKE 'prefix%' AND age > %(age)s"
    result = escape_like_clause_placeholders(sql)
    assert result == "SELECT * FROM users WHERE name LIKE 'prefix%%' AND age > %(age)s"


def test_escape_like_clause_placeholders_no_like():
    """Test SQL without LIKE clauses isn't modified"""
    # No LIKE clause
    sql = 'SELECT * FROM users WHERE age > 30'
    result = escape_like_clause_placeholders(sql)
    assert result == sql


def test_escape_like_clause_placeholders_dynamic_patterns():
    """Test real-world scenario with parameter and dynamic LIKE pattern"""
    # Create mock connections for each database type

    from database.utils.connection_utils import is_psycopg_connection
    from tests.fixtures.mocks import _create_simple_mock_connection

    # Create a proper mock connection
    pg_conn = _create_simple_mock_connection('postgresql')

    # Original problematic query
    sql = "SELECT name, value FROM test_table WHERE name LIKE 'RetryTest%'"

    # Apply our escaping function
    escaped_sql = escape_like_clause_placeholders(sql)

    # Verify it's properly escaped
    assert escaped_sql == "SELECT name, value FROM test_table WHERE name LIKE 'RetryTest%%'"

    # Verify that after escaping it would work with the database
    # (We can't actually execute it without a connection, but we can check the format)
    assert '%%' in escaped_sql
    assert '%%%' not in escaped_sql  # Ensure we don't over-escape

    # Verify we can correctly identify the connection type
    assert is_psycopg_connection(pg_conn)


def test_regex_pattern_not_affected_by_escaping():
    """Test that regex patterns with ? are not affected by LIKE clause escaping"""
    # SQL with regex pattern that should not be affected
    sql = r"""
    SELECT *
    FROM table1 t1
    JOIN table2 t2 ON t1.id = t2.id
    WHERE t1.status = 'active'
    AND regexp_replace(t1.code, '\/?[UV]? ?(CN|US)?$', '') = 'ABC'
    """

    # This shouldn't change the SQL since there's no LIKE clause
    result = escape_like_clause_placeholders(sql)

    # Verify regex pattern remains intact
    assert r'\/?[UV]? ?(CN|US)?$' in result

    # Test with actual LIKE clause elsewhere in the query
    sql_with_like = r"""
    SELECT *
    FROM table1 t1
    JOIN table2 t2 ON t1.id = t2.id
    WHERE t1.status LIKE 'act%'
    AND regexp_replace(t1.code, '\/?[UV]? ?(CN|US)?$', '') = 'ABC'
    """

    result = escape_like_clause_placeholders(sql_with_like)

    # Verify LIKE pattern is escaped but regex remains intact
    assert "LIKE 'act%%'" in result
    assert r'\/?[UV]? ?(CN|US)?$' in result


def test_full_query_with_regex_and_transaction():
    """Test a full query with regex pattern in a transaction-like context"""
    from database.utils.sql import process_query_parameters
    from tests.fixtures.mocks import _create_simple_mock_connection

    pg_conn = _create_simple_mock_connection('postgresql')

    # Complex query with regex patterns similar to the reported issue
    sql = r"""
    SELECT t1.id, t1.name,
           regexp_replace(t1.code, '\/?[UV]? ?(CN|US)?$', '') as clean_code
    FROM table1 t1
    JOIN table2 t2 ON t1.id = t2.id
    WHERE t1.date = %s
    AND t2.status NOT IN ('inactive', 'deleted')
    """

    # Process with the full parameter handling pipeline
    processed_sql, processed_args = process_query_parameters(pg_conn, sql, ['2023-01-01'])

    # Verify regex pattern remains intact
    assert r'\/?[UV]? ?(CN|US)?$' in processed_sql
    assert '%s' in processed_sql  # Parameters should still be %s for postgres


if __name__ == '__main__':
    __import__('pytest').main([__file__])
