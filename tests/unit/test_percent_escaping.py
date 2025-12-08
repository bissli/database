from database.sql import escape_percent_signs_in_literals


def test_escape_percent_signs_in_literals_basic():
    """Test basic string literal percent sign escaping"""
    # Simple LIKE pattern with single quotes
    sql = "SELECT * FROM users WHERE name LIKE 'test%'"
    result = escape_percent_signs_in_literals(sql)
    assert result == "SELECT * FROM users WHERE name LIKE 'test%%'"

    # Simple LIKE pattern with double quotes
    sql = 'SELECT * FROM users WHERE name LIKE "test%"'
    result = escape_percent_signs_in_literals(sql)
    assert result == 'SELECT * FROM users WHERE name LIKE "test%%"'


def test_escape_percent_signs_in_literals_multiple():
    """Test string literal percent sign escaping with multiple patterns"""
    # Multiple LIKE patterns
    sql = "SELECT * FROM users WHERE name LIKE 'test%' OR email LIKE 'example%'"
    result = escape_percent_signs_in_literals(sql)
    assert result == "SELECT * FROM users WHERE name LIKE 'test%%' OR email LIKE 'example%%'"


def test_escape_percent_signs_in_literals_case_insensitive():
    """Test string literal percent sign escaping with different cases"""
    # Mixed case LIKE
    sql = "SELECT * FROM users WHERE name like 'test%'"
    result = escape_percent_signs_in_literals(sql)
    assert result == "SELECT * FROM users WHERE name like 'test%%'"


def test_escape_percent_signs_in_literals_with_params():
    """Test string literal escaping doesn't affect parameter placeholders"""
    # With parameter placeholders
    sql = "SELECT * FROM users WHERE name LIKE 'prefix%' AND age > %s"
    result = escape_percent_signs_in_literals(sql)
    assert result == "SELECT * FROM users WHERE name LIKE 'prefix%%' AND age > %s"

    # With named parameters
    sql = "SELECT * FROM users WHERE name LIKE 'prefix%' AND age > %(age)s"
    result = escape_percent_signs_in_literals(sql)
    assert result == "SELECT * FROM users WHERE name LIKE 'prefix%%' AND age > %(age)s"


def test_escape_percent_signs_in_literals_no_percents():
    """Test SQL without percent signs isn't modified"""
    # No percent signs
    sql = 'SELECT * FROM users WHERE age > 30'
    result = escape_percent_signs_in_literals(sql)
    assert result == sql


def test_escape_percent_signs_in_literals_dynamic_patterns(create_simple_mock_connection):
    """Test real-world scenario with parameter and dynamic LIKE pattern."""
    from database.utils.connection_utils import get_dialect_name

    pg_conn = create_simple_mock_connection('postgresql')

    sql = "SELECT name, value FROM test_table WHERE name LIKE 'RetryTest%'"

    escaped_sql = escape_percent_signs_in_literals(sql)

    assert escaped_sql == "SELECT name, value FROM test_table WHERE name LIKE 'RetryTest%%'"
    assert '%%' in escaped_sql
    assert '%%%' not in escaped_sql  # Ensure we don't over-escape
    assert get_dialect_name(pg_conn) == 'postgresql'


def test_regex_pattern_not_affected_by_escaping():
    """Test that regex patterns with ? are not affected by string literal escaping"""
    # SQL with regex pattern that should not be affected
    sql = r"""
    SELECT *
    FROM table1 t1
    JOIN table2 t2 ON t1.id = t2.id
    WHERE t1.status = 'active'
    AND regexp_replace(t1.code, '\/?[UV]? ?(CN|US)?$', '') = 'ABC'
    """

    # Process with the escaping function
    result = escape_percent_signs_in_literals(sql)

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

    result = escape_percent_signs_in_literals(sql_with_like)

    # Verify LIKE pattern is escaped but regex remains intact
    assert "LIKE 'act%%'" in result
    assert r'\/?[UV]? ?(CN|US)?$' in result


def test_full_query_with_regex_and_transaction(create_simple_mock_connection):
    """Test a full query with regex pattern in a transaction-like context."""
    from database.sql import process_query_parameters

    pg_conn = create_simple_mock_connection('postgresql')

    sql = r"""
    SELECT t1.id, t1.name,
           regexp_replace(t1.code, '\/?[UV]? ?(CN|US)?$', '') as clean_code
    FROM table1 t1
    JOIN table2 t2 ON t1.id = t2.id
    WHERE t1.date = %s
    AND t2.status NOT IN ('inactive', 'deleted')
    """

    processed_sql, processed_args = process_query_parameters(pg_conn, sql, ['2023-01-01'])

    assert r'\/?[UV]? ?(CN|US)?$' in processed_sql
    assert '%s' in processed_sql  # Parameters should still be %s for postgres


def test_string_literals_with_percent_percent_capital_s():
    """Test string literals with '%%S' pattern that could be confused with placeholders"""
    # SQL with potential confusing pattern of %% followed by capital S
    sql = "SELECT * FROM documents WHERE status LIKE '%%Saved' AND author_id = %s"

    # Apply our escaping function
    result = escape_percent_signs_in_literals(sql)

    # The %%Saved shouldn't be changed (it's already properly escaped)
    assert "LIKE '%%Saved'" in result

    # The parameter placeholder should remain intact
    assert 'author_id = %s' in result

    # Add another test case with different quote style
    sql2 = 'SELECT * FROM documents WHERE status LIKE "%%Saved" AND author_id = %s'
    result2 = escape_percent_signs_in_literals(sql2)

    assert 'LIKE "%%Saved"' in result2
    assert 'author_id = %s' in result2

    # Test mixed escaped and unescaped patterns
    sql3 = "SELECT * FROM documents WHERE status LIKE '%%Saved%' AND author_id = %s"
    result3 = escape_percent_signs_in_literals(sql3)

    # The %%Saved shouldn't be changed, but the single % should be escaped
    assert "LIKE '%%Saved%%'" in result3


if __name__ == '__main__':
    __import__('pytest').main([__file__])
