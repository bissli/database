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
    # This simulates the error that occurred in test_transaction_retry
    conn = None  # We don't need an actual connection for this test

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


if __name__ == '__main__':
    __import__('pytest').main([__file__])
