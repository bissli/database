"""
Unit tests for SQL generation utilities.
"""
from unittest.mock import patch

import pytest
from database.utils.sql_generation import build_insert_sql, build_select_sql


@pytest.mark.parametrize(('dialect', 'table', 'expected'), [
    ('postgresql', 'users', 'SELECT * FROM "users"'),
    ('sqlite', 'users', 'SELECT * FROM "users"'),
])
def test_simple_select(dialect, table, expected):
    """Test basic SELECT statement generation"""
    with patch('database.sql.quote_identifier') as mock_quote:
        mock_quote.side_effect = lambda ident, dialect: f'"{ident}"'

        result = build_select_sql(table, dialect)
        assert expected in result


@pytest.mark.parametrize(('dialect', 'table', 'columns', 'expected'), [
    ('postgresql', 'users', ['id', 'name'], 'SELECT "id", "name" FROM "users"'),
    ('sqlite', 'users', ['id', 'name'], 'SELECT "id", "name" FROM "users"'),
])
def test_select_with_columns(dialect, table, columns, expected):
    """Test SELECT with column specifications"""
    with patch('database.sql.quote_identifier') as mock_quote:
        mock_quote.side_effect = lambda ident, dialect: f'"{ident}"'

        result = build_select_sql(table, dialect, columns=columns)
        assert expected in result


def test_select_with_where():
    """Test SELECT with WHERE clause"""
    with patch('database.sql.quote_identifier') as mock_quote:
        mock_quote.side_effect = lambda ident, dialect: f'"{ident}"'

        result = build_select_sql('users', 'postgresql',  where='active = TRUE')
        assert 'WHERE active = TRUE' in result


def test_select_with_limit():
    """Test SELECT with LIMIT clause"""
    # PostgreSQL uses LIMIT
    with patch('database.sql.quote_identifier') as mock_quote:
        mock_quote.side_effect = lambda ident, dialect: f'"{ident}"'

        result = build_select_sql('users', 'postgresql', limit=10)
        assert 'LIMIT 10' in result


def test_insert_sql_generation():
    """Test INSERT SQL generation"""
    # PostgreSQL
    with patch('database.sql.quote_identifier') as mock_quote:
        mock_quote.side_effect = lambda ident, dialect: f'"{ident}"'

        result = build_insert_sql(dialect='postgresql', table='users', columns=['id', 'name'])
        assert 'INSERT INTO "users" ("id", "name")' in result
        assert 'VALUES (%s, %s)' in result

    # SQLite
    with patch('database.sql.quote_identifier') as mock_quote:
        mock_quote.side_effect = lambda ident, dialect: f'"{ident}"'

        result = build_insert_sql(dialect='sqlite', table='users', columns=['id', 'name'])
        assert 'INSERT INTO "users" ("id", "name")' in result
        assert 'VALUES (?, ?)' in result


class TestSQLGeneration:
    """Tests for SQL generation functions across database types"""

    @pytest.mark.parametrize(('dialect', 'expected_sql'), [
        ('postgresql', 'SELECT "id", "name" FROM "users" WHERE active = true ORDER BY "name" LIMIT 10'),
        ('sqlite', 'SELECT "id", "name" FROM "users" WHERE active = true ORDER BY "name" LIMIT 10')
    ])
    def test_build_select_sql(self, dialect, expected_sql):
        """Test SELECT statement generation for different database types"""
        # Mock quote_identifier to use the expected format for each dialect
        with patch('database.utils.sql_generation.quote_identifier') as mock_quote:
            mock_quote.side_effect = lambda ident, dialect: f'"{ident}"'

            sql = build_select_sql('users', dialect, columns=['id', 'name'], where='active = true',
                                   order_by='"name"', limit=10)

            assert sql == expected_sql

    @pytest.mark.parametrize(('dialect', 'placeholder'), [
        ('postgresql', '%s'),
        ('sqlite', '?')
    ])
    def test_build_insert_sql(self, dialect, placeholder):
        """Test INSERT statement generation with appropriate placeholders"""
        # Mock quote_identifier to use the expected format
        with patch('database.utils.sql_generation.quote_identifier') as mock_quote:
            mock_quote.side_effect = lambda ident, dialect: f'"{ident}"'

            sql = build_insert_sql(
                dialect=dialect,
                table='users',
                columns=['id', 'name', 'email']
            )

            # Verify the SQL has the correct structure
            assert 'INSERT INTO "users"' in sql
            assert '"id", "name", "email"' in sql
            assert f'({placeholder}, {placeholder}, {placeholder})' in sql


if __name__ == '__main__':
    pytest.main([__file__])
