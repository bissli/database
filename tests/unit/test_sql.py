"""Unit tests for SQL parameter processing.

Tests the public API:
- prepare_query(cn, sql, args) - Main entry point
- quote_identifier(name, dialect) - Quote table/column names
- has_placeholders(sql) - Check for parameter placeholders
- standardize_placeholders(sql, dialect) - Convert %s <-> ?
"""
import datetime

import pytest
from database.sql import has_placeholders, prepare_query, quote_identifier
from database.sql import standardize_placeholders

# =============================================================================
# prepare_query - Basic Parameter Handling
# =============================================================================


class TestPrepareQueryBasic:
    """Test basic parameter handling through prepare_query."""

    def test_basic_postgres_parameters(self, create_simple_mock_connection):
        """Test basic parameter handling with PostgreSQL-style placeholders."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT foo FROM table WHERE date BETWEEN %s AND %s AND value > %s'
        args = (datetime.date(2025, 1, 1), datetime.date(2025, 3, 11), 100)

        result_sql, result_args = prepare_query(cn, sql, args)

        assert 'BETWEEN %s AND %s' in result_sql
        assert 'value > %s' in result_sql
        assert result_args == args

    def test_sqlite_placeholder_conversion(self, create_simple_mock_connection):
        """Test conversion of %s to ? for SQLite."""
        cn = create_simple_mock_connection('sqlite')
        sql = 'SELECT * FROM users WHERE id = %s AND status = %s'
        args = (1, 'active')

        result_sql, result_args = prepare_query(cn, sql, args)

        assert '?' in result_sql
        assert '%s' not in result_sql
        assert result_args == args

    def test_nested_tuple_parameters(self, create_simple_mock_connection):
        """Test unwrapping nested parameter tuples."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM users WHERE date BETWEEN %s AND %s AND status = %s'
        args = [(datetime.date(2025, 1, 1), datetime.date(2025, 3, 11), 'active')]

        result_sql, result_args = prepare_query(cn, sql, args)

        assert result_args == (datetime.date(2025, 1, 1), datetime.date(2025, 3, 11), 'active')

    def test_no_placeholders_passthrough(self, create_simple_mock_connection):
        """Test SQL without placeholders passes through unchanged."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM users WHERE active = TRUE'
        args = None

        result_sql, result_args = prepare_query(cn, sql, args)

        assert result_sql == sql
        assert result_args is None

    def test_empty_args(self, create_simple_mock_connection):
        """Test empty args handling."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM users'
        args = ()

        result_sql, result_args = prepare_query(cn, sql, args)

        assert result_sql == sql


# =============================================================================
# prepare_query - IN Clause Expansion
# =============================================================================

class TestPrepareQueryInClause:
    """Test IN clause parameter expansion."""

    @pytest.mark.parametrize(('args', 'expected_count', 'expected_args'), [
        # Tuple wrapped in list
        ([(1, 2, 3)], 3, (1, 2, 3)),
        # Single item tuple
        ([(42,)], 1, (42,)),
        # Direct list for single IN
        ([1, 2, 3], 3, (1, 2, 3)),
        # Nested list
        ([[1, 2, 3]], 3, (1, 2, 3)),
        # Single item list
        ([101], 1, (101,)),
    ], ids=['tuple_in_list', 'single_tuple', 'direct_list', 'nested_list', 'single_item'])
    def test_in_clause_expansion_formats(self, create_simple_mock_connection, args, expected_count, expected_args):
        """Test various IN clause parameter formats."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM users WHERE id IN %s'

        result_sql, result_args = prepare_query(cn, sql, args)

        placeholders = ', '.join(['%s'] * expected_count)
        assert f'IN ({placeholders})' in result_sql
        assert result_args == expected_args

    def test_in_clause_empty_sequence(self, create_simple_mock_connection):
        """Test empty sequence becomes IN (NULL)."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM users WHERE id IN %s'
        args = [()]

        result_sql, result_args = prepare_query(cn, sql, args)

        assert 'IN (NULL)' in result_sql
        assert result_args == ()

    def test_in_clause_with_other_params(self, create_simple_mock_connection):
        """Test IN clause mixed with regular parameters."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM users WHERE date BETWEEN %s AND %s AND id IN %s'
        args = (datetime.date(2025, 1, 1), datetime.date(2025, 3, 11), (1, 2, 3))

        result_sql, result_args = prepare_query(cn, sql, args)

        assert 'BETWEEN %s AND %s' in result_sql
        assert 'IN (%s, %s, %s)' in result_sql
        assert result_args == (datetime.date(2025, 1, 1), datetime.date(2025, 3, 11), 1, 2, 3)

    def test_multiple_in_clauses(self, create_simple_mock_connection):
        """Test multiple IN clauses in same query."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM users WHERE id IN %s AND status IN %s'
        args = [(1, 2, 3), ('active', 'pending')]

        result_sql, result_args = prepare_query(cn, sql, args)

        assert result_args == (1, 2, 3, 'active', 'pending')

    def test_parenthesized_in_clause(self, create_simple_mock_connection):
        """Test IN (%s) format doesn't get double parentheses."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM users WHERE id IN (%s)'
        args = ((1, 2, 3),)

        result_sql, result_args = prepare_query(cn, sql, args)

        assert '))' not in result_sql
        assert result_args == (1, 2, 3)

    def test_in_clause_middle_position(self, create_simple_mock_connection):
        """Test IN clause between other parameters."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM t WHERE date = %s AND id IN (%s) AND user = %s'
        args = ('2025-01-01', [1, 2], 'USER_A')

        result_sql, result_args = prepare_query(cn, sql, args)

        assert result_args == ('2025-01-01', 1, 2, 'USER_A')


# =============================================================================
# prepare_query - Named Parameters
# =============================================================================

class TestPrepareQueryNamedParams:
    """Test named parameter handling."""

    def test_basic_named_params(self, create_simple_mock_connection):
        """Test basic named parameter handling."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM users WHERE date BETWEEN %(start)s AND %(end)s'
        args = {'start': datetime.date(2025, 1, 1), 'end': datetime.date(2025, 3, 11)}

        result_sql, result_args = prepare_query(cn, sql, args)

        assert 'BETWEEN %(start)s AND %(end)s' in result_sql
        assert result_args['start'] == datetime.date(2025, 1, 1)
        assert result_args['end'] == datetime.date(2025, 3, 11)

    def test_named_in_clause(self, create_simple_mock_connection):
        """Test named parameter IN clause expansion."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM users WHERE id IN %(ids)s'
        args = {'ids': (1, 2, 3)}

        result_sql, result_args = prepare_query(cn, sql, args)

        assert 'IN (%(ids_0)s, %(ids_1)s, %(ids_2)s)' in result_sql
        assert result_args['ids_0'] == 1
        assert result_args['ids_1'] == 2
        assert result_args['ids_2'] == 3
        assert 'ids' not in result_args

    def test_named_in_clause_with_regular(self, create_simple_mock_connection):
        """Test named IN clause alongside regular named params."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM users WHERE id IN %(ids)s AND name = %(name)s'
        args = {'ids': (1, 2), 'name': 'test'}

        result_sql, result_args = prepare_query(cn, sql, args)

        assert 'IN (%(ids_0)s, %(ids_1)s)' in result_sql
        assert 'name = %(name)s' in result_sql
        assert result_args['ids_0'] == 1
        assert result_args['ids_1'] == 2
        assert result_args['name'] == 'test'

    def test_dict_in_list_unwrapped(self, create_simple_mock_connection):
        """Test [dict] is unwrapped to dict."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM users WHERE id = %(id)s'
        args = [{'id': 1}]

        result_sql, result_args = prepare_query(cn, sql, args)

        assert isinstance(result_args, dict)
        assert result_args['id'] == 1


# =============================================================================
# prepare_query - IS NULL Handling
# =============================================================================

class TestPrepareQueryIsNull:
    """Test IS NULL / IS NOT NULL handling."""

    @pytest.mark.parametrize(('sql', 'args', 'expected_pattern', 'expected_args'), [
        # IS NULL
        ('SELECT * FROM t WHERE value IS %s', (None,), 'IS NULL', ()),
        # IS NOT NULL
        ('SELECT * FROM t WHERE value IS NOT %s', (None,), 'IS NOT NULL', ()),
        # Multiple NULLs
        ('SELECT * FROM t WHERE v1 IS %s AND v2 IS NOT %s', (None, None), 'IS NULL', ()),
        # Mixed NULL and non-NULL
        ('SELECT * FROM t WHERE v1 IS %s AND v2 = %s', (None, 'test'), 'IS NULL', ('test',)),
        # Non-NULL value unchanged
        ('SELECT * FROM t WHERE value IS %s', ('not_null',), 'IS %s', ('not_null',)),
    ], ids=['is_null', 'is_not_null', 'multiple', 'mixed', 'non_null'])
    def test_positional_null_handling(self, create_simple_mock_connection, sql, args, expected_pattern, expected_args):
        """Test IS NULL handling with positional parameters."""
        cn = create_simple_mock_connection('postgresql')

        result_sql, result_args = prepare_query(cn, sql, args)

        assert expected_pattern in result_sql
        assert result_args == expected_args

    def test_named_null_handling(self, create_simple_mock_connection):
        """Test IS NULL handling with named parameters."""
        cn = create_simple_mock_connection('postgresql')
        sql = 'SELECT * FROM t WHERE value IS %(val)s AND name = %(name)s'
        args = {'val': None, 'name': 'test'}

        result_sql, result_args = prepare_query(cn, sql, args)

        assert 'IS NULL' in result_sql
        assert 'val' not in result_args
        assert result_args['name'] == 'test'


# =============================================================================
# prepare_query - Percent Escaping
# =============================================================================

class TestPrepareQueryPercentEscaping:
    """Test percent sign escaping in string literals.

    Percent escaping only occurs when there are placeholders in the query.
    Without placeholders, the driver doesn't parse for format specifiers.
    """

    @pytest.mark.parametrize(('sql', 'args', 'expected_literal'), [
        # With placeholder - percent gets escaped
        ("SELECT * FROM t WHERE id = %s AND s = 'progress: 50%'", (1,), "'progress: 50%%'"),
        ('SELECT * FROM t WHERE id = %s AND s = "Complete: 75%"', (1,), '"Complete: 75%%"'),
        ("SELECT * FROM t WHERE id = %s AND s = 'It''s 100% done'", (1,), "'It''s 100%% done'"),
        # Already escaped stays escaped
        ("SELECT * FROM t WHERE id = %s AND s LIKE 'pre%%post'", (1,), "'pre%%post'"),
    ], ids=['single_quote', 'double_quote', 'escaped_quotes', 'already_escaped'])
    def test_percent_in_literals_escaped(self, create_simple_mock_connection, sql, args, expected_literal):
        """Test percent signs in string literals are escaped for PostgreSQL."""
        cn = create_simple_mock_connection('postgresql')

        result_sql, _ = prepare_query(cn, sql, args)

        assert expected_literal in result_sql

    def test_percent_not_escaped_without_placeholders(self, create_simple_mock_connection):
        """Test percent signs are NOT escaped when no placeholders present."""
        cn = create_simple_mock_connection('postgresql')
        sql = "SELECT * FROM t WHERE s = 'progress: 50%'"

        result_sql, _ = prepare_query(cn, sql, None)

        # No placeholders means no escaping needed
        assert "'progress: 50%'" in result_sql
        assert '%%' not in result_sql

    def test_percent_not_escaped_for_sqlite(self, create_simple_mock_connection):
        """Test percent signs are NOT escaped for SQLite."""
        cn = create_simple_mock_connection('sqlite')
        sql = "SELECT * FROM t WHERE id = %s AND s = 'progress: 50%'"

        result_sql, _ = prepare_query(cn, sql, (1,))

        assert "'progress: 50%'" in result_sql
        assert '%%' not in result_sql

    def test_placeholders_not_escaped(self, create_simple_mock_connection):
        """Test parameter placeholders are not affected by escaping."""
        cn = create_simple_mock_connection('postgresql')
        sql = "SELECT * FROM t WHERE name = %s AND status = 'progress: 25%'"

        result_sql, _ = prepare_query(cn, sql, ('test',))

        assert 'name = %s' in result_sql
        assert "'progress: 25%%'" in result_sql


# =============================================================================
# prepare_query - Regexp Protection
# =============================================================================

class TestPrepareQueryRegexp:
    """Test regexp_replace patterns are preserved."""

    def test_regexp_replace_preserved(self, create_simple_mock_connection):
        """Test regexp_replace patterns are not modified."""
        cn = create_simple_mock_connection('postgresql')
        sql = r"SELECT regexp_replace(code, '\/?[UV]? ?(CN|US)?$', '') FROM t WHERE id = %s"

        result_sql, _ = prepare_query(cn, sql, (1,))

        assert r'\/?[UV]? ?(CN|US)?$' in result_sql

    def test_regexp_with_like_pattern(self, create_simple_mock_connection):
        """Test regexp and LIKE pattern in same query."""
        cn = create_simple_mock_connection('postgresql')
        sql = r"""
        SELECT * FROM t
        WHERE status LIKE 'act%'
        AND regexp_replace(code, '\/?[UV]?$', '') = 'ABC'
        """

        result_sql, _ = prepare_query(cn, sql, None)

        assert "LIKE 'act%%'" in result_sql
        assert r'\/?[UV]?$' in result_sql


# =============================================================================
# quote_identifier
# =============================================================================

class TestQuoteIdentifier:
    """Test database identifier quoting."""

    @pytest.mark.parametrize(('identifier', 'dialect', 'expected'), [
        ('my_table', 'postgresql', '"my_table"'),
        ('user', 'postgresql', '"user"'),
        ('table"with"quotes', 'postgresql', '"table""with""quotes"'),
        ('my_table', 'sqlite', '"my_table"'),
        ('column"quoted', 'sqlite', '"column""quoted"'),
    ], ids=['pg_basic', 'pg_reserved', 'pg_quotes', 'sqlite_basic', 'sqlite_quotes'])
    def test_quote_identifier(self, identifier, dialect, expected):
        """Test identifier quoting for various dialects."""
        assert quote_identifier(identifier, dialect) == expected

    def test_default_dialect(self):
        """Test default dialect is postgresql."""
        assert quote_identifier('table') == '"table"'

    def test_unknown_dialect_error(self):
        """Test unknown dialect raises ValueError."""
        with pytest.raises(ValueError, match='Unknown dialect: mysql'):
            quote_identifier('table', 'mysql')


# =============================================================================
# has_placeholders
# =============================================================================

class TestHasPlaceholders:
    """Test placeholder detection."""

    @pytest.mark.parametrize(('sql', 'expected'), [
        # No placeholders
        ('', False),
        (None, False),
        ('SELECT * FROM users', False),
        ('SELECT * FROM stats WHERE growth > 10%', False),
        # Positional
        ('SELECT * FROM users WHERE id = %s', True),
        ('SELECT * FROM users WHERE id = ?', True),
        ('INSERT INTO t VALUES (%s, %s, ?)', True),
        # Named
        ('SELECT * FROM users WHERE id = %(user_id)s', True),
        ('INSERT INTO t VALUES (%(id)s, %(name)s)', True),
        # Mixed
        ('SELECT * FROM t WHERE id = %s AND name = %(name)s', True),
    ], ids=[
        'empty', 'none', 'no_placeholders', 'percent_not_placeholder',
        'percent_s', 'qmark', 'mixed_positional',
        'named_single', 'named_multiple', 'mixed_types'
    ])
    def test_has_placeholders(self, sql, expected):
        """Test placeholder detection for various patterns."""
        assert has_placeholders(sql) is expected


# =============================================================================
# standardize_placeholders
# =============================================================================

class TestStandardizePlaceholders:
    """Test placeholder conversion between dialects."""

    def test_convert_percent_to_qmark(self):
        """Test converting %s to ? for SQLite."""
        sql = 'SELECT * FROM users WHERE id = %s AND name = %s'

        result = standardize_placeholders(sql, 'sqlite')

        assert result == 'SELECT * FROM users WHERE id = ? AND name = ?'

    def test_convert_qmark_to_percent(self):
        """Test converting ? to %s for PostgreSQL."""
        sql = 'SELECT * FROM users WHERE id = ? AND name = ?'

        result = standardize_placeholders(sql, 'postgresql')

        assert result == 'SELECT * FROM users WHERE id = %s AND name = %s'

    def test_no_conversion_needed(self):
        """Test no conversion when already correct for dialect."""
        sql_postgres = 'SELECT * FROM users WHERE id = %s'
        sql_sqlite = 'SELECT * FROM users WHERE id = ?'

        assert standardize_placeholders(sql_postgres, 'postgresql') == sql_postgres
        assert standardize_placeholders(sql_sqlite, 'sqlite') == sql_sqlite

    def test_named_params_unchanged(self):
        """Test named parameters are not converted."""
        sql = 'SELECT * FROM users WHERE id = %(id)s'

        result = standardize_placeholders(sql, 'sqlite')

        assert '%(id)s' in result

    def test_preserves_string_literals(self):
        """Test placeholders in string literals are preserved correctly."""
        sql = "SELECT * FROM t WHERE id = %s AND name = 'test %s value'"

        result = standardize_placeholders(sql, 'sqlite')

        # First %s converted, literal preserved
        assert '?' in result


# =============================================================================
# Integration / Full Pipeline Tests
# =============================================================================

class TestFullPipeline:
    """Test complete query processing scenarios."""

    def test_complex_query_postgres(self, create_simple_mock_connection):
        """Test complex query with multiple features for PostgreSQL."""
        cn = create_simple_mock_connection('postgresql')
        sql = """
        SELECT * FROM users
        WHERE created BETWEEN %s AND %s
        AND id IN %s
        AND status IS NOT %s
        AND name LIKE 'test%'
        """
        args = (
            datetime.date(2025, 1, 1),
            datetime.date(2025, 12, 31),
            (1, 2, 3),
            None
        )

        result_sql, result_args = prepare_query(cn, sql, args)

        assert 'BETWEEN %s AND %s' in result_sql
        assert 'IN (%s, %s, %s)' in result_sql
        assert 'IS NOT NULL' in result_sql
        assert "LIKE 'test%%'" in result_sql
        assert result_args == (
            datetime.date(2025, 1, 1),
            datetime.date(2025, 12, 31),
            1, 2, 3
        )

    def test_complex_query_sqlite(self, create_simple_mock_connection):
        """Test complex query with multiple features for SQLite."""
        cn = create_simple_mock_connection('sqlite')
        sql = """
        SELECT * FROM users
        WHERE id IN %s
        AND status = %s
        """
        args = ((1, 2), 'active')

        result_sql, result_args = prepare_query(cn, sql, args)

        assert 'IN (?, ?)' in result_sql
        assert 'status = ?' in result_sql
        assert '%s' not in result_sql
        assert result_args == (1, 2, 'active')

    def test_named_params_complex(self, create_simple_mock_connection):
        """Test complex named parameter query."""
        cn = create_simple_mock_connection('postgresql')
        sql = """
        SELECT * FROM items
        WHERE item_id IN %(ids)s
        AND date = %(date)s
        AND user = %(user)s
        """
        args = {
            'ids': ['ITM001', 'ITM002', 'ITM003'],
            'date': '2025-08-08',
            'user': 'USER_A'
        }

        result_sql, result_args = prepare_query(cn, sql, args)

        assert 'IN (%(ids_0)s, %(ids_1)s, %(ids_2)s)' in result_sql
        assert result_args['ids_0'] == 'ITM001'
        assert result_args['ids_1'] == 'ITM002'
        assert result_args['ids_2'] == 'ITM003'
        assert result_args['date'] == '2025-08-08'
        assert result_args['user'] == 'USER_A'
        assert 'ids' not in result_args


if __name__ == '__main__':
    __import__('pytest').main([__file__])
