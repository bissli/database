"""Unit tests for SQL parameter processing.

Tests the public API:
- prepare_query(sql, args, dialect) - Main entry point
- quote_identifier(name, dialect) - Quote table/column names
- has_placeholders(sql) - Check for parameter placeholders
- standardize_placeholders(sql, dialect) - Convert %s <-> ?
"""
import datetime

import pytest
from database.sql import has_placeholders, prepare_query, quote_identifier
from database.sql import standardize_placeholders


class TestPrepareQueryBasic:
    """Test basic parameter handling through prepare_query."""

    @pytest.mark.parametrize(('sql', 'args', 'dialect', 'expected_sql_check', 'expected_args_check'), [
        # PostgreSQL basic
        ('SELECT foo FROM table WHERE date BETWEEN %s AND %s AND value > %s',
         (datetime.date(2025, 1, 1), datetime.date(2025, 3, 11), 100),
         'postgresql',
         lambda s: 'BETWEEN %s AND %s' in s and 'value > %s' in s,
         lambda a: a == (datetime.date(2025, 1, 1), datetime.date(2025, 3, 11), 100)),
        # SQLite placeholder conversion
        ('SELECT * FROM users WHERE id = %s AND status = %s',
         (1, 'active'),
         'sqlite',
         lambda s: '?' in s and '%s' not in s,
         lambda a: a == (1, 'active')),
        # Nested tuple unwrapping
        ('SELECT * FROM users WHERE date BETWEEN %s AND %s AND status = %s',
         [(datetime.date(2025, 1, 1), datetime.date(2025, 3, 11), 'active')],
         'postgresql',
         lambda s: True,
         lambda a: a == (datetime.date(2025, 1, 1), datetime.date(2025, 3, 11), 'active')),
        # No placeholders passthrough
        ('SELECT * FROM users WHERE active = TRUE',
         None,
         'postgresql',
         lambda s: s == 'SELECT * FROM users WHERE active = TRUE',
         lambda a: a is None),
        # Empty args
        ('SELECT * FROM users',
         (),
         'postgresql',
         lambda s: s == 'SELECT * FROM users',
         lambda a: True),
    ], ids=['pg_basic', 'sqlite_conversion', 'nested_tuple', 'no_placeholders', 'empty_args'])
    def test_basic_handling(self, sql, args, dialect, expected_sql_check, expected_args_check):
        """Test basic parameter handling scenarios."""
        result_sql, result_args = prepare_query(sql, args, dialect)

        assert expected_sql_check(result_sql), f'SQL check failed: {result_sql}'
        assert expected_args_check(result_args), f'Args check failed: {result_args}'


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
    def test_in_clause_expansion_formats(self, args, expected_count, expected_args):
        """Test various IN clause parameter formats."""
        sql = 'SELECT * FROM users WHERE id IN %s'

        result_sql, result_args = prepare_query(sql, args, 'postgresql')

        placeholders = ', '.join(['%s'] * expected_count)
        assert f'IN ({placeholders})' in result_sql
        assert result_args == expected_args

    def test_in_clause_empty_sequence(self):
        """Test empty sequence becomes IN (NULL)."""
        sql = 'SELECT * FROM users WHERE id IN %s'
        args = [()]

        result_sql, result_args = prepare_query(sql, args, 'postgresql')

        assert 'IN (NULL)' in result_sql
        assert result_args == ()

    def test_in_clause_with_other_params(self):
        """Test IN clause mixed with regular parameters."""
        sql = 'SELECT * FROM users WHERE date BETWEEN %s AND %s AND id IN %s'
        args = (datetime.date(2025, 1, 1), datetime.date(2025, 3, 11), (1, 2, 3))

        result_sql, result_args = prepare_query(sql, args, 'postgresql')

        assert 'BETWEEN %s AND %s' in result_sql
        assert 'IN (%s, %s, %s)' in result_sql
        assert result_args == (datetime.date(2025, 1, 1), datetime.date(2025, 3, 11), 1, 2, 3)

    def test_multiple_in_clauses(self):
        """Test multiple IN clauses in same query."""
        sql = 'SELECT * FROM users WHERE id IN %s AND status IN %s'
        args = [(1, 2, 3), ('active', 'pending')]

        result_sql, result_args = prepare_query(sql, args, 'postgresql')

        assert result_args == (1, 2, 3, 'active', 'pending')

    def test_parenthesized_in_clause(self):
        """Test IN (%s) format doesn't get double parentheses."""
        sql = 'SELECT * FROM users WHERE id IN (%s)'
        args = ((1, 2, 3),)

        result_sql, result_args = prepare_query(sql, args, 'postgresql')

        assert '))' not in result_sql
        assert result_args == (1, 2, 3)

    def test_in_clause_middle_position(self):
        """Test IN clause between other parameters."""
        sql = 'SELECT * FROM t WHERE date = %s AND id IN (%s) AND user = %s'
        args = ('2025-01-01', [1, 2], 'USER_A')

        result_sql, result_args = prepare_query(sql, args, 'postgresql')

        assert result_args == ('2025-01-01', 1, 2, 'USER_A')

    def test_parenthesized_in_clause_preserves_closing_paren(self):
        """Test IN (%s) preserves closing paren in all cases.

        Regression test for bug where closing paren was stripped.
        """
        # Multiple values - should have closing paren
        sql = 'SELECT * FROM t WHERE id IN (%s)'
        result_sql, _ = prepare_query(sql, ((1, 2, 3),), 'postgresql')
        assert result_sql.endswith(')'), f'Missing closing paren: {result_sql}'
        assert 'IN (%s, %s, %s)' in result_sql

        # Single value list - should have closing paren
        result_sql, _ = prepare_query(sql, ([42],), 'postgresql')
        assert result_sql.endswith(')'), f'Missing closing paren: {result_sql}'
        assert 'IN (%s)' in result_sql

    def test_in_clause_with_scalar_value(self):
        """Test IN clause with scalar value (not a sequence).

        When a scalar is passed directly to an IN clause, it should still
        produce valid SQL with proper parentheses.
        """
        sql = 'SELECT * FROM t WHERE date = %s AND fund IN (%s)'
        args = ('2025-01-01', 'Tenor')  # 'Tenor' is scalar, not a list

        result_sql, result_args = prepare_query(sql, args, 'postgresql')

        # Should have closing paren after the IN clause
        assert 'IN (%s)' in result_sql, f'Missing proper IN clause: {result_sql}'
        # Should not be truncated
        assert result_sql.count('(') == result_sql.count(')'), f'Unbalanced parens: {result_sql}'
        assert result_args == ('2025-01-01', 'Tenor')

    def test_in_clause_with_single_element_list(self):
        """Test IN clause with single-element list."""
        sql = 'SELECT * FROM t WHERE date = %s AND fund IN (%s)'
        args = ('2025-01-01', ['Tenor'])

        result_sql, result_args = prepare_query(sql, args, 'postgresql')

        assert 'IN (%s)' in result_sql
        assert result_sql.count('(') == result_sql.count(')')
        assert result_args == ('2025-01-01', 'Tenor')


class TestPrepareQueryNamedParams:
    """Test named parameter handling."""

    @pytest.mark.parametrize(('sql', 'args', 'expected_sql_contains', 'expected_args_subset'), [
        # Basic named params
        ('SELECT * FROM users WHERE date BETWEEN %(start)s AND %(end)s',
         {'start': datetime.date(2025, 1, 1), 'end': datetime.date(2025, 3, 11)},
         'BETWEEN %(start)s AND %(end)s',
         {'start': datetime.date(2025, 1, 1), 'end': datetime.date(2025, 3, 11)}),
        # Named IN clause expansion
        ('SELECT * FROM users WHERE id IN %(ids)s',
         {'ids': (1, 2, 3)},
         'IN (%(ids_0)s, %(ids_1)s, %(ids_2)s)',
         {'ids_0': 1, 'ids_1': 2, 'ids_2': 3}),
        # Named IN with regular params
        ('SELECT * FROM users WHERE id IN %(ids)s AND name = %(name)s',
         {'ids': (1, 2), 'name': 'test'},
         'IN (%(ids_0)s, %(ids_1)s)',
         {'ids_0': 1, 'ids_1': 2, 'name': 'test'}),
        # Dict in list unwrapped
        ('SELECT * FROM users WHERE id = %(id)s',
         [{'id': 1}],
         '%(id)s',
         {'id': 1}),
    ], ids=['basic', 'in_clause', 'in_with_regular', 'dict_in_list'])
    def test_named_params(self, sql, args, expected_sql_contains, expected_args_subset):
        """Test named parameter handling scenarios."""
        result_sql, result_args = prepare_query(sql, args, 'postgresql')

        assert expected_sql_contains in result_sql
        assert isinstance(result_args, dict)
        for key, value in expected_args_subset.items():
            assert result_args.get(key) == value, f'Key {key}: expected {value}, got {result_args.get(key)}'


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
    def test_positional_null_handling(self, sql, args, expected_pattern, expected_args):
        """Test IS NULL handling with positional parameters."""
        result_sql, result_args = prepare_query(sql, args, 'postgresql')

        assert expected_pattern in result_sql
        assert result_args == expected_args

    def test_named_null_handling(self):
        """Test IS NULL handling with named parameters."""
        sql = 'SELECT * FROM t WHERE value IS %(val)s AND name = %(name)s'
        args = {'val': None, 'name': 'test'}

        result_sql, result_args = prepare_query(sql, args, 'postgresql')

        assert 'IS NULL' in result_sql
        assert 'val' not in result_args
        assert result_args['name'] == 'test'


class TestPrepareQueryPercentEscaping:
    """Test percent sign escaping in string literals.

    Percent escaping only occurs when there are placeholders in the query.
    Without placeholders, the driver doesn't parse for format specifiers.
    """

    @pytest.mark.parametrize(('sql', 'args', 'dialect', 'expected_contains', 'expected_not_contains'), [
        # PostgreSQL with placeholder - percent gets escaped
        ("SELECT * FROM t WHERE id = %s AND s = 'progress: 50%'",
         (1,), 'postgresql', "'progress: 50%%'", None),
        ('SELECT * FROM t WHERE id = %s AND s = "Complete: 75%"',
         (1,), 'postgresql', '"Complete: 75%%"', None),
        ("SELECT * FROM t WHERE id = %s AND s = 'It''s 100% done'",
         (1,), 'postgresql', "'It''s 100%% done'", None),
        # Already escaped stays escaped
        ("SELECT * FROM t WHERE id = %s AND s LIKE 'pre%%post'",
         (1,), 'postgresql', "'pre%%post'", None),
        # No placeholders means no escaping
        ("SELECT * FROM t WHERE s = 'progress: 50%'",
         None, 'postgresql', "'progress: 50%'", '%%'),
        # SQLite - no escaping even with placeholders
        ("SELECT * FROM t WHERE id = %s AND s = 'progress: 50%'",
         (1,), 'sqlite', "'progress: 50%'", '%%'),
        # Placeholders not affected by escaping
        ("SELECT * FROM t WHERE name = %s AND status = 'progress: 25%'",
         ('test',), 'postgresql', 'name = %s', None),
    ], ids=['pg_single_quote', 'pg_double_quote', 'pg_escaped_quotes', 'pg_already_escaped',
            'pg_no_placeholders', 'sqlite_no_escape', 'pg_placeholder_preserved'])
    def test_percent_escaping(self, sql, args, dialect, expected_contains, expected_not_contains):
        """Test percent sign escaping in various scenarios."""
        result_sql, _ = prepare_query(sql, args, dialect)

        assert expected_contains in result_sql
        if expected_not_contains:
            assert expected_not_contains not in result_sql


class TestPrepareQueryRegexp:
    """Test regexp_replace patterns are preserved."""

    @pytest.mark.parametrize(('sql', 'args', 'expected_preserved', 'expected_escaped'), [
        # Regexp pattern preserved
        (r"SELECT regexp_replace(code, '\/?[UV]? ?(CN|US)?$', '') FROM t WHERE id = %s",
         (1,), r'\/?[UV]? ?(CN|US)?$', None),
        # Regexp preserved while LIKE pattern escaped
        (r"""SELECT * FROM t WHERE status LIKE 'act%' AND regexp_replace(code, '\/?[UV]?$', '') = 'ABC'""",
         None, r'\/?[UV]?$', "LIKE 'act%%'"),
    ], ids=['regexp_preserved', 'regexp_with_like'])
    def test_regexp_protection(self, sql, args, expected_preserved, expected_escaped):
        """Test regexp_replace patterns are not modified."""
        result_sql, _ = prepare_query(sql, args, 'postgresql')

        assert expected_preserved in result_sql
        if expected_escaped:
            assert expected_escaped in result_sql


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


class TestStandardizePlaceholders:
    """Test placeholder conversion between dialects."""

    @pytest.mark.parametrize(('sql', 'dialect', 'expected'), [
        # %s to ? for SQLite
        ('SELECT * FROM users WHERE id = %s AND name = %s', 'sqlite',
         'SELECT * FROM users WHERE id = ? AND name = ?'),
        # ? to %s for PostgreSQL
        ('SELECT * FROM users WHERE id = ? AND name = ?', 'postgresql',
         'SELECT * FROM users WHERE id = %s AND name = %s'),
        # No conversion needed - already correct
        ('SELECT * FROM users WHERE id = %s', 'postgresql',
         'SELECT * FROM users WHERE id = %s'),
        ('SELECT * FROM users WHERE id = ?', 'sqlite',
         'SELECT * FROM users WHERE id = ?'),
    ], ids=['percent_to_qmark', 'qmark_to_percent', 'pg_no_change', 'sqlite_no_change'])
    def test_placeholder_conversion(self, sql, dialect, expected):
        """Test placeholder conversion between dialects."""
        assert standardize_placeholders(sql, dialect) == expected

    def test_named_params_unchanged(self):
        """Test named parameters are not converted."""
        sql = 'SELECT * FROM users WHERE id = %(id)s'
        result = standardize_placeholders(sql, 'sqlite')
        assert '%(id)s' in result

    def test_preserves_string_literals(self):
        """Test placeholders in string literals are preserved correctly."""
        sql = "SELECT * FROM t WHERE id = %s AND name = 'test %s value'"
        result = standardize_placeholders(sql, 'sqlite')
        assert '?' in result


class TestFullPipeline:
    """Test complete query processing scenarios."""

    @pytest.mark.parametrize(('sql', 'args', 'dialect', 'expected_sql_checks', 'expected_args'), [
        # PostgreSQL complex query
        ("SELECT * FROM users WHERE created BETWEEN %s AND %s AND id IN %s AND status IS NOT %s AND name LIKE 'test%'",
         (datetime.date(2025, 1, 1), datetime.date(2025, 12, 31), (1, 2, 3), None),
         'postgresql',
         ['BETWEEN %s AND %s', 'IN (%s, %s, %s)', 'IS NOT NULL', "LIKE 'test%%'"],
         (datetime.date(2025, 1, 1), datetime.date(2025, 12, 31), 1, 2, 3)),
        # SQLite complex query
        ('SELECT * FROM users WHERE id IN %s AND status = %s',
         ((1, 2), 'active'),
         'sqlite',
         ['IN (?, ?)', 'status = ?'],
         (1, 2, 'active')),
    ], ids=['postgresql_complex', 'sqlite_complex'])
    def test_complex_query(self, sql, args, dialect, expected_sql_checks, expected_args):
        """Test complex query with multiple features."""
        result_sql, result_args = prepare_query(sql, args, dialect)

        for check in expected_sql_checks:
            assert check in result_sql, f'Expected {check!r} in {result_sql!r}'
        assert result_args == expected_args

    def test_named_params_complex(self):
        """Test complex named parameter query."""
        sql = 'SELECT * FROM items WHERE item_id IN %(ids)s AND date = %(date)s AND user = %(user)s'
        args = {'ids': ['ITM001', 'ITM002', 'ITM003'], 'date': '2025-08-08', 'user': 'USER_A'}

        result_sql, result_args = prepare_query(sql, args, 'postgresql')

        assert 'IN (%(ids_0)s, %(ids_1)s, %(ids_2)s)' in result_sql
        assert result_args['ids_0'] == 'ITM001'
        assert result_args['ids_1'] == 'ITM002'
        assert result_args['ids_2'] == 'ITM003'
        assert result_args['date'] == '2025-08-08'
        assert result_args['user'] == 'USER_A'
        assert 'ids' not in result_args


class TestSQLMatrix:
    """Traceability matrix with explicit input → output mappings.

    Each case documents expected behavior for the clean rewrite.
    Format: (case_id, sql, args, dialect, expected_sql_contains, expected_args)
    """

    # All test cases as explicit input → output pairs
    POSITIONAL_CASES = [
        # Basic PostgreSQL
        ('pg_basic', 'SELECT * FROM t WHERE id = %s', (1,), 'postgresql',
         '%s', (1,)),
        ('pg_multi', 'SELECT * FROM t WHERE a = %s AND b = %s', (1, 2), 'postgresql',
         '%s', (1, 2)),
        # Basic SQLite conversion
        ('sqlite_basic', 'SELECT * FROM t WHERE id = %s', (1,), 'sqlite',
         '?', (1,)),
        ('sqlite_multi', 'SELECT * FROM t WHERE a = %s AND b = %s', (1, 2), 'sqlite',
         '?', (1, 2)),
    ]

    IN_CLAUSE_CASES = [
        # IN clause expansion - various input formats
        ('in_tuple_in_list', 'WHERE id IN %s', [(1, 2, 3)], 'postgresql',
         'IN (%s, %s, %s)', (1, 2, 3)),
        ('in_direct_list', 'WHERE id IN %s', [1, 2, 3], 'postgresql',
         'IN (%s, %s, %s)', (1, 2, 3)),
        ('in_nested_list', 'WHERE id IN %s', [[1, 2, 3]], 'postgresql',
         'IN (%s, %s, %s)', (1, 2, 3)),
        ('in_single_item', 'WHERE id IN %s', [101], 'postgresql',
         'IN (%s)', (101,)),
        ('in_single_tuple', 'WHERE id IN %s', [(42,)], 'postgresql',
         'IN (%s)', (42,)),
        ('in_empty', 'WHERE id IN %s', [()], 'postgresql',
         'IN (NULL)', ()),
        # IN clause with existing parens (no double parens)
        ('in_parens', 'WHERE id IN (%s)', ((1, 2, 3),), 'postgresql',
         '%s, %s, %s', (1, 2, 3)),
        # IN clause SQLite
        ('in_sqlite', 'WHERE id IN %s', [(1, 2)], 'sqlite',
         'IN (?, ?)', (1, 2)),
    ]

    MIXED_CASES = [
        # IN clause with other params
        ('mixed_in_after', 'WHERE a = %s AND id IN %s', ('x', (1, 2)), 'postgresql',
         'IN (%s, %s)', ('x', 1, 2)),
        ('mixed_in_middle', 'WHERE a = %s AND id IN (%s) AND b = %s', ('x', [1, 2], 'y'), 'postgresql',
         '%s, %s', ('x', 1, 2, 'y')),
        # Multiple IN clauses
        ('multi_in', 'WHERE id IN %s AND status IN %s', [(1, 2), ('a', 'b')], 'postgresql',
         'IN (%s, %s)', (1, 2, 'a', 'b')),
    ]

    IS_NULL_CASES = [
        # IS NULL transformations
        ('is_null', 'WHERE v IS %s', (None,), 'postgresql',
         'IS NULL', ()),
        ('is_not_null', 'WHERE v IS NOT %s', (None,), 'postgresql',
         'IS NOT NULL', ()),
        ('is_null_mixed', 'WHERE v IS %s AND x = %s', (None, 'test'), 'postgresql',
         'IS NULL', ('test',)),
        ('is_non_null', 'WHERE v IS %s', ('val',), 'postgresql',
         'IS %s', ('val',)),
    ]

    NAMED_CASES = [
        # Named parameters
        ('named_basic', 'WHERE id = %(id)s', {'id': 1}, 'postgresql',
         '%(id)s', {'id': 1}),
        ('named_multi', 'WHERE a = %(a)s AND b = %(b)s', {'a': 1, 'b': 2}, 'postgresql',
         '%(a)s', {'a': 1, 'b': 2}),
        # Named IN clause
        ('named_in', 'WHERE id IN %(ids)s', {'ids': (1, 2, 3)}, 'postgresql',
         '%(ids_0)s', {'ids_0': 1, 'ids_1': 2, 'ids_2': 3}),
        # Named IS NULL
        ('named_is_null', 'WHERE v IS %(v)s AND x = %(x)s', {'v': None, 'x': 'test'}, 'postgresql',
         'IS NULL', {'x': 'test'}),
        # Dict in list unwrap
        ('dict_in_list', 'WHERE id = %(id)s', [{'id': 1}], 'postgresql',
         '%(id)s', {'id': 1}),
    ]

    NORMALIZATION_CASES = [
        # Arg normalization edge cases
        ('norm_nested_tuple', 'WHERE a = %s AND b = %s AND c = %s',
         [(1, 2, 3)], 'postgresql', '%s', (1, 2, 3)),
        ('norm_empty_args', 'SELECT 1', (), 'postgresql',
         'SELECT 1', ()),
        ('norm_none_args', 'SELECT 1', None, 'postgresql',
         'SELECT 1', None),
    ]

    ESCAPE_CASES = [
        # Percent escaping in literals (PostgreSQL only)
        ('esc_single_quote', "WHERE s = 'foo%' AND id = %s", (1,), 'postgresql',
         "'foo%%'", (1,)),
        ('esc_double_quote', 'WHERE s = "foo%" AND id = %s', (1,), 'postgresql',
         '"foo%%"', (1,)),
        ('esc_already', "WHERE s LIKE 'foo%%' AND id = %s", (1,), 'postgresql',
         "'foo%%'", (1,)),
        # No escaping for SQLite
        ('esc_sqlite_none', "WHERE s = 'foo%' AND id = %s", (1,), 'sqlite',
         "'foo%'", (1,)),
        # No escaping without placeholders
        ('esc_no_ph', "WHERE s = 'foo%'", None, 'postgresql',
         "'foo%'", None),
    ]

    @pytest.mark.parametrize(('case_id', 'sql', 'args', 'dialect', 'expected_contains', 'expected_args'),
                             POSITIONAL_CASES, ids=[c[0] for c in POSITIONAL_CASES])
    def test_positional(self, case_id, sql, args, dialect, expected_contains, expected_args):
        """Test positional parameter cases."""
        result_sql, result_args = prepare_query(sql, args, dialect)
        assert expected_contains in result_sql, f'{case_id}: expected {expected_contains!r} in {result_sql!r}'
        assert result_args == expected_args, f'{case_id}: args mismatch'

    @pytest.mark.parametrize(('case_id', 'sql', 'args', 'dialect', 'expected_contains', 'expected_args'),
                             IN_CLAUSE_CASES, ids=[c[0] for c in IN_CLAUSE_CASES])
    def test_in_clause(self, case_id, sql, args, dialect, expected_contains, expected_args):
        """Test IN clause expansion cases."""
        result_sql, result_args = prepare_query(sql, args, dialect)
        assert expected_contains in result_sql, f'{case_id}: expected {expected_contains!r} in {result_sql!r}'
        assert result_args == expected_args, f'{case_id}: args mismatch'

    @pytest.mark.parametrize(('case_id', 'sql', 'args', 'dialect', 'expected_contains', 'expected_args'),
                             MIXED_CASES, ids=[c[0] for c in MIXED_CASES])
    def test_mixed(self, case_id, sql, args, dialect, expected_contains, expected_args):
        """Test mixed parameter cases."""
        result_sql, result_args = prepare_query(sql, args, dialect)
        assert expected_contains in result_sql, f'{case_id}: expected {expected_contains!r} in {result_sql!r}'
        assert result_args == expected_args, f'{case_id}: args mismatch'

    @pytest.mark.parametrize(('case_id', 'sql', 'args', 'dialect', 'expected_contains', 'expected_args'),
                             IS_NULL_CASES, ids=[c[0] for c in IS_NULL_CASES])
    def test_is_null(self, case_id, sql, args, dialect, expected_contains, expected_args):
        """Test IS NULL handling cases."""
        result_sql, result_args = prepare_query(sql, args, dialect)
        assert expected_contains in result_sql, f'{case_id}: expected {expected_contains!r} in {result_sql!r}'
        assert result_args == expected_args, f'{case_id}: args mismatch'

    @pytest.mark.parametrize(('case_id', 'sql', 'args', 'dialect', 'expected_contains', 'expected_args'),
                             NAMED_CASES, ids=[c[0] for c in NAMED_CASES])
    def test_named(self, case_id, sql, args, dialect, expected_contains, expected_args):
        """Test named parameter cases."""
        result_sql, result_args = prepare_query(sql, args, dialect)
        assert expected_contains in result_sql, f'{case_id}: expected {expected_contains!r} in {result_sql!r}'
        # For dicts, check subset (named IN adds keys)
        if isinstance(expected_args, dict):
            for k, v in expected_args.items():
                assert result_args.get(k) == v, f'{case_id}: {k} mismatch'
        else:
            assert result_args == expected_args, f'{case_id}: args mismatch'

    @pytest.mark.parametrize(('case_id', 'sql', 'args', 'dialect', 'expected_contains', 'expected_args'),
                             NORMALIZATION_CASES, ids=[c[0] for c in NORMALIZATION_CASES])
    def test_normalization(self, case_id, sql, args, dialect, expected_contains, expected_args):
        """Test argument normalization cases."""
        result_sql, result_args = prepare_query(sql, args, dialect)
        assert expected_contains in result_sql, f'{case_id}: expected {expected_contains!r} in {result_sql!r}'
        assert result_args == expected_args, f'{case_id}: args mismatch'

    @pytest.mark.parametrize(('case_id', 'sql', 'args', 'dialect', 'expected_contains', 'expected_args'),
                             ESCAPE_CASES, ids=[c[0] for c in ESCAPE_CASES])
    def test_escaping(self, case_id, sql, args, dialect, expected_contains, expected_args):
        """Test percent escaping cases."""
        result_sql, result_args = prepare_query(sql, args, dialect)
        assert expected_contains in result_sql, f'{case_id}: expected {expected_contains!r} in {result_sql!r}'
        assert result_args == expected_args, f'{case_id}: args mismatch'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
