"""Unit tests for SQL parameter processing.

Tests the public API:
- prepare_query(sql, args, dialect) - Main entry point
- quote_identifier(name, dialect) - Quote table/column names
- make_placeholders(count, dialect) - Comma-joined placeholder run
- has_placeholders(sql) - Check for parameter placeholders
- has_named_placeholders(sql, dialect) - Check for named placeholders only
- standardize_placeholders(sql, dialect) - Convert %s <-> ?

Every expected SQL string here is written out by hand, never rebuilt with
the same join the source uses.
"""
import datetime

import pytest
from database.exceptions import DatabaseError, ValidationError
from database.sql import has_named_placeholders, has_placeholders
from database.sql import make_placeholders, prepare_query, quote_identifier
from database.sql import standardize_placeholders

JAN_1 = datetime.date(2025, 1, 1)
MAR_11 = datetime.date(2025, 3, 11)
DEC_31 = datetime.date(2025, 12, 31)


class TestPrepareQueryBasic:
    """Test basic parameter handling through prepare_query."""

    @pytest.mark.parametrize(
        ('sql', 'args', 'dialect', 'expected_sql', 'expected_args'),
        [
            ('SELECT foo FROM t WHERE date BETWEEN %s AND %s AND value > %s',
             (JAN_1, MAR_11, 100),
             'postgresql',
             'SELECT foo FROM t WHERE date BETWEEN %s AND %s AND value > %s',
             (JAN_1, MAR_11, 100)),
            ('SELECT * FROM users WHERE id = %s AND status = %s',
             (1, 'active'),
             'sqlite',
             'SELECT * FROM users WHERE id = ? AND status = ?',
             (1, 'active')),
            ('SELECT * FROM users WHERE date BETWEEN %s AND %s AND status = %s',
             [(JAN_1, MAR_11, 'active')],
             'postgresql',
             'SELECT * FROM users WHERE date BETWEEN %s AND %s AND status = %s',
             (JAN_1, MAR_11, 'active')),
            ('SELECT * FROM users WHERE active = TRUE',
             None,
             'postgresql',
             'SELECT * FROM users WHERE active = TRUE',
             None),
            ('SELECT * FROM users',
             (),
             'postgresql',
             'SELECT * FROM users',
             ()),
            ('SELECT * FROM t WHERE a = %s AND b = %s',
             ([1, 2, 3],),
             'postgresql',
             'SELECT * FROM t WHERE a = %s AND b = %s',
             ([1, 2, 3], None)),
            ],
        ids=['pg_basic', 'sqlite_conversion', 'nested_tuple', 'no_placeholders',
             'empty_args', 'inner_longer_than_slots'])
    def test_basic_handling(self, sql, args, dialect, expected_sql, expected_args):
        """Verify the whole (sql, args) pair for plain positional binding.

        Mutation: swapping the marker in _transform so sqlite emits '%s',
            or dropping the `len(inner) == len(phs)` guard in _normalize
            rule 3, which flattens a longer inner sequence against fewer
            placeholders and silently truncates values.
        Oracle: hand-written full SQL strings and arg tuples.
        """
        assert prepare_query(sql, args, dialect) == (expected_sql, expected_args)


class TestDialectDefaults:
    """The default dialect of every public entry point is postgresql."""

    def test_prepare_query_defaults_to_postgresql(self):
        """prepare_query with no dialect keeps '%s' rather than emitting '?'.

        Mutation: changing prepare_query's `dialect: str = 'postgresql'`
            default to 'sqlite'.
        Oracle: hand-written SQL that only the postgres marker satisfies.
        """
        assert prepare_query('SELECT * FROM t WHERE id = %s', (1,)) == (
            'SELECT * FROM t WHERE id = %s', (1,))

    def test_standardize_placeholders_defaults_to_postgresql(self):
        """standardize_placeholders with no dialect converts '?' to '%s'.

        Mutation: changing standardize_placeholders' dialect default to
            'sqlite', which would leave the '?' untouched.
        Oracle: hand-written converted SQL.
        """
        assert standardize_placeholders('SELECT * FROM t WHERE id = ?') == (
            'SELECT * FROM t WHERE id = %s')

    def test_make_placeholders_defaults_to_postgresql(self):
        """make_placeholders with no dialect emits the pyformat marker.

        Mutation: flipping make_placeholders' marker choice to
            `'?' if dialect != 'sqlite' else '%s'`.
        Oracle: hand-written '%s, %s'.
        """
        assert make_placeholders(2) == '%s, %s'


class TestPrepareQueryInClause:
    """Test IN clause parameter expansion."""

    @pytest.mark.parametrize(
        ('args', 'expected_sql', 'expected_args'),
        [
            ([(1, 2, 3)], 'SELECT * FROM users WHERE id IN (%s, %s, %s)', (1, 2, 3)),
            ([(42,)], 'SELECT * FROM users WHERE id IN (%s)', (42,)),
            ([1, 2, 3], 'SELECT * FROM users WHERE id IN (%s, %s, %s)', (1, 2, 3)),
            ([[1, 2, 3]], 'SELECT * FROM users WHERE id IN (%s, %s, %s)', (1, 2, 3)),
            ([101], 'SELECT * FROM users WHERE id IN (%s)', (101,)),
            ([[1, 2], [3, 4]], 'SELECT * FROM users WHERE id IN (%s, %s)', (1, 2)),
            ],
        ids=['tuple_in_list', 'single_tuple', 'direct_list', 'nested_list',
             'single_item', 'list_of_lists'])
    def test_in_clause_expansion_formats(self, args, expected_sql, expected_args):
        """Verify every accepted IN-arg shape lands on the same expansion.

        Mutation: dropping `_is_flat(args)` from _normalize rule 2, which
            wraps [[1,2],[3,4]] as one value instead of expanding the first
            inner list; or emitting ',' in place of ', ' when _expand_in
            joins the markers.
        Oracle: hand-written full SQL strings, not a join of the marker.
        """
        sql = 'SELECT * FROM users WHERE id IN %s'

        assert prepare_query(sql, args, 'postgresql') == (expected_sql, expected_args)

    def test_in_clause_empty_sequence(self):
        """An empty IN sequence collapses to a parenthesized NULL, no args.

        Mutation: returning 'NULL' instead of '(NULL)' in _expand_in when
            in_parens is False, which yields the unparsable `IN NULL`.
        Oracle: hand-written `IN (NULL)` plus an empty arg tuple.
        """
        result = prepare_query('SELECT * FROM users WHERE id IN %s', [()], 'postgresql')

        assert result == ('SELECT * FROM users WHERE id IN (NULL)', ())

    def test_in_clause_with_other_params(self):
        """An IN run splices into the middle of the flat arg tuple in order.

        Mutation: appending the expanded IN values after the remaining
            scalars in _transform instead of at the placeholder's position.
        Oracle: hand-written SQL plus the interleaved arg tuple.
        """
        sql = 'SELECT * FROM users WHERE date BETWEEN %s AND %s AND id IN %s'

        result = prepare_query(sql, (JAN_1, MAR_11, (1, 2, 3)), 'postgresql')

        assert result == (
            'SELECT * FROM users WHERE date BETWEEN %s AND %s AND id IN (%s, %s, %s)',
            (JAN_1, MAR_11, 1, 2, 3))

    def test_multiple_in_clauses(self):
        """Two IN clauses each expand to their own width and stay ordered.

        Mutation: an off-by-one in `[marker] * len(val)` inside
            _expand_in, which drops one bind from each clause.
        Oracle: hand-written SQL with differing widths (3 then 2).
        """
        sql = 'SELECT * FROM users WHERE id IN %s AND status IN %s'

        result = prepare_query(sql, [(1, 2, 3), ('active', 'pending')], 'postgresql')

        assert result == (
            'SELECT * FROM users WHERE id IN (%s, %s, %s) AND status IN (%s, %s)',
            (1, 2, 3, 'active', 'pending'))

    def test_multiple_in_clauses_with_inner_lists(self):
        """Rule 4 turns inner lists into tuples for every IN placeholder.

        Mutation: swapping the in_parens arms of the sequence branch of
            _expand_in, so neither clause gets its own parens.
        Oracle: hand-written SQL where both clauses expand to width 2.
        """
        sql = 'SELECT * FROM t WHERE id IN %s AND status IN %s'

        result = prepare_query(sql, [[1, 2], [3, 4]], 'postgresql')

        assert result == (
            'SELECT * FROM t WHERE id IN (%s, %s) AND status IN (%s, %s)',
            (1, 2, 3, 4))

    def test_parenthesized_in_clause(self):
        """`IN (%s)` expands inside the parens the author already wrote.

        Mutation: ignoring PH.in_parens in _expand_in so the expansion
            re-wraps and produces `IN ((%s, %s, %s))`.
        Oracle: hand-written SQL with exactly one paren pair.
        """
        result = prepare_query(
            'SELECT * FROM users WHERE id IN (%s)', ((1, 2, 3),), 'postgresql')

        assert result == ('SELECT * FROM users WHERE id IN (%s, %s, %s)', (1, 2, 3))

    def test_in_clause_middle_position(self):
        """An IN clause between two scalars keeps the tail of the SQL intact.

        Mutation: setting `pos = ph.pos` instead of `pos = ph.end` in
            _transform, which duplicates the placeholder text.
        Oracle: hand-written full SQL plus the flat arg tuple.
        """
        sql = 'SELECT * FROM t WHERE date = %s AND id IN (%s) AND user = %s'

        result = prepare_query(sql, ('2025-01-01', [1, 2], 'USER_A'), 'postgresql')

        assert result == (
            'SELECT * FROM t WHERE date = %s AND id IN (%s, %s) AND user = %s',
            ('2025-01-01', 1, 2, 'USER_A'))

    def test_parenthesized_in_clause_preserves_closing_paren(self):
        """`IN (%s)` keeps its closing paren for multi- and single-value args.

        Mutation: consuming the trailing ')' as part of the placeholder
            span in _find_contexts (PH.end off by one).
        Oracle: hand-written SQL for both widths.
        """
        sql = 'SELECT * FROM t WHERE id IN (%s)'

        assert prepare_query(sql, ((1, 2, 3),), 'postgresql') == (
            'SELECT * FROM t WHERE id IN (%s, %s, %s)', (1, 2, 3))
        assert prepare_query(sql, ([42],), 'postgresql') == (
            'SELECT * FROM t WHERE id IN (%s)', (42,))

    def test_in_clause_with_scalar_value(self):
        """A bare scalar in an IN slot binds as one value, not per character.

        Mutation: swapping the in_parens arms of the scalar tail in
            _expand_in, which re-wraps and yields `IN ((%s))`.
        Oracle: hand-written SQL with one paren pair and the intact string.
        """
        sql = 'SELECT * FROM t WHERE date = %s AND fund IN (%s)'

        result = prepare_query(sql, ('2025-01-01', 'Growth'), 'postgresql')

        assert result == (
            'SELECT * FROM t WHERE date = %s AND fund IN (%s)',
            ('2025-01-01', 'Growth'))

    def test_in_clause_with_single_element_list(self):
        """A one-element list in an IN slot expands to exactly one marker.

        Mutation: `if not val` in _expand_in loosened to `if len(val) <= 1`,
            which would swallow the single value into NULL.
        Oracle: hand-written SQL and the unwrapped scalar arg.
        """
        sql = 'SELECT * FROM t WHERE date = %s AND fund IN (%s)'

        result = prepare_query(sql, ('2025-01-01', ['Growth']), 'postgresql')

        assert result == (
            'SELECT * FROM t WHERE date = %s AND fund IN (%s)',
            ('2025-01-01', 'Growth'))

    def test_in_context_survives_whitespace_after_open_paren(self):
        """`IN ( %s )` is still an IN context; the prefix is right-stripped.

        Mutation: dropping the `.rstrip()` from the prefix in
            _find_contexts, which drops this back to a plain value slot.
        Oracle: hand-written SQL that keeps the author's inner spaces.
        """
        result = prepare_query(
            'SELECT * FROM t WHERE id IN ( %s )', ((1, 2),), 'postgresql')

        assert result == ('SELECT * FROM t WHERE id IN ( %s, %s )', (1, 2))

    def test_not_in_expands_like_in(self):
        """NOT IN reaches the same expansion as IN.

        Mutation: matching the prefix with `== 'IN'` instead of
            `endswith('IN')` in _parse_ctx.
        Oracle: hand-written SQL expanding to width 2.
        """
        result = prepare_query(
            'SELECT * FROM t WHERE id NOT IN %s', [(1, 2)], 'postgresql')

        assert result == ('SELECT * FROM t WHERE id NOT IN (%s, %s)', (1, 2))

    def test_flat_list_is_not_spread_when_other_placeholders_exist(self):
        """Rule 2 only fires for a lone placeholder; two slots take two args.

        Mutation: dropping `len(phs) == 1` from _normalize rule 2, which
            would spread [1, 2] into the IN clause and starve `x = %s`.
        Oracle: boundary straddling the rule - the same arg list against
            one placeholder expands to width 2, against two it does not.
        """
        result = prepare_query(
            'SELECT * FROM t WHERE id IN %s AND x = %s', [1, 2], 'postgresql')

        assert result == ('SELECT * FROM t WHERE id IN (%s) AND x = %s', (1, 2))

    def test_in_clause_unwraps_single_nested_sequence(self):
        """A singly nested positional IN value is unwrapped before expansion.

        Mutation: removing the `if _isseq(val) and len(val) == 1 and
            _isseq(val[0])` unwrap in _expand_in, which binds the inner
            list itself as one array value.
        Oracle: hand-written two-marker SQL for the [[1, 2]] input.
        """
        result = prepare_query(
            'SELECT * FROM t WHERE id IN %s AND x = %s', ([[1, 2]], 'q'), 'postgresql')

        assert result == ('SELECT * FROM t WHERE id IN (%s, %s) AND x = %s', (1, 2, 'q'))


class TestPrepareQueryAnyAll:
    """Test ANY(%s) / ALL(%s) array parameter binding.

    ANY(%s) and ALL(%s) bind the whole sequence as one array parameter.
    A single-element list is never unwrapped into a scalar.
    """

    def test_any_single_element_list_stays_wrapped(self):
        """ANY(%s) with a one-element list keeps the list as a single param.

        Mutation: dropping `and len(phs) > 1` from _normalize rule 3, which
            unwraps ([1979],) into (1979,) and breaks the array bind.
        Oracle: hand-written ([1979],) plus the untouched SQL tail.
        """
        sql = 'select count(*) from t where id = ANY(%s) and y is null'

        result = prepare_query(sql, ([1979],), 'postgresql')

        assert result == (
            'select count(*) from t where id = ANY(%s) and y is null', ([1979],))

    def test_any_multi_element_list_stays_wrapped(self):
        """ANY(%s) with a multi-element list binds the whole list as array.

        Mutation: classifying the `ANY(` prefix as an IN context in
            _parse_ctx, which would expand the list into `ANY(%s, %s, %s)`.
        Oracle: hand-written SQL with one marker and the list intact.
        """
        result = prepare_query(
            'select * from t where id = ANY(%s)', ([1, 2, 3],), 'postgresql')

        assert result == ('select * from t where id = ANY(%s)', ([1, 2, 3],))

    def test_any_with_extra_param_preserves_list(self):
        """ANY combined with another placeholder preserves the array.

        Mutation: _normalize rule 3 unwrapping whenever `len(args) == 1`,
            regardless of the placeholder count.
        Oracle: hand-written ([1, 2], 'alice').
        """
        result = prepare_query(
            'select * from t where id = ANY(%s) and name = %s',
            ([1, 2], 'alice'), 'postgresql')

        assert result == (
            'select * from t where id = ANY(%s) and name = %s', ([1, 2], 'alice'))

    def test_all_single_element_list_stays_wrapped(self):
        """ALL(%s) follows the same array-binding rule as ANY(%s).

        Mutation: dropping `and len(phs) > 1` from _normalize rule 3.
        Oracle: hand-written ([100],).
        """
        result = prepare_query(
            'select * from t where price < ALL(%s)', ([100],), 'postgresql')

        assert result == ('select * from t where price < ALL(%s)', ([100],))

    def test_two_placeholders_do_unwrap_the_single_sequence(self):
        """The companion side of the rule-3 guard: two slots DO unwrap.

        Mutation: raising the guard to `len(phs) > 2`, or removing the
            unwrap branch entirely, which would bind [1, 2] to `a` and
            None to `b`.
        Oracle: boundary straddling `len(phs) > 1` - the same ([1, 2],)
            arg stays wrapped for one placeholder and splits for two.
        """
        result = prepare_query(
            'SELECT * FROM t WHERE a = %s AND b = %s', ([1, 2],), 'postgresql')

        assert result == ('SELECT * FROM t WHERE a = %s AND b = %s', (1, 2))


class TestPrepareQueryNamedParams:
    """Test named parameter handling."""

    @pytest.mark.parametrize(
        ('sql', 'args', 'expected_sql', 'expected_args'),
        [
            ('SELECT * FROM users WHERE date BETWEEN %(start)s AND %(end)s',
             {'start': JAN_1, 'end': MAR_11},
             'SELECT * FROM users WHERE date BETWEEN %(start)s AND %(end)s',
             {'start': JAN_1, 'end': MAR_11}),
            ('SELECT * FROM users WHERE id IN %(ids)s',
             {'ids': (1, 2, 3)},
             'SELECT * FROM users WHERE id IN (%(ids_0)s, %(ids_1)s, %(ids_2)s)',
             {'ids_0': 1, 'ids_1': 2, 'ids_2': 3}),
            ('SELECT * FROM users WHERE id IN %(ids)s AND name = %(name)s',
             {'ids': (1, 2), 'name': 'test'},
             'SELECT * FROM users WHERE id IN (%(ids_0)s, %(ids_1)s) AND name = %(name)s',
             {'ids_0': 1, 'ids_1': 2, 'name': 'test'}),
            ('SELECT * FROM users WHERE id = %(id)s',
             [{'id': 1}],
             'SELECT * FROM users WHERE id = %(id)s',
             {'id': 1}),
            ],
        ids=['basic', 'in_clause', 'in_with_regular', 'dict_in_list'])
    def test_named_params(self, sql, args, expected_sql, expected_args):
        """Verify the whole rewritten dict, so the source key cannot linger.

        Mutation: _expand_named_in adding its numbered keys without
            replacing 'ids', which leaves psycopg an unused key; or the
            key template changed from f'{name}_{i}' to f'{name}{i}'.
        Oracle: hand-written full SQL and the exact expected dict.
        """
        assert prepare_query(sql, args, 'postgresql') == (expected_sql, expected_args)

    @pytest.mark.parametrize(
        ('sql', 'args', 'expected_sql', 'expected_args'),
        [
            ('SELECT * FROM t WHERE id = %(id)s',
             {'id': 5},
             'SELECT * FROM t WHERE id = :id',
             {'id': 5}),
            ('SELECT * FROM t WHERE id IN %(ids)s',
             {'ids': (1, 2)},
             'SELECT * FROM t WHERE id IN (:ids_0, :ids_1)',
             {'ids_0': 1, 'ids_1': 2}),
            ('SELECT * FROM t WHERE v IS %(v)s AND x = %(x)s',
             {'v': None, 'x': 1},
             'SELECT * FROM t WHERE v IS NULL AND x = :x',
             {'x': 1}),
            ],
        ids=['scalar', 'in_clause', 'is_null_mixed'])
    def test_named_params_use_colon_syntax_for_sqlite(
        self, sql, args,
        expected_sql, expected_args):
        """sqlite3 only accepts ':name', so _named_ph must switch on dialect.

        Mutation: _named_ph returning f'%({name})s' unconditionally, which
            sqlite3 rejects at execute time.
        Oracle: hand-written ':name' SQL, contrasted with the pyformat
            output the postgres cases above pin.
        """
        assert prepare_query(sql, args, 'sqlite') == (expected_sql, expected_args)

    def test_unknown_named_key_left_untouched(self):
        """A placeholder with no matching key keeps its text and adds no arg.

        Mutation: _proc_named's `if name not in args` branch returning
            `{name: None}`, which would bind a silent NULL.
        Oracle: hand-written SQL unchanged plus the single-key dict.
        """
        sql = 'SELECT * FROM t WHERE a = %(a)s AND b = %(b)s'

        result = prepare_query(sql, {'a': 1}, 'postgresql')

        assert result == ('SELECT * FROM t WHERE a = %(a)s AND b = %(b)s', {'a': 1})

    def test_unknown_named_key_still_rewritten_for_sqlite(self):
        """The unknown-key branch still emits the dialect's own syntax.

        Mutation: returning `ph.name` raw, or the pyformat form, from the
            missing-key branch of _proc_named.
        Oracle: hand-written ':b' for the key sqlite cannot resolve.
        """
        sql = 'SELECT * FROM t WHERE a = %(a)s AND b = %(b)s'

        result = prepare_query(sql, {'a': 1}, 'sqlite')

        assert result == ('SELECT * FROM t WHERE a = :a AND b = :b', {'a': 1})

    def test_named_in_clause_with_scalar_value(self):
        """A scalar under a named IN still gets the `_0` suffix and parens.

        Mutation: _expand_named_in's scalar tail returning `_named_ph(name)`
            and `{name: val}`, which collides with the un-expanded key.
        Oracle: hand-written `IN (%(ids_0)s)` plus {'ids_0': 5}.
        """
        result = prepare_query(
            'SELECT * FROM t WHERE id IN %(ids)s', {'ids': 5}, 'postgresql')

        assert result == ('SELECT * FROM t WHERE id IN (%(ids_0)s)', {'ids_0': 5})

    def test_named_in_clause_empty_sequence(self):
        """An empty named IN collapses to (NULL) and contributes no keys.

        Mutation: swapping the in_parens arms of the empty branch in
            _expand_named_in, giving the unparsable `IN NULL`.
        Oracle: hand-written `IN (NULL)` plus an empty dict.
        """
        result = prepare_query(
            'SELECT * FROM t WHERE id IN %(ids)s', {'ids': []}, 'postgresql')

        assert result == ('SELECT * FROM t WHERE id IN (NULL)', {})

    def test_named_in_clause_already_parenthesized(self):
        """`IN (%(ids)s)` expands without adding a second paren pair.

        Mutation: ignoring in_parens in _expand_named_in, producing
            `IN ((%(ids_0)s, %(ids_1)s))`.
        Oracle: hand-written SQL with exactly one paren pair.
        """
        result = prepare_query(
            'SELECT * FROM t WHERE id IN (%(ids)s)', {'ids': (1, 2)}, 'postgresql')

        assert result == (
            'SELECT * FROM t WHERE id IN (%(ids_0)s, %(ids_1)s)',
            {'ids_0': 1, 'ids_1': 2})

    def test_named_in_clause_unwraps_single_nested_sequence(self):
        """A singly-nested named IN value is unwrapped before expansion.

        Mutation: dropping the unwrap guard in _expand_named_in, which
            would bind the inner list itself as one array value.
        Oracle: hand-written two-marker SQL for the [[1, 2]] input.
        """
        result = prepare_query(
            'SELECT * FROM t WHERE id IN %(ids)s', {'ids': [[1, 2]]}, 'postgresql')

        assert result == (
            'SELECT * FROM t WHERE id IN (%(ids_0)s, %(ids_1)s)',
            {'ids_0': 1, 'ids_1': 2})


class TestDictArgumentAsValue:
    """A lone dict binds by name only when the SQL names a placeholder.

    Against '?' or '%s' the dict is an ordinary value - a JSON column, say -
    so it has to reach the driver as one positional argument.
    """

    DICT_ARG_CASES = [
        ('qmark_sqlite', 'INSERT INTO t (doc) VALUES (?)', {'a': 1}, 'sqlite',
         'INSERT INTO t (doc) VALUES (?)', ({'a': 1},)),
        ('percent_s_pg', 'INSERT INTO t (doc) VALUES (%s)', {'a': 1},
         'postgresql', 'INSERT INTO t (doc) VALUES (%s)', ({'a': 1},)),
        ('percent_s_sqlite', 'INSERT INTO t (doc) VALUES (%s)', {'a': 1},
         'sqlite', 'INSERT INTO t (doc) VALUES (?)', ({'a': 1},)),
        ('dict_in_list_qmark', 'INSERT INTO t (doc) VALUES (?)', [{'a': 1}],
         'sqlite', 'INSERT INTO t (doc) VALUES (?)', ({'a': 1},)),
        ('named_pg_binds_by_name', 'UPDATE t SET doc = %(doc)s',
         {'doc': {'a': 1}}, 'postgresql', 'UPDATE t SET doc = %(doc)s',
         {'doc': {'a': 1}}),
        ('named_sqlite_binds_by_name', 'UPDATE t SET doc = %(doc)s',
         {'doc': {'a': 1}}, 'sqlite', 'UPDATE t SET doc = :doc',
         {'doc': {'a': 1}}),
        ]

    @pytest.mark.parametrize(
        ('case_id', 'sql', 'args', 'dialect', 'expected_sql', 'expected_args'),
        DICT_ARG_CASES,
        ids=[c[0] for c in DICT_ARG_CASES])
    def test_lone_dict_is_positional_unless_the_sql_names_a_placeholder(
            self, case_id, sql, args, dialect, expected_sql, expected_args):
        """A dict under '?' or '%s' stays one value; under a name it binds.

        Mutation: _normalize dropping its `has_named` test - `return args`
            for every dict, and unwrapping every one-element (dict,) -
            which routes a nameless placeholder through _proc_named and
            emits ':None' / '%(None)s' with an empty arg dict.
        Oracle: hand-written (sql, args) pairs; the named_* rows feed the
            same shape of dict to name-carrying SQL and must still bind by
            key, so the SQL is the only thing that moves.
        """
        result = prepare_query(sql, args, dialect)

        assert result == (expected_sql, expected_args), case_id


class TestPrepareQueryIsNull:
    """Test IS NULL / IS NOT NULL handling."""

    @pytest.mark.parametrize(
        ('sql', 'args', 'expected_sql', 'expected_args'),
        [
            ('SELECT * FROM t WHERE value IS %s', (None,),
             'SELECT * FROM t WHERE value IS NULL', ()),
            ('SELECT * FROM t WHERE value IS NOT %s', (None,),
             'SELECT * FROM t WHERE value IS NOT NULL', ()),
            ('SELECT * FROM t WHERE v1 IS %s AND v2 IS NOT %s', (None, None),
             'SELECT * FROM t WHERE v1 IS NULL AND v2 IS NOT NULL', ()),
            ('SELECT * FROM t WHERE v1 IS %s AND v2 = %s', (None, 'test'),
             'SELECT * FROM t WHERE v1 IS NULL AND v2 = %s', ('test',)),
            ('SELECT * FROM t WHERE value IS %s', ('not_null',),
             'SELECT * FROM t WHERE value IS %s', ('not_null',)),
            ],
        ids=['is_null', 'is_not_null', 'multiple', 'mixed', 'non_null'])
    def test_positional_null_handling(self, sql, args, expected_sql, expected_args):
        """Verify IS/IS NOT inline NULL only for a None value.

        Mutation: dropping `and val is None` from _proc_pos, which turns
            `IS %s` with 'not_null' into `IS NULL`; or returning
            ('NULL', [val]) so the consumed None stays in the arg tuple.
        Oracle: hand-written SQL plus arg tuples; the non_null row is the
            boundary that separates the two branches.
        """
        assert prepare_query(sql, args, 'postgresql') == (expected_sql, expected_args)

    def test_named_null_handling(self):
        """A named IS NULL drops its key so the driver sees no stray param.

        Mutation: _proc_named's IS-NULL branch returning ('NULL', {name: val})
            instead of ('NULL', {}).
        Oracle: hand-written SQL plus the exact one-key dict.
        """
        sql = 'SELECT * FROM t WHERE value IS %(val)s AND name = %(name)s'

        result = prepare_query(sql, {'val': None, 'name': 'test'}, 'postgresql')

        assert result == (
            'SELECT * FROM t WHERE value IS NULL AND name = %(name)s',
            {'name': 'test'})


class TestPrepareQueryPercentEscaping:
    """Test percent sign escaping in string literals.

    Percent escaping only occurs when there are placeholders in the query.
    Without placeholders, the driver doesn't parse for format specifiers.
    """

    @pytest.mark.parametrize(
        ('sql', 'args', 'dialect', 'expected_sql'),
        [
            ("SELECT * FROM t WHERE id = %s AND s = 'progress: 50%'",
             (1,), 'postgresql',
             "SELECT * FROM t WHERE id = %s AND s = 'progress: 50%%'"),
            ('SELECT * FROM t WHERE id = %s AND s = "Complete: 75%"',
             (1,), 'postgresql',
             'SELECT * FROM t WHERE id = %s AND s = "Complete: 75%%"'),
            ("SELECT * FROM t WHERE id = %s AND s = 'It''s 100% done'",
             (1,), 'postgresql',
             "SELECT * FROM t WHERE id = %s AND s = 'It''s 100%% done'"),
            ("SELECT * FROM t WHERE id = %s AND s LIKE 'pre%%post'",
             (1,), 'postgresql',
             "SELECT * FROM t WHERE id = %s AND s LIKE 'pre%%post'"),
            ("SELECT * FROM t WHERE s = 'progress: 50%'",
             None, 'postgresql',
             "SELECT * FROM t WHERE s = 'progress: 50%'"),
            ("SELECT * FROM t WHERE id = %s AND s = 'progress: 50%'",
             (1,), 'sqlite',
             "SELECT * FROM t WHERE id = ? AND s = 'progress: 50%'"),
            ("SELECT * FROM t WHERE name = %s AND status = 'progress: 25%'",
             ('test',), 'postgresql',
             "SELECT * FROM t WHERE name = %s AND status = 'progress: 25%%'"),
            ("SELECT * FROM t WHERE s = 'a%' AND id = %s",
             (1,), 'postgresql',
             "SELECT * FROM t WHERE s = 'a%%' AND id = %s"),
            ],
        ids=['pg_single_quote', 'pg_double_quote', 'pg_escaped_quotes',
             'pg_already_escaped', 'pg_no_placeholders', 'sqlite_no_escape',
             'pg_placeholder_preserved', 'pg_literal_before_placeholder'])
    def test_percent_escaping(self, sql, args, dialect, expected_sql):
        """Verify '%' doubling in literals, both sides of the placeholder.

        Mutation: dropping the `(?<!%)` guard in _UNESCAPE_PCT so '%%'
            becomes '%%%%'; skipping _escape_percents on the final segment
            of _transform; or applying it under the sqlite dialect too.
        Oracle: hand-written full SQL per row; the already_escaped and
            sqlite rows straddle the two guards.
        """
        result_sql, _ = prepare_query(sql, args, dialect)

        assert result_sql == expected_sql

    def test_percent_outside_a_literal_is_left_alone(self):
        """Only string-literal percents are doubled; the modulo op is not.

        Mutation: _escape_percents applying _UNESCAPE_PCT to the whole
            segment instead of only to _STR_RE matches.
        Oracle: hand-written SQL keeping the bare `50 % 3`.
        """
        sql = 'SELECT 50 % 3 AS r, id FROM t WHERE id = %s'

        assert prepare_query(sql, (1,), 'postgresql') == (sql, (1,))


class TestPrepareQueryRegexp:
    """Test regexp_replace patterns are preserved."""

    def test_regexp_pattern_survives_placeholder_processing(self):
        """A regexp_replace body keeps its '?' and '$' verbatim.

        Mutation: dropping the string-literal branch from
            _protected_ranges, which would make each `?` in `[UV]? ?(CN`
            a placeholder and shift every bound arg.
        Oracle: hand-written SQL identical to the input, plus (1,).
        """
        sql = (r"SELECT regexp_replace(code, '\/?[UV]? ?(CN|US)?$', '') "
               r'FROM t WHERE id = %s')

        assert prepare_query(sql, (1,), 'postgresql') == (sql, (1,))

    def test_regexp_preserved_while_like_literal_is_escaped(self):
        """The LIKE literal is escaped; the regexp body next to it is not.

        Mutation: dropping the save_regexp/restore pass in
            _escape_percents, which doubles the '%' inside the regexp
            pattern and changes what the pattern matches.
        Oracle: hand-written SQL where 'a%' doubles and '[0-9]%x' does
            not - two literals in one statement, treated differently.
        """
        sql = ("SELECT * FROM t WHERE s LIKE 'a%' "
               "AND regexp_replace(code, '[0-9]%x', '') = 'B' AND id = %s")
        expected = ("SELECT * FROM t WHERE s LIKE 'a%%' "
                    "AND regexp_replace(code, '[0-9]%x', '') = 'B' AND id = %s")

        assert prepare_query(sql, (1,), 'postgresql') == (expected, (1,))

    def test_regexp_body_percent_left_alone_without_a_like_neighbor(self):
        """A lone regexp_replace body is exempt from percent doubling.

        Mutation: _escape_percents restoring the saved regexp before the
            _STR_RE pass instead of after it.
        Oracle: hand-written SQL identical to the input, plus (1,).
        """
        sql = "SELECT regexp_replace(code, '%d+', '') FROM t WHERE id = %s"

        assert prepare_query(sql, (1,), 'postgresql') == (sql, (1,))


class TestPrepareQueryDollarQuotes:
    """Dollar-quoted strings ($$...$$, $tag$...$tag$) must be treated as
    protected context - placeholders inside the body are not real.
    """

    def test_anonymous_dollar_quoted_body_protects_percent_s(self):
        """$$ ... %s ... $$ has no real placeholder; one outside binds one arg.

        Mutation: removing the `c == '$'` branch from _protected_ranges,
            which would bind 42 to the body and leave the real slot empty.
        Oracle: hand-written SQL identical to the input, plus (42,).
        """
        sql = 'SELECT $$has %s inside$$, %s FROM t'

        assert prepare_query(sql, (42,), 'postgresql') == (sql, (42,))

    def test_anonymous_dollar_quoted_body_with_question_mark_postgresql(self):
        """? inside $$...$$ on postgres is literal, not a placeholder.

        Mutation: running the dollar-quote scan only for sqlite, or
            skipping it whenever the body holds no '%'.
        Oracle: hand-written SQL identical to the input, plus (7,).
        """
        sql = 'SELECT $$contains ? char$$ FROM t WHERE id = %s'

        assert prepare_query(sql, (7,), 'postgresql') == (sql, (7,))

    def test_tagged_dollar_quoted_body_protected(self):
        r"""$tag$ ... %s ... $tag$ body is literal.

        Mutation: _DOLLAR_OPEN_RE tightened to r'\\$\\$' so only anonymous
            tags are recognized.
        Oracle: hand-written SQL identical to the input, plus (1,).
        """
        sql = 'SELECT $body$any %s text$body$ FROM t WHERE id = %s'

        assert prepare_query(sql, (1,), 'postgresql') == (sql, (1,))

    def test_nested_dollar_tags_match_by_tag(self):
        """$outer$ closes only at its own tag, swallowing the inner one.

        Mutation: closing a dollar body at the next '$' rather than at
            `sql.find(tag, m.end())`, which would expose the inner '%s'.
        Oracle: hand-written SQL identical to the input; only the trailing
            placeholder binds, so args stay (99,).
        """
        sql = 'SELECT $outer$ %s and $inner$ %s $inner$ end $outer$ FROM t WHERE id = %s'

        assert prepare_query(sql, (99,), 'postgresql') == (sql, (99,))

    def test_dollar_quotes_are_not_protected_for_sqlite(self):
        """Dollar quoting is postgres-only; sqlite converts the inner %s.

        Mutation: dropping `dialect == 'postgresql'` from the dollar branch
            of _protected_ranges, which would leave this '%s' unconverted.
        Oracle: differential - the same SQL under postgres keeps '%s'
            (asserted here too), under sqlite it becomes '?'.
        """
        sql = 'SELECT $$a %s b$$ FROM t'

        assert prepare_query(sql, ('x',), 'sqlite') == (
            'SELECT $$a ? b$$ FROM t', ('x',))
        assert prepare_query(sql, None, 'postgresql')[0] == sql

    def test_unterminated_dollar_quote_protects_to_end_of_sql(self):
        """An unclosed $$ swallows the rest of the statement.

        Mutation: `j = n if close == -1 else close + len(tag)` changed to
            fall back to `m.end()`, which would expose the trailing '%s'
            and bind 'x' to it.
        Oracle: hand-written SQL unchanged and an empty arg tuple, since
            no placeholder survives the protected range.
        """
        sql = 'SELECT $$abc %s def'

        assert prepare_query(sql, ('x',), 'postgresql') == (sql, ())


class TestPrepareQueryComments:
    """SQL comments (-- line, /* */ block) must be protected from placeholder
    extraction.
    """

    def test_line_comment_protects_percent_s(self):
        r"""-- comment %s is literal; only the post-newline %s binds.

        Mutation: the '--' branch dropped from _protected_ranges, so a
            placeholder inside a comment binds an argument.
        Oracle: hand-written SQL identical to the input, plus (5,).
        """
        sql = 'SELECT * FROM t -- comment %s\nWHERE id = %s'

        assert prepare_query(sql, (5,), 'postgresql') == (sql, (5,))

    def test_block_comment_protects_percent_s(self):
        """/* %s */ is literal; only the outside placeholder binds.

        Mutation: dropping the `/*` branch from _protected_ranges, which
            binds 10 to the commented-out placeholder.
        Oracle: hand-written SQL identical to the input, plus (10,).
        """
        sql = 'SELECT /* has %s */ * FROM t WHERE id = %s'

        assert prepare_query(sql, (10,), 'postgresql') == (sql, (10,))

    def test_multiline_block_comment_protected(self):
        """A /* ... */ block spanning a newline stays protected throughout.

        Mutation: terminating a block comment at the newline, as the '--'
            branch does, which would expose the '%s' on line two.
        Oracle: hand-written SQL identical to the input, plus (2,).
        """
        sql = 'SELECT /* line1\n%s line2 */ * FROM t WHERE id = %s'

        assert prepare_query(sql, (2,), 'postgresql') == (sql, (2,))

    def test_unterminated_block_comment_protects_to_end_of_sql(self):
        """An unclosed /* swallows the rest of the statement.

        Mutation: `j = n if j == -1 else j + 2` changed to leave j at the
            comment start, which would bind 1 to the trailing '%s'.
        Oracle: hand-written SQL unchanged and an empty arg tuple.
        """
        sql = 'SELECT * FROM t /* %s WHERE id = %s'

        assert prepare_query(sql, (1,), 'postgresql') == (sql, ())

    def test_comment_marker_inside_string_literal_is_not_a_comment(self):
        """String protection wins over comment markers.

        Mutation: scanning for '--' before string literals in
            _protected_ranges, which would swallow the closing quote and
            the rest of the line into a comment.
        Oracle: hand-written SQL identical to the input, plus (1,).
        """
        sql = "SELECT 'foo -- bar baz' AS lit FROM t WHERE id = %s"

        assert prepare_query(sql, (1,), 'postgresql') == (sql, (1,))

    def test_apostrophe_inside_line_comment_does_not_open_a_literal(self):
        """The reverse precedence: a comment's quote never starts a string.

        Mutation: running the string-literal scan over the whole SQL before
            the comment scan, so the unmatched quote in "it's" would
            protect everything after it and swallow the real placeholder.
        Oracle: hand-written SQL identical to the input, plus (1,) - an
            arg tuple only the surviving placeholder can produce.
        """
        sql = "SELECT * FROM t -- it's fine %s\nWHERE id = %s"

        assert prepare_query(sql, (1,), 'postgresql') == (sql, (1,))

    def test_unterminated_string_literal_protects_to_end_of_sql(self):
        """An unclosed string literal swallows the rest of the statement.

        Mutation: the `else: j = n` arm in _protected_ranges changed to
            `j = i + 1`, which exposes the %s inside the literal.
        Oracle: hand-written SQL unchanged and an empty arg tuple.
        """
        sql = "SELECT * FROM t WHERE s = 'abc %s"

        assert prepare_query(sql, ('x',), 'postgresql') == (sql, ())

    def test_double_quoted_literal_is_protected(self):
        r"""A double-quoted literal's %s is not a real placeholder.

        Mutation: dropping '\"' from the string-literal branch of
            _protected_ranges, which exposes the %s inside the identifier.
        Oracle: hand-written SQL identical to the input, plus (1,).
        """
        sql = 'SELECT * FROM t WHERE "c %s n" = %s'

        assert prepare_query(sql, (1,), 'postgresql') == (sql, (1,))

    def test_question_mark_in_line_comment_protected_sqlite(self):
        """-- ? comment is literal in SQLite.

        Mutation: skipping the comment scan when the dialect is sqlite.
        Oracle: hand-written SQL identical to the input, plus (1,).
        """
        sql = 'SELECT * FROM t -- comment ?\nWHERE id = ?'

        assert prepare_query(sql, (1,), 'sqlite') == (sql, (1,))


class TestPrepareQueryJsonbOperator:
    """PostgreSQL JSONB ? operator must NOT be treated as a placeholder.

    Rule: in dialect='postgresql', '?' followed by a quoted literal or by
    one of the JSONB multi-char ops ('|', '&') is a JSONB operator, not a
    placeholder. SQLite '?' remains a placeholder.
    """

    def test_jsonb_question_mark_with_quoted_literal_not_placeholder(self):
        """`data ? 'key'` keeps its operator; only `id = %s` binds.

        Mutation: dropping the quoted-literal lookahead from _is_jsonb_op
            so it only recognizes '?|' and '?&'.
        Oracle: hand-written SQL identical to the input, plus (1,).
        """
        sql = "SELECT * FROM t WHERE data ? 'key' AND id = %s"

        assert prepare_query(sql, (1,), 'postgresql') == (sql, (1,))

    def test_jsonb_question_mark_pipe_not_placeholder(self):
        """`data ?| array[...]` is preserved.

        Mutation: narrowing `sql[pos + 1] in '|&'` to `== '&'`.
        Oracle: hand-written SQL identical to the input, plus (2,).
        """
        sql = "SELECT * FROM t WHERE data ?| array['a','b'] AND id = %s"

        assert prepare_query(sql, (2,), 'postgresql') == (sql, (2,))

    def test_jsonb_question_mark_amp_not_placeholder(self):
        """`data ?& array[...]` is preserved.

        Mutation: narrowing `sql[pos + 1] in '|&'` to `== '|'`.
        Oracle: hand-written SQL identical to the input, plus (3,).
        """
        sql = "SELECT * FROM t WHERE data ?& array['a','b'] AND id = %s"

        assert prepare_query(sql, (3,), 'postgresql') == (sql, (3,))

    def test_question_mark_before_a_word_is_still_a_placeholder(self):
        """The other side of _is_jsonb_op: '? AND' converts to '%s'.

        Mutation: _is_jsonb_op returning True for any '?' followed by
            whitespace, which would silently drop a real bind.
        Oracle: boundary straddling the quote test - `? 'key'` above stays
            an operator, `? AND` here becomes a marker.
        """
        sql = 'SELECT * FROM t WHERE id = ? AND x = 1'

        assert prepare_query(sql, (5,), 'postgresql') == (
            'SELECT * FROM t WHERE id = %s AND x = 1', (5,))

    def test_sqlite_question_mark_remains_placeholder(self):
        """The PG-only rule must not affect SQLite.

        Mutation: dropping `dialect == 'postgresql'` from the jsonb guard
            in _find_contexts, which drops the sqlite bind that sits
            against a '||' concatenation.
        Oracle: hand-written SQL identical to the input, plus the args -
            the second query puts '?' where _is_jsonb_op would answer yes.
        """
        sql = 'SELECT * FROM t WHERE id = ? AND name = ?'

        assert prepare_query(sql, (1, 'foo'), 'sqlite') == (sql, (1, 'foo'))

        concat_sql = "SELECT ?||'-suffix' AS s FROM t"

        assert prepare_query(concat_sql, ('a',), 'sqlite') == (concat_sql, ('a',))


class TestQuoteIdentifier:
    """Test database identifier quoting."""

    @pytest.mark.parametrize(
        ('identifier', 'dialect', 'expected'),
        [
            ('my_table', 'postgresql', '"my_table"'),
            ('user', 'postgresql', '"user"'),
            ('table"with"quotes', 'postgresql', '"table""with""quotes"'),
            ('my_table', 'sqlite', '"my_table"'),
            ('column"quoted', 'sqlite', '"column""quoted"'),
            ],
        ids=['pg_basic', 'pg_reserved', 'pg_quotes', 'sqlite_basic',
             'sqlite_quotes'])
    def test_quote_identifier(self, identifier, dialect, expected):
        """Verify wrapping plus doubling of embedded quotes.

        Mutation: dropping the `replace(chr(34), chr(34) * 2)` escape in
            quote_identifier, which reopens quoted-identifier injection.
        Oracle: hand-written expected strings.
        """
        assert quote_identifier(identifier, dialect) == expected

    def test_default_dialect(self):
        """The default dialect is a supported one, so no error is raised.

        Mutation: changing quote_identifier's dialect default to a value
            outside _SUPPORTED_DIALECTS, which raises DatabaseError.
        Oracle: hand-written '"table"'.
        """
        assert quote_identifier('table') == '"table"'

    def test_unknown_dialect_error(self):
        """An unsupported dialect is rejected rather than silently quoted.

        Mutation: dropping the `dialect not in _SUPPORTED_DIALECTS` guard,
            which would let a mysql caller receive ANSI double quotes.
        Oracle: the raised DatabaseError and its message.
        """
        with pytest.raises(DatabaseError, match='Unknown dialect: mysql'):
            quote_identifier('table', 'mysql')

    @pytest.mark.parametrize(
        ('identifier', 'dialect', 'expected'),
        [
            ('public.foo', 'postgresql', '"public"."foo"'),
            ('myschema.t', 'postgresql', '"myschema"."t"'),
            ('public.order', 'postgresql', '"public"."order"'),
            ('public.foo', 'sqlite', '"public"."foo"'),
            ],
        ids=['pg_public', 'pg_myschema', 'pg_reserved_word', 'sqlite_dotted'])
    def test_quote_identifier_dotted_splits_segments(
        self, identifier, dialect,
        expected):
        """Dotted identifiers split on unquoted dots and quote each part.

        Mutation: dropping the `c == '.'` branch from
            _split_qualified_identifier, which yields the single dead
            identifier '"public.foo"'.
        Oracle: hand-written two-segment expected strings.
        """
        assert quote_identifier(identifier, dialect) == expected

    def test_quote_identifier_unqualified_unchanged(self):
        """An unqualified identifier stays one segment.

        Mutation: _split_qualified_identifier appending an empty trailing
            segment, which would give '"foo".""'.
        Oracle: hand-written '"foo"'.
        """
        assert quote_identifier('foo', 'postgresql') == '"foo"'

    def test_dot_inside_a_quoted_segment_is_not_a_separator(self):
        """A pre-quoted segment keeps its dot instead of splitting on it.

        Mutation: letting the `c == '.'` split run while in_quote is set
            in _split_qualified_identifier, which splits '"weird.name"'
            into two segments and renames the table.
        Oracle: hand-written single-segment output for a dotted name.
        """
        assert quote_identifier('"weird.name"', 'postgresql') == '"weird.name"'

    def test_quoted_schema_with_dot_then_plain_table(self):
        """Only the unquoted dot separates: one quoted schema, one table.

        Mutation: letting the `c == '.'` split run while in_quote is set,
            which splits the schema name and produces three segments.
        Oracle: hand-written '"my.schema"."tbl"'.
        """
        assert quote_identifier('"my.schema".tbl', 'postgresql') == '"my.schema"."tbl"'

    def test_doubled_quote_inside_a_quoted_segment_round_trips(self):
        """'""' inside a quoted segment decodes to one quote and re-encodes.

        Mutation: dropping the `identifier[i + 1] == '"'` unescape branch,
            which would close the segment early and lose the 'b'.
        Oracle: hand-written '"a""b"', unchanged by a decode/encode pair.
        """
        assert quote_identifier('"a""b"', 'postgresql') == '"a""b"'


class TestQuoteIdentifierNullByteRejection:
    """Null bytes in identifiers must be rejected, not quoted."""

    def test_rejects_null_byte_in_identifier(self):
        r"""A bare identifier containing \x00 raises ValidationError.

        Mutation: dropping the `'\x00' in identifier` guard, which lets a
            truncating null byte through into the quoted identifier.
        Oracle: the raised ValidationError and its message.
        """
        with pytest.raises(ValidationError, match='null byte'):
            quote_identifier('foo\x00bar', 'postgresql')

    def test_rejects_null_byte_in_dotted_identifier(self):
        r"""Any segment carrying \x00 raises, dot-splitting included.

        Mutation: moving the null-byte check after
            _split_qualified_identifier and applying it to the first
            segment only.
        Oracle: the raised ValidationError, on a name whose null byte sits
            in the second segment.
        """
        with pytest.raises(ValidationError, match='null byte'):
            quote_identifier('public.foo\x00', 'postgresql')

    def test_rejects_null_byte_for_sqlite_dialect(self):
        """Null-byte rejection is dialect-independent.

        Mutation: guarding the null-byte check with
            `if dialect == 'postgresql'`.
        Oracle: the raised ValidationError under the sqlite dialect.
        """
        with pytest.raises(ValidationError, match='null byte'):
            quote_identifier('foo\x00', 'sqlite')


class TestMakePlaceholders:
    """Test the comma-joined placeholder run used by insert/upsert builders."""

    @pytest.mark.parametrize(
        ('count', 'dialect', 'expected'),
        [
            (3, 'postgresql', '%s, %s, %s'),
            (3, 'sqlite', '?, ?, ?'),
            (1, 'postgresql', '%s'),
            (1, 'sqlite', '?'),
            (0, 'postgresql', ''),
            (2, 'mysql', '%s, %s'),
            ],
        ids=['pg_three', 'sqlite_three', 'pg_one', 'sqlite_one', 'pg_zero',
             'unknown_dialect_defaults_pg'])
    def test_make_placeholders(self, count, dialect, expected):
        """Verify marker choice, separator, and exact count.

        Mutation: off-by-one in `[marker] * count`, joining on ',' instead
            of ', ', or selecting the marker with `dialect == 'postgresql'`
            so every unknown dialect gets '?'.
        Oracle: hand-written strings; the count-1 rows pin that no
            separator is emitted for a single placeholder.
        """
        assert make_placeholders(count, dialect) == expected


class TestHasPlaceholders:
    """Test placeholder detection."""

    @pytest.mark.parametrize(
        ('sql', 'expected'),
        [
            ('', False),
            (None, False),
            ('SELECT * FROM users', False),
            ('SELECT * FROM stats WHERE growth > 10%', False),
            ('SELECT id::text FROM t', False),
            ('SELECT * FROM users WHERE id = %s', True),
            ('SELECT * FROM users WHERE id = ?', True),
            ('INSERT INTO t VALUES (%s, %s, ?)', True),
            ('SELECT * FROM users WHERE id = %(user_id)s', True),
            ('INSERT INTO t VALUES (%(id)s, %(name)s)', True),
            ('SELECT * FROM t WHERE id = %s AND name = %(name)s', True),
            ('SELECT * FROM t WHERE id = :id', True),
            ('SELECT id::text FROM t WHERE id = :id', True),
            ],
        ids=[
            'empty', 'none', 'no_placeholders', 'percent_not_placeholder',
            'pg_type_cast', 'percent_s', 'qmark', 'mixed_positional',
            'named_single', 'named_multiple', 'mixed_types', 'sqlite_named',
            'cast_and_named'])
    def test_has_placeholders(self, sql, expected):
        r"""Verify detection, including the '::' cast carve-out.

        Mutation: dropping the `(?<!:)` lookbehind from _HAS_PH_RE, which
            reports every PostgreSQL '::text' cast as a named placeholder;
            or dropping the ':\\w+' alternative, which misses sqlite's
            named style.
        Oracle: hand-written verdicts; the pg_type_cast and cast_and_named
            rows straddle the lookbehind.
        """
        assert has_placeholders(sql) is expected


class TestHasNamedPlaceholders:
    """Named-placeholder detection: what tells a binding map from a value."""

    @pytest.mark.parametrize(
        ('sql', 'dialect', 'expected'),
        [
            ('SELECT * FROM t WHERE id = %(id)s', 'postgresql', True),
            ('SELECT * FROM t WHERE id = %(id)s', 'sqlite', True),
            ('SELECT * FROM t WHERE id = %s', 'postgresql', False),
            ('SELECT * FROM t WHERE id = ?', 'sqlite', False),
            ('SELECT * FROM t WHERE id = :id', 'sqlite', True),
            ('SELECT * FROM t WHERE id = :id', 'postgresql', False),
            ('SELECT id::text FROM t', 'postgresql', False),
            ('SELECT id::text FROM t', 'sqlite', False),
            ('SELECT arr[1:3] FROM t', 'postgresql', False),
            ('SELECT arr[1:3] FROM t WHERE id = %(id)s', 'postgresql', True),
            ('', 'postgresql', False),
            (None, 'postgresql', False),
            ],
        ids=[
            'pyformat_pg', 'pyformat_sqlite', 'percent_s_pg', 'qmark_sqlite',
            'colon_sqlite', 'colon_pg', 'cast_pg', 'cast_sqlite', 'slice_pg',
            'slice_beside_real_name_pg', 'empty', 'none'])
    def test_named_detection_by_dialect(self, sql, dialect, expected):
        """Only pyformat counts on postgres; sqlite also counts ':name'.

        Mutation: appending _NAMED_COLON_RE for every dialect rather than
            for sqlite alone, which reads 'arr[1:3]' and a bare ':id' as
            bound names under postgres; or dropping it, which blinds
            sqlite to its own named style.
        Oracle: hand-written verdicts; the colon and cast rows are the
            same SQL under both dialects, so they straddle the switch.
        """
        assert has_named_placeholders(sql, dialect) is expected

    @pytest.mark.parametrize(
        ('sql', 'dialect', 'expected'),
        [
            ("SELECT * FROM t WHERE s = '%(id)s'", 'postgresql', False),
            ("SELECT * FROM t WHERE s = '%(a)s' AND id = %(id)s",
             'postgresql', True),
            ('SELECT 1 -- %(id)s\n', 'postgresql', False),
            ('SELECT /* %(id)s */ 1', 'postgresql', False),
            ('SELECT $$ %(id)s $$ FROM t', 'postgresql', False),
            ('SELECT $body$ %(id)s $body$, %(real)s FROM t',
             'postgresql', True),
            ("SELECT ':id' FROM t", 'sqlite', False),
            ("SELECT ':id' FROM t WHERE x = :x", 'sqlite', True),
            ('SELECT 1 -- :id\n', 'sqlite', False),
            ('SELECT /* :id */ 1', 'sqlite', False),
            ],
        ids=[
            'literal_pg', 'literal_plus_real_pg', 'line_comment_pg',
            'block_comment_pg', 'dollar_body_pg', 'dollar_plus_real_pg',
            'literal_sqlite', 'literal_plus_real_sqlite',
            'line_comment_sqlite', 'block_comment_sqlite'])
    def test_protected_contexts_hold_no_named_placeholder(
        self, sql, dialect,
        expected):
        """A name inside a literal, comment or dollar body binds nothing.

        Mutation: dropping the _protected_ranges filter, so the function
            answers on the raw regex hit and a dict gets bound by name for
            SQL whose only ':id' sits in a comment.
        Oracle: paired rows differing only in whether a second, unprotected
            name follows the protected one.
        """
        assert has_named_placeholders(sql, dialect) is expected

    def test_dollar_body_is_protected_only_for_postgresql(self):
        """Dollar quoting is postgres-only, so the dialect must reach the scan.

        Mutation: calling _protected_ranges(sql) without forwarding
            `dialect`, which protects the $$ body under sqlite too and
            hides a real pyformat name from a sqlite caller.
        Oracle: differential - one SQL string, read False under postgres
            and True under sqlite, matching how prepare_query already
            treats a $$ body for each dialect.
        """
        sql = 'SELECT $$ %(name)s $$ FROM t'

        assert has_named_placeholders(sql, 'postgresql') is False
        assert has_named_placeholders(sql, 'sqlite') is True


class TestStandardizePlaceholders:
    """Test placeholder conversion between dialects."""

    @pytest.mark.parametrize(
        ('sql', 'dialect', 'expected'),
        [
            ('SELECT * FROM users WHERE id = %s AND name = %s', 'sqlite',
             'SELECT * FROM users WHERE id = ? AND name = ?'),
            ('SELECT * FROM users WHERE id = ? AND name = ?', 'postgresql',
             'SELECT * FROM users WHERE id = %s AND name = %s'),
            ('SELECT * FROM users WHERE id = %s', 'postgresql',
             'SELECT * FROM users WHERE id = %s'),
            ('SELECT * FROM users WHERE id = ?', 'sqlite',
             'SELECT * FROM users WHERE id = ?'),
            ],
        ids=['percent_to_qmark', 'qmark_to_percent', 'pg_no_change',
             'sqlite_no_change'])
    def test_placeholder_conversion(self, sql, dialect, expected):
        """Verify conversion in both directions and both no-op quick paths.

        Mutation: inverting `target = '?' if dialect == 'sqlite' else '%s'`,
            or inverting either quick-check so the wrong dialect returns
            early untouched.
        Oracle: hand-written expected strings for all four combinations.
        """
        assert standardize_placeholders(sql, dialect) == expected

    def test_named_params_survive_conversion_of_positional_ones(self):
        """A pyformat name stays put while the bare '%s' beside it converts.

        Mutation: dropping the `m.group(1)` check from the replace callback
            in standardize_placeholders, which rewrites '%(a)s' to '?'.
        Oracle: hand-written mixed SQL; the '%s' must move and the
            '%(a)s' must not.
        """
        sql = 'SELECT * FROM t WHERE a = %(a)s AND b = %s'

        assert standardize_placeholders(sql, 'sqlite') == (
            'SELECT * FROM t WHERE a = %(a)s AND b = ?')

    def test_named_only_sql_is_returned_unchanged(self):
        """A named-only query takes the sqlite quick path untouched.

        Mutation: the quick check `'%s' not in sql` widened to `'%' not in
            sql`, which drags '%(id)s' into the substitution pass.
        Oracle: hand-written SQL identical to the input.
        """
        sql = 'SELECT * FROM users WHERE id = %(id)s'

        assert standardize_placeholders(sql, 'sqlite') == sql

    def test_preserves_string_literals(self):
        """A '%s' inside a literal is not converted; the real one is.

        Mutation: dropping the `m.start() in protected` check from the
            replace callback, which would corrupt the literal's text.
        Oracle: hand-written SQL where exactly one of the two '%s' moves.
        """
        sql = "SELECT * FROM t WHERE id = %s AND name = 'test %s value'"

        assert standardize_placeholders(sql, 'sqlite') == (
            "SELECT * FROM t WHERE id = ? AND name = 'test %s value'")

    def test_preserves_line_comments(self):
        """A '%s' inside a -- comment is not converted.

        Mutation: standardize_placeholders calling _PH_RE.sub without
            consulting _protected_ranges at all.
        Oracle: hand-written SQL where the comment keeps '%s' and the
            WHERE clause takes '?'.
        """
        sql = 'SELECT * FROM t -- %s\nWHERE id = %s'

        assert standardize_placeholders(sql, 'sqlite') == (
            'SELECT * FROM t -- %s\nWHERE id = ?')

    def test_preserves_dollar_quoted_body_for_postgresql(self):
        """A '?' inside $$...$$ stays; the one outside becomes '%s'.

        Mutation: passing a fixed 'sqlite' dialect into _protected_ranges
            from standardize_placeholders, which disables dollar-quote
            protection.
        Oracle: hand-written SQL where exactly one of the two '?' moves.
        """
        sql = 'SELECT $$a ? b$$, ? FROM t'

        assert standardize_placeholders(sql, 'postgresql') == (
            'SELECT $$a ? b$$, %s FROM t')

    def test_jsonb_operator_survives_conversion(self):
        """The JSONB '?' operator is not converted to '%s'.

        Mutation: dropping the _is_jsonb_op guard from the replace callback
            in standardize_placeholders, which rewrites `data ? 'k'` into
            the meaningless `data %s 'k'`.
        Oracle: hand-written SQL where the operator stays and the trailing
            bind converts.
        """
        sql = "SELECT * FROM t WHERE data ? 'k' AND id = ?"

        assert standardize_placeholders(sql, 'postgresql') == (
            "SELECT * FROM t WHERE data ? 'k' AND id = %s")


class TestFullPipeline:
    """Test complete query processing scenarios."""

    def test_postgresql_complex_query(self):
        """IN expansion, IS NOT NULL inlining and '%' escaping in one query.

        Mutation: any single stage regressing - _expand_in's width, the
            IS NOT NULL branch of _proc_pos, or _escape_percents on the
            trailing segment.
        Oracle: one hand-written expected SQL string covering all three.
        """
        sql = ("SELECT * FROM users WHERE created BETWEEN %s AND %s AND id IN %s "
               "AND status IS NOT %s AND name LIKE 'test%'")
        expected = ("SELECT * FROM users WHERE created BETWEEN %s AND %s "
                    "AND id IN (%s, %s, %s) AND status IS NOT NULL "
                    "AND name LIKE 'test%%'")

        result = prepare_query(sql, (JAN_1, DEC_31, (1, 2, 3), None), 'postgresql')

        assert result == (expected, (JAN_1, DEC_31, 1, 2, 3))

    def test_sqlite_complex_query(self):
        """IN expansion and marker conversion compose for sqlite.

        Mutation: _expand_in hardcoding '%s' instead of using the marker
            _transform passes in, which leaves a mixed-style statement.
        Oracle: hand-written SQL with '?' in both the IN run and the tail.
        """
        sql = 'SELECT * FROM users WHERE id IN %s AND status = %s'

        result = prepare_query(sql, ((1, 2), 'active'), 'sqlite')

        assert result == (
            'SELECT * FROM users WHERE id IN (?, ?) AND status = ?',
            (1, 2, 'active'))

    def test_named_params_complex(self):
        """A named IN alongside two plain names rewrites keys exactly once.

        Mutation: _expand_named_in numbering from 1, or _transform using
            `new_args = args` so the original 'ids' key survives.
        Oracle: hand-written SQL plus the exact five-key dict.
        """
        sql = ('SELECT * FROM items WHERE item_id IN %(ids)s AND date = %(date)s '
               'AND user = %(user)s')
        expected = ('SELECT * FROM items WHERE item_id IN '
                    '(%(ids_0)s, %(ids_1)s, %(ids_2)s) AND date = %(date)s '
                    'AND user = %(user)s')
        args = {
            'ids': ['ITM001', 'ITM002', 'ITM003'],
            'date': '2025-08-08',
            'user': 'USER_A',
            }

        result = prepare_query(sql, args, 'postgresql')

        assert result == (expected, {
            'ids_0': 'ITM001',
            'ids_1': 'ITM002',
            'ids_2': 'ITM003',
            'date': '2025-08-08',
            'user': 'USER_A',
            })


class TestSQLMatrix:
    """Traceability matrix with explicit input -> output mappings.

    Every expected SQL below is a full hand-written string, so a case fails
    on any deviation rather than on a missing substring.
    """

    POSITIONAL_CASES = [
        ('pg_basic', 'SELECT * FROM t WHERE id = %s', (1,), 'postgresql',
         'SELECT * FROM t WHERE id = %s', (1,)),
        ('pg_multi', 'SELECT * FROM t WHERE a = %s AND b = %s', (1, 2), 'postgresql',
         'SELECT * FROM t WHERE a = %s AND b = %s', (1, 2)),
        ('sqlite_basic', 'SELECT * FROM t WHERE id = %s', (1,), 'sqlite',
         'SELECT * FROM t WHERE id = ?', (1,)),
        ('sqlite_multi', 'SELECT * FROM t WHERE a = %s AND b = %s', (1, 2), 'sqlite',
         'SELECT * FROM t WHERE a = ? AND b = ?', (1, 2)),
        ]

    IN_CLAUSE_CASES = [
        ('in_tuple_in_list', 'WHERE id IN %s', [(1, 2, 3)], 'postgresql',
         'WHERE id IN (%s, %s, %s)', (1, 2, 3)),
        ('in_direct_list', 'WHERE id IN %s', [1, 2, 3], 'postgresql',
         'WHERE id IN (%s, %s, %s)', (1, 2, 3)),
        ('in_nested_list', 'WHERE id IN %s', [[1, 2, 3]], 'postgresql',
         'WHERE id IN (%s, %s, %s)', (1, 2, 3)),
        ('in_single_item', 'WHERE id IN %s', [101], 'postgresql',
         'WHERE id IN (%s)', (101,)),
        ('in_single_tuple', 'WHERE id IN %s', [(42,)], 'postgresql',
         'WHERE id IN (%s)', (42,)),
        ('in_empty', 'WHERE id IN %s', [()], 'postgresql',
         'WHERE id IN (NULL)', ()),
        ('in_parens', 'WHERE id IN (%s)', ((1, 2, 3),), 'postgresql',
         'WHERE id IN (%s, %s, %s)', (1, 2, 3)),
        ('in_sqlite', 'WHERE id IN %s', [(1, 2)], 'sqlite',
         'WHERE id IN (?, ?)', (1, 2)),
        ('in_sqlite_empty', 'WHERE id IN %s', [()], 'sqlite',
         'WHERE id IN (NULL)', ()),
        ('in_lowercase', 'where id in %s', [(1, 2, 3)], 'postgresql',
         'where id in (%s, %s, %s)', (1, 2, 3)),
        ]

    MIXED_CASES = [
        ('mixed_in_after', 'WHERE a = %s AND id IN %s', ('x', (1, 2)), 'postgresql',
         'WHERE a = %s AND id IN (%s, %s)', ('x', 1, 2)),
        ('mixed_in_middle', 'WHERE a = %s AND id IN (%s) AND b = %s',
         ('x', [1, 2], 'y'), 'postgresql',
         'WHERE a = %s AND id IN (%s, %s) AND b = %s', ('x', 1, 2, 'y')),
        ('multi_in', 'WHERE id IN %s AND status IN %s', [(1, 2), ('a', 'b')],
         'postgresql', 'WHERE id IN (%s, %s) AND status IN (%s, %s)',
         (1, 2, 'a', 'b')),
        ('multi_in_sqlite', 'WHERE id IN %s AND status IN %s', [[1, 2], [3, 4]],
         'sqlite', 'WHERE id IN (?, ?) AND status IN (?, ?)', (1, 2, 3, 4)),
        ]

    IS_NULL_CASES = [
        ('is_null', 'WHERE v IS %s', (None,), 'postgresql',
         'WHERE v IS NULL', ()),
        ('is_not_null', 'WHERE v IS NOT %s', (None,), 'postgresql',
         'WHERE v IS NOT NULL', ()),
        ('is_null_mixed', 'WHERE v IS %s AND x = %s', (None, 'test'), 'postgresql',
         'WHERE v IS NULL AND x = %s', ('test',)),
        ('is_non_null', 'WHERE v IS %s', ('val',), 'postgresql',
         'WHERE v IS %s', ('val',)),
        ('is_null_sqlite', 'WHERE v IS %s AND x = %s', (None, 'test'), 'sqlite',
         'WHERE v IS NULL AND x = ?', ('test',)),
        ('is_null_lowercase', 'where v is %s', (None,), 'postgresql',
         'where v is NULL', ()),
        ]

    NAMED_CASES = [
        ('named_basic', 'WHERE id = %(id)s', {'id': 1}, 'postgresql',
         'WHERE id = %(id)s', {'id': 1}),
        ('named_multi', 'WHERE a = %(a)s AND b = %(b)s', {'a': 1, 'b': 2},
         'postgresql', 'WHERE a = %(a)s AND b = %(b)s', {'a': 1, 'b': 2}),
        ('named_in', 'WHERE id IN %(ids)s', {'ids': (1, 2, 3)}, 'postgresql',
         'WHERE id IN (%(ids_0)s, %(ids_1)s, %(ids_2)s)',
         {'ids_0': 1, 'ids_1': 2, 'ids_2': 3}),
        ('named_is_null', 'WHERE v IS %(v)s AND x = %(x)s', {'v': None, 'x': 'test'},
         'postgresql', 'WHERE v IS NULL AND x = %(x)s', {'x': 'test'}),
        ('dict_in_list', 'WHERE id = %(id)s', [{'id': 1}], 'postgresql',
         'WHERE id = %(id)s', {'id': 1}),
        ('named_sqlite', 'WHERE a = %(a)s AND b = %(b)s', {'a': 1, 'b': 2},
         'sqlite', 'WHERE a = :a AND b = :b', {'a': 1, 'b': 2}),
        ('named_is_not_null', 'WHERE v IS NOT %(v)s', {'v': None}, 'postgresql',
         'WHERE v IS NOT NULL', {}),
        ]

    NORMALIZATION_CASES = [
        ('norm_nested_tuple', 'WHERE a = %s AND b = %s AND c = %s',
         [(1, 2, 3)], 'postgresql', 'WHERE a = %s AND b = %s AND c = %s', (1, 2, 3)),
        ('norm_empty_args', 'SELECT 1', (), 'postgresql', 'SELECT 1', ()),
        ('norm_none_args', 'SELECT 1', None, 'postgresql', 'SELECT 1', None),
        ]

    ESCAPE_CASES = [
        ('esc_single_quote', "WHERE s = 'foo%' AND id = %s", (1,), 'postgresql',
         "WHERE s = 'foo%%' AND id = %s", (1,)),
        ('esc_double_quote', 'WHERE s = "foo%" AND id = %s', (1,), 'postgresql',
         'WHERE s = "foo%%" AND id = %s', (1,)),
        ('esc_already', "WHERE s LIKE 'foo%%' AND id = %s", (1,), 'postgresql',
         "WHERE s LIKE 'foo%%' AND id = %s", (1,)),
        ('esc_sqlite_none', "WHERE s = 'foo%' AND id = %s", (1,), 'sqlite',
         "WHERE s = 'foo%' AND id = ?", (1,)),
        ('esc_no_ph', "WHERE s = 'foo%'", None, 'postgresql',
         "WHERE s = 'foo%'", None),
        ]

    ALL_CASES = (POSITIONAL_CASES + IN_CLAUSE_CASES + MIXED_CASES + IS_NULL_CASES
                 + NAMED_CASES + NORMALIZATION_CASES + ESCAPE_CASES)

    @pytest.mark.parametrize(
        ('case_id', 'sql', 'args', 'dialect', 'expected_sql', 'expected_args'),
        ALL_CASES,
        ids=[c[0] for c in ALL_CASES])
    def test_matrix(self, case_id, sql, args, dialect, expected_sql, expected_args):
        """Verify the exact (sql, args) pair for every documented case.

        Mutation: any change to _normalize's four rules, _expand_in's
            paren handling, _proc_pos's NULL branch, _named_ph's dialect
            switch, or _escape_percents - each row pins one of them.
        Oracle: hand-written full SQL and args per row; dialect pairs
            (in_sqlite vs in_parens, named_sqlite vs named_basic) act as
            differential comparands.
        """
        result = prepare_query(sql, args, dialect)

        assert result == (expected_sql, expected_args), case_id


if __name__ == '__main__':
    __import__('pytest').main([__file__])
