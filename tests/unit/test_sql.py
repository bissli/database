"""Unit tests for SQL parameter processing and identifier quoting."""
import datetime
import time

import pytest
from database.exceptions import DatabaseError, QueryError, ValidationError
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
            ('select foo from t where date between %s and %s and value > %s',
             (JAN_1, MAR_11, 100),
             'postgresql',
             'select foo from t where date between %s and %s and value > %s',
             (JAN_1, MAR_11, 100)),
            ('select * from users where id = %s and status = %s',
             (1, 'active'),
             'sqlite',
             'select * from users where id = ? and status = ?',
             (1, 'active')),
            ('select * from users where date between %s and %s and status = %s',
             [(JAN_1, MAR_11, 'active')],
             'postgresql',
             'select * from users where date between %s and %s and status = %s',
             (JAN_1, MAR_11, 'active')),
            ('select * from users where active = true',
             None,
             'postgresql',
             'select * from users where active = true',
             None),
            ('select * from users',
             (),
             'postgresql',
             'select * from users',
             ()),
            ],
        ids=['pg_basic', 'sqlite_conversion', 'nested_tuple', 'no_placeholders',
             'empty_args'])
    def test_basic_handling(self, sql, args, dialect, expected_sql, expected_args):
        """Verify the whole (sql, args) pair for plain positional binding.

        Mutation: swapping the marker in _transform so sqlite emits '%s'.
        Oracle: hand-written full SQL strings and arg tuples.
        """
        assert prepare_query(sql, args, dialect) == (expected_sql, expected_args)

    @pytest.mark.parametrize(
        ('args', 'dialect'),
        [
            ((1,), 'postgresql'),
            ((1, 2, 3), 'postgresql'),
            ((1,), 'sqlite'),
            ((1, 2, 3), 'sqlite'),
            ((), 'sqlite'),
            (([1, 2, 3],), 'postgresql'),
            ],
        ids=['pg_too_few', 'pg_too_many', 'sqlite_too_few', 'sqlite_too_many',
             'none_for_two', 'inner_longer_than_slots'])
    def test_arg_count_mismatch_raises(self, args, dialect):
        """Verify a positional arg count unequal to the placeholders raises.

        Mutation: dropping the count check in _transform.
        Oracle: two placeholders against 1, 3 or 0 args, or one list value.
        """
        with pytest.raises(QueryError, match='Parameter count mismatch'):
            prepare_query('select * from t where a = %s and b = %s', args, dialect)

    def test_lone_bytes_arg_is_not_spread_across_placeholders(self):
        """One bytes arg against two placeholders raises, never binds its ints.

        Mutation: libb issequence counting bytes, so b'ab' fills both
            as 97, 98.
        Oracle: two placeholders against one arg.
        """
        with pytest.raises(QueryError, match='Parameter count mismatch'):
            prepare_query('select * from t where a = %s and b = %s', (b'ab',))


class TestDialectDefaults:
    """The default dialect of every public entry point is postgresql."""

    def test_prepare_query_defaults_to_postgresql(self):
        """prepare_query with no dialect keeps '%s' rather than emitting '?'.

        Mutation: prepare_query's dialect default changed to 'sqlite'.
        Oracle: hand-written SQL that only the postgres marker satisfies.
        """
        assert prepare_query('select * from t where id = %s', (1,)) == (
            'select * from t where id = %s', (1,))

    def test_standardize_placeholders_defaults_to_postgresql(self):
        """standardize_placeholders with no dialect converts '?' to '%s'.

        Mutation: its dialect default changed to 'sqlite'.
        Oracle: hand-written converted SQL.
        """
        assert standardize_placeholders('select * from t where id = ?') == (
            'select * from t where id = %s')

    def test_make_placeholders_defaults_to_postgresql(self):
        """make_placeholders with no dialect emits the pyformat marker.

        Mutation: make_placeholders' dialect default changed to 'sqlite'.
        Oracle: hand-written '%s, %s'.
        """
        assert make_placeholders(2) == '%s, %s'


class TestPrepareQueryInClause:
    """Test IN clause parameter expansion."""

    @pytest.mark.parametrize(
        ('args', 'expected_sql', 'expected_args'),
        [
            ([(1, 2, 3)], 'select * from users where id in (%s, %s, %s)', (1, 2, 3)),
            ([(42,)], 'select * from users where id in (%s)', (42,)),
            ([1, 2, 3], 'select * from users where id in (%s, %s, %s)', (1, 2, 3)),
            ([[1, 2, 3]], 'select * from users where id in (%s, %s, %s)', (1, 2, 3)),
            ([101], 'select * from users where id in (%s)', (101,)),
            ],
        ids=['tuple_in_list', 'single_tuple', 'direct_list', 'nested_list',
             'single_item'])
    def test_in_clause_expansion_formats(self, args, expected_sql, expected_args):
        """Verify every accepted IN-arg shape lands on the same expansion.

        Mutation: ',' in place of ', ' when _expand_in joins the markers.
        Oracle: hand-written full SQL strings.
        """
        sql = 'select * from users where id in %s'

        assert prepare_query(sql, args, 'postgresql') == (expected_sql, expected_args)

    def test_no_args_for_a_sole_in_slot_match_nothing(self):
        """Verify an empty spread list against one IN slot binds no values.

        Mutation: _normalize returning the empty args unchanged.
        Oracle: hand-written `in (null)`, which matches no row.
        """
        sql = 'select * from users where id in %s'

        assert prepare_query(sql, (), 'postgresql') == (
            'select * from users where id in (null)', ())

    def test_two_lists_for_one_in_slot_raise(self):
        """Verify two lists against one IN placeholder raise.

        Mutation: _normalize wrapping [[1, 2], [3, 4]] as one IN value.
        Oracle: one IN placeholder against two args.
        """
        with pytest.raises(QueryError, match='Parameter count mismatch'):
            prepare_query(
                'select * from users where id in %s', [[1, 2], [3, 4]], 'postgresql')

    def test_in_clause_empty_sequence(self):
        """An empty IN sequence collapses to a parenthesized null, no args.

        Mutation: 'null' in place of '(null)' in _expand_in without in_parens.
        Oracle: hand-written `in (null)` plus an empty arg tuple.
        """
        result = prepare_query('select * from users where id in %s', [()], 'postgresql')

        assert result == ('select * from users where id in (null)', ())

    def test_in_clause_with_other_params(self):
        """An IN run splices into the middle of the flat arg tuple in order.

        Mutation: appending the IN values after the remaining scalars.
        Oracle: hand-written SQL plus the interleaved arg tuple.
        """
        sql = 'select * from users where date between %s and %s and id in %s'

        result = prepare_query(sql, (JAN_1, MAR_11, (1, 2, 3)), 'postgresql')

        assert result == (
            'select * from users where date between %s and %s and id in (%s, %s, %s)',
            (JAN_1, MAR_11, 1, 2, 3))

    def test_multiple_in_clauses(self):
        """Two IN clauses each expand to their own width and stay ordered.

        Mutation: an off-by-one in `[marker] * len(val)` in _expand_in.
        Oracle: hand-written SQL with widths 3 then 2.
        """
        sql = 'select * from users where id in %s and status in %s'

        result = prepare_query(sql, [(1, 2, 3), ('active', 'pending')], 'postgresql')

        assert result == (
            'select * from users where id in (%s, %s, %s) and status in (%s, %s)',
            (1, 2, 3, 'active', 'pending'))

    def test_multiple_in_clauses_with_inner_lists(self):
        """Inner lists become tuples for every IN placeholder.

        Mutation: swapping the in_parens arms of _expand_in's sequence branch.
        Oracle: hand-written SQL where both clauses expand to width 2.
        """
        sql = 'select * from t where id in %s and status in %s'

        result = prepare_query(sql, [[1, 2], [3, 4]], 'postgresql')

        assert result == (
            'select * from t where id in (%s, %s) and status in (%s, %s)',
            (1, 2, 3, 4))

    def test_in_clause_middle_position(self):
        """An IN clause between two scalars keeps the tail of the SQL intact.

        Mutation: `pos = ph.pos` in place of `pos = ph.end` in _transform.
        Oracle: hand-written full SQL plus the flat arg tuple.
        """
        sql = 'select * from t where date = %s and id in (%s) and user = %s'

        result = prepare_query(sql, ('2025-01-01', [1, 2], 'USER_A'), 'postgresql')

        assert result == (
            'select * from t where date = %s and id in (%s, %s) and user = %s',
            ('2025-01-01', 1, 2, 'USER_A'))

    def test_parenthesized_in_clause_preserves_closing_paren(self):
        """`in (%s)` expands inside the author's parens, keeping the ')'.

        Mutation: ignoring PH.in_parens in _expand_in.
        Oracle: hand-written SQL with one paren pair for both widths.
        """
        sql = 'select * from t where id in (%s)'

        assert prepare_query(sql, ((1, 2, 3),), 'postgresql') == (
            'select * from t where id in (%s, %s, %s)', (1, 2, 3))
        assert prepare_query(sql, ([42],), 'postgresql') == (
            'select * from t where id in (%s)', (42,))

    def test_in_clause_with_scalar_value(self):
        """A bare scalar in an IN slot binds as one value, not per character.

        Mutation: swapping the in_parens arms of _expand_in's scalar tail.
        Oracle: hand-written SQL with one paren pair and the intact string.
        """
        sql = 'select * from t where date = %s and fund in (%s)'

        result = prepare_query(sql, ('2025-01-01', 'Growth'), 'postgresql')

        assert result == (
            'select * from t where date = %s and fund in (%s)',
            ('2025-01-01', 'Growth'))

    def test_in_clause_with_single_element_list(self):
        """A one-element list in an IN slot expands to exactly one marker.

        Mutation: `if not val` in _expand_in loosened to `if len(val) <= 1`.
        Oracle: hand-written SQL and the unwrapped scalar arg.
        """
        sql = 'select * from t where date = %s and fund in (%s)'

        result = prepare_query(sql, ('2025-01-01', ['Growth']), 'postgresql')

        assert result == (
            'select * from t where date = %s and fund in (%s)',
            ('2025-01-01', 'Growth'))

    def test_in_context_survives_whitespace_after_open_paren(self):
        """`in ( %s )` is still an IN context; the prefix is right-stripped.

        Mutation: dropping `.rstrip()` from the prefix in _find_contexts.
        Oracle: hand-written SQL that keeps the inner spaces.
        """
        result = prepare_query(
            'select * from t where id in ( %s )', ((1, 2),), 'postgresql')

        assert result == ('select * from t where id in ( %s, %s )', (1, 2))

    def test_not_in_expands_like_in(self):
        """Upper-case NOT IN reaches the same expansion as IN.

        Mutation: matching IN only as the whole prefix in _parse_ctx.
        Oracle: hand-written SQL expanding to width 2.
        """
        result = prepare_query(
            'select * from t where id NOT IN %s', [(1, 2)], 'postgresql')

        assert result == ('select * from t where id NOT IN (%s, %s)', (1, 2))

    @pytest.mark.parametrize(
        'sql',
        [
            'select min(%s) from t',
            'select bin(%s) from t',
            'select origin(%s) from t',
            'select * from t join %s',
            ],
        ids=['min_paren', 'bin_paren', 'origin_paren', 'join_bare'])
    def test_word_ending_in_in_is_not_an_in_context(self, sql):
        """A list after min(, bin(, origin( or join binds whole, unexpanded.

        Mutation: dropping the word boundary before IN in _parse_ctx.
        Oracle: SQL identical to the input, plus the list as one arg.
        """
        assert prepare_query(sql, ([1, 2],), 'postgresql') == (sql, ([1, 2],))

    @pytest.mark.parametrize(
        'value',
        [b'ab', bytearray(b'ab'), memoryview(b'ab')],
        ids=['bytes', 'bytearray', 'memoryview'])
    def test_bytes_like_in_value_is_one_item(self, value):
        """A bytes-like IN value binds as one item, never as its byte ints.

        Mutation: libb issequence counting bytes, bytearray or memoryview.
        Oracle: hand-written one-marker SQL; the spread gives (97, 98).
        """
        result = prepare_query('select * from t where h in %s', (value,), 'postgresql')

        assert result == ('select * from t where h in (%s)', (value,))

    def test_flat_list_is_not_spread_when_other_placeholders_exist(self):
        """A flat list against IN plus a second slot binds one arg to each.

        Mutation: dropping `len(phs) == 1` from _normalize's IN-list test.
        Oracle: hand-written SQL; a lone IN slot would take both args.
        """
        result = prepare_query(
            'select * from t where id in %s and x = %s', [1, 2], 'postgresql')

        assert result == ('select * from t where id in (%s) and x = %s', (1, 2))

    def test_in_clause_unwraps_single_nested_sequence(self):
        """A singly nested positional IN value is unwrapped before expansion.

        Mutation: removing the single-nested unwrap in _expand_in.
        Oracle: hand-written two-marker SQL for the [[1, 2]] input.
        """
        result = prepare_query(
            'select * from t where id in %s and x = %s', ([[1, 2]], 'q'), 'postgresql')

        assert result == (
            'select * from t where id in (%s, %s) and x = %s', (1, 2, 'q'))


class TestPrepareQueryAnyAll:
    """any(%s) and all(%s) bind a whole sequence as one array parameter."""

    @pytest.mark.parametrize(
        ('sql', 'args'),
        [
            ('select count(*) from t where id = any(%s) and y is null',
             ([1979],)),
            ('select * from t where price < all(%s)', ([100],)),
            ],
        ids=['any', 'all'])
    def test_single_element_list_stays_wrapped(self, sql, args):
        """A one-element list under any or all stays one array param.

        Mutation: dropping `and len(phs) > 1` from _normalize's unwrap test.
        Oracle: args and SQL equal to the input.
        """
        assert prepare_query(sql, args, 'postgresql') == (sql, args)

    def test_any_multi_element_list_stays_wrapped(self):
        """any(%s) with a multi-element list binds the whole list as array.

        Mutation: _parse_ctx reading an `any(` prefix as an IN context.
        Oracle: hand-written SQL with one marker and the list intact.
        """
        result = prepare_query(
            'select * from t where id = any(%s)', ([1, 2, 3],), 'postgresql')

        assert result == ('select * from t where id = any(%s)', ([1, 2, 3],))

    def test_any_with_extra_param_preserves_list(self):
        """Any combined with another placeholder preserves the array.

        Mutation: _normalize unwrapping whenever `len(args) == 1`.
        Oracle: hand-written ([1, 2], 'alice').
        """
        result = prepare_query(
            'select * from t where id = any(%s) and name = %s',
            ([1, 2], 'alice'), 'postgresql')

        assert result == (
            'select * from t where id = any(%s) and name = %s', ([1, 2], 'alice'))

    def test_two_placeholders_do_unwrap_the_single_sequence(self):
        """One sequence arg spreads over two placeholders.

        Mutation: raising the unwrap guard to `len(phs) > 2`.
        Oracle: hand-written (1, 2); one placeholder keeps ([1, 2],).
        """
        result = prepare_query(
            'select * from t where a = %s and b = %s', ([1, 2],), 'postgresql')

        assert result == ('select * from t where a = %s and b = %s', (1, 2))


class TestPrepareQueryNamedParams:
    """Test named parameter handling."""

    @pytest.mark.parametrize(
        ('sql', 'args', 'expected_sql', 'expected_args'),
        [
            ('select * from users where date between %(start)s and %(end)s',
             {'start': JAN_1, 'end': MAR_11},
             'select * from users where date between %(start)s and %(end)s',
             {'start': JAN_1, 'end': MAR_11}),
            ('select * from users where id in %(ids)s',
             {'ids': (1, 2, 3)},
             'select * from users where id in (%(ids_0)s, %(ids_1)s, %(ids_2)s)',
             {'ids_0': 1, 'ids_1': 2, 'ids_2': 3}),
            ('select * from users where id in %(ids)s and name = %(name)s',
             {'ids': (1, 2), 'name': 'test'},
             'select * from users where id in (%(ids_0)s, %(ids_1)s) and name = %(name)s',
             {'ids_0': 1, 'ids_1': 2, 'name': 'test'}),
            ('select * from users where id = %(id)s',
             [{'id': 1}],
             'select * from users where id = %(id)s',
             {'id': 1}),
            ],
        ids=['basic', 'in_clause', 'in_with_regular', 'dict_in_list'])
    def test_named_params(self, sql, args, expected_sql, expected_args):
        """Verify the whole rewritten dict, so the source key cannot linger.

        Mutation: _expand_named_in keeping 'ids', or keys named f'{name}{i}'.
        Oracle: hand-written full SQL and the exact expected dict.
        """
        assert prepare_query(sql, args, 'postgresql') == (expected_sql, expected_args)

    @pytest.mark.parametrize(
        ('sql', 'args', 'expected_sql', 'expected_args'),
        [
            ('select * from t where id = %(id)s',
             {'id': 5},
             'select * from t where id = :id',
             {'id': 5}),
            ('select * from t where id in %(ids)s',
             {'ids': (1, 2)},
             'select * from t where id in (:ids_0, :ids_1)',
             {'ids_0': 1, 'ids_1': 2}),
            ('select * from t where v is %(v)s and x = %(x)s',
             {'v': None, 'x': 1},
             'select * from t where v is null and x = :x',
             {'x': 1}),
            ],
        ids=['scalar', 'in_clause', 'is_null_mixed'])
    def test_named_params_use_colon_syntax_for_sqlite(
        self, sql, args,
        expected_sql, expected_args):
        """sqlite3 only accepts ':name', so _named_ph must switch on dialect.

        Mutation: _named_ph returning f'%({name})s' for every dialect.
        Oracle: hand-written ':name' SQL.
        """
        assert prepare_query(sql, args, 'sqlite') == (expected_sql, expected_args)

    def test_unknown_named_key_left_untouched(self):
        """A placeholder with no matching key keeps its text and adds no arg.

        Mutation: _proc_named binding `{name: None}` for a missing key.
        Oracle: hand-written SQL unchanged plus the single-key dict.
        """
        sql = 'select * from t where a = %(a)s and b = %(b)s'

        result = prepare_query(sql, {'a': 1}, 'postgresql')

        assert result == ('select * from t where a = %(a)s and b = %(b)s', {'a': 1})

    def test_unknown_named_key_still_rewritten_for_sqlite(self):
        """The unknown-key branch still emits the dialect's own syntax.

        Mutation: _proc_named's missing-key branch returning the pyformat form.
        Oracle: hand-written ':b' for the key sqlite cannot resolve.
        """
        sql = 'select * from t where a = %(a)s and b = %(b)s'

        result = prepare_query(sql, {'a': 1}, 'sqlite')

        assert result == ('select * from t where a = :a and b = :b', {'a': 1})

    def test_named_in_clause_with_scalar_value(self):
        """A scalar under a named IN still gets the `_0` suffix and parens.

        Mutation: _expand_named_in's scalar tail binding `{name: val}`.
        Oracle: hand-written `in (%(ids_0)s)` plus {'ids_0': 5}.
        """
        result = prepare_query(
            'select * from t where id in %(ids)s', {'ids': 5}, 'postgresql')

        assert result == ('select * from t where id in (%(ids_0)s)', {'ids_0': 5})

    def test_named_in_clause_empty_sequence(self):
        """An empty named IN collapses to (null) and contributes no keys.

        Mutation: swapping the in_parens arms of _expand_named_in's empty case.
        Oracle: hand-written `in (null)` plus an empty dict.
        """
        result = prepare_query(
            'select * from t where id in %(ids)s', {'ids': []}, 'postgresql')

        assert result == ('select * from t where id in (null)', {})

    def test_named_in_clause_already_parenthesized(self):
        """`in (%(ids)s)` expands without adding a second paren pair.

        Mutation: ignoring in_parens in _expand_named_in.
        Oracle: hand-written SQL with exactly one paren pair.
        """
        result = prepare_query(
            'select * from t where id in (%(ids)s)', {'ids': (1, 2)}, 'postgresql')

        assert result == (
            'select * from t where id in (%(ids_0)s, %(ids_1)s)',
            {'ids_0': 1, 'ids_1': 2})

    def test_named_in_clause_unwraps_single_nested_sequence(self):
        """A singly-nested named IN value is unwrapped before expansion.

        Mutation: dropping the unwrap guard in _expand_named_in.
        Oracle: hand-written two-marker SQL for the [[1, 2]] input.
        """
        result = prepare_query(
            'select * from t where id in %(ids)s', {'ids': [[1, 2]]}, 'postgresql')

        assert result == (
            'select * from t where id in (%(ids_0)s, %(ids_1)s)',
            {'ids_0': 1, 'ids_1': 2})


class TestDictArgumentAsValue:
    """A lone dict binds by name only when the SQL names a placeholder."""

    DICT_ARG_CASES = [
        ('qmark_sqlite', 'insert into t (doc) values (?)', {'a': 1}, 'sqlite',
         'insert into t (doc) values (?)', ({'a': 1},)),
        ('percent_s_pg', 'insert into t (doc) values (%s)', {'a': 1},
         'postgresql', 'insert into t (doc) values (%s)', ({'a': 1},)),
        ('percent_s_sqlite', 'insert into t (doc) values (%s)', {'a': 1},
         'sqlite', 'insert into t (doc) values (?)', ({'a': 1},)),
        ('dict_in_list_qmark', 'insert into t (doc) values (?)', [{'a': 1}],
         'sqlite', 'insert into t (doc) values (?)', ({'a': 1},)),
        ('named_pg_binds_by_name', 'update t set doc = %(doc)s',
         {'doc': {'a': 1}}, 'postgresql', 'update t set doc = %(doc)s',
         {'doc': {'a': 1}}),
        ('named_sqlite_binds_by_name', 'update t set doc = %(doc)s',
         {'doc': {'a': 1}}, 'sqlite', 'update t set doc = :doc',
         {'doc': {'a': 1}}),
        ]

    @pytest.mark.parametrize(
        ('case_id', 'sql', 'args', 'dialect', 'expected_sql', 'expected_args'),
        DICT_ARG_CASES,
        ids=[c[0] for c in DICT_ARG_CASES])
    def test_lone_dict_is_positional_unless_the_sql_names_a_placeholder(
            self, case_id, sql, args, dialect, expected_sql, expected_args):
        """A dict under '?' or '%s' stays one value; under a name it binds.

        Mutation: _normalize dropping its `has_named` test.
        Oracle: hand-written (sql, args) pairs; the named_* rows bind by key.
        """
        result = prepare_query(sql, args, dialect)

        assert result == (expected_sql, expected_args), case_id


class TestPrepareQueryIsNull:
    """Test IS NULL / IS NOT NULL handling."""

    @pytest.mark.parametrize(
        ('sql', 'args', 'expected_sql', 'expected_args'),
        [
            ('select * from t where value IS %s', (None,),
             'select * from t where value IS null', ()),
            ('select * from t where value IS NOT %s', (None,),
             'select * from t where value IS NOT null', ()),
            ('select * from t where v1 IS %s and v2 IS NOT %s', (None, None),
             'select * from t where v1 IS null and v2 IS NOT null', ()),
            ('select * from t where v1 IS %s and v2 = %s', (None, 'test'),
             'select * from t where v1 IS null and v2 = %s', ('test',)),
            ('select * from t where value IS %s', ('not_null',),
             'select * from t where value IS %s', ('not_null',)),
            ],
        ids=['is_null', 'is_not_null', 'multiple', 'mixed', 'non_null'])
    def test_positional_null_handling(self, sql, args, expected_sql, expected_args):
        """Verify upper-case IS and IS NOT inline null only for None.

        Mutation: dropping `and val is None` from _proc_pos.
        Oracle: hand-written SQL and args; the non_null row is the boundary.
        """
        assert prepare_query(sql, args, 'postgresql') == (expected_sql, expected_args)

    @pytest.mark.parametrize(
        ('sql', 'args', 'expected_sql', 'expected_args'),
        [
            ('select * from t where value is %(val)s and name = %(name)s',
             {'val': None, 'name': 'test'},
             'select * from t where value is null and name = %(name)s',
             {'name': 'test'}),
            ('where v is not %(v)s', {'v': None}, 'where v is not null', {}),
            ],
        ids=['is_null', 'is_not_null'])
    def test_named_null_handling(self, sql, args, expected_sql, expected_args):
        """A named IS or IS NOT null drops its key, leaving no stray param.

        Mutation: _proc_named's null branch returning ('null', {name: val}).
        Oracle: hand-written SQL plus the exact remaining dict.
        """
        assert prepare_query(sql, args, 'postgresql') == (expected_sql, expected_args)


class TestPrepareQueryPercentEscaping:
    """'%' doubling inside string literals, PostgreSQL only."""

    @pytest.mark.parametrize(
        ('sql', 'args', 'dialect', 'expected_sql'),
        [
            ("select * from t where id = %s and s = 'progress: 50%'",
             (1,), 'postgresql',
             "select * from t where id = %s and s = 'progress: 50%%'"),
            ('select * from t where id = %s and s = "Complete: 75%"',
             (1,), 'postgresql',
             'select * from t where id = %s and s = "Complete: 75%%"'),
            ("select * from t where id = %s and s = 'It''s 100% done'",
             (1,), 'postgresql',
             "select * from t where id = %s and s = 'It''s 100%% done'"),
            ("select * from t where id = %s and s like 'pre%%post'",
             (1,), 'postgresql',
             "select * from t where id = %s and s like 'pre%%post'"),
            ("select * from t where s = 'progress: 50%'",
             None, 'postgresql',
             "select * from t where s = 'progress: 50%'"),
            ("select * from t where id = %s and s = 'progress: 50%'",
             (1,), 'sqlite',
             "select * from t where id = ? and s = 'progress: 50%'"),
            ("select * from t where name = %s and status = 'progress: 25%'",
             ('test',), 'postgresql',
             "select * from t where name = %s and status = 'progress: 25%%'"),
            ("select * from t where s = 'a%' and id = %s",
             (1,), 'postgresql',
             "select * from t where s = 'a%%' and id = %s"),
            ],
        ids=['pg_single_quote', 'pg_double_quote', 'pg_escaped_quotes',
             'pg_already_escaped', 'pg_no_placeholders', 'sqlite_no_escape',
             'pg_placeholder_preserved', 'pg_literal_before_placeholder'])
    def test_percent_escaping(self, sql, args, dialect, expected_sql):
        """Verify '%' doubling in literals, both sides of the placeholder.

        Mutation: _escape_percents adding a '%' to an even run.
        Oracle: hand-written full SQL per row.
        """
        result_sql, _ = prepare_query(sql, args, dialect)

        assert result_sql == expected_sql

    def test_bare_modulo_percent_is_doubled(self):
        """Verify a modulo '%' outside a literal is doubled for psycopg.

        Mutation: _escape_percents limited to string literals again.
        Oracle: hand-written SQL with `50 %% 3`, which psycopg sends as
            `50 % 3`; a lone '%' there raises 'incomplete placeholder'.
        """
        sql = 'select 50 % 3 as r, id from t where id = %s'

        assert prepare_query(sql, (1,), 'postgresql') == (
            'select 50 %% 3 as r, id from t where id = %s', (1,))

    def test_odd_percent_run_is_made_even(self):
        """Verify a run of three '%' gains one, matching the no-args collapse.

        Mutation: _escape_percents doubling only a lone '%'.
        Oracle: psycopg halves '%%%%' to '%%', which is what '%%%' becomes
            without args after '%%' collapses to '%'.
        """
        sql = "select '%%%' as a, id from t where id = %s"

        assert prepare_query(sql, (1,), 'postgresql') == (
            "select '%%%%' as a, id from t where id = %s", (1,))

    def test_many_placeholders_prepare_in_linear_time(self):
        """Verify 5000 placeholders prepare well inside a second.

        Mutation: the IN/IS context regexes searching the whole prefix
                  per placeholder, which takes seconds at this size.
        Oracle: the linear scan takes a few hundredths of a second.
        """
        sql = 'insert into t values ' + ', '.join(['(%s, %s)'] * 2500)
        started = time.perf_counter()

        prepare_query(sql, tuple(range(5000)), 'postgresql')

        assert time.perf_counter() - started < 1.0

    @pytest.mark.parametrize(
        ('literal', 'expected'),
        [("'%smith%'", "'%%smith%%'"), ("'%(x)'", "'%%(x)'")],
        ids=['percent_s', 'percent_paren'])
    def test_placeholder_lookalike_in_a_literal_is_doubled(
            self, literal, expected):
        """A literal '%s' or '%(' reaches psycopg doubled, never as a marker.

        Mutation: _escape_percents skipping a '%' followed by 's' or '('.
        Oracle: psycopg reads an undoubled '%s' in a literal as a marker.
        """
        sql = f'select * from t where name like {literal} and id = %s'

        result_sql, _ = prepare_query(sql, (1,), 'postgresql')

        assert result_sql == (
            f'select * from t where name like {expected} and id = %s')


class TestPrepareQueryRegexp:
    """Test regexp_replace patterns are preserved."""

    def test_regexp_pattern_survives_placeholder_processing(self):
        """A regexp_replace body keeps its '?' and '$' verbatim.

        Mutation: dropping the string-literal branch from _protected_ranges.
        Oracle: SQL identical to the input, plus (1,).
        """
        sql = (r"select regexp_replace(code, '\/?[UV]? ?(CN|US)?$', '') "
               r'from t where id = %s')

        assert prepare_query(sql, (1,), 'postgresql') == (sql, (1,))

    def test_regexp_literal_percent_doubles_like_the_like_literal(self):
        """The like literal and the regexp body next to it both double '%'.

        Mutation: exempting regexp_replace from _escape_percents, which
                  leaves '%x' for psycopg to reject as a placeholder.
        Oracle: hand-written SQL where 'a%' and '[0-9]%x' both double.
        """
        sql = ("select * from t where s like 'a%' "
               "and regexp_replace(code, '[0-9]%x', '') = 'B' and id = %s")
        expected = ("select * from t where s like 'a%%' "
                    "and regexp_replace(code, '[0-9]%%x', '') = 'B' and id = %s")

        assert prepare_query(sql, (1,), 'postgresql') == (expected, (1,))

    def test_regexp_body_percent_doubles_without_a_like_neighbor(self):
        """A lone regexp_replace body doubles its '%' before a bound arg.

        Mutation: exempting regexp_replace from _escape_percents, which
                  leaves '%d' for psycopg to reject as a placeholder.
        Oracle: hand-written SQL with '%%d+', plus (1,).
        """
        sql = "select regexp_replace(code, '%d+', '') from t where id = %s"
        expected = "select regexp_replace(code, '%%d+', '') from t where id = %s"

        assert prepare_query(sql, (1,), 'postgresql') == (expected, (1,))

    @pytest.mark.parametrize(
        ('dialect', 'marker'),
        [('postgresql', '%s'), ('sqlite', '?')],
        ids=['postgresql', 'sqlite'])
    def test_placeholder_argument_of_regexp_replace_binds(self, dialect, marker):
        """A placeholder passed to regexp_replace binds like any other.

        Mutation: _protected_ranges masking the whole regexp_replace(...) span.
        Oracle: hand-written SQL with the dialect's marker, plus ('x',).
        """
        sql = "select regexp_replace(code, %s, '') from t"
        expected = f"select regexp_replace(code, {marker}, '') from t"

        assert prepare_query(sql, ('x',), dialect) == (expected, ('x',))

    def test_named_placeholder_inside_regexp_replace_counts(self):
        """A %(name)s argument of regexp_replace is a named placeholder.

        Mutation: _protected_ranges masking the whole regexp_replace(...) span.
        Oracle: the one placeholder in the SQL sits outside every literal.
        """
        sql = "select regexp_replace(code, %(pattern)s, '') from t"

        assert has_named_placeholders(sql, 'postgresql') is True


class TestPrepareQueryDollarQuotes:
    """A placeholder inside a $$ or $tag$ body is text, not a bind."""

    def test_anonymous_dollar_quoted_body_protects_percent_s(self):
        """$$ ... %s ... $$ has no real placeholder; one outside binds one arg.

        Mutation: removing the `c == '$'` branch from _protected_ranges.
        Oracle: the input with the body's '%s' doubled, plus (42,).
        """
        sql = 'select $$has %s inside$$, %s from t'

        assert prepare_query(sql, (42,), 'postgresql') == (
            'select $$has %%s inside$$, %s from t', (42,))

    def test_anonymous_dollar_quoted_body_with_question_mark_postgresql(self):
        """? inside $$...$$ on postgres is literal, not a placeholder.

        Mutation: running the dollar-quote scan only for sqlite.
        Oracle: SQL identical to the input, plus (7,).
        """
        sql = 'select $$contains ? char$$ from t where id = %s'

        assert prepare_query(sql, (7,), 'postgresql') == (sql, (7,))

    def test_tagged_dollar_quoted_body_protected(self):
        r"""$tag$ ... %s ... $tag$ body is literal.

        Mutation: _DOLLAR_OPEN_RE matching only the anonymous '$$' tag.
        Oracle: the input with the body's '%s' doubled, plus (1,).
        """
        sql = 'select $body$any %s text$body$ from t where id = %s'

        assert prepare_query(sql, (1,), 'postgresql') == (
            'select $body$any %%s text$body$ from t where id = %s', (1,))

    def test_nested_dollar_tags_match_by_tag(self):
        """$outer$ closes only at its own tag, swallowing the inner one.

        Mutation: closing a dollar body at the next '$' instead of its tag.
        Oracle: the input with both body '%s' doubled; only the trailing
            slot binds.
        """
        sql = 'select $outer$ %s and $inner$ %s $inner$ end $outer$ from t where id = %s'

        assert prepare_query(sql, (99,), 'postgresql') == (
            'select $outer$ %%s and $inner$ %%s $inner$ end $outer$ from t where id = %s',
            (99,))

    def test_dollar_quotes_are_not_protected_for_sqlite(self):
        """Dollar quoting is postgres-only; sqlite converts the inner %s.

        Mutation: dropping `dialect == 'postgresql'` from the dollar branch.
        Oracle: postgres keeps the '%s' as text, doubled to '%%s'; sqlite
            turns it into '?'.
        """
        sql = 'select $$a %s b$$ from t'

        assert prepare_query(sql, ('x',), 'sqlite') == (
            'select $$a ? b$$ from t', ('x',))
        assert prepare_query(sql, None, 'postgresql')[0] == (
            'select $$a %%s b$$ from t')

    def test_unterminated_dollar_quote_protects_to_end_of_sql(self):
        """An unclosed $$ swallows the rest of the statement.

        Mutation: falling back to `m.end()` when the closing tag is missing.
        Oracle: the '%s' kept as text, doubled, and an empty arg tuple.
        """
        sql = 'select $$abc %s def'

        assert prepare_query(sql, (), 'postgresql') == ('select $$abc %%s def', ())


class TestPrepareQueryComments:
    """A placeholder inside a -- or /* */ comment is text, not a bind."""

    def test_line_comment_protects_percent_s(self):
        r"""-- comment %s is literal; only the post-newline %s binds.

        Mutation: dropping the '--' branch from _protected_ranges.
        Oracle: the input with the comment's '%s' doubled, plus (5,).
        """
        sql = 'select * from t -- comment %s\nwhere id = %s'

        assert prepare_query(sql, (5,), 'postgresql') == (
            'select * from t -- comment %%s\nwhere id = %s', (5,))

    def test_block_comment_protects_percent_s(self):
        """/* %s */ is literal; only the outside placeholder binds.

        Mutation: dropping the '/*' branch from _protected_ranges.
        Oracle: the input with the comment's '%s' doubled, plus (10,).
        """
        sql = 'select /* has %s */ * from t where id = %s'

        assert prepare_query(sql, (10,), 'postgresql') == (
            'select /* has %%s */ * from t where id = %s', (10,))

    def test_multiline_block_comment_protected(self):
        """A /* ... */ block spanning a newline stays protected throughout.

        Mutation: ending a block comment at the newline.
        Oracle: the input with the comment's '%s' doubled, plus (2,).
        """
        sql = 'select /* line1\n%s line2 */ * from t where id = %s'

        assert prepare_query(sql, (2,), 'postgresql') == (
            'select /* line1\n%%s line2 */ * from t where id = %s', (2,))

    def test_unterminated_block_comment_protects_to_end_of_sql(self):
        """An unclosed /* swallows the rest of the statement.

        Mutation: leaving j at the comment start when '*/' is missing.
        Oracle: both '%s' kept as text, doubled, and an empty arg tuple.
        """
        sql = 'select * from t /* %s where id = %s'

        assert prepare_query(sql, (), 'postgresql') == (
            'select * from t /* %%s where id = %%s', ())

    def test_comment_marker_inside_string_literal_is_not_a_comment(self):
        """String protection wins over comment markers.

        Mutation: checking '--' before string literals in _protected_ranges.
        Oracle: SQL identical to the input, plus (1,).
        """
        sql = "select 'foo -- bar baz' as lit from t where id = %s"

        assert prepare_query(sql, (1,), 'postgresql') == (sql, (1,))

    def test_apostrophe_inside_line_comment_does_not_open_a_literal(self):
        """The reverse precedence: a comment's quote never starts a string.

        Mutation: running the literal scan over the whole SQL before comments.
        Oracle: the input with the comment's '%s' doubled, plus (1,).
        """
        sql = "select * from t -- it's fine %s\nwhere id = %s"

        assert prepare_query(sql, (1,), 'postgresql') == (
            "select * from t -- it's fine %%s\nwhere id = %s", (1,))

    def test_unterminated_string_literal_protects_to_end_of_sql(self):
        """An unclosed string literal swallows the rest of the statement.

        Mutation: `j = i + 1` in place of `j = n` for an unclosed literal.
        Oracle: the '%s' kept as text, doubled, and an empty arg tuple.
        """
        sql = "select * from t where s = 'abc %s"

        assert prepare_query(sql, (), 'postgresql') == (
            "select * from t where s = 'abc %%s", ())

    def test_double_quoted_literal_is_protected(self):
        r"""A double-quoted literal's %s is not a placeholder, and is doubled.

        Mutation: dropping '"' from the literal branch of _protected_ranges.
        Oracle: hand-written SQL; psycopg reads '%%' back as '%'.
        """
        sql = 'select * from t where "c %s n" = %s'

        assert prepare_query(sql, (1,), 'postgresql') == (
            'select * from t where "c %%s n" = %s', (1,))

    def test_question_mark_in_line_comment_protected_sqlite(self):
        """-- ? comment is literal in SQLite.

        Mutation: skipping the comment scan for sqlite.
        Oracle: SQL identical to the input, plus (1,).
        """
        sql = 'select * from t -- comment ?\nwhere id = ?'

        assert prepare_query(sql, (1,), 'sqlite') == (sql, (1,))


class TestPrepareQueryJsonbOperator:
    """On PostgreSQL a JSONB '?' operator is not a placeholder."""

    def test_jsonb_question_mark_with_quoted_literal_not_placeholder(self):
        """`data ? 'key'` keeps its operator; only `id = %s` binds.

        Mutation: dropping the quoted-literal lookahead from _is_jsonb_op.
        Oracle: SQL identical to the input, plus (1,).
        """
        sql = "select * from t where data ? 'key' and id = %s"

        assert prepare_query(sql, (1,), 'postgresql') == (sql, (1,))

    def test_jsonb_question_mark_pipe_not_placeholder(self):
        """`data ?| array[...]` is preserved.

        Mutation: narrowing `sql[pos + 1] in '|&'` to `== '&'`.
        Oracle: SQL identical to the input, plus (2,).
        """
        sql = "select * from t where data ?| array['a','b'] and id = %s"

        assert prepare_query(sql, (2,), 'postgresql') == (sql, (2,))

    def test_jsonb_question_mark_amp_not_placeholder(self):
        """`data ?& array[...]` is preserved.

        Mutation: narrowing `sql[pos + 1] in '|&'` to `== '|'`.
        Oracle: SQL identical to the input, plus (3,).
        """
        sql = "select * from t where data ?& array['a','b'] and id = %s"

        assert prepare_query(sql, (3,), 'postgresql') == (sql, (3,))

    def test_question_mark_before_a_word_is_still_a_placeholder(self):
        """The other side of _is_jsonb_op: '? and' converts to '%s'.

        Mutation: _is_jsonb_op answering True for any '?' before whitespace.
        Oracle: hand-written SQL where `? and` becomes `%s and`.
        """
        sql = 'select * from t where id = ? and x = 1'

        assert prepare_query(sql, (5,), 'postgresql') == (
            'select * from t where id = %s and x = 1', (5,))

    def test_sqlite_question_mark_remains_placeholder(self):
        """The PG-only rule must not affect SQLite.

        Mutation: dropping `dialect == 'postgresql'` from the jsonb guard.
        Oracle: SQL identical to the input, including a '?' before '||'.
        """
        sql = 'select * from t where id = ? and name = ?'

        assert prepare_query(sql, (1, 'foo'), 'sqlite') == (sql, (1, 'foo'))

        concat_sql = "select ?||'-suffix' as s from t"

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

        Mutation: dropping the quote-doubling replace in quote_identifier.
        Oracle: hand-written expected strings.
        """
        assert quote_identifier(identifier, dialect) == expected

    def test_default_dialect(self):
        """The default dialect is a supported one, so no error is raised.

        Mutation: quote_identifier's dialect default set to an unsupported one.
        Oracle: hand-written '"table"'.
        """
        assert quote_identifier('table') == '"table"'

    def test_unknown_dialect_error(self):
        """An unsupported dialect is rejected rather than silently quoted.

        Mutation: dropping the `dialect not in _SUPPORTED_DIALECTS` guard.
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

        Mutation: dropping the dot split from _split_qualified_identifier.
        Oracle: hand-written two-segment expected strings.
        """
        assert quote_identifier(identifier, dialect) == expected

    def test_quote_identifier_unqualified_unchanged(self):
        """An unqualified identifier stays one segment.

        Mutation: appending an empty trailing segment, giving '"foo".""'.
        Oracle: hand-written '"foo"'.
        """
        assert quote_identifier('foo', 'postgresql') == '"foo"'

    def test_dot_inside_a_quoted_segment_is_not_a_separator(self):
        """A pre-quoted segment keeps its dot instead of splitting on it.

        Mutation: splitting on '.' while in_quote is set.
        Oracle: hand-written single-segment output for a dotted name.
        """
        assert quote_identifier('"weird.name"', 'postgresql') == '"weird.name"'

    def test_quoted_schema_with_dot_then_plain_table(self):
        """Only the unquoted dot separates: one quoted schema, one table.

        Mutation: splitting on '.' while in_quote is set.
        Oracle: hand-written '"my.schema"."tbl"'.
        """
        assert quote_identifier('"my.schema".tbl', 'postgresql') == '"my.schema"."tbl"'

    def test_doubled_quote_inside_a_quoted_segment_round_trips(self):
        """'""' inside a quoted segment decodes to one quote and re-encodes.

        Mutation: dropping the doubled-quote unescape branch.
        Oracle: hand-written '"a""b"', unchanged by a decode/encode pair.
        """
        assert quote_identifier('"a""b"', 'postgresql') == '"a""b"'


class TestQuoteIdentifierNullByteRejection:
    """Null bytes in identifiers must be rejected, not quoted."""

    def test_rejects_null_byte_in_identifier(self):
        r"""A bare identifier containing \x00 raises ValidationError.

        Mutation: dropping the null-byte guard.
        Oracle: the raised ValidationError and its message.
        """
        with pytest.raises(ValidationError, match='null byte'):
            quote_identifier('foo\x00bar', 'postgresql')

    def test_rejects_null_byte_in_dotted_identifier(self):
        r"""Any segment carrying \x00 raises, dot-splitting included.

        Mutation: checking only the first segment for a null byte.
        Oracle: a null byte in the second segment raises ValidationError.
        """
        with pytest.raises(ValidationError, match='null byte'):
            quote_identifier('public.foo\x00', 'postgresql')

    def test_rejects_null_byte_for_sqlite_dialect(self):
        """Null-byte rejection is dialect-independent.

        Mutation: running the null-byte check for postgresql only.
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

        Mutation: an off-by-one in `[marker] * count`, or joining on ','.
        Oracle: hand-written strings; the count-1 rows emit no separator.
        """
        assert make_placeholders(count, dialect) == expected


class TestHasPlaceholders:
    """Test placeholder detection."""

    @pytest.mark.parametrize(
        ('sql', 'expected'),
        [
            ('', False),
            (None, False),
            ('select * from users', False),
            ('select * from stats where growth > 10%', False),
            ('select id::text from t', False),
            ('select * from users where id = %s', True),
            ('select * from users where id = ?', True),
            ('insert into t values (%s, %s, ?)', True),
            ('select * from users where id = %(user_id)s', True),
            ('insert into t values (%(id)s, %(name)s)', True),
            ('select * from t where id = %s and name = %(name)s', True),
            ('select * from t where id = :id', True),
            ('select id::text from t where id = :id', True),
            ],
        ids=[
            'empty', 'none', 'no_placeholders', 'percent_not_placeholder',
            'pg_type_cast', 'percent_s', 'qmark', 'mixed_positional',
            'named_single', 'named_multiple', 'mixed_types', 'sqlite_named',
            'cast_and_named'])
    def test_has_placeholders(self, sql, expected):
        """Verify detection, including the '::' cast carve-out.

        Mutation: dropping the `(?<!:)` lookbehind from _HAS_PH_RE.
        Oracle: hand-written verdicts; pg_type_cast straddles the lookbehind.
        """
        assert has_placeholders(sql) is expected


class TestHasNamedPlaceholders:
    """Named-placeholder detection: what tells a binding map from a value."""

    @pytest.mark.parametrize(
        ('sql', 'dialect', 'expected'),
        [
            ('select * from t where id = %(id)s', 'postgresql', True),
            ('select * from t where id = %(id)s', 'sqlite', True),
            ('select * from t where id = %s', 'postgresql', False),
            ('select * from t where id = ?', 'sqlite', False),
            ('select * from t where id = :id', 'sqlite', True),
            ('select * from t where id = :id', 'postgresql', False),
            ('select id::text from t', 'postgresql', False),
            ('select id::text from t', 'sqlite', False),
            ('select arr[1:3] from t', 'postgresql', False),
            ('select arr[1:3] from t where id = %(id)s', 'postgresql', True),
            ('', 'postgresql', False),
            (None, 'postgresql', False),
            ],
        ids=[
            'pyformat_pg', 'pyformat_sqlite', 'percent_s_pg', 'qmark_sqlite',
            'colon_sqlite', 'colon_pg', 'cast_pg', 'cast_sqlite', 'slice_pg',
            'slice_beside_real_name_pg', 'empty', 'none'])
    def test_named_detection_by_dialect(self, sql, dialect, expected):
        """Only pyformat counts on postgres; sqlite also counts ':name'.

        Mutation: appending _NAMED_SQLITE_RE for every dialect.
        Oracle: hand-written verdicts, same SQL under both dialects.
        """
        assert has_named_placeholders(sql, dialect) is expected

    @pytest.mark.parametrize(
        ('sql', 'dialect', 'expected'),
        [
            ("select * from t where s = '%(id)s'", 'postgresql', False),
            ("select * from t where s = '%(a)s' and id = %(id)s",
             'postgresql', True),
            ('select 1 -- %(id)s\n', 'postgresql', False),
            ('select /* %(id)s */ 1', 'postgresql', False),
            ('select $$ %(id)s $$ from t', 'postgresql', False),
            ('select $body$ %(id)s $body$, %(real)s from t',
             'postgresql', True),
            ("select ':id' from t", 'sqlite', False),
            ("select ':id' from t where x = :x", 'sqlite', True),
            ('select 1 -- :id\n', 'sqlite', False),
            ('select /* :id */ 1', 'sqlite', False),
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

        Mutation: dropping the _protected_ranges filter.
        Oracle: paired rows differing only by a second, unprotected name.
        """
        assert has_named_placeholders(sql, dialect) is expected

    def test_dollar_body_is_protected_only_for_postgresql(self):
        """Dollar quoting is postgres-only, so the dialect must reach the scan.

        Mutation: calling _protected_ranges without forwarding `dialect`.
        Oracle: one SQL string, False under postgres and True under sqlite.
        """
        sql = 'select $$ %(name)s $$ from t'

        assert has_named_placeholders(sql, 'postgresql') is False
        assert has_named_placeholders(sql, 'sqlite') is True


class TestStandardizePlaceholders:
    """Test placeholder conversion between dialects."""

    @pytest.mark.parametrize(
        ('sql', 'dialect', 'expected'),
        [
            ('select * from users where id = %s and name = %s', 'sqlite',
             'select * from users where id = ? and name = ?'),
            ('select * from users where id = ? and name = ?', 'postgresql',
             'select * from users where id = %s and name = %s'),
            ('select * from users where id = %s', 'postgresql',
             'select * from users where id = %s'),
            ('select * from users where id = ?', 'sqlite',
             'select * from users where id = ?'),
            ],
        ids=['percent_to_qmark', 'qmark_to_percent', 'pg_no_change',
             'sqlite_no_change'])
    def test_placeholder_conversion(self, sql, dialect, expected):
        """Verify conversion in both directions and both no-op quick paths.

        Mutation: inverting the target marker or either quick check.
        Oracle: hand-written expected strings for all four combinations.
        """
        assert standardize_placeholders(sql, dialect) == expected

    def test_named_and_positional_params_both_convert_for_sqlite(self):
        """A pyformat name becomes ':name' and the bare '%s' beside it '?'.

        Mutation: dropping the `m.group(1)` check from the replace callback.
        Oracle: hand-written SQL in sqlite3's ':name' and '?' forms.
        """
        sql = 'select * from t where a = %(a)s and b = %s'

        assert standardize_placeholders(sql, 'sqlite') == (
            'select * from t where a = :a and b = ?')

    def test_named_only_sql_converts_for_sqlite(self):
        """A named-only query reaches the substitution pass on sqlite.

        Mutation: the quick check `'%' not in sql` narrowed to `'%s'`.
        Oracle: hand-written SQL in sqlite3's ':name' form.
        """
        sql = 'select * from users where id = %(id)s'

        assert standardize_placeholders(sql, 'sqlite') == (
            'select * from users where id = :id')

    def test_named_params_unchanged_for_postgresql(self):
        """A pyformat name stays as written for psycopg.

        Mutation: _named_ph called with a fixed 'sqlite' dialect.
        Oracle: hand-written SQL where only the '?' moves.
        """
        sql = 'select * from t where a = %(a)s and b = ?'

        assert standardize_placeholders(sql, 'postgresql') == (
            'select * from t where a = %(a)s and b = %s')

    def test_preserves_string_literals(self):
        """A '%s' inside a literal is not converted; the real one is.

        Mutation: dropping the `m.start() in protected` check.
        Oracle: hand-written SQL where one of the two '%s' moves.
        """
        sql = "select * from t where id = %s and name = 'test %s value'"

        assert standardize_placeholders(sql, 'sqlite') == (
            "select * from t where id = ? and name = 'test %s value'")

    def test_preserves_line_comments(self):
        """A '%s' inside a -- comment is not converted.

        Mutation: substituting without consulting _protected_ranges.
        Oracle: hand-written SQL where the comment keeps its '%s'.
        """
        sql = 'select * from t -- %s\nwhere id = %s'

        assert standardize_placeholders(sql, 'sqlite') == (
            'select * from t -- %s\nwhere id = ?')

    def test_preserves_dollar_quoted_body_for_postgresql(self):
        """A '?' inside $$...$$ stays; the one outside becomes '%s'.

        Mutation: a fixed 'sqlite' dialect passed to _protected_ranges.
        Oracle: hand-written SQL where one of the two '?' moves.
        """
        sql = 'select $$a ? b$$, ? from t'

        assert standardize_placeholders(sql, 'postgresql') == (
            'select $$a ? b$$, %s from t')

    def test_jsonb_operator_survives_conversion(self):
        """The JSONB '?' operator is not converted to '%s'.

        Mutation: dropping the _is_jsonb_op guard from the replace callback.
        Oracle: hand-written SQL where only the trailing bind converts.
        """
        sql = "select * from t where data ? 'k' and id = ?"

        assert standardize_placeholders(sql, 'postgresql') == (
            "select * from t where data ? 'k' and id = %s")


class TestFullPipeline:
    """Test complete query processing scenarios."""

    def test_postgresql_complex_query(self):
        """IN expansion, IS NOT NULL inlining and '%' escaping in one query.

        Mutation: a regression in _expand_in, _proc_pos or _escape_percents.
        Oracle: one hand-written expected SQL string covering all three.
        """
        sql = ("select * from users where created between %s and %s and id in %s "
               "and status is not %s and name like 'test%'")
        expected = ("select * from users where created between %s and %s "
                    "and id in (%s, %s, %s) and status is not null "
                    "and name like 'test%%'")

        result = prepare_query(sql, (JAN_1, DEC_31, (1, 2, 3), None), 'postgresql')

        assert result == (expected, (JAN_1, DEC_31, 1, 2, 3))

    def test_sqlite_complex_query(self):
        """IN expansion and marker conversion compose for sqlite.

        Mutation: _expand_in hardcoding '%s' in place of the passed marker.
        Oracle: hand-written SQL with '?' in both the IN run and the tail.
        """
        sql = 'select * from users where id in %s and status = %s'

        result = prepare_query(sql, ((1, 2), 'active'), 'sqlite')

        assert result == (
            'select * from users where id in (?, ?) and status = ?',
            (1, 2, 'active'))

    def test_named_params_complex(self):
        """A named IN alongside two plain names rewrites keys exactly once.

        Mutation: _expand_named_in numbering from 1.
        Oracle: hand-written SQL plus the exact five-key dict.
        """
        sql = ('select * from items where item_id in %(ids)s and date = %(date)s '
               'and user = %(user)s')
        expected = ('select * from items where item_id in '
                    '(%(ids_0)s, %(ids_1)s, %(ids_2)s) and date = %(date)s '
                    'and user = %(user)s')
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


class TestLowerCaseKeywords:
    """Context keywords match whatever their case."""

    @pytest.mark.parametrize(
        ('sql', 'args', 'expected_sql', 'expected_args'),
        [
            ('where id in %s', [(1, 2, 3)], 'where id in (%s, %s, %s)', (1, 2, 3)),
            ('where v is %s', (None,), 'where v is null', ()),
            ],
        ids=['in', 'is_null'])
    def test_lower_case_keyword_sets_the_context(
            self, sql, args, expected_sql, expected_args):
        """Verify lower-case 'in' and 'is' reach the IN and null branches.

        Mutation: dropping `.upper()` from the prefix in _find_contexts.
        Oracle: hand-written SQL, matching the upper-case rows above.
        """
        assert prepare_query(sql, args, 'postgresql') == (expected_sql, expected_args)


if __name__ == '__main__':
    __import__('pytest').main([__file__])
