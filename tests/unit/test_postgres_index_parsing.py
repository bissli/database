"""Unit tests for PostgreSQL index and constraint definition parsing.

Three code paths are under test: the strict CREATE UNIQUE INDEX pattern in
extract_index_definition(), the loose paren-scanning fallback it drops to
when that pattern fails, and the row routing plus regex extraction in
PostgresStrategy.get_constraint_definition().

Every expected clause is a hand-written literal, never a re-derivation
using the regexes the source itself uses. Most definitions carry a nested
expression such as lower((email)::text), which the fallback collapses to
((email)::text). Any mutation that knocks a definition off the strict path
therefore lands in an assertion instead of being absorbed by the fallback.
"""
import pytest
from database.exceptions import QueryError
from database.strategy.postgres import PostgresStrategy
from database.strategy.postgres import extract_index_definition


class RecordingCursor:
    """Cursor stand-in: records each statement and serves fixed rows.
    """

    def __init__(self, rows):
        self.rows = [tuple(row) for row in rows]
        self.calls = []
        self.description = [('definition',), ('source',)]

    def execute(self, sql, params=()):
        self.calls.append((sql, params))

    def fetchall(self):
        return list(self.rows)

    def close(self):
        pass


class StubConnection:
    """Connection exposing only what DatabaseStrategy._cursor touches.

    Rows are (definition, source) tuples, so the dict assembly in
    _select_raw runs for real instead of being handed finished dicts.
    """

    def __init__(self, rows):
        self.recorder = RecordingCursor(rows)

    @property
    def dbapi_connection(self):
        return self

    def cursor(self):
        return self.recorder


class TestExtractIndexDefinition:
    """Strict CREATE UNIQUE INDEX pattern, on real pg_indexes definitions.
    """

    def test_simple_index(self):
        """Verify a single-column index yields the column clause alone.

        Mutation: column_clause taking match.group(0) instead of group(1),
        which returns the whole CREATE statement as the conflict target.
        Oracle: hand-written '(id)'.
        """
        definition = 'CREATE UNIQUE INDEX user_id_index ON public.users USING btree (id)'
        assert extract_index_definition(definition) == '(id)'

    def test_multicolumn_index(self):
        """Verify a nested expression and its ordering keywords survive.

        Mutation: tightening the lazy column-clause group so it stops at
        the first closing paren, which truncates lower((email)::text) and
        drops the definition to the fallback.
        Oracle: hand-written clause, DESC NULLS LAST included.
        """
        definition = ('CREATE UNIQUE INDEX orders_customer_email_idx'
                      ' ON public.orders USING btree'
                      ' (customer_id, lower((email)::text) DESC NULLS LAST)')
        expected = '(customer_id, lower((email)::text) DESC NULLS LAST)'
        assert extract_index_definition(definition) == expected

    def test_where_clause_index(self):
        """Verify a partial index keeps its predicate on the returned clause.

        Mutation: returning column_clause alone from the WHERE branch, so
        the conflict target no longer matches the partial index.
        Oracle: hand-written '(email) WHERE is_active'.
        """
        definition = ('CREATE UNIQUE INDEX uq_customer_email_active'
                      ' ON public.customers USING btree (email) WHERE is_active')
        assert extract_index_definition(definition) == '(email) WHERE is_active'

    def test_complex_coalesce_index(self):
        """Verify a schema-qualified index of nested COALESCE casts parses.

        Mutation: dropping the optional schema prefix from the table part
        of the pattern, so 'public.financial_records' no longer matches and
        the fallback returns only the first balanced paren group.
        Oracle: hand-written eight-item clause.
        """
        definition = (
            'CREATE UNIQUE INDEX financial_records_date_entity_values_idx'
            ' ON public.financial_records USING btree (record_date, entity_id,'
            " COALESCE(value1, ('-1'::integer)::double precision),"
            " COALESCE(value2, ('-1'::integer)::double precision),"
            " COALESCE(region, '-'::character varying),"
            " COALESCE(manager, '-'::character varying),"
            " COALESCE(group_code, '-'::character varying),"
            " COALESCE(custom_field, '-'::character varying))"
            )
        expected = (
            '(record_date, entity_id,'
            " COALESCE(value1, ('-1'::integer)::double precision),"
            " COALESCE(value2, ('-1'::integer)::double precision),"
            " COALESCE(region, '-'::character varying),"
            " COALESCE(manager, '-'::character varying),"
            " COALESCE(group_code, '-'::character varying),"
            " COALESCE(custom_field, '-'::character varying))"
            )
        assert extract_index_definition(definition) == expected

    def test_partial_index_with_expression_and_predicate(self):
        """Verify the split lands between the last column paren and WHERE.

        Mutation: dropping the end anchor from the pattern, which lets the
        lazy column group stop inside lower((setting_type)::text) and
        return a truncated, unbalanced clause.
        Oracle: hand-written clause plus the two-term predicate.
        """
        definition = ('CREATE UNIQUE INDEX account_settings_account_id_setting_type_idx'
                      ' ON public.account_settings USING btree'
                      ' (account_id, lower((setting_type)::text))'
                      ' WHERE ((is_default = true) AND (deleted_at IS NULL))')
        expected = ('(account_id, lower((setting_type)::text)) '
                    'WHERE ((is_default = true) AND (deleted_at IS NULL))')
        assert extract_index_definition(definition) == expected

    def test_nulls_not_distinct(self):
        """Verify NULLS NOT DISTINCT is dropped, not folded into the clause.

        Mutation: the NULLS NOT DISTINCT group changed to NULLS DISTINCT,
        after which the option and the WHERE keyword are swallowed into the
        returned column clause.
        Oracle: hand-written clause and predicate, option absent.
        """
        definition = ('CREATE UNIQUE INDEX account_preferences_user_id_category_option_date_idx'
                      ' ON public.account_preferences USING btree'
                      ' (user_id, category, option, valid_until)'
                      ' NULLS NOT DISTINCT WHERE (is_active = true)')
        expected = '(user_id, category, option, valid_until) WHERE (is_active = true)'
        assert extract_index_definition(definition) == expected

    def test_nulls_not_distinct_without_where(self):
        """Verify the option is dropped when no predicate follows it.

        Mutation: moving the NULLS NOT DISTINCT group inside the captured
        column group, which appends the option to the conflict target.
        Oracle: hand-written clause, option absent.
        """
        definition = ('CREATE UNIQUE INDEX users_email_tenant_idx ON public.users USING btree'
                      ' (lower((email)::text), tenant_id) NULLS NOT DISTINCT')
        expected = '(lower((email)::text), tenant_id)'
        assert extract_index_definition(definition) == expected

    def test_coalesce_multiple_columns(self):
        """Verify an unqualified table with no USING clause still parses.

        Mutation: making the USING group mandatory, which pushes this
        definition to the fallback and returns ((name)::text).
        Oracle: hand-written clause with both nesting levels.
        """
        definition = ('CREATE UNIQUE INDEX complex_unique_constraint'
                      " ON test_complex_constraint"
                      " (id, COALESCE(lower((name)::text), 'unknown'))")
        expected = "(id, COALESCE(lower((name)::text), 'unknown'))"
        assert extract_index_definition(definition) == expected

    def test_quoted_identifiers_holding_comma_and_paren(self):
        """Verify quoted column names carrying ',' and ')' are not split.

        Mutation: tightening the lazy column-clause group so it stops at
        the first closing paren, which here is the one inside the quoted
        name "addr(2)", truncating the clause mid-identifier.
        Oracle: hand-written clause, both quoted names intact.
        """
        definition = ('CREATE UNIQUE INDEX weird_cols_idx ON public.t USING btree'
                      ' ("last, first", COALESCE("addr(2)", -1))')
        expected = '("last, first", COALESCE("addr(2)", -1))'
        assert extract_index_definition(definition) == expected

    def test_malformed_definition(self):
        """Verify a paren-free definition raises and one paren group does not.

        Mutation: returning the raw definition instead of raising, which
        would splice an unusable string into ON CONFLICT.
        Oracle: boundary pair straddling the presence of a paren group.
        """
        with pytest.raises(QueryError, match='Failed to extract column definition'):
            extract_index_definition('NOT A VALID INDEX DEFINITION')
        with_paren_group = 'NOT A VALID INDEX DEFINITION (a, b)'
        assert extract_index_definition(with_paren_group) == '(a, b)'


class TestIndexDefinitionFallback:
    """Definitions the strict pattern rejects, handled by paren scanning.

    A quoted table name is the shape that reaches the fallback here: the
    strict pattern only accepts a bare-word table name.
    """

    def test_quoted_table_name_with_nested_expression(self):
        """Verify the fallback spans one level of nested parens.

        Mutation: dropping the nested-group alternation from the fallback
        paren regex, which then returns the inner (qty, -1) instead of the
        whole column list.
        Oracle: hand-written clause.
        """
        definition = ('CREATE UNIQUE INDEX uq_oi ON public."Order Items"'
                      ' USING btree (id, COALESCE(qty, -1))')
        assert extract_index_definition(definition) == '(id, COALESCE(qty, -1))'

    def test_quoted_table_name_with_predicate(self):
        """Verify the fallback keeps the parens and appends the predicate.

        Mutation: the fallback taking paren_match.group(1) instead of
        group(0), which strips the parens off the conflict target.
        Oracle: hand-written '(email) WHERE (is_active = true)'.
        """
        definition = ('CREATE UNIQUE INDEX uq_oi_w ON public."Order Items"'
                      ' USING btree (email) WHERE (is_active = true)')
        expected = '(email) WHERE (is_active = true)'
        assert extract_index_definition(definition) == expected


class TestGetConstraintDefinition:
    """Row routing and extraction in get_constraint_definition().
    """

    def test_index_row_is_parsed_as_index_definition(self):
        """Verify the definition is stripped before the index parser sees it.

        Mutation: definition.strip() weakened to lstrip(), which leaves the
        trailing blanks a real catalog row can carry on the returned clause.
        Oracle: hand-written clause with no trailing blanks.
        """
        cn = StubConnection([
            (('CREATE UNIQUE INDEX uq_t_a_lower_b ON public.t'
             ' USING btree (a, lower((b)::text)) WHERE (c IS NULL)   '),
             'index'),
            ])
        result = PostgresStrategy().get_constraint_definition(
            cn, 'public.t', 'uq_t_a_lower_b')
        assert result == '(a, lower((b)::text)) WHERE (c IS NULL)'

    @pytest.mark.parametrize(('definition', 'expected'), [
        ('UNIQUE (sku, warehouse)', 'sku, warehouse'),
        ('PRIMARY KEY (id)', 'id'),
        ('UNIQUE (a, b) DEFERRABLE INITIALLY DEFERRED', 'a, b'),
        ('UNIQUE (a) INCLUDE (c)', 'a'),
        ('UNIQUE NULLS NOT DISTINCT (a, b)', 'a, b'),
        ], ids=['unique', 'primary-key', 'deferrable', 'include-clause', 'nulls-not-distinct'])
    def test_constraint_row_returns_bare_column_list(self, definition, expected):
        """Verify a constraint row yields its columns without the keyword.

        Mutation: source == 'constraint' flipped to 'index' (routing, caught
                  by all params); [^)]+ widened to .+ in either regex
                  (include-clause param); generic re.search fallback deleted
                  (nulls-not-distinct param).
        Oracle: hand-written column list per definition shape.
        """
        cn = StubConnection([(definition, 'constraint')])
        result = PostgresStrategy().get_constraint_definition(
            cn, 'inventory', 'uq_inventory')
        assert result == expected

    def test_lookup_uses_unqualified_unquoted_table_name(self):
        """Verify the placeholders get constraint, table, constraint, table.

        Mutation: parts[0] instead of parts[-1] for table_name, which sends
        the schema 'public' where the table name belongs.
        Oracle: hand-written parameter tuple, quotes and schema removed.
        """
        cn = StubConnection([('UNIQUE (sku)', 'constraint')])
        PostgresStrategy().get_constraint_definition(
            cn, 'public."Order Items"', 'uq_order_items_sku')
        expected = (
            'uq_order_items_sku',
            'Order Items',
            'uq_order_items_sku',
            'Order Items',
            )
        assert len(cn.recorder.calls) == 1
        sql, params = cn.recorder.calls[0]
        assert params == expected
        assert sql.count('%s') == len(expected)

    def test_missing_constraint_raises(self):
        """Verify an empty result set raises QueryError, not IndexError.

        Mutation: the `if not result:` guard narrowed to `result is None`,
        after which result[0] raises IndexError on an empty list.
        Oracle: the exception type plus the constraint name in the message.
        """
        cn = StubConnection([])
        with pytest.raises(QueryError, match='uq_missing'):
            PostgresStrategy().get_constraint_definition(
                cn, 'public.t', 'uq_missing')

    def test_first_row_wins_when_index_and_constraint_both_match(self):
        """Verify the leading union row is used when a name matches both arms.

        Mutation: result[-1]['definition'] instead of result[0], which pairs
        the trailing row's definition with the leading row's source.
        Oracle: hand-written clause from the leading row only.
        """
        cn = StubConnection([
            ('CREATE UNIQUE INDEX uq_t ON public.t USING btree (lower((email)::text))', 'index'),
            ('UNIQUE (email)', 'constraint'),
            ])
        result = PostgresStrategy().get_constraint_definition(cn, 'public.t', 'uq_t')
        assert result == '(lower((email)::text))'

    def test_unextractable_constraint_definition_raises(self):
        """Verify a constraint definition with no paren group raises.

        Mutation: returning the raw definition instead of raising at the
        end of the constraint branch, which would splice an unusable
        expression into ON CONFLICT.
        Oracle: the exception type plus its 'Failed to extract regex' text.
        """
        cn = StubConnection([('', 'constraint')])
        with pytest.raises(QueryError, match='Failed to extract regex'):
            PostgresStrategy().get_constraint_definition(cn, 'public.t', 'uq_t')


if __name__ == '__main__':
    __import__('pytest').main([__file__])
