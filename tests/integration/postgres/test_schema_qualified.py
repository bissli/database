"""Schema-qualified table names ('myschema.t') against a live server.
"""
import database as db
import pytest
from database.strategy import get_db_strategy


@pytest.fixture
def decoy_public_t(pg_schema_conn):
    """A public.t whose one column is a serial integer named decoy.
    """
    db.execute(pg_schema_conn, 'drop table if exists public.t')
    db.execute(pg_schema_conn, 'create table public.t (decoy serial primary key)')
    try:
        yield pg_schema_conn
    finally:
        db.execute(pg_schema_conn, 'drop table if exists public.t')


@pytest.mark.usefixtures('psql_docker')
class TestSchemaQualifiedSchemaMetadata:
    """Metadata lookups on 'myschema.t', with a decoy public.t present.
    """

    def test_get_table_columns_with_schema(self, decoy_public_t):
        """Verify the wrapper's column lookup reads the qualified table.

        Mutation: the Inspector given schema=None, reading public.t.
        Oracle: the fixture's columns in declaration order.
        """
        cols = decoy_public_t.get_table_columns('myschema.t')
        assert cols == ['id', 'name', 'value']

    def test_get_table_primary_keys_with_schema(self, decoy_public_t):
        """Verify the wrapper's primary key lookup reads the qualified table.

        Mutation: the Inspector given schema=None, reading public.t.
        Oracle: the fixture's key column, name.
        """
        assert decoy_public_t.get_table_primary_keys('myschema.t') == ['name']

    def test_get_sequence_columns_with_schema(self, decoy_public_t):
        """Verify the sequence lookup filters on the schema.

        Mutation: dropping the table_schema filter.
        Oracle: the fixture's serial column, id.
        """
        strategy = get_db_strategy(decoy_public_t)
        assert strategy.get_sequence_columns(decoy_public_t, 'myschema.t') == ['id']

    def test_get_default_columns_with_schema(self, decoy_public_t):
        """Verify the default-column lookup filters on the schema.

        Mutation: dropping the table_schema filter.
        Oracle: the fixture's three columns, all of display types.
        """
        strategy = get_db_strategy(decoy_public_t)
        cols = strategy.get_default_columns(decoy_public_t, 'myschema.t')
        assert cols == ['id', 'name', 'value']

    def test_get_ordered_columns_with_schema(self, decoy_public_t):
        """Verify the ordered-column lookup filters on the schema.

        Mutation: dropping the table_schema filter.
        Oracle: the fixture's columns in declaration order.
        """
        strategy = get_db_strategy(decoy_public_t)
        cols = strategy.get_ordered_columns(decoy_public_t, 'myschema.t')
        assert cols == ['id', 'name', 'value']


@pytest.mark.usefixtures('psql_docker')
class TestUnqualifiedNameFollowsSearchPath:
    """Metadata lookups on a bare 't' with public.t and myschema.t present.
    """

    @pytest.mark.parametrize(('method', 'public_cols', 'myschema_cols'), [
        ('get_ordered_columns', ['decoy'], ['id', 'name', 'value']),
        ('get_default_columns', ['decoy'], ['id', 'name', 'value']),
        ('get_sequence_columns', ['decoy'], ['id']),
        ])
    def test_unqualified_table_reads_only_the_visible_table(
            self, decoy_public_t, method, public_cols, myschema_cols):
        """Verify a bare name reads the one table search_path resolves.

        Mutation: no schema filter on an unqualified name, mixing in
                  every schema's t.
        Oracle: each table's hand-written columns, picked by search_path.
        """
        lookup = getattr(get_db_strategy(decoy_public_t), method)
        assert lookup(decoy_public_t, 't') == public_cols

        db.execute(decoy_public_t, 'set search_path to myschema, public')
        assert lookup(decoy_public_t, 't', bypass_cache=True) == myschema_cols


@pytest.mark.usefixtures('psql_docker')
class TestConstraintDefinitionSchema:
    """get_constraint_definition with one index name in two schemas.
    """

    @pytest.fixture
    def two_uq_t(self, decoy_public_t):
        """uq_t on public.t (decoy), then uq_t on myschema.t (value).
        """
        db.execute(decoy_public_t, 'create unique index uq_t on public.t (decoy)')
        db.execute(decoy_public_t, 'create unique index uq_t on myschema.t (value)')
        return decoy_public_t

    def test_qualified_name_reads_its_own_schema(self, two_uq_t):
        """Verify the conflict target comes from the named schema's index.

        Mutation: matching the bare table name, so public.t's uq_t wins.
        Oracle: each index's hand-written column list.
        """
        strategy = get_db_strategy(two_uq_t)
        assert strategy.get_constraint_definition(two_uq_t, 'myschema.t', 'uq_t') == '(value)'
        assert strategy.get_constraint_definition(two_uq_t, 'public.t', 'uq_t') == '(decoy)'
        assert strategy.get_constraint_definition(two_uq_t, 't', 'uq_t') == '(decoy)'

    def test_upsert_on_qualified_table_uses_its_own_index(self, two_uq_t):
        """Verify upsert_rows conflicts on myschema.t's uq_t, not public's.

        Mutation: the conflict target read from public.t's uq_t (decoy).
        Oracle: value 1 belongs to 'alpha', so the row renames it.
        """
        db.upsert_rows(two_uq_t, 'myschema.t', [{'name': 'zeta', 'value': 1}],
                       constraint_name='uq_t', update_cols_always=['name'])
        rows = db.select(two_uq_t, 'select name, value from myschema.t order by value')
        assert [(row['name'], row['value']) for row in rows] == [
            ('zeta', 1), ('beta', 2)]

    def test_quoted_table_name_holding_a_dot(self, pg_conn):
        """Verify a quoted name with a dot inside is one table, not two parts.

        Mutation: table.split('.') on the raw name, looking up table 'b"'.
        Oracle: the hand-written column list of uq_ab.
        """
        db.execute(pg_conn, 'drop table if exists "a.b"')
        db.execute(pg_conn, 'create table "a.b" (k int)')
        db.execute(pg_conn, 'create unique index uq_ab on "a.b" (k)')
        try:
            strategy = get_db_strategy(pg_conn)
            assert strategy.get_constraint_definition(pg_conn, '"a.b"', 'uq_ab') == '(k)'
        finally:
            db.execute(pg_conn, 'drop table "a.b"')


@pytest.mark.usefixtures('psql_docker')
class TestSchemaQualifiedQueries:
    """Reads and writes through the library on 'myschema.t'.
    """

    def test_select_from_schema_qualified_table(self, pg_schema_conn):
        """Verify a qualified table name passes through query processing.

        Mutation: query processing rewriting the dotted name.
        Oracle: the fixture's two rows.
        """
        rows = db.select(
            pg_schema_conn, 'select name, value from myschema.t order by value')
        assert [(row['name'], row['value']) for row in rows] == [
            ('alpha', 1), ('beta', 2)]

    def test_upsert_rows_into_schema_qualified_table(self, pg_schema_conn):
        """Verify an upsert updates one row and inserts the other.

        Mutation: key lookup on the bare table name, so no key is found.
        Oracle: hand-chosen values for one existing and one new name.
        """
        new_rows = (
            {'name': 'alpha', 'value': 100},
            {'name': 'gamma', 'value': 3},
            )
        affected = db.upsert_rows(pg_schema_conn, 'myschema.t', new_rows,
                                  update_cols_always=['value'])
        assert affected == 2

        rows = db.select(
            pg_schema_conn, 'select name, value from myschema.t order by name')
        assert [(row['name'], row['value']) for row in rows] == [
            ('alpha', 100), ('beta', 2), ('gamma', 3)]

    def test_reset_table_sequence_with_schema(self, pg_schema_conn):
        """Verify the reset moves the qualified table's sequence past max(id).

        Mutation: pg_get_serial_sequence given the bare table name.
        Oracle: a row at id 10 with the sequence at 3, so the next id is 11.
        """
        db.execute(pg_schema_conn,
                   "insert into myschema.t (id, name, value) values (10, 'ten', 10)")

        db.reset_table_sequence(pg_schema_conn, 'myschema.t', identity='id')

        db.execute(pg_schema_conn,
                   "insert into myschema.t (name, value) values ('delta', 4)")
        new_id = db.select_scalar(pg_schema_conn,
                                  "select id from myschema.t where name = 'delta'")
        assert new_id == 11


@pytest.mark.usefixtures('psql_docker')
def test_mixed_case_table_keeps_its_own_primary_keys(pg_conn):
    """Verify "MixedCase" and mixedcase never share a cached key list.

    Mutation: a .lower() on the cacheable_strategy key.
    Oracle: the primary key each table's DDL declares.
    """
    db.execute(pg_conn, 'create table "MixedCase" (code text primary key, id integer)')
    db.execute(pg_conn, 'create table mixedcase (code text, id integer primary key)')
    try:
        strategy = get_db_strategy(pg_conn)

        assert strategy.get_primary_keys(pg_conn, 'MixedCase') == ['code']
        assert strategy.get_primary_keys(pg_conn, 'mixedcase') == ['id']
    finally:
        db.execute(pg_conn, 'drop table if exists "MixedCase", mixedcase')
