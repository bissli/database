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
