"""Integration tests for schema-qualified table names (e.g. 'myschema.t').

These tests exercise the path through quote_identifier + the
information_schema filters in PostgresStrategy that previously misquoted
or dropped the schema. They use the pg_schema_conn fixture which creates
'myschema.t' and tears it down on exit.
"""
import database as db
import pytest


@pytest.mark.usefixtures('psql_docker')
class TestSchemaQualifiedSchemaMetadata:
    """get_columns / get_primary_keys / get_sequence_columns / get_default_columns
    must all resolve a schema-qualified table.
    """

    def test_get_table_columns_with_schema(self, pg_schema_conn):
        cols = pg_schema_conn.get_table_columns('myschema.t')
        assert set(cols) == {'id', 'name', 'value'}

    def test_get_table_primary_keys_with_schema(self, pg_schema_conn):
        keys = pg_schema_conn.get_table_primary_keys('myschema.t')
        assert keys == ['name']

    def test_get_sequence_columns_with_schema(self, pg_schema_conn):
        from database.strategy import get_db_strategy
        strategy = get_db_strategy(pg_schema_conn)
        seq_cols = strategy.get_sequence_columns(pg_schema_conn, 'myschema.t')
        assert seq_cols == ['id']

    def test_get_default_columns_with_schema(self, pg_schema_conn):
        from database.strategy import get_db_strategy
        strategy = get_db_strategy(pg_schema_conn)
        cols = strategy.get_default_columns(pg_schema_conn, 'myschema.t')
        assert set(cols) == {'id', 'name', 'value'}

    def test_get_ordered_columns_with_schema(self, pg_schema_conn):
        from database.strategy import get_db_strategy
        strategy = get_db_strategy(pg_schema_conn)
        cols = strategy.get_ordered_columns(pg_schema_conn, 'myschema.t')
        assert cols == ['id', 'name', 'value']


@pytest.mark.usefixtures('psql_docker')
class TestSchemaQualifiedQueries:
    """End-to-end queries against a schema-qualified table go through the
    library's SQL processing without corruption.
    """

    def test_select_from_schema_qualified_table(self, pg_schema_conn):
        rows = db.select(pg_schema_conn,
                         'SELECT name, value FROM myschema.t ORDER BY value')
        # Either DataFrame or list-of-dicts depending on configured loader.
        if hasattr(rows, 'to_dict'):
            rows = rows.to_dict(orient='records')
        rows = list(rows)
        assert [r['value'] for r in rows] == [1, 2]
        assert [r['name'] for r in rows] == ['alpha', 'beta']

    def test_upsert_rows_into_schema_qualified_table(self, pg_schema_conn):
        new_rows = (
            {'name': 'alpha', 'value': 100},     # update
            {'name': 'gamma', 'value': 3},       # insert
        )
        affected = db.upsert_rows(pg_schema_conn, 'myschema.t', new_rows,
                                  update_cols_always=['value'])
        assert affected >= 2

        val = db.select_scalar(pg_schema_conn,
                               'SELECT value FROM myschema.t WHERE name = %s',
                               'alpha')
        assert val == 100

        val = db.select_scalar(pg_schema_conn,
                               'SELECT value FROM myschema.t WHERE name = %s',
                               'gamma')
        assert val == 3

    def test_reset_table_sequence_with_schema(self, pg_schema_conn):
        db.reset_table_sequence(pg_schema_conn, 'myschema.t', identity='id')

        db.execute(pg_schema_conn,
                   "INSERT INTO myschema.t (name, value) VALUES ('delta', 4)")
        max_id = db.select_scalar(pg_schema_conn,
                                  'SELECT max(id) FROM myschema.t')
        assert max_id >= 3
