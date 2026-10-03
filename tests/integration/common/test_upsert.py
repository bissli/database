"""upsert_rows tests run against PostgreSQL and SQLite.
"""
import time

import database as db
import pandas as pd
import pytest
from tests.integration.common.conftest import col


def name_value_pairs(result: list[dict] | pd.DataFrame) -> list[tuple]:
    """(name, value) pairs of a select result, in row order.
    """
    return list(zip(col(result, 'name'), col(result, 'value')))


class TestUpsertBasic:
    """Insert and update through upsert_rows on test_table.
    """

    def test_upsert_insert_new_rows(self, db_conn):
        """Verify upsert inserts rows whose key is new and counts them.

        Mutation: returning 0 or the batch count in place of the row count.
        Oracle: two hand-written rows absent from the staged data.
        """
        rows = [{'name': 'Barry', 'value': 50}, {'name': 'Wallace', 'value': 92}]
        row_count = db.upsert_rows(
            db_conn, 'test_table', rows, update_cols_always=['value'])
        assert row_count == 2

        result = db.select(
            db_conn,
            'select name, value from test_table where name in (%s, %s) order by name',
            'Barry', 'Wallace')
        assert name_value_pairs(result) == [('Barry', 50), ('Wallace', 92)]

    def test_upsert_update_existing_rows(self, db_conn):
        """Verify a second upsert of the same keys updates every row.

        Mutation: updating only a batch's first row, or inserting duplicates.
        Oracle: the second batch's hand-written values.
        """
        rows = [{'name': 'Barry', 'value': 50}, {'name': 'Wallace', 'value': 92}]
        db.upsert_rows(db_conn, 'test_table', rows, update_cols_always=['value'])
        rows = [{'name': 'Barry', 'value': 51}, {'name': 'Wallace', 'value': 93}]
        db.upsert_rows(db_conn, 'test_table', rows, update_cols_always=['value'])

        result = db.select(
            db_conn,
            'select name, value from test_table where name in (%s, %s) order by name',
            'Barry', 'Wallace')
        assert name_value_pairs(result) == [('Barry', 51), ('Wallace', 93)]

    def test_upsert_empty_rows(self, db_conn):
        """Verify an empty row list returns 0 without raising.

        Mutation: reading rows[0] or building SQL before the empty check.
        Oracle: zero rows given, zero written.
        """
        assert db.upsert_rows(db_conn, 'test_table', []) == 0


class TestUpsertIfNull:
    """update_cols_ifnull writes a column only where it holds null.
    """

    def test_upsert_ifnull_does_not_overwrite(self, db_conn):
        """Verify update_cols_ifnull leaves a non-null value in place.

        Mutation: treating update_cols_ifnull as update_cols_always.
        Oracle: the value 100 inserted before the upsert.
        """
        db.insert(
            db_conn,
            'insert into test_table (name, value) values (%s, %s)',
            'UpsertNull', 100)

        rows = [{'name': 'UpsertNull', 'value': 200}]
        db.upsert_rows(db_conn, 'test_table', rows, update_cols_ifnull=['value'])

        value = db.select_scalar(
            db_conn, 'select value from test_table where name = %s', 'UpsertNull')
        assert value == 100

    def test_upsert_ifnull_updates_null_value(self, db_conn):
        """Verify update_cols_ifnull writes a column that holds null.

        Mutation: dropping update_cols_ifnull from the update clause.
        Oracle: the row is set to null by hand; the upsert supplies 200.
        """
        db.execute(
            db_conn,
            'create temporary table test_nullable (name text primary key, value integer null)')
        db.insert(
            db_conn,
            'insert into test_nullable (name, value) values (%s, %s)',
            'UpsertNull', 100)
        db.execute(
            db_conn,
            'update test_nullable set value = null where name = %s',
            'UpsertNull')

        rows = [{'name': 'UpsertNull', 'value': 200}]
        db.upsert_rows(db_conn, 'test_nullable', rows, update_cols_ifnull=['value'])

        value = db.select_scalar(
            db_conn, 'select value from test_nullable where name = %s', 'UpsertNull')
        assert value == 200


class TestUpsertMixedOperations:
    """One batch that both updates and inserts.
    """

    def test_upsert_mixed_inserts_and_updates(self, db_conn):
        """Verify one batch updates an existing key and inserts new keys.

        Mutation: counting only inserts, or skipping updates in a mixed batch.
        Oracle: Alice is staged with 10; NewPerson1 and NewPerson2 are new.
        """
        rows = [
            {'name': 'Alice', 'value': 1000},
            {'name': 'NewPerson1', 'value': 500},
            {'name': 'NewPerson2', 'value': 600},
            ]
        row_count = db.upsert_rows(
            db_conn, 'test_table', rows, update_cols_always=['value'])
        assert row_count == 3

        result = db.select(
            db_conn,
            'select name, value from test_table where name in (%s, %s, %s) order by name',
            'Alice', 'NewPerson1', 'NewPerson2')
        assert name_value_pairs(result) == [
            ('Alice', 1000),
            ('NewPerson1', 500),
            ('NewPerson2', 600),
            ]


class TestUpsertColumnFiltering:
    """Row keys are matched to table columns before the insert.
    """

    def test_upsert_filters_invalid_columns(self, db_conn):
        """Verify keys with no column are dropped, in any case, row by row.

        Mutation: using only the first row's keys, or case-sensitive matching.
        Oracle: three rows with unknown keys, two in mixed case.
        """
        rows = [
            {'name': 'ValidationTest1', 'value': 100, 'nonexistent_column': 'dropped'},
            {'NAME': 'ValidationTest2', 'Value': 200, 'INVALID_COLUMN': 'dropped'},
            {'NaMe': 'ValidationTest3', 'VaLuE': 300, 'bad_col': False},
            ]
        assert db.upsert_rows(db_conn, 'test_table', rows) == 3

        result = db.select(
            db_conn,
            'select name, value from test_table where name like %s order by name',
            'ValidationTest%')
        assert name_value_pairs(result) == [
            ('ValidationTest1', 100),
            ('ValidationTest2', 200),
            ('ValidationTest3', 300),
            ]

    def test_upsert_all_invalid_columns(self, db_conn):
        """Verify a batch with no matching column returns 0 without raising.

        Mutation: building an insert with an empty column list.
        Oracle: neither key names a column of test_table.
        """
        rows = [{'nonexistent1': 'Invalid data', 'nonexistent2': 123}]
        assert db.upsert_rows(db_conn, 'test_table', rows, use_primary_key=True) == 0

    def test_upsert_column_order_independence(self, db_conn):
        """Verify each row's values bind by key, whatever the dict order.

        Mutation: binding row.values() in dict order.
        Oracle: two hand-written rows with reversed key order.
        """
        rows = [
            {'name': 'OrderTest1', 'value': 300},
            {'value': 400, 'name': 'OrderTest2'},
            ]
        assert db.upsert_rows(db_conn, 'test_table', rows, use_primary_key=True) == 2

        result = db.select(
            db_conn,
            'select name, value from test_table where name like %s order by name',
            'OrderTest%')
        assert name_value_pairs(result) == [('OrderTest1', 300), ('OrderTest2', 400)]


class TestUpsertCaseInsensitiveColumns:
    """Row keys match columns case-insensitively and take the column's case.
    """

    def test_upsert_case_insensitive_columns_insert(self, db_conn):
        """Verify an insert maps upper and mixed case keys to their columns.

        Mutation: matching keys case-sensitively, which drops NAME and VALUE.
        Oracle: two hand-written rows with assorted key casings.
        """
        rows = [
            {'NAME': 'CaseTest1', 'Value': 101},
            {'name': 'CaseTest2', 'VALUE': 102},
            ]
        assert db.upsert_rows(db_conn, 'test_table', rows, use_primary_key=True) == 2

        result = db.select(
            db_conn,
            'select name, value from test_table where name like %s order by name',
            'CaseTest%')
        assert name_value_pairs(result) == [('CaseTest1', 101), ('CaseTest2', 102)]

    def test_upsert_case_insensitive_columns_update(self, db_conn):
        """Verify mixed case keys update the right row and column.

        Mutation: matching the conflict key case-sensitively.
        Oracle: the second upsert's hand-written value 999.
        """
        db.upsert_rows(
            db_conn,
            'test_table',
            [{'name': 'CaseUpdate', 'value': 1}],
            use_primary_key=True)
        db.upsert_rows(
            db_conn,
            'test_table',
            [{'NAme': 'CaseUpdate', 'vaLUE': 999}],
            use_primary_key=True,
            update_cols_always=['value'])

        result = db.select_row(
            db_conn, 'select value from test_table where name = %s', 'CaseUpdate')
        assert result.value == 999


class TestUpsertPrimaryKeyFiltering:
    """Key columns listed in update_cols_* are left out of the update.
    """

    def test_pk_columns_excluded_from_update_set(self, db_conn):
        """Verify the non-key columns update when key columns are also listed.

        Mutation: inverting the key-column test in the update-column filter.
        Oracle: hand-written row before and after; one row in the table.
        """
        create_sql = """
create temporary table test_pk_filtering (
    id integer,
    code text,
    description text not null,
    value integer not null,
    primary key (id, code)
)
"""
        db.execute(db_conn, create_sql)
        db.execute(
            db_conn,
            'insert into test_pk_filtering values (%s, %s, %s, %s)',
            1, 'ABC', 'Initial description', 100)

        rows = [{
            'id': 1,
            'code': 'ABC',
            'description': 'Updated description',
            'value': 200,
            }]
        db.upsert_rows(
            db_conn,
            'test_pk_filtering',
            rows,
            use_primary_key=True,
            update_cols_always=['id', 'code', 'description', 'value'],
            update_cols_ifnull=['id', 'code'])

        result = db.select_row(
            db_conn, 'select id, code, description, value from test_pk_filtering')
        assert (result.id, result.code) == (1, 'ABC')
        assert result.description == 'Updated description'
        assert result.value == 200


class TestUpsertUnknownKwargRejection:
    """An unknown keyword argument raises TypeError.
    """

    def test_module_level_upsert_rejects_unknown_kwarg(self, db_conn):
        """Verify db.upsert_rows raises TypeError naming an unknown keyword.

        Mutation: a **kw catch-all on the module function.
        Oracle: update_cols_key is no parameter of upsert_rows.
        """
        rows = [{'name': 'KwargReject', 'value': 1}]
        with pytest.raises(TypeError, match='update_cols_key'):
            db.upsert_rows(db_conn, 'test_table', rows, update_cols_key=['name'])

    def test_method_level_upsert_rejects_unknown_kwarg(self, db_conn):
        """Verify cn.upsert_rows raises TypeError naming an unknown keyword.

        Mutation: a **kw catch-all on the method.
        Oracle: update_cols_key is no parameter of upsert_rows.
        """
        rows = [{'name': 'KwargReject2', 'value': 1}]
        with pytest.raises(TypeError, match='update_cols_key'):
            db_conn.upsert_rows('test_table', rows, update_cols_key=['name'])


class TestUpsertNoPrimaryKey:
    """A table with no key takes every upserted row as an insert.
    """

    def test_upsert_no_primary_keys_inserts_all(self, db_conn):
        """Verify upserting the same names twice keeps all four rows.

        Mutation: an update path on a keyless table.
        Oracle: two hand-written batches; the table holds their union.
        """
        db.execute(
            db_conn,
            'create temporary table test_no_pk (name text not null, value integer not null)')

        rows = [{'name': 'NoPK1', 'value': 100}, {'name': 'NoPK2', 'value': 200}]
        assert db.upsert_rows(db_conn, 'test_no_pk', rows) == 2

        rows = [{'name': 'NoPK1', 'value': 101}, {'name': 'NoPK2', 'value': 201}]
        assert db.upsert_rows(
            db_conn, 'test_no_pk', rows, update_cols_always=['value']) == 2

        result = db.select(
            db_conn, 'select name, value from test_no_pk order by name, value')
        assert name_value_pairs(result) == [
            ('NoPK1', 100),
            ('NoPK1', 101),
            ('NoPK2', 200),
            ('NoPK2', 201),
            ]


class TestUpsertLargeBatch:
    """A batch that fills the default batch_size of 500 exactly.
    """

    def test_upsert_large_batch(self, db_conn):
        """Verify a full batch inserts, then updates, every row.

        Mutation: an off-by-one at the batch boundary, dropping the last row.
        Oracle: ids 1 to 500 with values built from the id.
        """
        db.execute(
            db_conn,
            'create temporary table test_large_batch (id integer primary key, value text not null)')
        batch_size = 500
        ids = range(1, batch_size + 1)

        start = time.time()
        rows = [{'id': i, 'value': f'value-{i}'} for i in ids]
        assert db.upsert_rows(db_conn, 'test_large_batch', rows) == batch_size
        insert_time = time.time() - start

        stored = db.select_column(
            db_conn, 'select value from test_large_batch order by id')
        assert stored == [f'value-{i}' for i in ids]

        start = time.time()
        rows = [{'id': i, 'value': f'updated-{i}'} for i in ids]
        update_count = db.upsert_rows(
            db_conn, 'test_large_batch', rows, update_cols_always=['value'])
        update_time = time.time() - start
        assert update_count == batch_size

        stored = db.select_column(
            db_conn, 'select value from test_large_batch order by id')
        assert stored == [f'updated-{i}' for i in ids]

        assert insert_time < 30, f'Insert too slow: {insert_time:.2f}s'
        assert update_time < 30, f'Update too slow: {update_time:.2f}s'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
