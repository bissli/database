"""PostgresStrategy maintenance, metadata, and copy against a live server.
"""
import io

import database as db
import psycopg
import pytest
from database.strategy import PostgresStrategy


def _relfilenode(cn, relation):
    """Storage file id of a table or index; a rewrite assigns a new one.
    """
    return db.select_scalar(
        cn, 'select relfilenode from pg_class where relname = %s', relation)


def test_vacuum_table_rewrites_the_table(psql_docker, pg_conn):
    """Verify vacuum_table runs vacuum full and keeps the rows.

    Mutation: dropping full, so the table keeps its storage file.
    Oracle: the storage file id before the call, and the six staged rows.
    """
    before = _relfilenode(pg_conn, 'test_table')

    db.vacuum_table(pg_conn, 'test_table')

    assert _relfilenode(pg_conn, 'test_table') != before
    assert db.select_scalar(pg_conn, 'select count(*) from test_table') == 6


def test_reindex_table_rebuilds_its_indexes(psql_docker, pg_conn):
    """Verify reindex_table rebuilds an index on the table.

    Mutation: a statement other than reindex table, keeping the index file.
    Oracle: the index's storage file id before the call.
    """
    db.execute(pg_conn, 'create index test_idx_value on test_table (value)')
    before = _relfilenode(pg_conn, 'test_idx_value')

    db.reindex_table(pg_conn, 'test_table')

    assert _relfilenode(pg_conn, 'test_idx_value') != before


def test_cluster_table_orders_by_the_named_index(psql_docker, pg_conn):
    """Verify cluster_table uses the index it is given, then reuses it.

    Mutation: ignoring index, so cluster runs with no using clause.
    Oracle: the server's no-index error, then pg_index.indisclustered.
    """
    db.execute(pg_conn, 'create index test_idx_cluster on test_table (value)')
    with pytest.raises(Exception, match='no previously clustered index'):
        db.cluster_table(pg_conn, 'test_table')

    db.cluster_table(pg_conn, 'test_table', 'test_idx_cluster')
    before = _relfilenode(pg_conn, 'test_table')
    db.cluster_table(pg_conn, 'test_table')

    is_clustered = db.select_scalar(pg_conn, """
select i.indisclustered
from pg_index i
join pg_class c on c.oid = i.indexrelid
where c.relname = 'test_idx_cluster'
""")
    assert is_clustered is True
    assert _relfilenode(pg_conn, 'test_table') != before
    assert db.select_scalar(pg_conn, 'select count(*) from test_table') == 6


def test_strategy_get_primary_keys(psql_docker, pg_conn):
    """Verify every column of a composite primary key is returned.

    Mutation: i.indkey[0] in place of any(i.indkey), keeping one column.
    Oracle: the hand-written key of each table.
    """
    db.execute(pg_conn, """
create temporary table test_composite_pk (
    id1 int,
    id2 int,
    data text,
    primary key (id1, id2)
)
""")
    strategy = PostgresStrategy()

    composite_pk = strategy.get_primary_keys(pg_conn, 'test_composite_pk')
    assert set(composite_pk) == {'id1', 'id2'}
    assert strategy.get_primary_keys(pg_conn, 'test_table') == ['name']


@pytest.mark.parametrize('table', ['Order Items', 'MixedCase', 'public.MixedCase'])
def test_strategy_get_primary_keys_on_a_name_needing_quotes(
        psql_docker, pg_conn, table):
    """Verify the key lookup resolves a name as the sequence lookup does.

    Mutation: the raw name cast to regclass, which folds MixedCase to
              lower case and rejects the space in Order Items.
    Oracle: the hand-written serial key, which get_sequence_columns
            also finds under the same spelling.
    """
    name = table.removeprefix('public.')
    db.execute(pg_conn, f'create table "{name}" (id serial primary key, data text)')
    try:
        strategy = PostgresStrategy()
        sequence_cols = strategy.get_sequence_columns(pg_conn, table, bypass_cache=True)
        assert sequence_cols == ['id']
        assert strategy.get_primary_keys(pg_conn, table, bypass_cache=True) == ['id']
    finally:
        db.execute(pg_conn, f'drop table "{name}"')


def test_strategy_get_primary_keys_on_a_missing_table(psql_docker, pg_conn):
    """Verify a missing table raises, so no fallback sequence column is cached.

    Mutation: to_regclass in place of the ::regclass cast, which returns
              [] and lets find_sequence_column cache the 'id' fallback.
    Oracle: PostgreSQL's UndefinedTable for a relation that does not exist.
    """
    strategy = PostgresStrategy()
    with pytest.raises(psycopg.errors.UndefinedTable):
        strategy.get_primary_keys(pg_conn, 'no_such_table', bypass_cache=True)
    with pytest.raises(psycopg.errors.UndefinedTable):
        strategy.find_sequence_column(pg_conn, 'no_such_table', bypass_cache=True)


def test_strategy_get_sequence_columns(psql_docker, pg_conn):
    """Verify only a column defaulting to nextval counts as a sequence.

    Mutation: dropping the column_default filter.
    Oracle: the hand-written serial column of the table.
    """
    db.execute(pg_conn, """
create temporary table test_sequence_columns (
    id serial primary key,
    non_serial_id int,
    data text
)
""")
    strategy = PostgresStrategy()

    assert strategy.get_sequence_columns(pg_conn, 'test_sequence_columns') == ['id']


def test_copy_from(psql_docker, pg_conn):
    """Verify copy loads each CSV line into the named columns.

    Mutation: dropping the column list from the copy statement.
    Oracle: a table whose column order is the reverse of the CSV's.
    """
    db.execute(pg_conn, 'create temporary table test_copy (value integer, name text)')

    csv_data = io.StringIO('David,40\nEva,50\nFrank,60\n')
    rowcount = db.copy_from(pg_conn, 'test_copy', csv_data, ['name', 'value'])

    assert rowcount == 3
    rows = db.select(pg_conn, 'select name, value from test_copy order by value')
    assert [(row['name'], row['value']) for row in rows] == [
        ('David', 40), ('Eva', 50), ('Frank', 60)]


def test_copy_from_without_columns(psql_docker, pg_conn):
    """Verify copy with no columns fills the table in its column order.

    Mutation: an empty '()' column list when columns is None.
    Oracle: the hand-written rows, in table column order.
    """
    db.execute(pg_conn,
               'create temporary table test_copy_nocol (col1 text, col2 integer)')

    csv_data = io.StringIO('Alice,100\nBob,200\n')
    rowcount = db.copy_from(pg_conn, 'test_copy_nocol', csv_data)

    assert rowcount == 2
    rows = db.select(pg_conn, 'select col1, col2 from test_copy_nocol order by col2')
    assert [(row['col1'], row['col2']) for row in rows] == [
        ('Alice', 100), ('Bob', 200)]


def test_copy_from_empty_file(psql_docker, pg_conn):
    """Verify an empty file loads no rows and reports 0, not None.

    Mutation: returning early on empty input, which reports None.
    Oracle: 0, the row count of an empty load.
    """
    db.execute(pg_conn,
               'create temporary table test_copy_empty (name text, value integer)')

    rowcount = db.copy_from(
        pg_conn, 'test_copy_empty', io.StringIO(''), ['name', 'value'])

    assert rowcount == 0
    assert db.select_scalar(pg_conn, 'select count(*) from test_copy_empty') == 0


if __name__ == '__main__':
    __import__('pytest').main([__file__])
