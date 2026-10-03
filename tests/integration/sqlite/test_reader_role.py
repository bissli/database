"""Integration tests for connect(role='reader') against SQLite.
"""
import sqlite3

import database as db
import pytest
from database.exceptions import ReadOnlyError

pytestmark = [pytest.mark.sqlite, pytest.mark.integration]


@pytest.fixture
def sqlite_reader_pair(tmp_path):
    """(writer, reader) over one file database holding three rows.
    """
    options = {'drivername': 'sqlite', 'database': str(tmp_path / 'reader.db')}

    writer = db.connect(dict(options))
    db.execute(writer, """
create table test_table (
    id integer primary key,
    name text not null unique,
    value integer not null
)
""")
    db.execute(writer, """
insert into test_table (name, value) values
('Alice', 10), ('Bob', 20), ('Charlie', 30)
""")

    reader = db.connect(dict(options), role='reader')
    try:
        yield writer, reader
    finally:
        reader.close()
        writer.close()


def test_reader_reads_the_writer_committed_rows(sqlite_reader_pair):
    """Verify the reader opens the same database and reads it.

    Mutation: skipping configure_connection for a reader.
    Oracle: the three rows the writer committed.
    """
    _, reader = sqlite_reader_pair

    assert reader.readonly is True
    assert db.select_scalar(reader, 'select count(*) from test_table') == 3


def test_reader_connection_carries_query_only(sqlite_reader_pair):
    """Verify the query_only pragma reached the connection.

    Mutation: dropping the set_session_readonly call in configure_connection.
    Oracle: SQLite's own report of query_only on that connection.
    """
    _, reader = sqlite_reader_pair
    raw = reader.dbapi_connection.driver_connection

    assert raw.execute('pragma query_only').fetchone()[0] == 1


def test_writer_connection_is_left_writable(sqlite_reader_pair):
    """Verify query_only does not reach the writer.

    Mutation: set_session_readonly for both roles, or readonly off the key.
    Oracle: SQLite's report on the writer, plus a real insert.
    """
    writer, _ = sqlite_reader_pair
    raw = writer.dbapi_connection.driver_connection

    assert raw.execute('pragma query_only').fetchone()[0] == 0
    assert db.execute(
        writer, "insert into test_table (name, value) values ('Writer', 1)") == 1


@pytest.mark.parametrize('sql', [
    "insert into test_table (name, value) values ('Zed', 1)",
    'update test_table set value = 0',
    'delete from test_table',
    'create table reader_ddl_probe (a int)',
    'drop table test_table',
    ])
def test_reader_raw_write_refused_by_server(sqlite_reader_pair, sql):
    """Verify the server backstop blocks raw DML and DDL on a reader.

    Mutation: dropping the query_only pragma from configure_connection.
    Oracle: OperationalError from the server, and the writer's count of 3.
    """
    writer, reader = sqlite_reader_pair

    with pytest.raises(sqlite3.OperationalError):
        db.execute(reader, sql)

    assert db.select_scalar(writer, 'select count(*) from test_table') == 3


def test_query_only_survives_a_refused_pragma(sqlite_reader_pair):
    """Verify a refused 'pragma query_only = off' leaves it on.

    Mutation: removing the disarm check from raise_on_readonly_disarm.
    Oracle: SQLite's report of query_only, and a later refused write.
    """
    _, reader = sqlite_reader_pair
    raw = reader.dbapi_connection.driver_connection

    with pytest.raises(ReadOnlyError):
        db.execute(reader, 'PRAGMA query_only = OFF')

    assert raw.execute('pragma query_only').fetchone()[0] == 1

    with pytest.raises(sqlite3.OperationalError):
        db.execute(reader, 'delete from test_table')


def test_reader_refuses_every_data_operation(sqlite_reader_pair):
    """Verify each named write helper raises on a reader.

    Mutation: dropping _reject_if_readonly from insert_rows or upsert_rows.
    Oracle: ReadOnlyError from each of the four public entry points.
    """
    _, reader = sqlite_reader_pair
    rows = ({'name': 'Zed', 'value': 1},)

    with pytest.raises(ReadOnlyError):
        db.insert_row(reader, 'test_table', ['name', 'value'], ['Zed', 1])
    with pytest.raises(ReadOnlyError):
        db.insert_rows(reader, 'test_table', rows)
    with pytest.raises(ReadOnlyError):
        db.update_row(reader, 'test_table', ['name'], ['Alice'], ['value'], [0])
    with pytest.raises(ReadOnlyError):
        db.upsert_rows(reader, 'test_table', rows, update_cols_always=['value'])


def test_reader_refuses_maintenance_operations(sqlite_reader_pair):
    """Verify maintenance calls are refused at the wrapper.

    Mutation: dropping _reject_if_readonly from any one of the four.
    Oracle: ReadOnlyError from each of the four methods.
    """
    _, reader = sqlite_reader_pair

    with pytest.raises(ReadOnlyError):
        db.vacuum_table(reader, 'test_table')
    with pytest.raises(ReadOnlyError):
        db.reindex_table(reader, 'test_table')
    with pytest.raises(ReadOnlyError):
        db.cluster_table(reader, 'test_table')
    with pytest.raises(ReadOnlyError):
        db.reset_table_sequence(reader, 'test_table')


def test_reader_leaves_the_data_untouched(sqlite_reader_pair):
    """Verify no refused write partially landed.

    Mutation: guarding after the statement runs.
    Oracle: the writer's own row count, unchanged after refused writes.
    """
    writer, reader = sqlite_reader_pair

    row = {'name': 'Zed', 'value': 1}

    with pytest.raises(sqlite3.OperationalError):
        db.execute(reader, 'delete from test_table')
    with pytest.raises(ReadOnlyError):
        db.insert_row(reader, 'test_table', ['name'], ['Zed'])
    with pytest.raises(ReadOnlyError):
        db.insert_rows(reader, 'test_table', (row,))
    with pytest.raises(ReadOnlyError):
        db.vacuum_table(reader, 'test_table')

    assert db.select_scalar(writer, 'select count(*) from test_table') == 3
    assert db.select_scalar(reader, 'select count(*) from test_table') == 3


def test_reader_survives_a_connection_rebuild(sqlite_reader_pair):
    """Verify the read-only setting is reapplied after a reconnect.

    Mutation: _ensure_connection dropping the readonly flag.
    Oracle: SQLite's report of query_only after the rebuild, and a refusal.
    """
    _, reader = sqlite_reader_pair

    reader._invalidate()
    assert db.select_scalar(reader, 'select count(*) from test_table') == 3

    raw = reader.dbapi_connection.driver_connection
    assert raw.execute('pragma query_only').fetchone()[0] == 1
    with pytest.raises(sqlite3.OperationalError):
        db.execute(reader, 'delete from test_table')


def test_reader_does_not_change_the_journal_mode(tmp_path):
    """Verify opening a reader leaves the database file untouched.

    Mutation: calling configure_writer_connection for a reader too.
    Oracle: journal_mode on a separate connection, before and after.
    """
    db_file = tmp_path / 'journal.db'
    setup = sqlite3.connect(db_file)
    setup.execute('create table t (a int)')
    setup.commit()
    before = setup.execute('pragma journal_mode').fetchone()[0]
    setup.close()

    reader = db.connect({'drivername': 'sqlite', 'database': str(db_file)},
                        role='reader')
    reader.close()

    check = sqlite3.connect(db_file)
    after = check.execute('pragma journal_mode').fetchone()[0]
    check.close()

    assert (before, after) == ('delete', 'delete')


def test_reader_opens_a_file_the_process_cannot_write(tmp_path):
    """Verify a reader works on a database opened read-only.

    Mutation: calling configure_writer_connection for a reader too.
    Oracle: a file and directory with the write bits cleared.
    """
    db_file = tmp_path / 'locked.db'
    setup = sqlite3.connect(db_file)
    setup.execute('create table t (a int)')
    setup.execute('insert into t values (1)')
    setup.commit()
    setup.close()

    db_file.chmod(0o400)
    tmp_path.chmod(0o500)
    try:
        reader = db.connect({'drivername': 'sqlite', 'database': str(db_file)},
                            role='reader')
        try:
            assert db.select_scalar(reader, 'select count(*) from t') == 1
        finally:
            reader.close()
    finally:
        tmp_path.chmod(0o700)
        db_file.chmod(0o600)


def test_reader_refuses_an_in_memory_database():
    """Verify role='reader' on ':memory:' raises instead of misleading.

    Mutation: dropping the ':memory:' check from connect().
    Oracle: ValidationError naming the database.
    """
    with pytest.raises(db.ValidationError, match=':memory:'):
        db.connect({'drivername': 'sqlite', 'database': ':memory:'},
                   role='reader')


def test_reader_refuses_an_empty_batch(sqlite_reader_pair):
    """Verify an empty row collection raises on a reader.

    Mutation: dropping _reject_if_readonly from insert_rows or upsert_rows.
    Oracle: ReadOnlyError on an input that produces no SQL at all.
    """
    _, reader = sqlite_reader_pair

    with pytest.raises(ReadOnlyError):
        db.insert_rows(reader, 'test_table', [])
    with pytest.raises(ReadOnlyError):
        db.upsert_rows(reader, 'test_table', ())


def test_comment_hidden_statement_never_runs(sqlite_reader_pair):
    """Verify a write behind a trailing comment is neither seen nor run.

    Mutation: sql.split(';') in place of split_statements in _execute_query.
    Oracle: the writer's row count and query_only, unchanged.
    """
    writer, reader = sqlite_reader_pair
    raw = reader.dbapi_connection.driver_connection
    sql = ('select ? -- ; PRAGMA query_only = OFF; '
           "insert into test_table (name, value) values ('Hidden', 1)")

    db.execute(reader, sql, 1)

    assert raw.execute('pragma query_only').fetchone()[0] == 1
    assert db.select_scalar(writer, 'select count(*) from test_table') == 3


def test_transaction_reports_the_connection_readonly_flag(sqlite_reader_pair):
    """Verify a Transaction carries the flag the disarm guard reads.

    Mutation: deleting Transaction.readonly.
    Oracle: the flag on both a reader's and a writer's transaction.
    """
    writer, reader = sqlite_reader_pair

    with db.transaction(reader) as tx:
        assert tx.readonly is True
    with db.transaction(writer) as tx:
        assert tx.readonly is False
