"""Integration tests for connect(role='reader') against SQLite.

SQLite has no reader endpoint, so role='reader' opens the same database
and relies on the guard. That makes it the cheapest place to prove the
guard itself, and the one place to prove the PRAGMA query_only backstop
survives the journal_mode pragma that configure_connection runs first.
"""
import pathlib
import sqlite3
import time

import database as db
import pytest
from database.exceptions import ReadOnlyError

pytestmark = [pytest.mark.sqlite, pytest.mark.integration]


@pytest.fixture
def sqlite_reader_pair():
    """A writer and a reader over one file-based SQLite database.

    Yields (writer, reader). The file carries three staged rows and is
    removed on exit along with any WAL sidecar.
    """
    db_file = f'./test_reader_{int(time.time() * 1000)}.db'
    options = {'drivername': 'sqlite', 'database': db_file}

    writer = db.connect(dict(options))
    db.execute(writer, """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            value INTEGER NOT NULL
        )
    """)
    db.execute(writer, """
        INSERT INTO test_table (name, value) VALUES
        ('Alice', 10), ('Bob', 20), ('Charlie', 30)
    """)

    reader = db.connect(dict(options), role='reader')
    try:
        yield writer, reader
    finally:
        reader.close()
        writer.close()
        for suffix in ('', '-wal', '-shm'):
            path = pathlib.Path(db_file + suffix)
            if path.exists():
                path.unlink()


def test_reader_reads_the_writer_committed_rows(sqlite_reader_pair):
    """Verify the reader opens the same database and reads it.

    Mutation: skipping configure_connection for a reader along with
        the writer half, which drops the row factory and makes every
        column lookup fail.
    Oracle: the three rows the writer committed.
    """
    _, reader = sqlite_reader_pair

    assert reader.readonly is True
    assert db.select_scalar(reader, 'select count(*) from test_table') == 3


def test_reader_connection_carries_query_only(sqlite_reader_pair):
    """Verify the PRAGMA reached the connection.

    Mutation: dropping the strategy.set_session_readonly call from
        configure_connection, which leaves only the local guard, so
        anything reaching the driver directly writes freely.
    Oracle: SQLite's own report of query_only on that connection.
    """
    _, reader = sqlite_reader_pair
    raw = reader.dbapi_connection.driver_connection

    assert raw.execute('PRAGMA query_only').fetchone()[0] == 1


def test_writer_connection_is_left_writable(sqlite_reader_pair):
    """Verify query_only does not reach the writer.

    Mutation: calling set_session_readonly unconditionally, or dropping
        readonly from the engine registry key so both roles share one
        engine and its pooled connection.
    Oracle: SQLite's report on the writer, plus a real insert.
    """
    writer, _ = sqlite_reader_pair
    raw = writer.dbapi_connection.driver_connection

    assert raw.execute('PRAGMA query_only').fetchone()[0] == 0
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

    Mutation: dropping PRAGMA query_only from configure_connection,
        which leaves a raw statement nothing to answer to, since the
        library reads no SQL to decide what writes.
    Oracle: sqlite3.OperationalError from the server; row count still 3
        on the writer confirms the data did not land.
    """
    writer, reader = sqlite_reader_pair

    with pytest.raises(sqlite3.OperationalError):
        db.execute(reader, sql)

    assert db.select_scalar(writer, 'select count(*) from test_table') == 3


def test_query_only_survives_a_refused_pragma(sqlite_reader_pair):
    """Verify a refused 'PRAGMA query_only = OFF' leaves it on.

    Mutation: removing the disarm check from raise_on_readonly_disarm,
        which lets the PRAGMA through, clears query_only, and leaves the
        reader writable with no server backstop.
    Oracle: ReadOnlyError on the PRAGMA; SQLite's report of query_only
        still 1 after the refusal; a following write still refused by
        the server, proving the backstop stayed armed.
    """
    _, reader = sqlite_reader_pair
    raw = reader.dbapi_connection.driver_connection

    with pytest.raises(ReadOnlyError):
        db.execute(reader, 'PRAGMA query_only = OFF')

    assert raw.execute('PRAGMA query_only').fetchone()[0] == 1

    with pytest.raises(sqlite3.OperationalError):
        db.execute(reader, 'delete from test_table')


def test_reader_refuses_every_data_operation(sqlite_reader_pair):
    """Verify the named write helpers are refused, not just raw SQL.

    Mutation: dropping _reject_if_readonly from insert_rows or
        upsert_rows, which lets a batch through to the driver on a
        connection the caller asked to be read-only.
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
        cluster_table and reset_table_sequence are no-ops on SQLite,
        so nothing here catches them but the guard itself.
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

    Mutation: guarding after the statement runs rather than before -
        the error would surface but the row would already be written.
    Oracle: the writer's own row count, unchanged after refused writes
        from both the server backstop and the wrapper guard.
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

    Mutation: calling configure_connection without the readonly flag in
        _ensure_connection, which silently hands back a writable
        connection the first time the server drops one.
    Oracle: SQLite's report of query_only after the wrapper rebuilds,
        plus a still-refused write from the server backstop.
    """
    _, reader = sqlite_reader_pair

    reader._invalidate()
    assert db.select_scalar(reader, 'select count(*) from test_table') == 3

    raw = reader.dbapi_connection.driver_connection
    assert raw.execute('PRAGMA query_only').fetchone()[0] == 1
    with pytest.raises(sqlite3.OperationalError):
        db.execute(reader, 'delete from test_table')


def test_reader_does_not_change_the_journal_mode(tmp_path):
    """Verify opening a reader leaves the database file untouched.

    journal_mode is stored in the database header, so setting it is a
    write - and query_only does not cover it, so only skipping the
    pragma keeps a reader honest.

    Mutation: calling configure_writer_connection for a reader too,
        which switches the file to WAL the moment a reader connects.
    Oracle: sqlite3's own report of journal_mode, read on a separate
        connection before and after.
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
        Its journal_mode pragma then raises 'attempt to write a
        readonly database' and the connection cannot be opened at all.
    Oracle: a file and directory with the write bits cleared, which is
        what a genuine read-only SQLite deployment looks like.
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


def test_reader_refuses_an_in_memory_database(tmp_path):
    """Verify role='reader' on ':memory:' raises instead of misleading.

    Each connection to ':memory:' owns a private database, so a reader
    would get an empty one and report every table as missing.

    Mutation: dropping the ':memory:' test from connect(), which hands
        back a healthy-looking connection whose sqlite_master is
        empty.
    Oracle: ValidationError naming the database, raised before any
        connection opens.
    """
    with pytest.raises(db.ValidationError, match=':memory:'):
        db.connect({'drivername': 'sqlite', 'database': ':memory:'},
                   role='reader')


def test_reader_refuses_an_empty_batch(sqlite_reader_pair):
    """Verify an empty row collection is refused, not silently zeroed.

    insert_rows and upsert_rows return early on an empty collection,
    before any statement exists for the server to refuse, so a job
    misrouted to the reader would report a clean zero.

    Mutation: dropping _reject_if_readonly from insert_rows or
        upsert_rows, which returns 0 and looks like a successful
        no-op write.
    Oracle: ReadOnlyError on an input that produces no SQL at all.
    """
    _, reader = sqlite_reader_pair

    with pytest.raises(ReadOnlyError):
        db.insert_rows(reader, 'test_table', [])
    with pytest.raises(ReadOnlyError):
        db.upsert_rows(reader, 'test_table', ())


def test_comment_hidden_statement_never_runs(sqlite_reader_pair):
    """Verify a write behind a trailing comment is neither seen nor run.

    The guard masks comments before splitting; the cursor used to
    split the raw text, so a statement hidden behind '--' skipped
    classification and then executed.

    Mutation: sql.split(';') in place of split_statements in
        Cursor._is_multi_statement and _execute_multi_statement, which
        clears query_only and commits the row.
    Oracle: the writer's row count and SQLite's report of query_only,
        both unchanged after the call.
    """
    writer, reader = sqlite_reader_pair
    raw = reader.dbapi_connection.driver_connection
    sql = ('select ? -- ; PRAGMA query_only = OFF; '
           "insert into test_table (name, value) values ('Hidden', 1)")

    db.execute(reader, sql, 1)

    assert raw.execute('PRAGMA query_only').fetchone()[0] == 1
    assert db.select_scalar(writer, 'select count(*) from test_table') == 3


def test_transaction_reports_the_connection_readonly_flag(sqlite_reader_pair):
    """Verify a Transaction carries the flag the disarm guard reads.

    raise_on_readonly_disarm reads 'readonly' off whatever it is handed,
    and strategy helpers do receive a Transaction.

    Mutation: deleting Transaction.readonly, which makes the property
        fall through to nothing and reads a reader's transaction as a
        writer.
    Oracle: the flag on both a reader's and a writer's transaction.
    """
    writer, reader = sqlite_reader_pair

    with db.transaction(reader) as tx:
        assert tx.readonly is True
    with db.transaction(writer) as tx:
        assert tx.readonly is False
