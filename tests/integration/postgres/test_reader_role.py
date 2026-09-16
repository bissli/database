"""Integration tests for connect(role='reader') against PostgreSQL.

Two contracts need a real server to prove:

- Endpoint selection. A reader opens reader_hostname and reader_port,
  falling back to the writer's own value for whichever is unset. The
  oracle is whether the connection succeeds when only one of the two
  pairs points at the live container.
- The read-only guarantee. The server session setting refuses a write,
  and the disarm guard refuses any statement that would turn it off.
  The oracle for the server layer is a raw DBAPI statement that never
  reaches the disarm guard.
"""
import io

import config
import database as db
import psycopg
import pytest
from database.exceptions import ReadOnlyError

pytestmark = [pytest.mark.postgres, pytest.mark.integration]


def _base_options():
    """Writer options pointing at the live test container."""
    return {
        'drivername': 'postgresql',
        'hostname': config.postgresql.hostname,
        'port': config.postgresql.port,
        'username': config.postgresql.username,
        'password': config.postgresql.password,
        'database': config.postgresql.database,
        'timeout': config.postgresql.timeout,
        'data_loader': config.postgresql.data_loader,
        }


@pytest.fixture
def pg_reader(psql_docker, pg_conn):
    """Reader connection to the container, with test_table staged.

    pg_conn stages the data and owns the writer side; the reader is a
    second connection carrying role='reader'.
    """
    reader = db.connect(_base_options(), role='reader')
    try:
        yield reader
    finally:
        reader.close()


def test_reader_endpoint_is_used_when_declared(psql_docker):
    """Verify reader_hostname and reader_port beat hostname and port.

    Mutation: reading options.hostname instead of
        options.reader_hostname in connect(), which would route the
        reader to the writer and leave the replica idle.
    Oracle: a writer endpoint that cannot resolve, so only a
        connection built from the reader pair can open at all.
    """
    options = _base_options()
    options['reader_hostname'] = options['hostname']
    options['reader_port'] = options['port']
    options['hostname'] = 'writer.invalid.example'
    options['port'] = 1

    reader = db.connect(options, role='reader')
    try:
        assert db.select_scalar(reader, 'select 1') == 1
    finally:
        reader.close()


def test_reader_falls_back_to_the_writer_endpoint(psql_docker):
    """Verify a database declaring no reader still answers role='reader'.

    Mutation: dropping the 'or options.hostname' fallback, which
        leaves hostname None and makes every partially configured
        database unusable as a reader.
    Oracle: options carrying no reader field at all, against the live
        container.
    """
    reader = db.connect(_base_options(), role='reader')
    try:
        assert reader.readonly is True
        assert db.select_scalar(reader, 'select 1') == 1
    finally:
        reader.close()


def test_reader_port_falls_back_independently_of_hostname(psql_docker):
    """Verify reader_port is honored on its own, with no reader_hostname.

    Mutation: resolving the pair together - taking port from
        reader_port only when reader_hostname is also set - which
        would send this connection to the writer's port.
    Oracle: a writer port nothing listens on, so the connection opens
        only if reader_port was used.
    """
    options = _base_options()
    options['reader_port'] = options['port']
    options['port'] = 1

    reader = db.connect(options, role='reader')
    try:
        assert db.select_scalar(reader, 'select 1') == 1
    finally:
        reader.close()


def test_reader_reads_the_writer_committed_rows(pg_reader, pg_conn):
    """Verify a reader is a working connection, not a crippled one.

    Mutation: applying the read-only session setting before
        strategy.configure_connection, which leaves psycopg in an open
        transaction and breaks the connection outright.
    Oracle: the six rows stage_test_data commits on the writer.
    """
    alice = "select value from test_table where name = 'Alice'"

    assert db.select_scalar(pg_reader, 'select count(*) from test_table') == 6
    assert db.select_scalar(pg_reader, alice) == 10


def test_reader_session_is_read_only_on_the_server(pg_reader):
    """Verify the session-level setting reached the server.

    Mutation: dropping the strategy.set_session_readonly call from
        configure_connection, which leaves no server-side guard and
        lets a write through any path that skips the wrapper's method
        guards.
    Oracle: the server's own report of default_transaction_read_only.
    """
    assert db.select_scalar(
        pg_reader, 'show default_transaction_read_only') == 'on'


def test_writer_session_is_left_writable(pg_conn):
    """Verify the read-only setting does not leak to a writer.

    Mutation: calling set_session_readonly unconditionally, or dropping
        readonly from the engine registry key so a writer checks out a
        read-only pooled connection.
    Oracle: the server's report on the writer, plus a real insert.
    """
    assert db.select_scalar(
        pg_conn, 'show default_transaction_read_only') == 'off'
    assert db.execute(
        pg_conn, "insert into test_table (name, value) values ('Writer', 1)") == 1


def test_reader_server_refuses_raw_dml(pg_reader, pg_conn):
    """Verify PostgreSQL refuses DML on a reader session.

    Mutation: dropping set_session_readonly, so the server permits
        writes that arrive through database.execute() or any path that
        does not call a wrapper write method.
    Oracle: psycopg.errors.ReadOnlySqlTransaction on execute(), and
        the writer row count stays at 6.
    """
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        db.execute(
            pg_reader,
            "insert into test_table (name, value) values ('Zed', 1)")

    assert db.select_scalar(pg_conn, 'select count(*) from test_table') == 6


def test_reader_disarm_guard_refuses_set_readonly_off(pg_reader):
    """Verify the disarm guard blocks SET default_transaction_read_only = off.

    Mutation: removing raise_on_readonly_disarm from Cursor.execute,
        which would let a caller silence the server-side backstop with
        one SET statement.
    Oracle: ReadOnlyError (in-process, before the server sees it); a
        subsequent insert still raises psycopg.errors.ReadOnlySqlTransaction,
        confirming the backstop is still armed.
    """
    with pytest.raises(ReadOnlyError):
        db.execute(pg_reader, 'SET default_transaction_read_only = off')

    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        db.execute(
            pg_reader,
            "insert into test_table (name, value) values ('Zed', 1)")


def test_reader_disarm_guard_refuses_reset_all(pg_reader):
    """Verify the disarm guard blocks RESET ALL on a reader.

    Mutation: removing raise_on_readonly_disarm from Cursor.execute,
        which would let RESET ALL clear default_transaction_read_only
        and open the session to writes.
    Oracle: ReadOnlyError (in-process, before the server sees it); a
        subsequent insert still raises psycopg.errors.ReadOnlySqlTransaction,
        confirming the backstop is still armed.
    """
    with pytest.raises(ReadOnlyError):
        db.execute(pg_reader, 'RESET ALL')

    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        db.execute(
            pg_reader,
            "insert into test_table (name, value) values ('Zed', 1)")


def test_reader_refuses_every_data_operation(pg_reader):
    """Verify the named write helpers are refused at the wrapper.

    Mutation: dropping _reject_if_readonly from insert_row, insert_rows,
        update_row, upsert_rows, or copy_from, which would leave that
        entry point unguarded.
    Oracle: ReadOnlyError from each of the five public entry points.
    """
    rows = ({'name': 'Zed', 'value': 1},)

    with pytest.raises(ReadOnlyError):
        db.insert_row(pg_reader, 'test_table', ['name', 'value'], ['Zed', 1])
    with pytest.raises(ReadOnlyError):
        db.insert_rows(pg_reader, 'test_table', rows)
    with pytest.raises(ReadOnlyError):
        db.update_row(pg_reader, 'test_table', ['name'], ['Alice'], ['value'], [0])
    with pytest.raises(ReadOnlyError):
        db.upsert_rows(pg_reader, 'test_table', rows, update_cols_always=['value'])
    with pytest.raises(ReadOnlyError):
        db.copy_from(pg_reader, 'test_table', io.StringIO('Zed,1\n'))


def test_reader_refuses_every_maintenance_operation(pg_reader):
    """Verify maintenance calls are refused at the wrapper.

    Mutation: dropping _reject_if_readonly from reset_table_sequence,
        vacuum_table, reindex_table, or cluster_table; reset_table_sequence
        is the most dangerous gap because its SQL reads as a SELECT and
        would reach the server unblocked.
    Oracle: ReadOnlyError from each of the four maintenance methods.
    """
    with pytest.raises(ReadOnlyError):
        db.vacuum_table(pg_reader, 'test_table')
    with pytest.raises(ReadOnlyError):
        db.reindex_table(pg_reader, 'test_table')
    with pytest.raises(ReadOnlyError):
        db.cluster_table(pg_reader, 'test_table')
    with pytest.raises(ReadOnlyError):
        db.reset_table_sequence(pg_reader, 'test_table')


def test_reader_transaction_refuses_a_write(pg_reader):
    """Verify a transaction on a reader cannot write through either path.

    Mutation: dropping set_session_readonly, which would let the server
        accept the write inside the transaction and land the row.
    Oracle: psycopg.errors.ReadOnlySqlTransaction from the plain path
        and the returnid path; then a working read on the same
        connection proves transaction cleanup succeeded.
    """
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        with db.transaction(pg_reader) as tx:
            tx.execute(
                "insert into test_table (name, value) values ('Zed', 1)")

    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        with db.transaction(pg_reader) as tx:
            tx.execute(
                "insert into test_table (name, value) values ('Zed', 1)"
                ' returning id',
                returnid='id')

    assert db.select_scalar(pg_reader, 'select count(*) from test_table') == 6


def test_reader_transaction_still_reads(pg_reader):
    """Verify a read-only transaction opens and reads normally.

    Mutation: raising in Transaction.__enter__ for a reader, which
        would make the read-only suite unable to group its reads.
    Oracle: the staged row count, read inside the transaction.
    """
    with db.transaction(pg_reader) as tx:
        assert tx.select_scalar('select count(*) from test_table') == 6


def test_server_refuses_a_write_that_skips_the_guard(pg_reader):
    """Verify the session setting catches writes that bypass the wrapper.

    Mutation: dropping set_session_readonly, leaving a write through a
        function call ('select setval(...)') or a raw DBAPI statement
        with nothing at all standing in its way.
    Oracle: a statement issued on the raw psycopg connection, which
        never reaches raise_on_readonly_disarm.
    """
    raw = pg_reader.dbapi_connection.driver_connection

    with pytest.raises(Exception, match='read-only transaction'):
        raw.execute(
            "insert into test_table (name, value) values ('Bypass', 1)")
    raw.rollback()

    with pytest.raises(Exception, match='read-only transaction'):
        db.select(pg_reader, "select setval('test_table_id_seq', 99, false)")


def test_writer_and_reader_hold_separate_engines(pg_conn, pg_reader):
    """Verify a reader and a writer never share an engine.

    Mutation: dropping readonly from _build_engine_registry_key, which
        lets one pooled engine serve both roles - a writer then checks
        out a connection whose session the server refuses to write on.
    Oracle: object identity of the two engines, both built from the
        same host, port, and credentials.
    """
    assert pg_conn.options.hostname == pg_reader.options.hostname
    assert pg_conn.options.port == pg_reader.options.port
    assert pg_conn.engine is not pg_reader.engine


def test_unknown_role_is_rejected_before_any_connection(psql_docker):
    """Verify a misspelled role fails loudly instead of defaulting.

    Mutation: dropping the role check, which would treat 'read',
        'replica', or a typo as a writer and route read-only work to
        the writer in silence.
    Oracle: ValidationError naming the two accepted values.
    """
    with pytest.raises(db.ValidationError, match='reader'):
        db.connect(_base_options(), role='read')
