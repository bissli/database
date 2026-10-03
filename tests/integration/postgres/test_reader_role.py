"""connect(role='reader') against a live PostgreSQL server.
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
    """
    reader = db.connect(_base_options(), role='reader')
    try:
        yield reader
    finally:
        reader.close()


def test_reader_endpoint_is_used_when_declared(psql_docker):
    """Verify reader_hostname and reader_port beat hostname and port.

    Mutation: connect() reading hostname in place of reader_hostname.
    Oracle: a writer endpoint that cannot resolve.
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

    Mutation: dropping the 'or options.hostname' fallback.
    Oracle: options with no reader field, against the live container.
    """
    reader = db.connect(_base_options(), role='reader')
    try:
        assert reader.readonly is True
        assert db.select_scalar(reader, 'select 1') == 1
    finally:
        reader.close()


def test_reader_port_falls_back_independently_of_hostname(psql_docker):
    """Verify reader_port is honored on its own, with no reader_hostname.

    Mutation: reader_port honored only when reader_hostname is set.
    Oracle: a writer port nothing listens on.
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
    """Verify a reader reads the rows the writer committed.

    Mutation: the read-only setting applied before configure_connection.
    Oracle: the six rows stage_test_data commits on the writer.
    """
    alice = "select value from test_table where name = 'Alice'"

    assert db.select_scalar(pg_reader, 'select count(*) from test_table') == 6
    assert db.select_scalar(pg_reader, alice) == 10


def test_reader_session_is_read_only_on_the_server(pg_reader):
    """Verify the session-level setting reached the server.

    Mutation: dropping the set_session_readonly call.
    Oracle: the server's own report of default_transaction_read_only.
    """
    assert db.select_scalar(
        pg_reader, 'show default_transaction_read_only') == 'on'


def test_writer_session_is_left_writable(pg_conn):
    """Verify the read-only setting does not leak to a writer.

    Mutation: set_session_readonly on every connection, or readonly
        dropped from the engine registry key.
    Oracle: the server's report on the writer, plus a real insert.
    """
    assert db.select_scalar(
        pg_conn, 'show default_transaction_read_only') == 'off'
    assert db.execute(
        pg_conn, "insert into test_table (name, value) values ('Writer', 1)") == 1


def test_reader_server_refuses_raw_dml(pg_reader, pg_conn):
    """Verify PostgreSQL refuses DML on a reader session.

    Mutation: dropping set_session_readonly.
    Oracle: ReadOnlySqlTransaction, and the writer still counts 6 rows.
    """
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        db.execute(
            pg_reader,
            "insert into test_table (name, value) values ('Zed', 1)")

    assert db.select_scalar(pg_conn, 'select count(*) from test_table') == 6


def test_reader_disarm_guard_refuses_set_readonly_off(pg_reader):
    """Verify the disarm guard blocks set default_transaction_read_only = off.

    Mutation: removing raise_on_readonly_disarm from Cursor.execute.
    Oracle: ReadOnlyError, then ReadOnlySqlTransaction on an insert.
    """
    with pytest.raises(ReadOnlyError):
        db.execute(pg_reader, 'SET default_transaction_read_only = off')

    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        db.execute(
            pg_reader,
            "insert into test_table (name, value) values ('Zed', 1)")


def test_reader_disarm_guard_refuses_reset_all(pg_reader):
    """Verify the disarm guard blocks reset all on a reader.

    Mutation: removing raise_on_readonly_disarm from Cursor.execute.
    Oracle: ReadOnlyError, then ReadOnlySqlTransaction on an insert.
    """
    with pytest.raises(ReadOnlyError):
        db.execute(pg_reader, 'RESET ALL')

    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        db.execute(
            pg_reader,
            "insert into test_table (name, value) values ('Zed', 1)")


def test_reader_refuses_every_data_operation(pg_reader):
    """Verify the named write helpers are refused at the wrapper.

    Mutation: dropping _reject_if_readonly from any one write helper.
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

    Mutation: dropping _reject_if_readonly from any maintenance method.
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

    Mutation: dropping set_session_readonly.
    Oracle: ReadOnlySqlTransaction on both paths, then a working read.
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

    Mutation: Transaction.__enter__ raising for a reader.
    Oracle: the staged row count, read inside the transaction.
    """
    with db.transaction(pg_reader) as tx:
        assert tx.select_scalar('select count(*) from test_table') == 6


def test_server_refuses_a_write_that_skips_the_guard(pg_reader):
    """Verify the session setting catches writes that bypass the wrapper.

    Mutation: dropping set_session_readonly.
    Oracle: a raw psycopg insert and a setval select, both unguarded.
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

    Mutation: dropping readonly from _build_engine_registry_key.
    Oracle: engine identity for two connections to one host and port.
    """
    assert pg_conn.options.hostname == pg_reader.options.hostname
    assert pg_conn.options.port == pg_reader.options.port
    assert pg_conn.engine is not pg_reader.engine


def test_unknown_role_is_rejected_before_any_connection(psql_docker):
    """Verify a misspelled role fails loudly instead of defaulting.

    Mutation: dropping the role check, so 'read' opens a writer.
    Oracle: ValidationError naming the two accepted values.
    """
    with pytest.raises(db.ValidationError, match='reader'):
        db.connect(_base_options(), role='read')
