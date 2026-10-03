"""Retry classification and connection recovery against PostgreSQL.
"""
from unittest.mock import patch

import config
import database as db
import psycopg
import pytest
import sqlalchemy.exc
from database.connection import check_connection
from database.exceptions import is_retryable_error


@pytest.mark.parametrize('exc', [
    psycopg.OperationalError('SSL SYSCALL error: EOF detected'),
    psycopg.OperationalError('server closed the connection unexpectedly'),
    psycopg.OperationalError('connection reset by peer'),
    psycopg.OperationalError('connection timed out'),
    psycopg.OperationalError('network is unreachable'),
    psycopg.OperationalError('too many connections for role'),
    ], ids=['ssl', 'server-closed', 'reset', 'timed-out', 'unreachable',
            'too-many-connections'])
def test_transient_errors_are_retryable(exc):
    """Verify each transient driver message is classed retryable.

    Mutation: a pattern dropped, or compiled without re.IGNORECASE.
    Oracle: libpq messages, one per transient failure family.
    """
    assert is_retryable_error(exc) is True


@pytest.mark.parametrize('exc', [
    psycopg.OperationalError('some random error'),
    psycopg.OperationalError('invalid input syntax for type integer'),
    psycopg.ProgrammingError('syntax error at or near "SELECT"'),
    ], ids=['generic', 'bad-input', 'syntax'])
def test_permanent_errors_are_not_retryable(exc):
    """Verify an error that a retry cannot fix is classed permanent.

    Mutation: a pattern widened to match a bare 'error'.
    Oracle: messages that name no transient failure.
    """
    assert is_retryable_error(exc) is False


def test_check_connection_decorator_retries_transient_errors(psql_docker, pg_conn):
    """Verify check_connection retries a transient error until it succeeds.

    Mutation: raising on the first caught error instead of retrying.
    Oracle: a stub that fails twice, counting its calls.
    """
    retry_count = [0]

    @check_connection(max_retries=3, retry_delay=0.01)
    def failing_function(conn, fail_count=2):
        if retry_count[0] < fail_count:
            retry_count[0] += 1
            raise psycopg.OperationalError('SSL SYSCALL error: connection reset')
        return 'Success'

    assert failing_function(pg_conn, fail_count=2) == 'Success'
    assert retry_count[0] == 2


def test_check_connection_decorator_fails_immediately_for_non_retryable(
        psql_docker, pg_conn):
    """Verify a non-retryable error raises on the first attempt.

    Mutation: dropping the is_retryable_error check.
    Oracle: a stub counting its calls, which must run once.
    """
    retry_count = [0]

    @check_connection(max_retries=3, retry_delay=0.01)
    def failing_function(conn):
        retry_count[0] += 1
        raise psycopg.OperationalError('invalid input syntax for type integer')

    with pytest.raises(psycopg.OperationalError):
        failing_function(pg_conn)

    assert retry_count[0] == 1


def test_check_connection_decorator_max_retries_exceeded(psql_docker, pg_conn):
    """Verify a transient error raises once max_retries attempts are spent.

    Mutation: an off-by-one in the attempt loop.
    Oracle: a stub that always fails, counting its calls against 3.
    """
    retry_count = [0]

    @check_connection(max_retries=3, retry_delay=0.01)
    def failing_function(conn):
        retry_count[0] += 1
        raise psycopg.OperationalError('SSL connection has been closed unexpectedly')

    with pytest.raises(psycopg.OperationalError):
        failing_function(pg_conn)

    assert retry_count[0] == 3


def test_check_connection_with_check_retryable_disabled(psql_docker, pg_conn):
    """Verify check_retryable=False retries an error the classifier rejects.

    Mutation: consulting is_retryable_error whatever check_retryable says.
    Oracle: a stub raising a non-transient message twice, counting calls.
    """
    retry_count = [0]

    @check_connection(max_retries=3, retry_delay=0.01, check_retryable=False)
    def failing_function(conn, fail_count=2):
        if retry_count[0] < fail_count:
            retry_count[0] += 1
            raise psycopg.OperationalError('some generic error')
        return 'Success'

    assert failing_function(pg_conn, fail_count=2) == 'Success'
    assert retry_count[0] == 2


def test_connect_to_unresolvable_host_raises():
    """Verify connect() raises when the server cannot be reached.

    Mutation: connect() deferring the first connection to the first query.
    Oracle: a host name that does not resolve.
    """
    with pytest.raises((*db.DbConnectionError, sqlalchemy.exc.OperationalError)):
        db.connect({
            'drivername': 'postgresql',
            'hostname': 'nonexistent.host',
            'username': 'postgresql',
            'password': 'postgresql',
            'database': 'test',
            'port': 5432,
            'timeout': 1,
            })


def test_upsert_rows_retries_on_connection_error(psql_docker, pg_conn):
    """Verify upsert_rows retries a transient error from executemany.

    Mutation: dropping @check_connection from the upsert path.
    Oracle: a patched executemany failing twice, then the row it writes.
    """
    pg_conn.execute(
        'create table if not exists test_retry (id integer primary key, name text)')

    call_count = [0]
    cursor_class = pg_conn.cursor().__class__
    original_executemany = cursor_class.executemany

    def mock_executemany(self, operation, seq_of_parameters, *args, **kwargs):
        call_count[0] += 1
        if call_count[0] < 3:
            raise psycopg.OperationalError('SSL SYSCALL error: EOF detected')
        return original_executemany(
            self, operation, seq_of_parameters, *args, **kwargs)

    try:
        with patch.object(cursor_class, 'executemany', mock_executemany):
            pg_conn.upsert_rows('test_retry', [{'id': 1, 'name': 'test'}])

        assert call_count[0] == 3
        stored = db.select_scalar(pg_conn, 'select name from test_retry where id = 1')
        assert stored == 'test'
    finally:
        pg_conn.execute('drop table test_retry')


def test_upsert_rows_recovers_when_server_drops_connection(psql_docker, pg_conn):
    """Verify upsert_rows reconnects after the server ends its session.

    Mutation: retrying on the dead connection instead of rebuilding it.
    Oracle: pg_terminate_backend from a second connection, then the row.
    """
    pg_conn.execute(
        'create table if not exists test_drop_recover (id integer primary key, name text)')

    backend_pid = pg_conn.select('select pg_backend_pid() as pid')[0]['pid']
    killer = db.connect('postgresql', config=config)
    try:
        db.execute(killer, 'select pg_terminate_backend(%s)', backend_pid)
    finally:
        killer.close()

    assert getattr(pg_conn.sa_connection, 'closed', False) is False

    try:
        pg_conn.upsert_rows('test_drop_recover', [{'id': 1, 'name': 'recovered'}])

        rows = pg_conn.select('select name from test_drop_recover where id = 1')
        assert rows[0]['name'] == 'recovered'
    finally:
        pg_conn.execute('drop table test_drop_recover')


if __name__ == '__main__':
    __import__('pytest').main([__file__])
