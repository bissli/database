"""Retry classification by SQLSTATE, and identifiers that name a keyword.
"""
import sqlite3

import psycopg
import pytest
import sqlalchemy.exc
from database.exceptions import is_retryable_error


@pytest.mark.parametrize('exc', [
    psycopg.errors.UndefinedColumn('column "timeout" does not exist'),
    psycopg.errors.UndefinedTable('relation "ssl_certs" does not exist'),
    sqlalchemy.exc.ProgrammingError(
        'select timeout from t',
        None,
        psycopg.errors.UndefinedColumn('column "timeout" does not exist')),
    sqlite3.OperationalError('no such column: timeout'),
    sqlite3.OperationalError('no such table: ssl_certs'),
    psycopg.OperationalError(
        'connection to server at "ssl-db" (10.0.0.1), port 5432 failed: '
        'FATAL:  password authentication failed for user "timeout"'),
    ], ids=['pg-column', 'pg-table', 'sa-wrapped', 'sqlite-column',
            'sqlite-table', 'quoted-host-and-user'])
def test_identifier_holding_a_retry_keyword_is_not_retryable(exc):
    """Verify an identifier naming a retry keyword leaves an error permanent.

    Mutation: matching RETRYABLE_PATTERNS against the whole message again.
    Oracle: errors a retry cannot fix, each naming 'timeout' or 'ssl'.
    """
    assert is_retryable_error(exc) is False


@pytest.mark.parametrize('exc', [
    psycopg.errors.AdminShutdown('x'),
    psycopg.errors.CannotConnectNow('x'),
    psycopg.errors.ConnectionFailure('x'),
    psycopg.errors.TooManyConnections('x'),
    psycopg.errors.IdleInTransactionSessionTimeout('x'),
    ], ids=['57P01', '57P03', '08006', '53300', '25P03'])
def test_transient_sqlstate_is_retryable_whatever_the_message(exc):
    """Verify a transient SQLSTATE is retryable under any message.

    Mutation: a code dropped from RETRYABLE_SQLSTATES.
    Oracle: PostgreSQL Appendix A codes for shutdown, connection, capacity.
    """
    assert is_retryable_error(exc) is True
