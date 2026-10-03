"""Library exception classes, driver exception groups, and retry rules.
"""
import re
import sqlite3

import psycopg
import sqlalchemy.exc

RETRYABLE_PATTERNS = [
    r'ssl',
    r'tls',
    r'connection.*(closed|reset|refused|lost|terminated|broken)',
    r'server closed',
    r'eof detected',
    r'broken pipe',
    r'connection reset',
    r'terminating connection',
    r'administrator command',
    r'system is shutting down',
    r'timeout',
    r'timed out',
    r'could not connect',
    r'no route to host',
    r'network.*(unreachable|error)',
    r'host.*(unreachable|down)',
    r'database.*unavailable',
    r'too many connections',
    r'connection pool',
]

_RETRYABLE_REGEX = re.compile('|'.join(RETRYABLE_PATTERNS), re.IGNORECASE)


def is_retryable_error(exc: BaseException) -> bool:
    """True when exc looks transient, so a retry may succeed.

    Parameters
    ----------
    exc : BaseException
        Any exception. A SQLAlchemy wrapper is judged by its orig.

    Returns
    -------
    bool
        True when exc has connection_invalidated set, or its message
        matches a RETRYABLE_PATTERNS entry, ignoring case.
    """
    if getattr(exc, 'connection_invalidated', False):
        return True
    error_msg = str(getattr(exc, 'orig', None) or exc).lower()
    return bool(_RETRYABLE_REGEX.search(error_msg))


class DatabaseError(Exception):
    """Base class for all database module errors.
    """


class ConnectionFailure(DatabaseError):
    """Error establishing or maintaining database connection.
    """


class QueryError(DatabaseError):
    """Error in query syntax or execution.
    """


class TypeConversionError(DatabaseError):
    """Error converting types between Python and database.
    """


class IntegrityViolationError(DatabaseError):
    """Database constraint violation error.
    """


class ValidationError(DatabaseError):
    """Error in input validation.
    """


class ReadOnlyError(DatabaseError):
    """Write attempted on a connection opened for reading only.
    """


DbConnectionError = (
    psycopg.OperationalError,
    psycopg.InterfaceError,
    sqlite3.OperationalError,
    sqlite3.InterfaceError,
    sqlalchemy.exc.OperationalError,
    sqlalchemy.exc.InterfaceError,
    ConnectionFailure,
    )

IntegrityError = (
    psycopg.IntegrityError,
    sqlite3.IntegrityError,
    IntegrityViolationError,
    )

ProgrammingError = (
    psycopg.ProgrammingError,
    psycopg.DatabaseError,
    sqlite3.ProgrammingError,
    sqlite3.DatabaseError,
    QueryError,
    )

OperationalError = (
    psycopg.OperationalError,
    sqlite3.OperationalError,
    )

UniqueViolation = (
    psycopg.errors.UniqueViolation,
    sqlite3.IntegrityError,
    IntegrityViolationError,
    )
