"""
Database-specific exception classes.
"""
import re
import sqlite3

import psycopg

RETRYABLE_PATTERNS = [
    # SSL/TLS errors
    r'ssl',
    r'tls',
    # Connection drops
    r'connection.*(closed|reset|refused|lost|terminated|broken)',
    r'server closed',
    r'eof detected',
    r'broken pipe',
    r'connection reset',
    # Timeouts
    r'timeout',
    r'timed out',
    # Network issues
    r'could not connect',
    r'no route to host',
    r'network.*(unreachable|error)',
    r'host.*(unreachable|down)',
    # Database unavailable
    r'database.*unavailable',
    r'too many connections',
    r'connection pool',
]

_RETRYABLE_REGEX = re.compile('|'.join(RETRYABLE_PATTERNS), re.IGNORECASE)


def is_retryable_error(exc: BaseException) -> bool:
    """Check if an exception represents a transient error worth retrying.

    Returns True for errors that are likely transient and may succeed on retry:
    - SSL/TLS errors
    - Connection drops/resets
    - Timeouts
    - Network issues
    - Database temporarily unavailable

    Returns False for errors that will definitely fail again:
    - Syntax errors
    - Type mismatches
    - Constraint violations
    - Permission errors
    - Programming errors

    :param exc: The exception to check.
    :returns: True if the error is likely transient and worth retrying.
    """
    error_msg = str(exc).lower()
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


DbConnectionError = (
    psycopg.OperationalError,
    psycopg.InterfaceError,
    sqlite3.OperationalError,
    sqlite3.InterfaceError,
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
