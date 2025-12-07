"""
Database-specific exception classes.
"""
import sqlite3

import psycopg


class DatabaseError(Exception):
    """Base class for all database module errors"""


class ConnectionError(DatabaseError):
    """Error establishing or maintaining database connection"""


class QueryError(DatabaseError):
    """Error in query syntax or execution"""


class TypeConversionError(DatabaseError):
    """Error converting types between Python and database"""


class IntegrityViolationError(DatabaseError):
    """Database constraint violation error"""


# Define exception groups
DbConnectionError = (
    psycopg.OperationalError,    # Connection/timeout issues
    psycopg.InterfaceError,      # Connection interface issues
    sqlite3.OperationalError,    # SQLite connection issues
    sqlite3.InterfaceError,      # SQLite interface issues
    ConnectionError,             # Our custom exception
)

IntegrityError = (
    psycopg.IntegrityError,      # Postgres constraint violations
    sqlite3.IntegrityError,      # SQLite constraint violations
    IntegrityViolationError,     # Our custom exception
)

ProgrammingError = (
    psycopg.ProgrammingError,    # Postgres syntax/query errors
    psycopg.DatabaseError,       # Postgres general database errors
    sqlite3.ProgrammingError,    # SQLite syntax/query errors
    sqlite3.DatabaseError,       # SQLite general database errors
    QueryError,                  # Our custom exception
)

OperationalError = (
    psycopg.OperationalError,    # Postgres operational issues
    sqlite3.OperationalError,    # SQLite operational issues
)

UniqueViolation = (
    psycopg.errors.UniqueViolation,    # Postgres specific unique violation error
    sqlite3.IntegrityError,            # SQLite error for unique/primary key violations
    IntegrityViolationError,           # Our custom exception
)
