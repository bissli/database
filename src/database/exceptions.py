"""
Database-specific exception classes.
"""
import sqlite3

import psycopg


class DatabaseError(Exception):
    """Base class for all database module errors.
    """


class ConnectionError(DatabaseError):
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
    ConnectionError,
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
