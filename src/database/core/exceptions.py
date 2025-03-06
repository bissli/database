"""
Database-specific exception classes.
"""
import psycopg
import pymssql
import sqlite3

class DatabaseError(Exception):
    """Base class for all database module errors"""
    pass


class ConnectionError(DatabaseError):
    """Error establishing or maintaining database connection"""
    pass


class QueryError(DatabaseError):
    """Error in query syntax or execution"""
    pass


class TypeConversionError(DatabaseError):
    """Error converting types between Python and database"""
    pass


class IntegrityViolationError(DatabaseError):
    """Database constraint violation error"""
    pass


# Define exception groups (for compatibility with client_old.py)
DbConnectionError = (
    psycopg.OperationalError,    # Connection/timeout issues
    psycopg.InterfaceError,      # Connection interface issues
    pymssql.OperationalError,    # SQL Server connection issues
    pymssql.InterfaceError,      # SQL Server interface issues
    sqlite3.OperationalError,    # SQLite connection issues
    sqlite3.InterfaceError,      # SQLite interface issues
    ConnectionError,             # Our custom exception
)

IntegrityError = (
    psycopg.IntegrityError,      # Postgres constraint violations
    pymssql.IntegrityError,      # SQL Server constraint violations
    sqlite3.IntegrityError,      # SQLite constraint violations
    IntegrityViolationError,     # Our custom exception
)

ProgrammingError = (
    psycopg.ProgrammingError,    # Postgres syntax/query errors
    psycopg.DatabaseError,       # Postgres general database errors
    pymssql.ProgrammingError,    # SQL Server syntax/query errors
    pymssql.DatabaseError,       # SQL Server general database errors
    sqlite3.ProgrammingError,    # SQLite syntax/query errors
    sqlite3.DatabaseError,       # SQLite general database errors
    QueryError,                  # Our custom exception
)

OperationalError = (
    psycopg.OperationalError,    # Postgres operational issues
    pymssql.OperationalError,    # SQL Server operational issues
    sqlite3.OperationalError,    # SQLite operational issues
)

UniqueViolation = (
    psycopg.errors.UniqueViolation,    # Postgres specific unique violation error
    sqlite3.IntegrityError,            # SQLite error for unique/primary key violations
    pymssql.IntegrityError,            # SQL Server unique constraint violations
    IntegrityViolationError,           # Our custom exception
)
