"""
Fixtures for database-agnostic integration tests.

This module provides parametrized fixtures that allow tests to run
against multiple database backends (PostgreSQL and SQLite).
"""
import database as db
import pytest


def row(result, index):
    """Get row from result in a database-agnostic way.

    PostgreSQL returns list of Row objects, SQLite returns DataFrame.
    This function provides consistent dict-like access for both.
    """
    # Check if result has iloc (DataFrame)
    if hasattr(result, 'iloc'):
        return result.iloc[index]
    # Otherwise assume it's a list
    return result[index]


def col(result, column_name):
    """Get column values as a list from result.

    PostgreSQL returns list of Row objects, SQLite returns DataFrame.
    """
    if hasattr(result, 'iloc'):
        return list(result[column_name])
    return [r[column_name] for r in result]


def _stage_common_test_data(conn, dialect):
    """Stage common test data for both database types."""
    # Drop table if exists
    if dialect == 'postgresql':
        db.execute(conn, 'DROP TABLE IF EXISTS test_table CASCADE')
    else:
        db.execute(conn, 'DROP TABLE IF EXISTS test_table')

    # Create table with database-appropriate syntax
    # Note: name is the primary key for consistent upsert behavior
    if dialect == 'postgresql':
        create_sql = """
        CREATE TABLE test_table (
            id SERIAL NOT NULL,
            name VARCHAR(255) NOT NULL,
            value INTEGER NOT NULL,
            PRIMARY KEY (name)
        )
        """
    else:
        create_sql = """
        CREATE TABLE test_table (
            id INTEGER,
            name TEXT NOT NULL,
            value INTEGER NOT NULL,
            PRIMARY KEY (name)
        )
        """
    db.execute(conn, create_sql)

    # Insert common test data
    insert_sql = """
    INSERT INTO test_table (name, value) VALUES
    ('Alice', 10),
    ('Bob', 20),
    ('Charlie', 30)
    """
    db.execute(conn, insert_sql)


@pytest.fixture(params=['postgresql', 'sqlite'], ids=['pg', 'sl'])
def db_conn(request, pg_conn, sl_conn):
    """Parametrized fixture providing connection for both databases.

    Tests using this fixture will run twice - once for each database.
    The fixture automatically stages common test data for consistency.
    """
    if request.param == 'postgresql':
        conn = pg_conn
    else:
        conn = sl_conn

    # Reset test data for consistency
    dialect = request.param
    _stage_common_test_data(conn, dialect)

    return conn


@pytest.fixture
def dialect(db_conn):
    """Get the dialect name from the connection."""
    return db_conn.engine.dialect.name
